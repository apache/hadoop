/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;

/**
 * A standalone HRegion directory reader.  Currently reads content on
 * file system only.
 * TODO: Add dumping of HStoreFile content and HLog.
 */
class HRegiondirReader {
  private final Configuration conf;
  private final Path parentdir;
  
  static final Pattern REGION_NAME_PARSER =
    Pattern.compile(HConstants.HREGIONDIR_PREFIX +
        "([^_]+)_([^_]*)_([^_]*)");
  
  private static final String USAGE = "Usage: " +
      "java org.apache.hadoop.hbase.HRegionDirReader <regiondir> " +
      "[<tablename>]";
  
  private final List<HRegionInfo> infos;
  
  HRegiondirReader(final HBaseConfiguration conf,
      final String parentdirName)
  throws IOException {
    this.conf = conf;
    FileSystem fs = FileSystem.get(conf);
    this.parentdir = new Path(parentdirName);
    if (!fs.exists(parentdir)) {
      throw new FileNotFoundException(parentdirName);
    }
    if (!fs.isDirectory(parentdir)) {
      throw new IOException(parentdirName + " not a directory");
    }
    // Look for regions in parentdir.
    Path [] regiondirs =
      fs.listPaths(parentdir, new PathFilter() {
        /* (non-Javadoc)
         * @see org.apache.hadoop.fs.PathFilter#accept(org.apache.hadoop.fs.Path)
         */
        public boolean accept(Path path) {
          Matcher m = REGION_NAME_PARSER.matcher(path.getName());
          return m != null && m.matches();
        }
    });
    // Create list of HRegionInfos for all regions found in
    // parentdir.
    this.infos = new ArrayList<HRegionInfo>();
    for (Path d: regiondirs) {
      Matcher m = REGION_NAME_PARSER.matcher(d.getName());
      if (m == null || !m.matches()) {
        throw new IOException("Unparseable region dir name");
      }
      String tableName = m.group(1);
      String endKey = m.group(2);
      long regionid = Long.parseLong(m.group(3));
      HTableDescriptor desc = getTableDescriptor(fs, d, tableName);
      HRegionInfo info = new HRegionInfo(regionid, desc,
          new Text(), (endKey == null || endKey.length() == 0)?
              new Text(): new Text(endKey));
      infos.add(info);
    }
  }
  
  /**
   * Returns a populated table descriptor.
   * @param fs Current filesystem.
   * @param d The regiondir for <code>tableName</code>
   * @param tableName Name of this table.
   * @return A HTableDescriptor populated with all known column
   * families.
   * @throws IOException
   */
  private HTableDescriptor getTableDescriptor(final FileSystem fs,
      final Path d, final String tableName)
  throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    Text [] families = getFamilies(fs, d);
    for (Text f: families) {
      desc.addFamily(new HColumnDescriptor(f.toString()));
    }
    return desc;
  }
  
  /**
   * Get column families for this region by looking at
   * directory names under this region.
   * This is a hack. HRegions only know what columns they have
   * because they are told by passed-in metadata.
   * @param regiondir
   * @return Array of family names.
   * @throws IOException
   */
  private Text [] getFamilies(final FileSystem fs,
      final Path regiondir)
  throws IOException {
    Path [] subdirs = fs.listPaths(regiondir, new PathFilter() {
      public boolean accept(Path path) {
        return !path.getName().equals("log");
      }
    });
    List<Text> families = new ArrayList<Text>();
    for (Path d: subdirs) {
      // Convert names of subdirectories into column family names
      // by adding the colon.
      Text family = new Text(d.getName() + ":");
      families.add(family);
    }
    return families.toArray(new Text [] {});
  }
  
  List <HRegionInfo> getRegions() {
    return this.infos;
  }
  
  HRegionInfo getRegionInfo(final String tableName) {
    HRegionInfo result = null;
    for(HRegionInfo i: getRegions()) {
      if(i.tableDesc.getName().equals(tableName)) {
        result = i;
        break;
      }
    }
    if (result == null) {
      throw new NullPointerException("No such table: " +
          tableName);
    }
    return result;
  }
  
  private void dump(final String tableName) throws IOException {
    dump(getRegionInfo(tableName));
  }
  
  private void dump(final HRegionInfo info) throws IOException {
    HRegion r = new HRegion(this.parentdir, null,
        FileSystem.get(this.conf), conf, info, null);
    Text [] families = info.tableDesc.families().keySet().toArray(new Text [] {});
    HInternalScannerInterface scanner = r.getScanner(families, new Text());
    HStoreKey key = new HStoreKey();
    TreeMap<Text, byte []> results = new TreeMap<Text, byte []>();
    // Print out table header line.
    String s = info.startKey.toString();
    String startKey = (s == null || s.length() <= 0)? "<>": s;
    s = info.endKey.toString();
    String endKey = (s == null || s.length() <= 0)? "<>": s;
    String tableName = info.tableDesc.getName().toString();
    System.out.println("table: " + tableName +
      ", regionid: " + info.regionId +
      ", startkey: " +  startKey +
      ", endkey: " + endKey);
    // Now print rows.  Offset by a space to distingush rows from
    // table headers. TODO: Add in better formatting of output.
    // Every line starts with row name followed by column name
    // followed by cell content.
    while(scanner.next(key, results)) {
      for (Map.Entry<Text, byte []> es: results.entrySet()) {
        Text colname = es.getKey();
        byte [] colvalue = es.getValue();
        Object value = null;
        if (colname.toString().equals("info:regioninfo")) {
          value = new HRegionInfo(colvalue);
        } else {
          value = new String(colvalue, HConstants.UTF8_ENCODING);
        }
        System.out.println(" " + key + ", " + colname.toString() + ": \"" +
            value.toString() + "\"");
      }
    }
  }
  
  /**
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println(USAGE);
      System.exit(-1);
    }
    HBaseConfiguration c = new HBaseConfiguration();
    HRegiondirReader reader = new HRegiondirReader(c, args[0]);
    if (args.length == 1) {
      // Do all regions.
      for(HRegionInfo info: reader.getRegions()) {
        reader.dump(info);
      }
    } else {
      for (int i = 1; i < args.length; i++) {
        reader.dump(args[i]);
      }
    }
  }
}
