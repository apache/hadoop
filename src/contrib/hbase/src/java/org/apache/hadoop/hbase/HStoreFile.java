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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

import java.io.*;
import java.util.*;

/*******************************************************************************
 * Each HStore maintains a bunch of different data files.
 *
 * The filename is a mix of the parent dir, the region name, the column name, 
 * and the file identifier.
 * 
 * This class handles all that path-building stuff for you.
 ******************************************************************************/
public class HStoreFile implements HConstants, WritableComparable {
  static final byte INFO_SEQ_NUM = 0;
  static final String HSTORE_DATFILE_PREFIX = "mapfile.dat.";
  static final String HSTORE_INFOFILE_PREFIX = "mapfile.info.";
  static final String HSTORE_DATFILE_DIR = "mapfiles";
  static final String HSTORE_INFO_DIR = "info";
  static final String HSTORE_FILTER_DIR = "filter";
  private static Random rand = new Random();

  Path dir;
  Text regionName;
  Text colFamily;
  long fileId;
  Configuration conf;

  /**
   * An HStoreFile tracks 4 things: its parent dir, the region identifier, the 
   * column family, and the file identifier.  If you know those four things, you
   * know how to obtain the right HStoreFile.
   *
   * When merging or splitting HRegions, we might want to modify one of the 
   * params for an HStoreFile (effectively moving it elsewhere).
   */
  HStoreFile(Configuration conf) {
    this.conf = conf;
    this.dir = new Path(Path.CUR_DIR);
    this.regionName = new Text();
    this.colFamily = new Text();
    this.fileId = 0;
  }
  
  HStoreFile(Configuration conf, Path dir, Text regionName, 
      Text colFamily, long fileId) {
    
    this.conf = conf;
    this.dir = dir;
    this.regionName = new Text(regionName);
    this.colFamily = new Text(colFamily);
    this.fileId = fileId;
  }

  // Get the individual components
  
  Path getDir() {
    return dir;
  }
  
  Text getRegionName() {
    return regionName;
  }
  
  Text getColFamily() {
    return colFamily;
  }
  
  long fileId() {
    return fileId;
  }

  // Build full filenames from those components
  
  Path getMapFilePath() {
    return new Path(HStoreFile.getMapDir(dir, regionName, colFamily), 
        HSTORE_DATFILE_PREFIX + fileId);
  }
  
  Path getInfoFilePath() {
    return new Path(HStoreFile.getInfoDir(dir, regionName, colFamily), 
        HSTORE_INFOFILE_PREFIX + fileId);
  }

  // Static methods to build partial paths to internal directories.  Useful for 
  // HStore construction and log-rebuilding.
  
  static Path getMapDir(Path dir, Text regionName, Text colFamily) {
    return new Path(dir, new Path(HREGIONDIR_PREFIX + regionName, 
        new Path(colFamily.toString(), HSTORE_DATFILE_DIR)));
  }

  static Path getInfoDir(Path dir, Text regionName, Text colFamily) {
    return new Path(dir, new Path(HREGIONDIR_PREFIX + regionName, 
        new Path(colFamily.toString(), HSTORE_INFO_DIR)));
  }
  
  static Path getFilterDir(Path dir, Text regionName, Text colFamily) {
    return new Path(dir, new Path(HREGIONDIR_PREFIX + regionName,
        new Path(colFamily.toString(), HSTORE_FILTER_DIR)));
  }

  static Path getHStoreDir(Path dir, Text regionName, Text colFamily) {
    return new Path(dir, new Path(HREGIONDIR_PREFIX + regionName, 
        colFamily.toString()));
  }

  static Path getHRegionDir(Path dir, Text regionName) {
    return new Path(dir, new Path(HREGIONDIR_PREFIX + regionName));
  }

  /**
   * Obtain a brand-new randomly-named HStoreFile.  Checks the existing
   * filesystem if the file already exists.
   */
  static HStoreFile obtainNewHStoreFile(Configuration conf, Path dir, 
      Text regionName, Text colFamily, FileSystem fs) throws IOException {
    
    Path mapdir = HStoreFile.getMapDir(dir, regionName, colFamily);
    long fileId = Math.abs(rand.nextLong());

    Path testpath1 = new Path(mapdir, HSTORE_DATFILE_PREFIX + fileId);
    Path testpath2 = new Path(mapdir, HSTORE_INFOFILE_PREFIX + fileId);
    while(fs.exists(testpath1) || fs.exists(testpath2)) {
      fileId = Math.abs(rand.nextLong());
      testpath1 = new Path(mapdir, HSTORE_DATFILE_PREFIX + fileId);
      testpath2 = new Path(mapdir, HSTORE_INFOFILE_PREFIX + fileId);
    }
    return new HStoreFile(conf, dir, regionName, colFamily, fileId);
  }

  /**
   * Create a series of HStoreFiles loaded from the given directory.
   * 
   * There must be a matching 'mapdir' and 'loginfo' pair of files.
   * If only one exists, we'll delete it.
   */
  static Vector<HStoreFile> loadHStoreFiles(Configuration conf, Path dir, 
      Text regionName, Text colFamily, FileSystem fs) throws IOException {
    
    Vector<HStoreFile> results = new Vector<HStoreFile>();
    Path mapdir = HStoreFile.getMapDir(dir, regionName, colFamily);

    Path datfiles[] = fs.listPaths(mapdir);
    for(int i = 0; i < datfiles.length; i++) {
      String name = datfiles[i].getName();
      
      if(name.startsWith(HSTORE_DATFILE_PREFIX)) {
        Long fileId = Long.parseLong(name.substring(HSTORE_DATFILE_PREFIX.length()));
        HStoreFile curfile = new HStoreFile(conf, dir, regionName, colFamily, fileId);
        Path mapfile = curfile.getMapFilePath();
        Path infofile = curfile.getInfoFilePath();
        
        if(fs.exists(infofile)) {
          results.add(curfile);
          
        } else {
          fs.delete(mapfile);
        }
      }
    }

    Path infodir = HStoreFile.getInfoDir(dir, regionName, colFamily);
    Path infofiles[] = fs.listPaths(infodir);
    for(int i = 0; i < infofiles.length; i++) {
      String name = infofiles[i].getName();
      
      if(name.startsWith(HSTORE_INFOFILE_PREFIX)) {
        long fileId = Long.parseLong(name.substring(HSTORE_INFOFILE_PREFIX.length()));
        HStoreFile curfile = new HStoreFile(conf, dir, regionName, colFamily, fileId);
        Path mapfile = curfile.getMapFilePath();
        
        if(! fs.exists(mapfile)) {
          fs.delete(curfile.getInfoFilePath());
        }
      }
    }
    return results;
  }

  //////////////////////////////////////////////////////////////////////////////
  // File handling
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Break this HStoreFile file into two new parts, which live in different 
   * brand-new HRegions.
   */
  void splitStoreFile(Text midKey, HStoreFile dstA, HStoreFile dstB,
      FileSystem fs, Configuration conf) throws IOException {

    // Copy the appropriate tuples to one MapFile or the other.

    MapFile.Reader in = new MapFile.Reader(fs, getMapFilePath().toString(), conf);
    try {
      MapFile.Writer outA = new MapFile.Writer(conf, fs, 
        dstA.getMapFilePath().toString(), HStoreKey.class,
        ImmutableBytesWritable.class);
      try {
        MapFile.Writer outB = new MapFile.Writer(conf, fs, 
          dstB.getMapFilePath().toString(), HStoreKey.class,
          ImmutableBytesWritable.class);
        try {
          HStoreKey readkey = new HStoreKey();
          ImmutableBytesWritable readval = new ImmutableBytesWritable();
          while(in.next(readkey, readval)) {
            Text key = readkey.getRow();
            if(key.compareTo(midKey) < 0) {
              outA.append(readkey, readval);
            } else {
              outB.append(readkey, readval);
            }
          }
        } finally {
          outB.close();
        }
      } finally {
        outA.close();
      }
    } finally {
      in.close();
    }

    // Build an InfoFile for each output
    long seqid = loadInfo(fs);
    dstA.writeInfo(fs, seqid);
    dstB.writeInfo(fs, seqid);
  }

  /**
   * Write to this HStoreFile with all the contents of the given source HStoreFiles.
   * We are merging multiple regions into a single new one.
   */
  void mergeStoreFiles(Vector<HStoreFile> srcFiles, FileSystem fs, 
      Configuration conf) throws IOException {

    // Copy all the source MapFile tuples into this HSF's MapFile

    MapFile.Writer out = new MapFile.Writer(conf, fs,
      getMapFilePath().toString(),
      HStoreKey.class, ImmutableBytesWritable.class);
    
    try {
      for(Iterator<HStoreFile> it = srcFiles.iterator(); it.hasNext(); ) {
        HStoreFile src = it.next();
        MapFile.Reader in = new MapFile.Reader(fs, src.getMapFilePath().toString(), conf);
        
        try {
          HStoreKey readkey = new HStoreKey();
          ImmutableBytesWritable readval = new ImmutableBytesWritable();
          while(in.next(readkey, readval)) {
            out.append(readkey, readval);
          }
        } finally {
          in.close();
        }
      }
      
    } finally {
      out.close();
    }

    // Build a unified InfoFile from the source InfoFiles.
    long unifiedSeqId = -1;
    for(Iterator<HStoreFile> it = srcFiles.iterator(); it.hasNext(); ) {
      HStoreFile hsf = it.next();
      long curSeqId = hsf.loadInfo(fs);
      if(curSeqId > unifiedSeqId) {
        unifiedSeqId = curSeqId;
      }
    }
    writeInfo(fs, unifiedSeqId);
  }

  /** Read in an info file, give it a unique ID. */
  long loadInfo(FileSystem fs) throws IOException {
    Path p = getInfoFilePath();
    DataInputStream in = new DataInputStream(fs.open(p));
    
    try {
      byte flag = in.readByte();
      if(flag == INFO_SEQ_NUM) {
        return in.readLong();
        
      }
      throw new IOException("Cannot process log file: " + p);
      
    } finally {
      in.close();
    }
  }
  
  /** Write the file-identifier to disk */
  void writeInfo(FileSystem fs, long infonum) throws IOException {
    Path p = getInfoFilePath();
    DataOutputStream out = new DataOutputStream(fs.create(p));
    
    try {
      out.writeByte(INFO_SEQ_NUM);
      out.writeLong(infonum);
      
    } finally {
      out.close();
    }
  }

  @Override
  public boolean equals(Object o) {
    return this.compareTo(o) == 0;
  }
  
  @Override
  public int hashCode() {
    int result = this.dir.hashCode();
    result ^= this.regionName.hashCode();
    result ^= this.colFamily.hashCode();
    result ^= Long.valueOf(this.fileId).hashCode();
    return result;
  }

  //////////////////////////////////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////////////////////////////////

  /* (non-Javadoc)
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  public void write(DataOutput out) throws IOException {
    out.writeUTF(dir.toString());
    regionName.write(out);
    colFamily.write(out);
    out.writeLong(fileId);
  }
  
  /* (non-Javadoc)
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  public void readFields(DataInput in) throws IOException {
    this.dir = new Path(in.readUTF());
    this.regionName.readFields(in);
    this.colFamily.readFields(in);
    this.fileId = in.readLong();
  }

  //////////////////////////////////////////////////////////////////////////////
  // Comparable
  //////////////////////////////////////////////////////////////////////////////

  /* (non-Javadoc)
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  public int compareTo(Object o) {
    HStoreFile other = (HStoreFile) o;
    int result = this.dir.compareTo(other.dir);    
    if(result == 0) {
      this.regionName.compareTo(other.regionName);
    }
    if(result == 0) {
      result = this.colFamily.compareTo(other.colFamily);
    }    
    if(result == 0) {
      if(this.fileId < other.fileId) {
        result = -1;
        
      } else if(this.fileId > other.fileId) {
        result = 1;
      }
    }
    return result;
  }
}