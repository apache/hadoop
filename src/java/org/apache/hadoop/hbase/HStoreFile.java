/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

import java.io.*;
import java.util.*;

/**
 * Each HStore maintains a bunch of different data files.
 *
 * An HStoreFile tracks 4 things: its parent dir, the region identifier, the 
 * column family, and the file identifier.  If you know those four things, you
 * know how to obtain the right HStoreFile.
 *
 * When merging or splitting HRegions, we might want to modify one of the 
 * params for an HStoreFile (effectively moving it elsewhere).
 * 
 * The filename is a mix of the parent dir, the region name, the column name, 
 * and the file identifier.
 * 
 * This class handles all that path-building stuff for you.
 */
public class HStoreFile implements HConstants, WritableComparable {
  private static final Log LOG = LogFactory.getLog(HStoreFile.class.getName());
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

  /** Constructor used by Writable */
  HStoreFile(Configuration conf) {
    this.conf = conf;
    this.dir = new Path(Path.CUR_DIR);
    this.regionName = new Text();
    this.colFamily = new Text();
    this.fileId = 0;
  }
  
  /**
   * Constructor that fully initializes the object
   * @param conf Configuration object
   * @param dir directory path
   * @param regionName name of the region
   * @param colFamily name of the column family
   * @param fileId file identifier
   */
  HStoreFile(Configuration conf, Path dir, Text regionName, 
      Text colFamily, long fileId) {
    
    this.conf = conf;
    this.dir = dir;
    this.regionName = new Text(regionName);
    this.colFamily = new Text(colFamily);
    this.fileId = fileId;
  }

  /** @return the directory path */
  Path getDir() {
    return dir;
  }

  /** @return the region name */
  Text getRegionName() {
    return regionName;
  }

  /** @return the column family */
  Text getColFamily() {
    return colFamily;
  }

  /** @return the file identifier */
  long fileId() {
    return fileId;
  }

  // Build full filenames from those components
  
  /** @return path for MapFile */
  Path getMapFilePath() {
    return new Path(HStoreFile.getMapDir(dir, regionName, colFamily), 
        HSTORE_DATFILE_PREFIX + fileId);
  }
  
  /** @return path for info file */
  Path getInfoFilePath() {
    return new Path(HStoreFile.getInfoDir(dir, regionName, colFamily), 
        HSTORE_INFOFILE_PREFIX + fileId);
  }

  // Static methods to build partial paths to internal directories.  Useful for 
  // HStore construction and log-rebuilding.

  /** @return the map directory path */
  static Path getMapDir(Path dir, Text regionName, Text colFamily) {
    return new Path(dir, new Path(HREGIONDIR_PREFIX + regionName, 
        new Path(colFamily.toString(), HSTORE_DATFILE_DIR)));
  }

  /** @return the info directory path */
  static Path getInfoDir(Path dir, Text regionName, Text colFamily) {
    return new Path(dir, new Path(HREGIONDIR_PREFIX + regionName, 
        new Path(colFamily.toString(), HSTORE_INFO_DIR)));
  }

  /** @return the bloom filter directory path */
  static Path getFilterDir(Path dir, Text regionName, Text colFamily) {
    return new Path(dir, new Path(HREGIONDIR_PREFIX + regionName,
        new Path(colFamily.toString(), HSTORE_FILTER_DIR)));
  }

  /** @return the HStore directory path */
  static Path getHStoreDir(Path dir, Text regionName, Text colFamily) {
    return new Path(dir, new Path(HREGIONDIR_PREFIX + regionName, 
        colFamily.toString()));
  }

  /** @return the HRegion directory path */
  static Path getHRegionDir(Path dir, Text regionName) {
    return new Path(dir, new Path(HREGIONDIR_PREFIX + regionName));
  }

  /**
   * @return a brand-new randomly-named HStoreFile.
   * 
   * Checks the filesystem to determine if the file already exists. If so, it
   * will keep generating names until it generates a name that does not exist.
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
   * Creates a series of HStoreFiles loaded from the given directory.
   * 
   * There must be a matching 'mapdir' and 'loginfo' pair of files.
   * If only one exists, we'll delete it.
   *
   * @param conf Configuration object
   * @param dir directory path
   * @param regionName region name
   * @param colFamily column family
   * @param fs file system
   * @return Vector of HStoreFiles
   * @throws IOException
   */
  static Vector<HStoreFile> loadHStoreFiles(Configuration conf, Path dir, 
      Text regionName, Text colFamily, FileSystem fs) throws IOException {
    
    Vector<HStoreFile> results = new Vector<HStoreFile>();
    Path mapdir = HStoreFile.getMapDir(dir, regionName, colFamily);

    Path datfiles[] = fs.listPaths(mapdir);
    for(int i = 0; i < datfiles.length; i++) {
      String name = datfiles[i].getName();
      
      if(name.startsWith(HSTORE_DATFILE_PREFIX)) {
        Long fileId =
          Long.parseLong(name.substring(HSTORE_DATFILE_PREFIX.length()));

        HStoreFile curfile =
          new HStoreFile(conf, dir, regionName, colFamily, fileId);

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
        long fileId =
          Long.parseLong(name.substring(HSTORE_INFOFILE_PREFIX.length()));

        HStoreFile curfile =
          new HStoreFile(conf, dir, regionName, colFamily, fileId);

        Path mapfile = curfile.getMapFilePath();
        
        if(! fs.exists(mapfile)) {
          fs.delete(curfile.getInfoFilePath());
        }
      }
    }
    return results;
  }

  // File handling

  /**
   * Break this HStoreFile file into two new parts, which live in different 
   * brand-new HRegions.
   *
   * @param midKey the key which will be the starting key of the second region
   * @param dstA the file which will contain keys from the start of the source
   * @param dstB the file which will contain keys from midKey to end of source
   * @param fs file system
   * @param c configuration
   * @throws IOException
   */
  void splitStoreFile(Text midKey, HStoreFile dstA, HStoreFile dstB,
      FileSystem fs, Configuration c) throws IOException {
    
    // Copy the appropriate tuples to one MapFile or the other.
    
    MapFile.Reader in = new MapFile.Reader(fs, getMapFilePath().toString(), c);
    try {
      MapFile.Writer outA = new MapFile.Writer(c, fs, 
        dstA.getMapFilePath().toString(), HStoreKey.class,
        ImmutableBytesWritable.class);
      
      try {
        MapFile.Writer outB = new MapFile.Writer(c, fs, 
          dstB.getMapFilePath().toString(), HStoreKey.class,
          ImmutableBytesWritable.class);
        
        try {
          long count = 0;
          HStoreKey readkey = new HStoreKey();
          ImmutableBytesWritable readval = new ImmutableBytesWritable();
          
          while(in.next(readkey, readval)) {
            if(readkey.getRow().compareTo(midKey) < 0) {
              outA.append(readkey, readval);
            } else {
              outB.append(readkey, readval);
            }
            if (LOG.isDebugEnabled()) {
              count++;
              if ((count % 10000) == 0) {
                LOG.debug("Write " + count + " records");
              }
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
   * Merges the contents of the given source HStoreFiles into a single new one.
   *
   * @param srcFiles files to be merged
   * @param fs file system
   * @param conf configuration object
   * @throws IOException
   */
  void mergeStoreFiles(Vector<HStoreFile> srcFiles, FileSystem fs, 
      Configuration conf) throws IOException {

    // Copy all the source MapFile tuples into this HSF's MapFile

    MapFile.Writer out = new MapFile.Writer(conf, fs,
      getMapFilePath().toString(),
      HStoreKey.class, ImmutableBytesWritable.class);
    
    try {
      for(HStoreFile src: srcFiles) {
        MapFile.Reader in =
          new MapFile.Reader(fs, src.getMapFilePath().toString(), conf);
        
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

  /** 
   * Reads in an info file, and gives it a unique ID.
   *
   * @param fs file system
   * @return new unique id
   * @throws IOException
   */
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
  
  /**
   * Writes the file-identifier to disk
   * 
   * @param fs file system
   * @param infonum file id
   * @throws IOException
   */
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

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object o) {
    return this.compareTo(o) == 0;
  }
  
  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    int result = this.dir.hashCode();
    result ^= this.regionName.hashCode();
    result ^= this.colFamily.hashCode();
    result ^= Long.valueOf(this.fileId).hashCode();
    return result;
  }

  // Writable

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    out.writeUTF(dir.toString());
    regionName.write(out);
    colFamily.write(out);
    out.writeLong(fileId);
  }
  
  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    this.dir = new Path(in.readUTF());
    this.regionName.readFields(in);
    this.colFamily.readFields(in);
    this.fileId = in.readLong();
  }

  // Comparable

  /** {@inheritDoc} */
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