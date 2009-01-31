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
package org.apache.hadoop.hbase.regionserver;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.io.BloomFilterMapFile;
import org.apache.hadoop.hbase.io.HalfMapFileReader;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.MapFile;
import org.apache.hadoop.hbase.io.SequenceFile;

/**
 * A HStore data file.  HStores usually have one or more of these files.  They
 * are produced by flushing the memcache to disk.
 *
 * <p>Each HStore maintains a bunch of different data files. The filename is a
 * mix of the parent dir, the region name, the column name, and a file
 * identifier. The name may also be a reference to a store file located
 * elsewhere. This class handles all that path-building stuff for you.
 * 
 * <p>An HStoreFile usually tracks 4 things: its parent dir, the region
 * identifier, the column family, and the file identifier.  If you know those
 * four things, you know how to obtain the right HStoreFile.  HStoreFiles may
 * also reference store files in another region serving either from
 * the top-half of the remote file or from the bottom-half.  Such references
 * are made fast splitting regions.
 * 
 * <p>Plain HStoreFiles are named for a randomly generated id as in:
 * <code>1278437856009925445</code>  A file by this name is made in both the
 * <code>mapfiles</code> and <code>info</code> subdirectories of a
 * HStore columnfamily directoy: E.g. If the column family is 'anchor:', then
 * under the region directory there is a subdirectory named 'anchor' within
 * which is a 'mapfiles' and 'info' subdirectory.  In each will be found a
 * file named something like <code>1278437856009925445</code>, one to hold the
 * data in 'mapfiles' and one under 'info' that holds the sequence id for this
 * store file.
 * 
 * <p>References to store files located over in some other region look like
 * this:
 * <code>1278437856009925445.hbaserepository,qAReLZD-OyQORZWq_vqR1k==,959247014679548184</code>:
 * i.e. an id followed by the name of the referenced region.  The data
 * ('mapfiles') of HStoreFile references are empty. The accompanying
 * <code>info</code> file contains the
 * midkey, the id of the remote store we're referencing and whether we're
 * to serve the top or bottom region of the remote store file.  Note, a region
 * is not splitable if it has instances of store file references (References
 * are cleaned up by compactions).
 * 
 * <p>When merging or splitting HRegions, we might want to modify one of the 
 * params for an HStoreFile (effectively moving it elsewhere).
 */
public class HStoreFile implements HConstants {
  static final Log LOG = LogFactory.getLog(HStoreFile.class.getName());
  static final byte INFO_SEQ_NUM = 0;
  static final byte MAJOR_COMPACTION = INFO_SEQ_NUM + 1;
  static final String HSTORE_DATFILE_DIR = "mapfiles";
  static final String HSTORE_INFO_DIR = "info";
  static final String HSTORE_FILTER_DIR = "filter";
  
  private final static Random rand = new Random();

  private final Path basedir;
  private final int encodedRegionName;
  private final byte [] colFamily;
  private final long fileId;
  private final HBaseConfiguration conf;
  private final FileSystem fs;
  private final Reference reference;
  private final HRegionInfo hri;
  /* If true, this file was product of a major compaction.
   */
  private boolean majorCompaction = false;
  private long indexLength;

  /**
   * Constructor that fully initializes the object
   * @param conf Configuration object
   * @param basedir qualified path that is parent of region directory
   * @param colFamily name of the column family
   * @param fileId file identifier
   * @param ref Reference to another HStoreFile.
   * @param hri The region info for this file (HACK HBASE-868). TODO: Fix.
   * @throws IOException
   */
  HStoreFile(HBaseConfiguration conf, FileSystem fs, Path basedir,
      final HRegionInfo hri, byte [] colFamily, long fileId,
      final Reference ref)
  throws IOException {
    this(conf, fs, basedir, hri, colFamily, fileId, ref, false);
  }
  
  /**
   * Constructor that fully initializes the object
   * @param conf Configuration object
   * @param basedir qualified path that is parent of region directory
   * @param colFamily name of the column family
   * @param fileId file identifier
   * @param ref Reference to another HStoreFile.
   * @param hri The region info for this file (HACK HBASE-868). TODO: Fix.
   * @param mc Try if this file was result of a major compression.
   * @throws IOException
   */
  HStoreFile(HBaseConfiguration conf, FileSystem fs, Path basedir,
      final HRegionInfo hri, byte [] colFamily, long fileId,
      final Reference ref, final boolean mc)
  throws IOException {
    this.conf = conf;
    this.fs = fs;
    this.basedir = basedir;
    this.encodedRegionName = hri.getEncodedName();
    this.colFamily = colFamily;
    this.hri = hri;
    
    long id = fileId;
    if (id == -1) {
      Path mapdir = HStoreFile.getMapDir(basedir, encodedRegionName, colFamily);
      Path testpath = null;
      do {
        id = Math.abs(rand.nextLong());
        testpath = new Path(mapdir, createHStoreFilename(id, -1));
      } while(fs.exists(testpath));
    }
    this.fileId = id;
    
    // If a reference, construction does not write the pointer files.  Thats
    // done by invocations of writeReferenceFiles(hsf, fs). Happens at split.
    this.reference = ref;
    this.majorCompaction = mc;
  }

  /** @return the region name */
  boolean isReference() {
    return reference != null;
  }
  
  Reference getReference() {
    return reference;
  }

  int getEncodedRegionName() {
    return this.encodedRegionName;
  }

  /** @return the column family */
  byte [] getColFamily() {
    return colFamily;
  }

  /** @return the file identifier */
  long getFileId() {
    return fileId;
  }

  // Build full filenames from those components
  
  /** @return path for MapFile */
  Path getMapFilePath() {
    if (isReference()) {
      return getMapFilePath(encodedRegionName, fileId,
          reference.getEncodedRegionName());
    }
    return getMapFilePath(this.encodedRegionName, fileId);
  }

  private Path getMapFilePath(final Reference r) {
    if (r == null) {
      return getMapFilePath();
    }
    return getMapFilePath(r.getEncodedRegionName(), r.getFileId());
  }

  private Path getMapFilePath(final int encodedName, final long fid) {
    return getMapFilePath(encodedName, fid, HRegionInfo.NO_HASH);
  }
  
  private Path getMapFilePath(final int encodedName, final long fid,
      final int ern) {
    return new Path(HStoreFile.getMapDir(basedir, encodedName, colFamily), 
      createHStoreFilename(fid, ern));
  }

  /** @return path for info file */
  Path getInfoFilePath() {
    if (isReference()) {
      return getInfoFilePath(encodedRegionName, fileId,
          reference.getEncodedRegionName());
 
    }
    return getInfoFilePath(encodedRegionName, fileId);
  }
  
  private Path getInfoFilePath(final int encodedName, final long fid) {
    return getInfoFilePath(encodedName, fid, HRegionInfo.NO_HASH);
  }
  
  private Path getInfoFilePath(final int encodedName, final long fid,
      final int ern) {
    return new Path(HStoreFile.getInfoDir(basedir, encodedName, colFamily), 
      createHStoreFilename(fid, ern));
  }

  // File handling

  /*
   * Split by making two new store files that reference top and bottom regions
   * of original store file.
   * @param midKey
   * @param dstA
   * @param dstB
   * @param fs
   * @param c
   * @throws IOException
   *
   * @param midKey the key which will be the starting key of the second region
   * @param dstA the file which will contain keys from the start of the source
   * @param dstB the file which will contain keys from midKey to end of source
   * @param fs file system
   * @param c configuration
   * @throws IOException
   */
  void splitStoreFile(final HStoreFile dstA, final HStoreFile dstB,
      final FileSystem fs)
  throws IOException {
    dstA.writeReferenceFiles(fs);
    dstB.writeReferenceFiles(fs);
  }
  
  void writeReferenceFiles(final FileSystem fs)
  throws IOException {
    createOrFail(fs, getMapFilePath());
    writeSplitInfo(fs);
  }
  
  /*
   * If reference, create and write the remote store file id, the midkey and
   * whether we're going against the top file region of the referent out to
   * the info file. 
   * @param p Path to info file.
   * @param hsf
   * @param fs
   * @throws IOException
   */
  private void writeSplitInfo(final FileSystem fs) throws IOException {
    Path p = getInfoFilePath();
    if (fs.exists(p)) {
      throw new IOException("File already exists " + p.toString());
    }
    FSDataOutputStream out = fs.create(p);
    try {
      reference.write(out);
    } finally {
      out.close();
   }
  }

  /**
   * @see #writeSplitInfo(FileSystem fs)
   */
  static Reference readSplitInfo(final Path p, final FileSystem fs)
  throws IOException {
    FSDataInputStream in = fs.open(p);
    try {
      Reference r = new Reference();
      r.readFields(in);
      return r;
    } finally {
      in.close();
    }
  }

  private void createOrFail(final FileSystem fs, final Path p)
  throws IOException {
    if (fs.exists(p)) {
      throw new IOException("File already exists " + p.toString());
    }
    if (!fs.createNewFile(p)) {
      throw new IOException("Failed create of " + p);
    }
  }

  /** 
   * Reads in an info file
   *
   * @param filesystem file system
   * @return The sequence id contained in the info file
   * @throws IOException
   */
  long loadInfo(final FileSystem filesystem) throws IOException {
    Path p = null;
    if (isReference()) {
      p = getInfoFilePath(reference.getEncodedRegionName(),
        this.reference.getFileId());
    } else {
      p = getInfoFilePath();
    }
    long length = filesystem.getFileStatus(p).getLen();
    boolean hasMoreThanSeqNum = length > (Byte.SIZE + Bytes.SIZEOF_LONG);
    DataInputStream in = new DataInputStream(filesystem.open(p));
    try {
      byte flag = in.readByte();
      if (flag == INFO_SEQ_NUM) {
        if (hasMoreThanSeqNum) {
          flag = in.readByte();
          if (flag == MAJOR_COMPACTION) {
            this.majorCompaction = in.readBoolean();
          }
        }
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
   * @param filesystem file system
   * @param infonum file id
   * @throws IOException
   */
  void writeInfo(final FileSystem filesystem, final long infonum)
  throws IOException {
    writeInfo(filesystem, infonum, false);
  }
  
  /**
   * Writes the file-identifier to disk
   * 
   * @param filesystem file system
   * @param infonum file id
   * @param mc True if this file is product of a major compaction
   * @throws IOException
   */
  void writeInfo(final FileSystem filesystem, final long infonum,
    final boolean mc)
  throws IOException {
    Path p = getInfoFilePath();
    FSDataOutputStream out = filesystem.create(p);
    try {
      out.writeByte(INFO_SEQ_NUM);
      out.writeLong(infonum);
      if (mc) {
        // Set whether major compaction flag on this file.
        this.majorCompaction = mc;
        out.writeByte(MAJOR_COMPACTION);
        out.writeBoolean(mc);
      }
    } finally {
      out.close();
    }
  }

  /**
   * Delete store map files.
   * @throws IOException 
   */
  public void delete() throws IOException {
    fs.delete(getMapFilePath(), true);
    fs.delete(getInfoFilePath(), true);
  }
  
  /**
   * Renames the mapfiles and info directories under the passed
   * <code>hsf</code> directory.
   * @param fs
   * @param hsf
   * @return True if succeeded.
   * @throws IOException
   */
  public boolean rename(final FileSystem fs, final HStoreFile hsf)
  throws IOException {
    Path src = getMapFilePath();
    if (!fs.exists(src)) {
      throw new FileNotFoundException(src.toString());
    }
    boolean success = fs.rename(src, hsf.getMapFilePath());
    if (!success) {
      LOG.warn("Failed rename of " + src + " to " + hsf.getMapFilePath());
    } else {
      src = getInfoFilePath();
      if (!fs.exists(src)) {
        throw new FileNotFoundException(src.toString());
      }
      success = fs.rename(src, hsf.getInfoFilePath());
      if (!success) {
        LOG.warn("Failed rename of " + src + " to " + hsf.getInfoFilePath());
      }
    }
    return success;
  }
  
  /**
   * Get reader for the store file map file.
   * Client is responsible for closing file when done.
   * @param fs
   * @param bloomFilter If true, a bloom filter exists
   * @param blockCacheEnabled If true, MapFile blocks should be cached.
   * @return BloomFilterMapFile.Reader
   * @throws IOException
   */
  public synchronized BloomFilterMapFile.Reader getReader(final FileSystem fs,
      final boolean bloomFilter, final boolean blockCacheEnabled)
  throws IOException {
    if (isReference()) {
      return new HalfMapFileReader(fs,
          getMapFilePath(reference).toString(), conf, 
          reference.getFileRegion(), reference.getMidkey(), bloomFilter,
          blockCacheEnabled, this.hri);
    }
    return new BloomFilterMapFile.Reader(fs, getMapFilePath().toString(),
        conf, bloomFilter, blockCacheEnabled, this.hri);
  }

  /**
   * Get a store file writer.
   * Client is responsible for closing file when done.
   * @param fs
   * @param compression Pass <code>SequenceFile.CompressionType.NONE</code>
   * for none.
   * @param bloomFilter If true, create a bloom filter
   * @param nrows number of rows expected. Required if bloomFilter is true.
   * @return MapFile.Writer
   * @throws IOException
   */
  public MapFile.Writer getWriter(final FileSystem fs,
      final SequenceFile.CompressionType compression,
      final boolean bloomFilter, int nrows)
  throws IOException {
    if (isReference()) {
      throw new IOException("Illegal Access: Cannot get a writer on a" +
        "HStoreFile reference");
    }
    return new BloomFilterMapFile.Writer(conf, fs,
      getMapFilePath().toString(), compression, bloomFilter, nrows, this.hri);
  }

  /**
   * @return Length of the store map file.  If a reference, size is
   * approximation.
   * @throws IOException
   */
  public long length() throws IOException {
    Path p = new Path(getMapFilePath(reference), MapFile.DATA_FILE_NAME);
    long l = p.getFileSystem(conf).getFileStatus(p).getLen();
    return (isReference())? l / 2: l;
  }

  /**
   * @return Length of the store map file index.
   * @throws IOException
   */
  public synchronized long indexLength() throws IOException {
    if (indexLength == 0) {
      Path p = new Path(getMapFilePath(reference), MapFile.INDEX_FILE_NAME);
      indexLength = p.getFileSystem(conf).getFileStatus(p).getLen();
    }
    return indexLength;
  }

  @Override
  public String toString() {
    return encodedRegionName + "/" + Bytes.toString(colFamily) + "/" + fileId +
      (isReference()? "-" + reference.toString(): "");
  }
  
  /**
   * @return True if this file was made by a major compaction.
   */
  public boolean isMajorCompaction() {
    return this.majorCompaction;
  }

  private static String createHStoreFilename(final long fid,
      final int encodedRegionName) {
    return Long.toString(fid) + 
      ((encodedRegionName != HRegionInfo.NO_HASH)?
        "." + encodedRegionName : "");
  }

  /**
   * @param dir Base directory
   * @param encodedRegionName Encoding of region name.
   * @param f Column family.
   * @return path for map file directory
   */
  public static Path getMapDir(Path dir, int encodedRegionName,
      final byte [] f) {
    return getFamilySubDir(dir, encodedRegionName, f, HSTORE_DATFILE_DIR);
  }

  /**
   * @param dir Base directory
   * @param encodedRegionName Encoding of region name.
   * @param f Column family.
   * @return the info directory path
   */
  public static Path getInfoDir(Path dir, int encodedRegionName, byte [] f) {
    return getFamilySubDir(dir, encodedRegionName, f, HSTORE_INFO_DIR);
  }

  /**
   * @param dir Base directory
   * @param encodedRegionName Encoding of region name.
   * @param f Column family.
   * @return the bloom filter directory path
   */
  @Deprecated
  public static Path getFilterDir(Path dir, int encodedRegionName,
      final byte [] f) {
    return getFamilySubDir(dir, encodedRegionName, f, HSTORE_FILTER_DIR);
  }
  
  /*
   * @param base Base directory
   * @param encodedRegionName Encoding of region name.
   * @param f Column family.
   * @param subdir Subdirectory to create under column family/store directory.
   * @return
   */
  private static Path getFamilySubDir(final Path base,
      final int encodedRegionName, final byte [] f, final String subdir) {
    return new Path(base, new Path(Integer.toString(encodedRegionName),
        new Path(Bytes.toString(f), subdir)));
  }
}