/**
 * Copyright 2009 The Apache Software Foundation
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.io.HalfHFileReader;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A Store data file.  Stores usually have one or more of these files.  They
 * are produced by flushing the memcache to disk.  To
 * create, call {@link #getWriter(FileSystem, Path)} and append data.  Be
 * sure to add any metadata before calling close on the Writer
 * (Use the appendMetadata convenience methods). On close, a StoreFile is
 * sitting in the Filesystem.  To refer to it, create a StoreFile instance
 * passing filesystem and path.  To read, call {@link #getReader()}.
 * <p>StoreFiles may also reference store files in another Store.
 */
public class StoreFile implements HConstants {
  static final Log LOG = LogFactory.getLog(StoreFile.class.getName());
  
  // Make default block size for StoreFiles 8k while testing.  TODO: FIX!
  // Need to make it 8k for testing.
  private static final int DEFAULT_BLOCKSIZE_SMALL = 8 * 1024;

  private final FileSystem fs;
  // This file's path.
  private final Path path;
  // If this storefile references another, this is the reference instance.
  private Reference reference;
  // If this StoreFile references another, this is the other files path.
  private Path referencePath;

  // Keys for metadata stored in backing HFile.
  private static final byte [] MAX_SEQ_ID_KEY = Bytes.toBytes("MAX_SEQ_ID_KEY");
  // Set when we obtain a Reader.
  private long sequenceid = -1;

  private static final byte [] MAJOR_COMPACTION_KEY =
    Bytes.toBytes("MAJOR_COMPACTION_KEY");
  // If true, this file was product of a major compaction.  Its then set
  // whenever you get a Reader.
  private AtomicBoolean majorCompaction = null;

  /*
   * Regex that will work for straight filenames and for reference names.
   * If reference, then the regex has more than just one group.  Group 1 is
   * this files id.  Group 2 the referenced region name, etc.
   */
  private static final Pattern REF_NAME_PARSER =
    Pattern.compile("^(\\d+)(?:\\.(.+))?$");

  private volatile HFile.Reader reader;

  // Used making file ids.
  private final static Random rand = new Random();

  /**
   * Constructor.
   * Loads up a Reader (and its indices, etc.).
   * @param fs Filesystem.
   * @param p qualified path
   * @throws IOException
   */
  StoreFile(final FileSystem fs, final Path p)
  throws IOException {
    this.fs = fs;
    this.path = p;
    if (isReference(p)) {
      this.reference = Reference.read(fs, p);
      this.referencePath = getReferredToFile(this.path);
    }
    this.reader = open();
  }

  /**
   * @return Path or null if this StoreFile was made with a Stream.
   */
  Path getPath() {
    return this.path;
  }

  /**
   * @return The Store/ColumnFamily this file belongs to.
   */
  byte [] getFamily() {
    return Bytes.toBytes(this.path.getParent().getName());
  }

  /**
   * @return True if this is a StoreFile Reference; call after {@link #open()}
   * else may get wrong answer.
   */
  boolean isReference() {
    return this.reference != null;
  }

  /**
   * @param p Path to check.
   * @return True if the path has format of a HStoreFile reference.
   */
  public static boolean isReference(final Path p) {
    return isReference(p, REF_NAME_PARSER.matcher(p.getName()));
  }

  /**
   * @param p Path to check.
   * @param m Matcher to use.
   * @return True if the path has format of a HStoreFile reference.
   */
  public static boolean isReference(final Path p, final Matcher m) {
    if (m == null || !m.matches()) {
      LOG.warn("Failed match of store file name " + p.toString());
      throw new RuntimeException("Failed match of store file name " +
          p.toString());
    }
    return m.groupCount() > 1 && m.group(2) != null;
  }

  /*
   * Return path to the file referred to by a Reference.  Presumes a directory
   * hierarchy of <code>${hbase.rootdir}/tablename/regionname/familyname</code>.
   * @param p Path to a Reference file.
   * @return Calculated path to parent region file.
   * @throws IOException
   */
  static Path getReferredToFile(final Path p) {
    Matcher m = REF_NAME_PARSER.matcher(p.getName());
    if (m == null || !m.matches()) {
      LOG.warn("Failed match of store file name " + p.toString());
      throw new RuntimeException("Failed match of store file name " +
          p.toString());
    }
    // Other region name is suffix on the passed Reference file name
    String otherRegion = m.group(2);
    // Tabledir is up two directories from where Reference was written.
    Path tableDir = p.getParent().getParent().getParent();
    String nameStrippedOfSuffix = m.group(1);
    // Build up new path with the referenced region in place of our current
    // region in the reference path.  Also strip regionname suffix from name.
    return new Path(new Path(new Path(tableDir, otherRegion),
      p.getParent().getName()), nameStrippedOfSuffix);
  }

  /**
   * @return True if this file was made by a major compaction.
   */
  boolean isMajorCompaction() {
    if (this.majorCompaction == null) {
      throw new NullPointerException("This has not been set yet");
    }
    return this.majorCompaction.get();
  }

  /**
   * @return This files maximum edit sequence id.
   */
  public long getMaxSequenceId() {
    if (this.sequenceid == -1) {
      throw new IllegalAccessError("Has not been initialized");
    }
    return this.sequenceid;
  }

  /**
   * Opens reader on this store file.  Called by Constructor.
   * @return Reader for the store file.
   * @throws IOException
   * @see #close()
   */
  protected HFile.Reader open()
  throws IOException {
    if (this.reader != null) {
      throw new IllegalAccessError("Already open");
    }
    if (isReference()) {
      this.reader = new HalfHFileReader(this.fs, this.referencePath, null,
        this.reference);
    } else {
      this.reader = new StoreFileReader(this.fs, this.path, null);
    }
    // Load up indices and fileinfo.
    Map<byte [], byte []> map = this.reader.loadFileInfo();
    // Read in our metadata.
    byte [] b = map.get(MAX_SEQ_ID_KEY);
    if (b != null) {
      // By convention, if halfhfile, top half has a sequence number > bottom
      // half. Thats why we add one in below. Its done for case the two halves
      // are ever merged back together --rare.  Without it, on open of store,
      // since store files are distingushed by sequence id, the one half would
      // subsume the other.
      this.sequenceid = Bytes.toLong(b);
      if (isReference()) {
        if (Reference.isTopFileRegion(this.reference.getFileRegion())) {
          this.sequenceid += 1;
        }
      }
      
    }
    b = map.get(MAJOR_COMPACTION_KEY);
    if (b != null) {
      boolean mc = Bytes.toBoolean(b);
      if (this.majorCompaction == null) {
        this.majorCompaction = new AtomicBoolean(mc);
      } else {
        this.majorCompaction.set(mc);
      }
    }
    return this.reader;
  }
  
  /**
   * Override to add some customization on HFile.Reader
   */
  static class StoreFileReader extends HFile.Reader {
    public StoreFileReader(FileSystem fs, Path path, BlockCache cache)
        throws IOException {
      super(fs, path, cache);
    }

    @Override
    protected String toStringFirstKey() {
      String result = "";
      try {
        result = HStoreKey.create(getFirstKey()).toString();
      } catch (IOException e) {
        LOG.warn("Failed toString first key", e);
      }
      return result;
    }

    @Override
    protected String toStringLastKey() {
      String result = "";
      try {
        result = HStoreKey.create(getLastKey()).toString();
      } catch (IOException e) {
        LOG.warn("Failed toString last key", e);
      }
      return result;
    }
  }

  /**
   * Override to add some customization on HalfHFileReader
   */
  static class HalfStoreFileReader extends HalfHFileReader {
    public HalfStoreFileReader(FileSystem fs, Path p, BlockCache c, Reference r)
        throws IOException {
      super(fs, p, c, r);
    }

    @Override
    public String toString() {
      return super.toString() + (isTop()? ", half=top": ", half=bottom");
    }

    @Override
    protected String toStringFirstKey() {
      String result = "";
      try {
        result = HStoreKey.create(getFirstKey()).toString();
      } catch (IOException e) {
        LOG.warn("Failed toString first key", e);
      }
      return result;
    }

    @Override
    protected String toStringLastKey() {
      String result = "";
      try {
        result = HStoreKey.create(getLastKey()).toString();
      } catch (IOException e) {
        LOG.warn("Failed toString last key", e);
      }
      return result;
    }
  }

  /**
   * @return Current reader.  Must call open first.
   */
  public HFile.Reader getReader() {
    if (this.reader == null) {
      throw new IllegalAccessError("Call open first");
    }
    return this.reader;
  }

  /**
   * @throws IOException
   */
  public synchronized void close() throws IOException {
    if (this.reader != null) {
      this.reader.close();
      this.reader = null;
    }
  }

  @Override
  public String toString() {
    return this.path.toString() +
      (isReference()? "-" + this.referencePath + "-" + reference.toString(): "");
  }

  /**
   * Delete this file
   * @throws IOException 
   */
  public void delete() throws IOException {
    close();
    this.fs.delete(getPath(), true);
  }

  /**
   * Utility to help with rename.
   * @param fs
   * @param src
   * @param tgt
   * @return True if succeeded.
   * @throws IOException
   */
  public static Path rename(final FileSystem fs, final Path src,
      final Path tgt)
  throws IOException {
    if (!fs.exists(src)) {
      throw new FileNotFoundException(src.toString());
    }
    if (!fs.rename(src, tgt)) {
      throw new IOException("Failed rename of " + src + " to " + tgt);
    }
    return tgt;
  }

  /**
   * Get a store file writer. Client is responsible for closing file when done.
   * If metadata, add BEFORE closing using
   * {@link #appendMetadata(org.apache.hadoop.hbase.io.hfile.HFile.Writer, long)}.
   * @param fs
   * @param dir Path to family directory.  Makes the directory if doesn't exist.
   * Creates a file with a unique name in this directory.
   * @return HFile.Writer
   * @throws IOException
   */
  public static HFile.Writer getWriter(final FileSystem fs, final Path dir)
  throws IOException {
    return getWriter(fs, dir, DEFAULT_BLOCKSIZE_SMALL, null, null, false);
  }

  /**
   * Get a store file writer. Client is responsible for closing file when done.
   * If metadata, add BEFORE closing using
   * {@link #appendMetadata(org.apache.hadoop.hbase.io.hfile.HFile.Writer, long)}.
   * @param fs
   * @param dir Path to family directory.  Makes the directory if doesn't exist.
   * Creates a file with a unique name in this directory.
   * @param blocksize
   * @param algorithm Pass null to get default.
   * @param c Pass null to get default.
   * @param bloomfilter 
   * @return HFile.Writer
   * @throws IOException
   */
  public static HFile.Writer getWriter(final FileSystem fs, final Path dir,
    final int blocksize, final Compression.Algorithm algorithm,
    final HStoreKey.StoreKeyComparator c, final boolean bloomfilter)
  throws IOException {
    if (!fs.exists(dir)) {
      fs.mkdirs(dir);
    }
    Path path = getUniqueFile(fs, dir);
    return new HFile.Writer(fs, path, blocksize,
      algorithm == null? HFile.DEFAULT_COMPRESSION_ALGORITHM: algorithm,
      c == null? new HStoreKey.StoreKeyComparator(): c, bloomfilter);
  }

  /**
   * @param fs
   * @param p
   * @return random filename inside passed <code>dir</code>
   */
  static Path getUniqueFile(final FileSystem fs, final Path p)
  throws IOException {
    if (!fs.getFileStatus(p).isDir()) {
      throw new IOException("Expecting a directory");
    }
    return fs.getFileStatus(p).isDir()? getRandomFilename(fs, p): p;
  }

  /**
   * @param fs
   * @param dir
   * @param encodedRegionName
   * @param family
   * @return Path to a file that doesn't exist at time of this invocation.
   * @throws IOException
   */
  static Path getRandomFilename(final FileSystem fs, final Path dir)
  throws IOException {
    return getRandomFilename(fs, dir, null);
  }

  /**
   * @param fs
   * @param dir
   * @param encodedRegionName
   * @param family
   * @param suffix
   * @return Path to a file that doesn't exist at time of this invocation.
   * @throws IOException
   */
  static Path getRandomFilename(final FileSystem fs, final Path dir,
      final String suffix)
  throws IOException {
    long id = -1;
    Path p = null;
    do {
      id = Math.abs(rand.nextLong());
      p = new Path(dir, Long.toString(id) +
        ((suffix == null || suffix.length() <= 0)? "": suffix));
    } while(fs.exists(p));
    return p;
  }

  /**
   * Write file metadata.
   * Call before you call close on the passed <code>w</code> since its written
   * as metadata to that file.
   * 
   * @param filesystem file system
   * @param maxSequenceId Maximum sequence id.
   * @throws IOException
   */
  static void appendMetadata(final HFile.Writer w, final long maxSequenceId)
  throws IOException {
    appendMetadata(w, maxSequenceId, false);
  }

  /**
   * Writes metadata.
   * Call before you call close on the passed <code>w</code> since its written
   * as metadata to that file.
   * @param maxSequenceId Maximum sequence id.
   * @param mc True if this file is product of a major compaction
   * @throws IOException
   */
  static void appendMetadata(final HFile.Writer w, final long maxSequenceId,
    final boolean mc)
  throws IOException {
    w.appendFileInfo(MAX_SEQ_ID_KEY, Bytes.toBytes(maxSequenceId));
    w.appendFileInfo(MAJOR_COMPACTION_KEY, Bytes.toBytes(mc));
  }

  /*
   * Write out a split reference.
   * @param fs
   * @param splitDir Presumes path format is actually
   * <code>SOME_DIRECTORY/REGIONNAME/FAMILY</code>.
   * @param f File to split.
   * @param splitRow
   * @param range
   * @return Path to created reference.
   * @throws IOException
   */
  static Path split(final FileSystem fs, final Path splitDir,
    final StoreFile f, final byte [] splitRow, final Reference.Range range)
  throws IOException {
    // A reference to the bottom half of the hsf store file.
    Reference r = new Reference(new HStoreKey(splitRow).getBytes(), range);
    // Add the referred-to regions name as a dot separated suffix. 
    // See REF_NAME_PARSER regex above.  The referred-to regions name is
    // up in the path of the passed in <code>f</code> -- parentdir is family,
    // then the directory above is the region name.
    String parentRegionName = f.getPath().getParent().getParent().getName();
    // Write reference with same file id only with the other region name as
    // suffix and into the new region location (under same family).
    Path p = new Path(splitDir, f.getPath().getName() + "." + parentRegionName);
    return r.write(fs, p);
  }
}