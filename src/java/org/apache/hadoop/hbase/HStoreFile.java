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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Random;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.onelab.filter.Filter;
import org.onelab.filter.Key;


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
 * also refernce store files in another region serving either from
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
public class HStoreFile implements HConstants, WritableComparable {
  static final Log LOG = LogFactory.getLog(HStoreFile.class.getName());
  static final byte INFO_SEQ_NUM = 0;
  static final String HSTORE_DATFILE_DIR = "mapfiles";
  static final String HSTORE_INFO_DIR = "info";
  static final String HSTORE_FILTER_DIR = "filter";
  
  /** 
   * For split HStoreFiles, specifies if the file covers the lower half or
   * the upper half of the key range
   */
  public static enum Range {
    /** HStoreFile contains upper half of key range */
    top,
    /** HStoreFile contains lower half of key range */
    bottom
  }
  
  /*
   * Regex that will work for straight filenames and for reference names.
   * If reference, then the regex has more than just one group.  Group 1 is
   * this files id.  Group 2 the referenced region name, etc.
   */
  private static Pattern REF_NAME_PARSER =
    Pattern.compile("^(\\d+)(?:\\.(.+))?$");
  
  private static Random rand = new Random();

  private Path dir;
  private Text regionName;
  private Text colFamily;
  private long fileId;
  private final Configuration conf;
  private Reference reference;

  /** Shutdown constructor used by Writable */
  HStoreFile(Configuration conf) {
    this(conf, new Path(Path.CUR_DIR), new Text(), new Text(), 0);
  }
  
  /**
   * Constructor that fully initializes the object
   * @param conf Configuration object
   * @param dir directory path
   * @param regionName name of the region
   * @param colFamily name of the column family
   * @param fileId file identifier
   */
  HStoreFile(final Configuration conf, final Path dir, final Text regionName, 
      final Text colFamily, final long fileId) {
    this(conf, dir, regionName, colFamily, fileId, null);
  }

  /**
   * Constructor that fully initializes the object
   * @param conf Configuration object
   * @param dir directory path
   * @param regionName name of the region
   * @param colFamily name of the column family
   * @param fileId file identifier
   * @param ref Reference to another HStoreFile.
   */
  HStoreFile(Configuration conf, Path dir, Text regionName, 
      Text colFamily, long fileId, final Reference ref) {
    this.conf = conf;
    this.dir = dir;
    this.regionName = new Text(regionName);
    this.colFamily = new Text(colFamily);
    this.fileId = fileId;
    // If a reference, construction does not write the pointer files.  Thats
    // done by invocations of writeReferenceFiles(hsf, fs).  Happens at fast
    // split time.
    this.reference = ref;
  }

  /*
   * Data structure to hold reference to a store file over in another region.
   */
  static class Reference implements Writable {
    Text regionName;
    long fileid;
    Range region;
    HStoreKey midkey;
    
    Reference(final Text rn, final long fid, final HStoreKey m,
        final Range fr) {
      this.regionName = rn;
      this.fileid = fid;
      this.region = fr;
      this.midkey = m;
    }
    
    Reference() {
      this(null, -1, null, Range.bottom);
    }

    long getFileId() {
      return this.fileid;
    }

    Range getFileRegion() {
      return this.region;
    }
    
    HStoreKey getMidkey() {
      return this.midkey;
    }
    
    Text getRegionName() {
      return this.regionName;
    }
   
    /** {@inheritDoc} */
    @Override
    public String toString() {
      return this.regionName + "/" + this.fileid + "/" + this.region;
    }

    // Make it serializable.

    /** {@inheritDoc} */
    public void write(DataOutput out) throws IOException {
      this.regionName.write(out);
      out.writeLong(this.fileid);
      // Write true if we're doing top of the file.
      out.writeBoolean(isTopFileRegion(this.region));
      this.midkey.write(out);
    }

    /** {@inheritDoc} */
    public void readFields(DataInput in) throws IOException {
      this.regionName = new Text();
      this.regionName.readFields(in);
      this.fileid = in.readLong();
      boolean tmp = in.readBoolean();
      // If true, set region to top.
      this.region = tmp? Range.top: Range.bottom;
      this.midkey = new HStoreKey();
      this.midkey.readFields(in);
    }
  }

  static boolean isTopFileRegion(final Range r) {
    return r.equals(Range.top);
  }

  /** @return the region name */
  boolean isReference() {
    return this.reference != null;
  }
  
  Reference getReference() {
    return this.reference;
  }

  Text getRegionName() {
    return this.regionName;
  }

  /** @return the column family */
  Text getColFamily() {
    return this.colFamily;
  }

  /** @return the file identifier */
  long getFileId() {
    return this.fileId;
  }

  // Build full filenames from those components
  /** @return path for MapFile */
  Path getMapFilePath() {
    return isReference()?
      getMapFilePath(this.regionName, this.fileId,
        this.reference.getRegionName()):
      getMapFilePath(this.regionName, this.fileId);
  }

  private Path getMapFilePath(final Reference r) {
    return r == null?
      getMapFilePath():
      getMapFilePath(r.getRegionName(), r.getFileId());
  }

  private Path getMapFilePath(final Text name, final long fid) {
    return new Path(HStoreFile.getMapDir(dir, name, colFamily), 
      createHStoreFilename(fid, null));
  }
  
  private Path getMapFilePath(final Text name, final long fid, final Text rn) {
    return new Path(HStoreFile.getMapDir(dir, name, colFamily), 
      createHStoreFilename(fid, rn));
  }

  /** @return path for info file */
  Path getInfoFilePath() {
    return isReference()?
      getInfoFilePath(this.regionName, this.fileId,
        this.reference.getRegionName()):
      getInfoFilePath(this.regionName, this.fileId);
  }
  
  private Path getInfoFilePath(final Text name, final long fid) {
    return new Path(HStoreFile.getInfoDir(dir, name, colFamily), 
      createHStoreFilename(fid, null));
  }
  
  private Path getInfoFilePath(final Text name, final long fid, final Text rn) {
    return new Path(HStoreFile.getInfoDir(dir, name, colFamily), 
      createHStoreFilename(fid, rn));
  }

  // Static methods to build partial paths to internal directories.  Useful for 
  // HStore construction and log-rebuilding.
  private static String createHStoreFilename(final long fid) {
    return createHStoreFilename(fid, null);
  }
  
  private static String createHStoreFilename(final long fid,
      final Text regionName) {
    return Long.toString(fid) +
      ((regionName != null)? "." + regionName.toString(): "");
  }
  
  private static String createHStoreInfoFilename(final long fid) {
    return createHStoreFilename(fid, null);
  }
  
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

    Path testpath1 = new Path(mapdir, createHStoreFilename(fileId));
    Path testpath2 = new Path(mapdir, createHStoreInfoFilename(fileId));
    while(fs.exists(testpath1) || fs.exists(testpath2)) {
      fileId = Math.abs(rand.nextLong());
      testpath1 = new Path(mapdir, createHStoreFilename(fileId));
      testpath2 = new Path(mapdir, createHStoreInfoFilename(fileId));
    }
    return new HStoreFile(conf, dir, regionName, colFamily, fileId);
  }

  /*
   * Creates a series of HStoreFiles loaded from the given directory.
   * There must be a matching 'mapdir' and 'loginfo' pair of files.
   * If only one exists, we'll delete it.
   *
   * @param conf Configuration object
   * @param dir directory path
   * @param regionName region name
   * @param colFamily column family
   * @param fs file system
   * @return List of store file instances loaded from passed dir.
   * @throws IOException
   */
  static Vector<HStoreFile> loadHStoreFiles(Configuration conf, Path dir, 
      Text regionName, Text colFamily, FileSystem fs)
  throws IOException {
    // Look first at info files.  If a reference, these contain info we need
    // to create the HStoreFile.
    Path infodir = HStoreFile.getInfoDir(dir, regionName, colFamily);
    Path infofiles[] = fs.listPaths(new Path[] {infodir});
    Vector<HStoreFile> results = new Vector<HStoreFile>(infofiles.length);
    Vector<Path> mapfiles = new Vector<Path>(infofiles.length);
    for (int i = 0; i < infofiles.length; i++) {
      Path p = infofiles[i];
      Matcher m = REF_NAME_PARSER.matcher(p.getName());
      boolean isReference =  isReference(p, m);
      long fid = Long.parseLong(m.group(1));
      HStoreFile curfile = null;
      if (isReference) {
        Reference reference = readSplitInfo(infofiles[i], fs);
        curfile = new HStoreFile(conf, dir, regionName, colFamily, fid,
          reference);
      } else {
        curfile = new HStoreFile(conf, dir, regionName, colFamily, fid);
      }
      Path mapfile = curfile.getMapFilePath();
      if (!fs.exists(mapfile)) {
        fs.delete(curfile.getInfoFilePath());
        LOG.warn("Mapfile " + mapfile.toString() + " does not exist. " +
          "Cleaned up info file.  Continuing...");
        continue;
      }
      
      // TODO: Confirm referent exists.
      
      // Found map and sympathetic info file.  Add this hstorefile to result.
      results.add(curfile);
      // Keep list of sympathetic data mapfiles for cleaning info dir in next
      // section.  Make sure path is fully qualified for compare.
      Path qualified = fs.makeQualified(mapfile);
      mapfiles.add(qualified);
    }
    
    Path mapdir = HStoreFile.getMapDir(dir, regionName, colFamily);
    // List paths by experience returns fully qualified names -- at least when
    // running on a mini hdfs cluster.
    Path datfiles[] = fs.listPaths(new Path[] {mapdir});
    for (int i = 0; i < datfiles.length; i++) {
      // If does not have sympathetic info file, delete.
      if (!mapfiles.contains(fs.makeQualified(datfiles[i]))) {
        fs.delete(datfiles[i]);
      }
    }
    return results;
  }
  
  /**
   * @param p Path to check.
   * @return True if the path has format of a HStoreFile reference.
   */
  static boolean isReference(final Path p) {
    return isReference(p, REF_NAME_PARSER.matcher(p.getName()));
  }
 
  private static boolean isReference(final Path p, final Matcher m) {
    if (m == null || !m.matches()) {
      LOG.warn("Failed match of store file name " + p.toString());
      throw new RuntimeException("Failed match of store file name " +
          p.toString());
    }
    return m.groupCount() > 1 && m.group(2) != null;
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
  private void writeSplitInfo(final FileSystem fs)
  throws IOException {
    Path p = getInfoFilePath();
    if (fs.exists(p)) {
      throw new IOException("File already exists " + p.toString());
    }
    FSDataOutputStream out = fs.create(p);
    getReference().getRegionName().write(out);
    getReference().getMidkey().write(out);
    out.writeLong(getReference().getFileId());
    out.writeBoolean(isTopFileRegion(getReference().getFileRegion()));
    out.close();
  }
  
  /*
   * @see writeSplitInfo(Path p, HStoreFile hsf, FileSystem fs)
   */
  static Reference readSplitInfo(final Path p, final FileSystem fs)
  throws IOException {
    FSDataInputStream in = fs.open(p);
    Text rn = new Text();
    rn.readFields(in);
    HStoreKey midkey = new HStoreKey();
    midkey.readFields(in);
    long fid = in.readLong();
    boolean tmp = in.readBoolean();
    return new Reference(rn, fid, midkey, tmp? Range.top: Range.bottom);
    
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
   * Merges the contents of the given source HStoreFiles into a single new one.
   *
   * @param srcFiles files to be merged
   * @param fs file system
   * @param conf configuration object
   * @throws IOException
   */
  void mergeStoreFiles(Vector<HStoreFile> srcFiles, FileSystem fs, 
      @SuppressWarnings("hiding") Configuration conf)
  throws IOException {
    // Copy all the source MapFile tuples into this HSF's MapFile
    MapFile.Writer out = new MapFile.Writer(conf, fs,
      getMapFilePath().toString(),
      HStoreKey.class, ImmutableBytesWritable.class);
    
    try {
      for(HStoreFile src: srcFiles) {
        MapFile.Reader in = src.getReader(fs, null);
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
    for(HStoreFile hsf: srcFiles) {
      long curSeqId = hsf.loadInfo(fs);
      if(curSeqId > unifiedSeqId) {
        unifiedSeqId = curSeqId;
      }
    }
    writeInfo(fs, unifiedSeqId);
  }

  /** 
   * Reads in an info file
   *
   * @param fs file system
   * @return The sequence id contained in the info file
   * @throws IOException
   */
  long loadInfo(FileSystem fs) throws IOException {
    Path p = isReference()?
      getInfoFilePath(this.reference.getRegionName(),
        this.reference.getFileId()):
      getInfoFilePath();
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
  
  /**
   * Delete store map files.
   * @throws IOException 
   */
  public void delete() throws IOException {
    delete(getMapFilePath());
    delete(getInfoFilePath());
  }
  
  private void delete(final Path p) throws IOException {
    p.getFileSystem(this.conf).delete(p);
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
      return success;
    }
    src = getInfoFilePath();
    if (!fs.exists(src)) {
      throw new FileNotFoundException(src.toString());
    }
    success = fs.rename(src, hsf.getInfoFilePath());
    if (!success) {
      LOG.warn("Failed rename of " + src + " to " + hsf.getInfoFilePath());
    }
    return success;
  }
  
  /**
   * A facade for a {@link MapFile.Reader} that serves up either the top or
   * bottom half of a MapFile (where 'bottom' is the first half of the file
   * containing the keys that sort lowest and 'top' is the second half of the
   * file with keys that sort greater than those of the bottom half).
   * Subclasses BloomFilterMapFile.Reader in case 
   * 
   * <p>This file is not splitable.  Calls to {@link #midKey()} return null.
   */
  static class HalfMapFileReader extends BloomFilterMapFile.Reader {
    private final boolean top;
    private final WritableComparable midkey;
    private boolean topFirstNextCall = true;
    
    HalfMapFileReader(final FileSystem fs, final String dirName, 
        final Configuration conf, final Range r,
        final WritableComparable midKey)
    throws IOException {
      this(fs, dirName, conf, r, midKey, null);
    }
    
    HalfMapFileReader(final FileSystem fs, final String dirName, 
        final Configuration conf, final Range r,
        final WritableComparable midKey, final Filter filter)
    throws IOException {
      super(fs, dirName, conf, filter);
      this.top = isTopFileRegion(r);
      this.midkey = midKey;
    }
    
    @SuppressWarnings("unchecked")
    private void checkKey(final WritableComparable key)
    throws IOException {
      if (this.top) {
        if (key.compareTo(this.midkey) < 0) {
          throw new IOException("Illegal Access: Key is less than midKey of " +
          "backing mapfile");
        }
      } else if (key.compareTo(this.midkey) >= 0) {
        throw new IOException("Illegal Access: Key is greater than or equal " +
        "to midKey of backing mapfile");
      }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({ "unused"})
    @Override
    public synchronized void finalKey(WritableComparable key)
    throws IOException {
      throw new UnsupportedOperationException("Unsupported");
    }

    /** {@inheritDoc} */
    @Override
    public synchronized Writable get(WritableComparable key, Writable val)
        throws IOException {
      checkKey(key);
      return super.get(key, val);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override
    public synchronized WritableComparable getClosest(WritableComparable key,
        Writable val)
    throws IOException {
      if (this.top) {
        if (key.compareTo(this.midkey) < 0) {
          return this.midkey;
        }
      } else if (key.compareTo(this.midkey) >= 0) {
        // Contract says return null if EOF.
        return null;
      }
      return super.getClosest(key, val);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unused")
    @Override
    public synchronized WritableComparable midKey() throws IOException {
      // Returns null to indicate file is not splitable.
      return null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override
    public synchronized boolean next(WritableComparable key, Writable val)
    throws IOException {
      if (this.top && this.topFirstNextCall) {
        this.topFirstNextCall = false;
        return doFirstNextProcessing(key, val);
      }
      boolean result = super.next(key, val);
      if (!top && key.compareTo(this.midkey) >= 0) {
        result = false;
      }
      return result;
    }
    
    private boolean doFirstNextProcessing(WritableComparable key, Writable val)
    throws IOException {
      // Seek to midkey.  Midkey may not exist in this file.  That should be
      // fine.  Then we'll either be positioned at end or start of file.
      WritableComparable nearest = getClosest(this.midkey, val);
      // Now copy the mid key into the passed key.
      if (nearest != null) {
        Writables.copyWritable(nearest, key);
        return true;
      }
      return false;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void reset() throws IOException {
      if (top) {
        this.topFirstNextCall = true;
        seek(this.midkey);
        return;
      }
      super.reset();
    }

    /** {@inheritDoc} */
    @Override
    public synchronized boolean seek(WritableComparable key)
    throws IOException {
      checkKey(key);
      return super.seek(key);
    }
  }
  
  /**
   * On write, all keys are added to a bloom filter.  On read, all keys are
   * tested first against bloom filter. Keys are HStoreKey.  If passed bloom
   * filter is null, just passes invocation to parent.
   */
  static class BloomFilterMapFile extends MapFile {
    protected BloomFilterMapFile() {
      super();
    }
    
    static class Reader extends MapFile.Reader {
      private final Filter bloomFilter;

      /**
       * Constructor
       * 
       * @param fs
       * @param dirName
       * @param conf
       * @param filter
       * @throws IOException
       */
      public Reader(FileSystem fs, String dirName, Configuration conf,
          final Filter filter)
      throws IOException {
        super(fs, dirName, conf);
        this.bloomFilter = filter;
      }
      
      /** {@inheritDoc} */
      @Override
      public Writable get(WritableComparable key, Writable val)
      throws IOException {
        if (this.bloomFilter == null) {
          return super.get(key, val);
        }
        if(this.bloomFilter.membershipTest(getBloomFilterKey(key))) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("bloom filter reported that key exists");
          }
          return super.get(key, val);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("bloom filter reported that key does not exist");
        }
        return null;
      }

      /** {@inheritDoc} */
      @Override
      public WritableComparable getClosest(WritableComparable key,
          Writable val)
      throws IOException {
        if (this.bloomFilter == null) {
          return super.getClosest(key, val);
        }
        // Note - the key being passed to us is always a HStoreKey
        if(this.bloomFilter.membershipTest(getBloomFilterKey(key))) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("bloom filter reported that key exists");
          }
          return super.getClosest(key, val);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("bloom filter reported that key does not exist");
        }
        return null;
      }
    }
    
    static class Writer extends MapFile.Writer {
      private final Filter bloomFilter;
      

      /**
       * Constructor
       * 
       * @param conf
       * @param fs
       * @param dirName
       * @param keyClass
       * @param valClass
       * @param compression
       * @param filter
       * @throws IOException
       */
      @SuppressWarnings("unchecked")
      public Writer(Configuration conf, FileSystem fs, String dirName,
          Class keyClass, Class valClass,
          SequenceFile.CompressionType compression, final Filter filter)
      throws IOException {
        super(conf, fs, dirName, keyClass, valClass, compression);
        this.bloomFilter = filter;
      }

      /** {@inheritDoc} */
      @Override
      public void append(WritableComparable key, Writable val)
      throws IOException {
        if (this.bloomFilter != null) {
          this.bloomFilter.add(getBloomFilterKey(key));
        }
        super.append(key, val);
      }
    }
  }
  
  /**
   * Custom bloom filter key maker.
   * @param key
   * @return Key made of bytes of row and column only.
   * @throws IOException
   */
  static Key getBloomFilterKey(WritableComparable key)
  throws IOException {
    HStoreKey hsk = (HStoreKey)key;
    byte [] bytes = null;
    try {
      bytes = (hsk.getRow().toString() + hsk.getColumn().toString()).
        getBytes(UTF8_ENCODING);
    } catch (UnsupportedEncodingException e) {
      throw new IOException(e.toString());
    }
    return new Key(bytes);
  }

  /**
   * Get reader for the store file map file.
   * Client is responsible for closing file when done.
   * @param fs
   * @param bloomFilter If null, no filtering is done.
   * @return MapFile.Reader
   * @throws IOException
   */
  public synchronized MapFile.Reader getReader(final FileSystem fs,
      final Filter bloomFilter)
  throws IOException {
    return isReference()?
      new HStoreFile.HalfMapFileReader(fs,
        getMapFilePath(getReference().getRegionName(),
          getReference().getFileId()).toString(),
        this.conf, getReference().getFileRegion(), getReference().getMidkey(),
        bloomFilter):
      new BloomFilterMapFile.Reader(fs, getMapFilePath().toString(),
        this.conf, bloomFilter);
  }

  /**
   * Get a store file writer.
   * Client is responsible for closing file when done.
   * @param fs
   * @param compression Pass <code>SequenceFile.CompressionType.NONE</code>
   * for none.
   * @param bloomFilter If null, no filtering is done.
   * @return MapFile.Writer
   * @throws IOException
   */
  public MapFile.Writer getWriter(final FileSystem fs,
      final SequenceFile.CompressionType compression,
      final Filter bloomFilter)
  throws IOException {
    if (isReference()) {
      throw new IOException("Illegal Access: Cannot get a writer on a" +
        "HStoreFile reference");
    }
    return new BloomFilterMapFile.Writer(conf, fs,
      getMapFilePath().toString(), HStoreKey.class,
      ImmutableBytesWritable.class, compression, bloomFilter);
  }

  /**
   * @return Length of the store map file.  If a reference, size is
   * approximation.
   * @throws IOException
   */
  public long length() throws IOException {
    Path p = new Path(getMapFilePath(getReference()), MapFile.DATA_FILE_NAME);
    long l = p.getFileSystem(this.conf).getFileStatus(p).getLen();
    return (isReference())? l / 2: l;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return this.regionName.toString() + "/" + this.colFamily.toString() +
      "/" + this.fileId +
      (isReference()? "/" + this.reference.toString(): "");
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
    this.regionName.write(out);
    this.colFamily.write(out);
    out.writeLong(fileId);
    out.writeBoolean(isReference());
    if (isReference()) {
      this.reference.write(out);
    }
  }
  
  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    this.dir = new Path(in.readUTF());
    this.regionName.readFields(in);
    this.colFamily.readFields(in);
    this.fileId = in.readLong();
    this.reference = null;
    boolean isReferent = in.readBoolean();
    this.reference = new HStoreFile.Reference();
    if (isReferent) {
      this.reference.readFields(in);
    }
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
