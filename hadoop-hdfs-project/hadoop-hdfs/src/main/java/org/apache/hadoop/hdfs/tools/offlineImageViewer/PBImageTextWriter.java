/**
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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.SectionName;
import org.apache.hadoop.hdfs.server.namenode.FSImageUtil;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.hdfs.server.namenode.SerialNumberManager;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.LimitInputStream;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Time;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.XATTR_ERASURECODING_POLICY;

/**
 * This class reads the protobuf-based fsimage and generates text output
 * for each inode to {@link PBImageTextWriter#out}. The sub-class can override
 * {@link #getEntry(String, INode)} to generate formatted string for each inode.
 *
 * Since protobuf-based fsimage does not guarantee the order of inodes and
 * directories, PBImageTextWriter runs two-phase scans:
 *
 * <ol>
 *   <li>The first phase, PBImageTextWriter scans the INode sections to reads the
 *   filename of each directory. It also scans the INode_Dir sections to loads
 *   the relationships between a directory and its children. It uses these metadata
 *   to build FS namespace and stored in {@link MetadataMap}</li>
 *   <li>The second phase, PBImageTextWriter re-scans the INode sections. For each
 *   inode, it looks up the path of the parent directory in the {@link MetadataMap},
 *   and generate output.</li>
 * </ol>
 *
 * Two various of {@link MetadataMap} are provided. {@link InMemoryMetadataDB}
 * stores all metadata in memory (O(n) memory) while
 * {@link LevelDBMetadataMap} stores metadata in LevelDB on disk (O(1) memory).
 * User can choose between them based on the time/space tradeoffs.
 */
abstract class PBImageTextWriter implements Closeable {
  private static final Logger LOG =
      LoggerFactory.getLogger(PBImageTextWriter.class);

  static final String DEFAULT_DELIMITER = "\t";
  static final String CRLF = StringUtils.CR + StringUtils.LF;

  /**
   * This metadata map is used to construct the namespace before generating
   * text outputs.
   *
   * It contains two mapping relationships:
   * <p>
   *   <li>It maps each inode (inode Id) to its parent directory (inode Id).</li>
   *   <li>It maps each directory from its inode Id.</li>
   * </p>
   */
  private static interface MetadataMap extends Closeable {
    /**
     * Associate an inode with its parent directory.
     */
    public void putDirChild(long parentId, long childId) throws IOException;

    /**
     * Associate a directory with its inode Id.
     */
    public void putDir(INode dir) throws IOException;

    /** Get the full path of the parent directory for the given inode. */
    public String getParentPath(long inode) throws IOException;

    /** Synchronize metadata to persistent storage, if possible */
    public void sync() throws IOException;

    /** Returns the name of inode. */
    String getName(long id) throws IOException;

    /**
     * Returns the id of the parent's inode, if mentioned in
     * INodeDirectorySection, throws IgnoreSnapshotException otherwise.
     */
    long getParentId(long id) throws IOException;
  }

  /**
   * Maintain all the metadata in memory.
   */
  private static class InMemoryMetadataDB implements MetadataMap {
    /**
     * Represent a directory in memory.
     */
    private static class Dir {
      private final long inode;
      private Dir parent = null;
      private String name;
      private String path = null;  // cached full path of the directory.

      Dir(long inode, String name) {
        this.inode = inode;
        this.name = name;
      }

      private void setParent(Dir parent) {
        Preconditions.checkState(this.parent == null);
        this.parent = parent;
      }

      /**
       * Returns the full path of this directory.
       */
      String getPath() throws IgnoreSnapshotException {
        if (this.parent == null) {
          if (this.inode == INodeId.ROOT_INODE_ID) {
            return "/";
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Not root inode with id {} having no parent.", inode);
            }
            throw PBImageTextWriter.createIgnoredSnapshotException(inode);
          }
        }
        if (this.path == null) {
          this.path = new Path(parent.getPath(), name.isEmpty() ? "/" : name).
              toString();
        }
        return this.path;
      }

      String getName() throws IgnoreSnapshotException {
        return name;
      }

      long getId() {
        return inode;
      }

      @Override
      public boolean equals(Object o) {
        return o instanceof Dir && inode == ((Dir) o).inode;
      }

      @Override
      public int hashCode() {
        return Long.valueOf(inode).hashCode();
      }
    }

    /**
     * If the Dir entry does not exist (i.e. the inode was not contained in
     * INodeSection) we still create a Dir entry which throws exceptions
     * for calls other than getId().
     * We can make sure this way, the getId and getParentId calls will
     * always succeed if we have the information.
     */
    private static class CorruptedDir extends Dir {
      CorruptedDir(long inode) {
        super(inode, null);
      }

      @Override
      String getPath() throws IgnoreSnapshotException {
        throw PBImageTextWriter.createIgnoredSnapshotException(getId());
      }

      @Override
      String getName() throws IgnoreSnapshotException {
        throw PBImageTextWriter.createIgnoredSnapshotException(getId());
      }
    }

    /** INode Id to Dir object mapping */
    private Map<Long, Dir> dirMap = new HashMap<>();

    /** Children to parent directory INode ID mapping. */
    private Map<Long, Dir> dirChildMap = new HashMap<>();

    InMemoryMetadataDB() {
    }

    @Override
    public void close() throws IOException {
    }

    private Dir getOrCreateCorrupted(long id) {
      Dir dir = dirMap.get(id);
      if (dir == null) {
        dir = new CorruptedDir(id);
        dirMap.put(id, dir);
      }
      return dir;
    }

    @Override
    public void putDirChild(long parentId, long childId) {
      Dir parent = getOrCreateCorrupted(parentId);
      Dir child = getOrCreateCorrupted(childId);
      child.setParent(parent);
      Preconditions.checkState(!dirChildMap.containsKey(childId));
      dirChildMap.put(childId, parent);
    }

    @Override
    public void putDir(INode p) {
      Preconditions.checkState(!dirMap.containsKey(p.getId()));
      Dir dir = new Dir(p.getId(), p.getName().toStringUtf8());
      dirMap.put(p.getId(), dir);
    }

    @Override
    public String getParentPath(long inode) throws IOException {
      if (inode == INodeId.ROOT_INODE_ID) {
        return "/";
      }
      Dir parent = dirChildMap.get(inode);
      if (parent == null) {
        // The inode is an INodeReference, which is generated from snapshot.
        // For delimited oiv tool, no need to print out metadata in snapshots.
        throw PBImageTextWriter.createIgnoredSnapshotException(inode);
      }
      return parent.getPath();
    }

    @Override
    public void sync() {
    }

    @Override
    public String getName(long id) throws IgnoreSnapshotException {
      Dir dir = dirMap.get(id);
      if (dir != null) {
        return dir.getName();
      }
      throw PBImageTextWriter.createIgnoredSnapshotException(id);
    }

    @Override
    public long getParentId(long id) throws IgnoreSnapshotException {
      Dir parentDir = dirChildMap.get(id);
      if (parentDir != null) {
        return parentDir.getId();
      }
      throw PBImageTextWriter.createIgnoredSnapshotException(id);
    }
  }

  /**
   * A MetadataMap that stores metadata in LevelDB.
   */
  private static class LevelDBMetadataMap implements MetadataMap {
    /**
     * Store metadata in LevelDB.
     */
    private static class LevelDBStore implements Closeable {
      private DB db = null;
      private WriteBatch batch = null;
      private int writeCount = 0;
      private static final int BATCH_SIZE = 1024;

      LevelDBStore(final File dbPath) throws IOException {
        Options options = new Options();
        options.createIfMissing(true);
        options.errorIfExists(true);
        db = JniDBFactory.factory.open(dbPath, options);
        batch = db.createWriteBatch();
      }

      @Override
      public void close() throws IOException {
        if (batch != null) {
          IOUtils.cleanupWithLogger(null, batch);
          batch = null;
        }
        IOUtils.cleanupWithLogger(null, db);
        db = null;
      }

      public void put(byte[] key, byte[] value) throws IOException {
        batch.put(key, value);
        writeCount++;
        if (writeCount >= BATCH_SIZE) {
          sync();
        }
      }

      public byte[] get(byte[] key) throws IOException {
        return db.get(key);
      }

      public void sync() throws IOException {
        try {
          db.write(batch);
        } finally {
          batch.close();
          batch = null;
        }
        batch = db.createWriteBatch();
        writeCount = 0;
      }
    }

    /**
     * A LRU cache for directory path strings.
     *
     * The key of this LRU cache is the inode of a directory.
     */
    private static class DirPathCache extends LinkedHashMap<Long, String> {
      private final static int CAPACITY = 16 * 1024;

      DirPathCache() {
        super(CAPACITY);
      }

      @Override
      protected boolean removeEldestEntry(Map.Entry<Long, String> entry) {
        return super.size() > CAPACITY;
      }
    }

    /** Map the child inode to the parent directory inode. */
    private LevelDBStore dirChildMap = null;
    /** Directory entry map */
    private LevelDBStore dirMap = null;
    private DirPathCache dirPathCache = new DirPathCache();

    LevelDBMetadataMap(String baseDir) throws IOException {
      File dbDir = new File(baseDir);
      if (dbDir.exists()) {
        throw new IOException("Folder " + dbDir + " already exists! Delete " +
            "manually or provide another (not existing) directory!");
      }
      if (!dbDir.mkdirs()) {
        throw new IOException("Failed to mkdir on " + dbDir);
      }
      try {
        dirChildMap = new LevelDBStore(new File(dbDir, "dirChildMap"));
        dirMap = new LevelDBStore(new File(dbDir, "dirMap"));
      } catch (IOException e) {
        LOG.error("Failed to open LevelDBs", e);
        IOUtils.cleanupWithLogger(null, this);
      }
    }

    @Override
    public void close() throws IOException {
      IOUtils.cleanupWithLogger(null, dirChildMap, dirMap);
      dirChildMap = null;
      dirMap = null;
    }

    private static byte[] toBytes(long value) {
      return ByteBuffer.allocate(8).putLong(value).array();
    }

    private static byte[] toBytes(String value) {
      return value.getBytes(StandardCharsets.UTF_8);
    }

    private static long toLong(byte[] bytes) {
      Preconditions.checkArgument(bytes.length == 8);
      return ByteBuffer.wrap(bytes).getLong();
    }

    private static String toString(byte[] bytes) throws IOException {
      return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public void putDirChild(long parentId, long childId) throws IOException {
      dirChildMap.put(toBytes(childId), toBytes(parentId));
    }

    @Override
    public void putDir(INode dir) throws IOException {
      Preconditions.checkArgument(dir.hasDirectory(),
          "INode %s (%s) is not a directory.", dir.getId(), dir.getName());
      dirMap.put(toBytes(dir.getId()), toBytes(dir.getName().toStringUtf8()));
    }

    private long getFromDirChildMap(long inode) throws IOException {
      byte[] bytes = dirChildMap.get(toBytes(inode));
      if (bytes == null) {
        // The inode is an INodeReference, which is generated from snapshot.
        // For delimited oiv tool, no need to print out metadata in snapshots.
        throw PBImageTextWriter.createIgnoredSnapshotException(inode);
      }
      if (bytes.length != 8) {
        throw new IOException(
            "bytes array length error. Actual length is " + bytes.length);
      }
      return toLong(bytes);
    }

    @Override
    public String getParentPath(long inode) throws IOException {
      if (inode == INodeId.ROOT_INODE_ID) {
        return "/";
      }
      long parent = getFromDirChildMap(inode);
      byte[] bytes = dirMap.get(toBytes(parent));
      synchronized (this) {
        if (!dirPathCache.containsKey(parent)) {
          if (parent != INodeId.ROOT_INODE_ID && bytes == null) {
            // The parent is an INodeReference, which is generated from snapshot.
            // For delimited oiv tool, no need to print out metadata in snapshots.
            throw PBImageTextWriter.createIgnoredSnapshotException(inode);
          }
          String parentName = toString(bytes);
          String parentPath =
              new Path(getParentPath(parent),
                  parentName.isEmpty() ? "/" : parentName).toString();
          dirPathCache.put(parent, parentPath);
        }
        return dirPathCache.get(parent);
      }
    }

    @Override
    public void sync() throws IOException {
      dirChildMap.sync();
      dirMap.sync();
    }

    @Override
    public String getName(long id) throws IOException {
      byte[] bytes = dirMap.get(toBytes(id));
      if (bytes != null) {
        return toString(bytes);
      }
      throw PBImageTextWriter.createIgnoredSnapshotException(id);
    }

    @Override
    public long getParentId(long id) throws IOException {
      return getFromDirChildMap(id);
    }
  }

  private SerialNumberManager.StringTable stringTable;
  private final PrintStream out;
  private MetadataMap metadataMap = null;
  private String delimiter;
  private File filename;
  private int numThreads;
  private String parallelOutputFile;
  private final XAttr ecXAttr =
      XAttrHelper.buildXAttr(XATTR_ERASURECODING_POLICY);

  /**
   * Construct a PB FsImage writer to generate text file.
   * @param out the writer to output text information of fsimage.
   * @param tempPath the path to store metadata. If it is empty, store metadata
   *                 in memory instead.
   */
  PBImageTextWriter(PrintStream out, String delimiter, String tempPath,
      int numThreads, String parallelOutputFile) throws IOException {
    this.out = out;
    this.delimiter = delimiter;
    if (tempPath.isEmpty()) {
      metadataMap = new InMemoryMetadataDB();
    } else {
      metadataMap = new LevelDBMetadataMap(tempPath);
    }
    this.numThreads = numThreads;
    this.parallelOutputFile = parallelOutputFile;
  }

  PBImageTextWriter(PrintStream out, String delimiter, String tempPath)
      throws IOException {
    this(out, delimiter, tempPath, 1, "-");
  }

  protected PrintStream serialOutStream() {
    return out;
  }

  @Override
  public void close() throws IOException {
    out.flush();
    IOUtils.cleanupWithLogger(null, metadataMap);
  }

  void append(StringBuffer buffer, int field) {
    buffer.append(delimiter);
    buffer.append(field);
  }

  void append(StringBuffer buffer, long field) {
    buffer.append(delimiter);
    buffer.append(field);
  }

  void append(StringBuffer buffer, String field) {
    buffer.append(delimiter);

    String escapedField = StringEscapeUtils.escapeCsv(field);
    if (escapedField.contains(CRLF)) {
      escapedField = escapedField.replace(CRLF, "%x0D%x0A");
    } else if (escapedField.contains(StringUtils.LF)) {
      escapedField = escapedField.replace(StringUtils.LF, "%x0A");
    }

    buffer.append(escapedField);
  }

  /**
   * Get text output for the given inode.
   * @param parent the path of parent directory
   * @param inode the INode object to output.
   */
  abstract protected String getEntry(String parent, INode inode);

  /**
   * Get text output for the header line.
   */
  abstract protected String getHeader();

  /**
   * Method called at the end of output() phase after all the inodes
   * with known parentPath has been printed out. Can be used to print
   * additional data depending on the written inodes.
   */
  abstract protected void afterOutput() throws IOException;

  public void visit(String filePath) throws IOException {
    filename = new File(filePath);
    RandomAccessFile file = new RandomAccessFile(filePath, "r");
    Configuration conf = new Configuration();
    if (!FSImageUtil.checkFileFormat(file)) {
      throw new IOException("Unrecognized FSImage");
    }

    FileSummary summary = FSImageUtil.loadSummary(file);

    try (FileInputStream fin = new FileInputStream(file.getFD())) {
      InputStream is;
      ArrayList<FileSummary.Section> sections =
          Lists.newArrayList(summary.getSectionsList());
      Collections.sort(sections,
          new Comparator<FileSummary.Section>() {
            @Override
            public int compare(FsImageProto.FileSummary.Section s1,
                FsImageProto.FileSummary.Section s2) {
              FSImageFormatProtobuf.SectionName n1 =
                  FSImageFormatProtobuf.SectionName.fromString(s1.getName());
              FSImageFormatProtobuf.SectionName n2 =
                  FSImageFormatProtobuf.SectionName.fromString(s2.getName());
              if (n1 == null) {
                return n2 == null ? 0 : -1;
              } else if (n2 == null) {
                return -1;
              } else {
                return n1.ordinal() - n2.ordinal();
              }
            }
          });

      ImmutableList<Long> refIdList = null;
      for (FileSummary.Section section : sections) {
        fin.getChannel().position(section.getOffset());
        is = FSImageUtil.wrapInputStreamForCompression(conf,
            summary.getCodec(), new BufferedInputStream(new LimitInputStream(
                fin, section.getLength())));
        SectionName sectionName = SectionName.fromString(section.getName());
        if (sectionName == null) {
          throw new IOException("Unrecognized section " + section.getName());
        }
        switch (sectionName) {
        case STRING_TABLE:
          LOG.info("Loading string table");
          stringTable = FSImageLoader.loadStringTable(is);
          break;
        case INODE_REFERENCE:
          // Load INodeReference so that all INodes can be processed.
          // Snapshots are not handled and will just be ignored for now.
          LOG.info("Loading inode references");
          refIdList = FSImageLoader.loadINodeReferenceSection(is);
          break;
        default:
          break;
        }
      }

      loadDirectories(fin, sections, summary, conf);
      loadINodeDirSection(fin, sections, summary, conf, refIdList);
      metadataMap.sync();
      output(conf, summary, fin, sections);
    }
  }

  void putDirChildToMetadataMap(long parentId, long childId)
      throws IOException {
    metadataMap.putDirChild(parentId, childId);
  }

  String getNodeName(long id) throws IOException {
    return metadataMap.getName(id);
  }

  long getParentId(long id) throws IOException {
    return metadataMap.getParentId(id);
  }

  private void output(Configuration conf, FileSummary summary,
      FileInputStream fin, ArrayList<FileSummary.Section> sections)
      throws IOException {
    ArrayList<FileSummary.Section> allINodeSubSections =
        getINodeSubSections(sections);
    if (numThreads > 1 && !parallelOutputFile.equals("-") &&
        allINodeSubSections.size() > 1) {
      outputInParallel(conf, summary, allINodeSubSections);
    } else {
      LOG.info("Serial output due to threads num: {}, parallel output file: {}, " +
          "subSections: {}.", numThreads, parallelOutputFile, allINodeSubSections.size());
      outputInSerial(conf, summary, fin, sections);
    }
  }

  private void outputInSerial(Configuration conf, FileSummary summary,
      FileInputStream fin, ArrayList<FileSummary.Section> sections)
      throws IOException {
    InputStream is;
    long startTime = Time.monotonicNow();
    serialOutStream().println(getHeader());
    for (FileSummary.Section section : sections) {
      if (SectionName.fromString(section.getName()) == SectionName.INODE) {
        fin.getChannel().position(section.getOffset());
        is = FSImageUtil.wrapInputStreamForCompression(conf,
            summary.getCodec(), new BufferedInputStream(new LimitInputStream(
                fin, section.getLength())));
        INodeSection s = INodeSection.parseDelimitedFrom(is);
        LOG.info("Found {} INodes in the INode section", s.getNumInodes());
        int count = outputINodes(is, serialOutStream());
        LOG.info("Outputted {} INodes.", count);
      }
    }
    afterOutput();
    long timeTaken = Time.monotonicNow() - startTime;
    LOG.debug("Time to output inodes: {} ms", timeTaken);
  }

  /**
   * STEP1: Multi-threaded process sub-sections.
   * Given n (n>1) threads to process k (k>=n) sections,
   * output parsed results of each section to tmp file in order.
   * STEP2: Merge tmp files.
   */
  private void outputInParallel(Configuration conf, FileSummary summary,
      ArrayList<FileSummary.Section> subSections)
      throws IOException {
    int nThreads = Integer.min(numThreads, subSections.size());
    LOG.info("Outputting in parallel with {} sub-sections using {} threads",
        subSections.size(), nThreads);
    final CopyOnWriteArrayList<IOException> exceptions = new CopyOnWriteArrayList<>();
    CountDownLatch latch = new CountDownLatch(subSections.size());
    ExecutorService executorService = Executors.newFixedThreadPool(nThreads);
    AtomicLong expectedINodes = new AtomicLong(0);
    AtomicLong totalParsed = new AtomicLong(0);
    String codec = summary.getCodec();
    String[] paths = new String[subSections.size()];

    for (int i = 0; i < subSections.size(); i++) {
      paths[i] = parallelOutputFile + ".tmp." + i;
      int index = i;
      executorService.submit(() -> {
        LOG.info("Output iNodes of section-{}", index);
        InputStream is = null;
        try (PrintStream outStream = new PrintStream(paths[index], "UTF-8")) {
          long startTime = Time.monotonicNow();
          is = getInputStreamForSection(subSections.get(index), codec, conf);
          if (index == 0) {
            // The first iNode section has a header which must be processed first
            INodeSection s = INodeSection.parseDelimitedFrom(is);
            expectedINodes.set(s.getNumInodes());
          }
          totalParsed.addAndGet(outputINodes(is, outStream));
          long timeTaken = Time.monotonicNow() - startTime;
          LOG.info("Time to output iNodes of section-{}: {} ms", index, timeTaken);
        } catch (Exception e) {
          exceptions.add(new IOException(e));
        } finally {
          latch.countDown();
          try {
            if (is != null) {
              is.close();
            }
          } catch (IOException ioe) {
            LOG.warn("Failed to close the input stream, ignoring", ioe);
          }
        }
      });
    }

    try {
      latch.await();
    } catch (InterruptedException e) {
      LOG.error("Interrupted waiting for countdown latch", e);
      throw new IOException(e);
    }

    executorService.shutdown();
    if (exceptions.size() != 0) {
      LOG.error("Failed to output INode sub-sections, {} exception(s) occurred.",
          exceptions.size());
      throw exceptions.get(0);
    }
    if (totalParsed.get() != expectedINodes.get()) {
      throw new IOException("Expected to parse " + expectedINodes + " in parallel, " +
          "but parsed " + totalParsed.get() + ". The image may be corrupt.");
    }
    LOG.info("Completed outputting all INode sub-sections to {} tmp files.",
        subSections.size());

    try (PrintStream ps = new PrintStream(parallelOutputFile, "UTF-8")) {
      ps.println(getHeader());
    }

    // merge tmp files
    long startTime = Time.monotonicNow();
    mergeFiles(paths, parallelOutputFile);
    long timeTaken = Time.monotonicNow() - startTime;
    LOG.info("Completed all stages. Time to merge files: {} ms", timeTaken);
  }

  protected PermissionStatus getPermission(long perm) {
    return FSImageFormatPBINode.Loader.loadPermission(perm, stringTable);
  }

  /** Load the directories in the INode section. */
  private void loadDirectories(
      FileInputStream fin, List<FileSummary.Section> sections,
      FileSummary summary, Configuration conf)
      throws IOException {
    LOG.info("Loading directories");
    long startTime = Time.monotonicNow();
    for (FileSummary.Section section : sections) {
      if (SectionName.fromString(section.getName())
          == SectionName.INODE) {
        fin.getChannel().position(section.getOffset());
        InputStream is = FSImageUtil.wrapInputStreamForCompression(conf,
            summary.getCodec(), new BufferedInputStream(new LimitInputStream(
                fin, section.getLength())));
        loadDirectoriesInINodeSection(is);
      }
    }
    long timeTaken = Time.monotonicNow() - startTime;
    LOG.info("Finished loading directories in {}ms", timeTaken);
  }

  private void loadINodeDirSection(
      FileInputStream fin, List<FileSummary.Section> sections,
      FileSummary summary, Configuration conf, List<Long> refIdList)
      throws IOException {
    LOG.info("Loading INode directory section.");
    long startTime = Time.monotonicNow();
    for (FileSummary.Section section : sections) {
      if (SectionName.fromString(section.getName())
          == SectionName.INODE_DIR) {
        fin.getChannel().position(section.getOffset());
        InputStream is = FSImageUtil.wrapInputStreamForCompression(conf,
            summary.getCodec(), new BufferedInputStream(
                new LimitInputStream(fin, section.getLength())));
        buildNamespace(is, refIdList);
      }
    }
    long timeTaken = Time.monotonicNow() - startTime;
    LOG.info("Finished loading INode directory section in {}ms", timeTaken);
  }

  /**
   * Checks the inode (saves if directory), and counts them. Can be overridden
   * if additional steps are taken when iterating through INodeSection.
   */
  protected void checkNode(INode p, AtomicInteger numDirs) throws IOException {
    if (p.hasDirectory()) {
      metadataMap.putDir(p);
      numDirs.incrementAndGet();
    }
  }

  /**
   * Load the filenames of the directories from the INode section.
   */
  private void loadDirectoriesInINodeSection(InputStream in)
      throws IOException {
    INodeSection s = INodeSection.parseDelimitedFrom(in);
    LOG.info("Loading directories in INode section.");
    AtomicInteger numDirs = new AtomicInteger(0);
    for (int i = 0; i < s.getNumInodes(); ++i) {
      INode p = INode.parseDelimitedFrom(in);
      if (LOG.isDebugEnabled() && i % 10000 == 0) {
        LOG.debug("Scanned {} inodes.", i);
      }
      checkNode(p, numDirs);
    }
    LOG.info("Found {} directories in INode section.", numDirs);
  }

  /**
   * Scan the INodeDirectory section to construct the namespace.
   */
  protected void buildNamespace(InputStream in, List<Long> refIdList)
      throws IOException {
    int count = 0;
    while (true) {
      FsImageProto.INodeDirectorySection.DirEntry e =
          FsImageProto.INodeDirectorySection.DirEntry.parseDelimitedFrom(in);
      if (e == null) {
        break;
      }
      count++;
      if (LOG.isDebugEnabled() && count % 10000 == 0) {
        LOG.debug("Scanned {} directories.", count);
      }
      long parentId = e.getParent();
      for (int i = 0; i < e.getChildrenCount(); i++) {
        long childId = e.getChildren(i);
        metadataMap.putDirChild(parentId, childId);
      }
      for (int i = e.getChildrenCount();
           i < e.getChildrenCount() + e.getRefChildrenCount(); i++) {
        int refId = e.getRefChildren(i - e.getChildrenCount());
        metadataMap.putDirChild(parentId, refIdList.get(refId));
      }
    }
    LOG.info("Scanned {} INode directories to build namespace.", count);
  }

  void printIfNotEmpty(PrintStream outStream, String line) {
    if (!line.isEmpty()) {
      outStream.println(line);
    }
  }

  private int outputINodes(InputStream in, PrintStream outStream)
      throws IOException {
    long ignored = 0;
    long ignoredSnapshots = 0;
    // As the input stream is a LimitInputStream, the reading will stop when
    // EOF is encountered at the end of the stream.
    int count = 0;
    while (true) {
      INode p = INode.parseDelimitedFrom(in);
      if (p == null) {
        break;
      }
      try {
        String parentPath = metadataMap.getParentPath(p.getId());
        printIfNotEmpty(outStream, getEntry(parentPath, p));
      } catch (IOException ioe) {
        ignored++;
        if (!(ioe instanceof IgnoreSnapshotException)) {
          LOG.warn("Exception caught, ignoring node:{}", p.getId(), ioe);
        } else {
          ignoredSnapshots++;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Exception caught, ignoring node:{}.", p.getId(), ioe);
          }
        }
      }
      count++;
      if (LOG.isDebugEnabled() && count % 100000 == 0) {
        LOG.debug("Outputted {} INodes.", count);
      }
    }
    if (ignored > 0) {
      LOG.warn("Ignored {} nodes, including {} in snapshots. Please turn on"
              + " debug log for details", ignored, ignoredSnapshots);
    }
    return count;
  }

  private static IgnoreSnapshotException createIgnoredSnapshotException(
      long inode) {
    // Ignore snapshots - we want the output similar to -ls -R.
    if (LOG.isDebugEnabled()) {
      LOG.debug("No snapshot name found for inode {}", inode);
    }
    return new IgnoreSnapshotException();
  }

  public int getStoragePolicy(
      INodeSection.XAttrFeatureProto xattrFeatureProto) {
    List<XAttr> xattrs =
        FSImageFormatPBINode.Loader.loadXAttrs(xattrFeatureProto, stringTable);
    for (XAttr xattr : xattrs) {
      if (BlockStoragePolicySuite.isStoragePolicyXAttr(xattr)) {
        return xattr.getValue()[0];
      }
    }
    return HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
  }

  private ArrayList<FileSummary.Section> getINodeSubSections(
      ArrayList<FileSummary.Section> sections) {
    ArrayList<FileSummary.Section> subSections = new ArrayList<>();
    Iterator<FileSummary.Section> iter = sections.iterator();
    while (iter.hasNext()) {
      FileSummary.Section s = iter.next();
      if (SectionName.fromString(s.getName()) == SectionName.INODE_SUB) {
        subSections.add(s);
      }
    }
    return subSections;
  }

  /**
   * Given a FSImage FileSummary.section, return a LimitInput stream set to
   * the starting position of the section and limited to the section length.
   * @param section The FileSummary.Section containing the offset and length
   * @param compressionCodec The compression codec in use, if any
   * @return An InputStream for the given section
   * @throws IOException
   */
  private InputStream getInputStreamForSection(FileSummary.Section section,
      String compressionCodec, Configuration conf)
      throws IOException {
    // channel of RandomAccessFile is not thread safe, use File
    FileInputStream fin = new FileInputStream(filename);
    try {
      FileChannel channel = fin.getChannel();
      channel.position(section.getOffset());
      InputStream in = new BufferedInputStream(new LimitInputStream(fin,
          section.getLength()));

      in = FSImageUtil.wrapInputStreamForCompression(conf,
          compressionCodec, in);
      return in;
    } catch (IOException e) {
      fin.close();
      throw e;
    }
  }

  /**
   * @param srcPaths Source files of contents to be merged
   * @param resultPath Merged file path
   * @throws IOException
   */
  public static void mergeFiles(String[] srcPaths, String resultPath)
      throws IOException {
    if (srcPaths == null || srcPaths.length < 1) {
      LOG.warn("no source files to merge.");
      return;
    }

    File[] files = new File[srcPaths.length];
    for (int i = 0; i < srcPaths.length; i++) {
      files[i] = new File(srcPaths[i]);
    }

    File resultFile = new File(resultPath);
    try (FileChannel resultChannel =
             new FileOutputStream(resultFile, true).getChannel()) {
      for (File file : files) {
        try (FileChannel src = new FileInputStream(file).getChannel()) {
          resultChannel.transferFrom(src, resultChannel.size(), src.size());
        }
      }
    }

    for (File file : files) {
      if (!file.delete() && file.exists()) {
        LOG.warn("delete tmp file: {} returned false", file);
      }
    }
  }

  public String getErasureCodingPolicyName
      (INodeSection.XAttrFeatureProto xattrFeatureProto) {
    List<XAttr> xattrs =
        FSImageFormatPBINode.Loader.loadXAttrs(xattrFeatureProto, stringTable);
    for (XAttr xattr : xattrs) {
      if (xattr.equalsIgnoreValue(ecXAttr)){
        try{
          ByteArrayInputStream bIn = new ByteArrayInputStream(xattr.getValue());
          DataInputStream dIn = new DataInputStream(bIn);
          return WritableUtils.readString(dIn);
        } catch (IOException ioException){
          return null;
        }
      }
    }
    return null;
  }

}
