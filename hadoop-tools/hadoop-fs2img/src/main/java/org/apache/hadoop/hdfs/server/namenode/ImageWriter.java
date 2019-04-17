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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Charsets;
import com.google.protobuf.CodedOutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.SectionName;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.CacheManagerSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FilesUnderConstructionSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeDirectorySection.DirEntry;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INode;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.NameSystemSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SecretManagerSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SnapshotDiffSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.StringTableSection;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressorStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import static org.apache.hadoop.hdfs.server.namenode.FSImageUtil.MAGIC_HEADER;

/**
 * Utility crawling an existing hierarchical FileSystem and emitting
 * a valid FSImage/NN storage.
 */
// TODO: generalize to types beyond FileRegion
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ImageWriter implements Closeable {

  private static final int ONDISK_VERSION = 1;
  private static final int LAYOUT_VERSION =
      NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION;

  private final Path outdir;
  private final FileSystem outfs;
  private final File dirsTmp;
  private final OutputStream dirs;
  private final File inodesTmp;
  private final OutputStream inodes;
  private final MessageDigest digest;
  private final FSImageCompression compress;
  private final long startBlock;
  private final long startInode;
  private final UGIResolver ugis;
  private final BlockAliasMap.Writer<FileRegion> blocks;
  private final BlockResolver blockIds;
  private final Map<Long, DirEntry.Builder> dircache;
  private final TrackedOutputStream<DigestOutputStream> raw;

  private boolean closed = false;
  private long curSec;
  private long curBlock;
  private final AtomicLong curInode;
  private final FileSummary.Builder summary = FileSummary.newBuilder()
      .setOndiskVersion(ONDISK_VERSION)
      .setLayoutVersion(LAYOUT_VERSION);

  private final String blockPoolID;

  public static Options defaults() {
    return new Options();
  }

  @SuppressWarnings("unchecked")
  public ImageWriter(Options opts) throws IOException {
    final OutputStream out;
    if (null == opts.outStream) {
      FileSystem fs = opts.outdir.getFileSystem(opts.getConf());
      outfs = (fs instanceof LocalFileSystem)
          ? ((LocalFileSystem)fs).getRaw()
          : fs;
      Path tmp = opts.outdir;
      if (!outfs.mkdirs(tmp)) {
        throw new IOException("Failed to create output dir: " + tmp);
      }
      try (NNStorage stor = new NNStorage(opts.getConf(),
          Arrays.asList(tmp.toUri()), Arrays.asList(tmp.toUri()))) {
        NamespaceInfo info = NNStorage.newNamespaceInfo();
        if (info.getLayoutVersion() != LAYOUT_VERSION) {
          throw new IllegalStateException("Incompatible layout " +
              info.getLayoutVersion() + " (expected " + LAYOUT_VERSION + ")");
        }
        // set the cluster id, if given
        if (opts.clusterID.length() > 0) {
          info.setClusterID(opts.clusterID);
        }
        // if block pool id is given
        if (opts.blockPoolID.length() > 0) {
          info.setBlockPoolID(opts.blockPoolID);
        }

        stor.format(info);
        blockPoolID = info.getBlockPoolID();
      }
      outdir = new Path(tmp, "current");
      out = outfs.create(new Path(outdir, "fsimage_0000000000000000000"));
    } else {
      outdir = null;
      outfs = null;
      out = opts.outStream;
      blockPoolID = "";
    }
    digest = MD5Hash.getDigester();
    raw = new TrackedOutputStream<>(new DigestOutputStream(
            new BufferedOutputStream(out), digest));
    compress = opts.compress;
    CompressionCodec codec = compress.getImageCodec();
    if (codec != null) {
      summary.setCodec(codec.getClass().getCanonicalName());
    }
    startBlock = opts.startBlock;
    curBlock = startBlock;
    startInode = opts.startInode;
    curInode = new AtomicLong(startInode);
    dircache = Collections.synchronizedMap(new DirEntryCache(opts.maxdircache));

    ugis = null == opts.ugis
        ? ReflectionUtils.newInstance(opts.ugisClass, opts.getConf())
        : opts.ugis;
    BlockAliasMap<FileRegion> fmt = null == opts.blocks
        ? ReflectionUtils.newInstance(opts.aliasMap, opts.getConf())
        : opts.blocks;
    blocks = fmt.getWriter(null, blockPoolID);
    blockIds = null == opts.blockIds
        ? ReflectionUtils.newInstance(opts.blockIdsClass, opts.getConf())
        : opts.blockIds;

    // create directory and inode sections as side-files.
    // The details are written to files to avoid keeping them in memory.
    FileOutputStream dirsTmpStream = null;
    try {
      dirsTmp = File.createTempFile("fsimg_dir", null);
      dirsTmp.deleteOnExit();
      dirsTmpStream = new FileOutputStream(dirsTmp);
      dirs = beginSection(dirsTmpStream);
    } catch (IOException e) {
      IOUtils.cleanupWithLogger(null, raw, dirsTmpStream);
      throw e;
    }

    try {
      inodesTmp = File.createTempFile("fsimg_inode", null);
      inodesTmp.deleteOnExit();
      inodes = new FileOutputStream(inodesTmp);
    } catch (IOException e) {
      IOUtils.cleanupWithLogger(null, raw, dirsTmpStream, dirs);
      throw e;
    }

    raw.write(MAGIC_HEADER);
    curSec = raw.pos;
    assert raw.pos == MAGIC_HEADER.length;
  }

  public void accept(TreePath e) throws IOException {
    assert e.getParentId() < curInode.get();
    // allocate ID
    long id = curInode.getAndIncrement();
    e.accept(id);
    assert e.getId() < curInode.get();
    INode n = e.toINode(ugis, blockIds, blocks);
    writeInode(n);

    if (e.getParentId() > 0) {
      // add DirEntry to map, which may page out entries
      DirEntry.Builder de = DirEntry.newBuilder()
          .setParent(e.getParentId())
          .addChildren(e.getId());
      dircache.put(e.getParentId(), de);
    }
  }

  @SuppressWarnings("serial")
  class DirEntryCache extends LinkedHashMap<Long, DirEntry.Builder> {

    // should cache path to root, not evict LRCached
    private final int nEntries;

    DirEntryCache(int nEntries) {
      this.nEntries = nEntries;
    }

    @Override
    public DirEntry.Builder put(Long p, DirEntry.Builder b) {
      DirEntry.Builder e = get(p);
      if (null == e) {
        return super.put(p, b);
      }
      // merge
      e.addAllChildren(b.getChildrenList());
      // not strictly conforming
      return e;
    }

    @Override
    protected boolean removeEldestEntry(Entry<Long, DirEntry.Builder> be) {
      if (size() > nEntries) {
        DirEntry d = be.getValue().build();
        try {
          writeDirEntry(d);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        return true;
      }
      return false;
    }
  }

  synchronized void writeInode(INode n) throws IOException {
    n.writeDelimitedTo(inodes);
  }

  synchronized void writeDirEntry(DirEntry e) throws IOException {
    e.writeDelimitedTo(dirs);
  }

  private static int getOndiskSize(com.google.protobuf.GeneratedMessage s) {
    return CodedOutputStream.computeRawVarint32Size(s.getSerializedSize())
        + s.getSerializedSize();
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    for (DirEntry.Builder b : dircache.values()) {
      DirEntry e = b.build();
      writeDirEntry(e);
    }
    dircache.clear();

    // close side files
    IOUtils.cleanupWithLogger(null, dirs, inodes, blocks);
    if (null == dirs || null == inodes) {
      // init failed
      if (raw != null) {
        raw.close();
      }
      return;
    }
    try {
      writeNameSystemSection();
      writeINodeSection();
      writeDirSection();
      writeStringTableSection();

      // write summary directly to raw
      FileSummary s = summary.build();
      s.writeDelimitedTo(raw);
      int length = getOndiskSize(s);
      byte[] lengthBytes = new byte[4];
      ByteBuffer.wrap(lengthBytes).asIntBuffer().put(length);
      raw.write(lengthBytes);
    } finally {
      raw.close();
    }
    writeMD5("fsimage_0000000000000000000");
    closed = true;
  }

  /**
   * Write checksum for image file. Pulled from MD5Utils/internals. Awkward to
   * reuse existing tools/utils.
   */
  void writeMD5(String imagename) throws IOException {
    if (null == outdir) {
      return;
    }
    MD5Hash md5 = new MD5Hash(digest.digest());
    String digestString = StringUtils.byteToHexString(md5.getDigest());
    Path chk = new Path(outdir, imagename + ".md5");
    try (OutputStream out = outfs.create(chk)) {
      String md5Line = digestString + " *" + imagename + "\n";
      out.write(md5Line.getBytes(Charsets.UTF_8));
    }
  }

  OutputStream beginSection(OutputStream out) throws IOException {
    CompressionCodec codec = compress.getImageCodec();
    if (null == codec) {
      return out;
    }
    return codec.createOutputStream(out);
  }

  void endSection(OutputStream out, SectionName name) throws IOException {
    CompressionCodec codec = compress.getImageCodec();
    if (codec != null) {
      ((CompressorStream)out).finish();
    }
    out.flush();
    long length = raw.pos - curSec;
    summary.addSections(FileSummary.Section.newBuilder()
        .setName(name.toString()) // not strictly correct, but name not visible
        .setOffset(curSec).setLength(length));
    curSec += length;
  }

  void writeNameSystemSection() throws IOException {
    NameSystemSection.Builder b = NameSystemSection.newBuilder()
        .setGenstampV1(1000)
        .setGenstampV1Limit(0)
        .setGenstampV2(1001)
        .setLastAllocatedBlockId(blockIds.lastId())
        .setTransactionId(0);
    NameSystemSection s = b.build();

    OutputStream sec = beginSection(raw);
    s.writeDelimitedTo(sec);
    endSection(sec, SectionName.NS_INFO);
  }

  void writeINodeSection() throws IOException {
    // could reset dict to avoid compression cost in close
    INodeSection.Builder b = INodeSection.newBuilder()
        .setNumInodes(curInode.get() - startInode)
        .setLastInodeId(curInode.get());
    INodeSection s = b.build();

    OutputStream sec = beginSection(raw);
    s.writeDelimitedTo(sec);
    // copy inodes
    try (FileInputStream in = new FileInputStream(inodesTmp)) {
      IOUtils.copyBytes(in, sec, 4096, false);
    }
    endSection(sec, SectionName.INODE);
  }

  void writeDirSection() throws IOException {
    // No header, so dirs can be written/compressed independently
    OutputStream sec = raw;
    // copy dirs
    try (FileInputStream in = new FileInputStream(dirsTmp)) {
      IOUtils.copyBytes(in, sec, 4096, false);
    }
    endSection(sec, SectionName.INODE_DIR);
  }

  void writeFilesUCSection() throws IOException {
    FilesUnderConstructionSection.Builder b =
        FilesUnderConstructionSection.newBuilder();
    FilesUnderConstructionSection s = b.build();

    OutputStream sec = beginSection(raw);
    s.writeDelimitedTo(sec);
    endSection(sec, SectionName.FILES_UNDERCONSTRUCTION);
  }

  void writeSnapshotDiffSection() throws IOException {
    SnapshotDiffSection.Builder b = SnapshotDiffSection.newBuilder();
    SnapshotDiffSection s = b.build();

    OutputStream sec = beginSection(raw);
    s.writeDelimitedTo(sec);
    endSection(sec, SectionName.SNAPSHOT_DIFF);
  }

  void writeSecretManagerSection() throws IOException {
    SecretManagerSection.Builder b = SecretManagerSection.newBuilder()
        .setCurrentId(0)
        .setTokenSequenceNumber(0);
    SecretManagerSection s = b.build();

    OutputStream sec = beginSection(raw);
    s.writeDelimitedTo(sec);
    endSection(sec, SectionName.SECRET_MANAGER);
  }

  void writeCacheManagerSection() throws IOException {
    CacheManagerSection.Builder b = CacheManagerSection.newBuilder()
        .setNumPools(0)
        .setNumDirectives(0)
        .setNextDirectiveId(1);
    CacheManagerSection s = b.build();

    OutputStream sec = beginSection(raw);
    s.writeDelimitedTo(sec);
    endSection(sec, SectionName.CACHE_MANAGER);
  }

  void writeStringTableSection() throws IOException {
    StringTableSection.Builder b = StringTableSection.newBuilder();
    Map<Integer, String> u = ugis.ugiMap();
    b.setNumEntry(u.size());
    StringTableSection s = b.build();

    OutputStream sec = beginSection(raw);
    s.writeDelimitedTo(sec);
    for (Map.Entry<Integer, String> e : u.entrySet()) {
      StringTableSection.Entry.Builder x =
          StringTableSection.Entry.newBuilder()
              .setId(e.getKey())
              .setStr(e.getValue());
      x.build().writeDelimitedTo(sec);
    }
    endSection(sec, SectionName.STRING_TABLE);
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{ codec=\"").append(compress.getImageCodec());
    sb.append("\", startBlock=").append(startBlock);
    sb.append(", curBlock=").append(curBlock);
    sb.append(", startInode=").append(startInode);
    sb.append(", curInode=").append(curInode);
    sb.append(", ugi=").append(ugis);
    sb.append(", blockIds=").append(blockIds);
    sb.append(", offset=").append(raw.pos);
    sb.append(" }");
    return sb.toString();
  }

  static class TrackedOutputStream<T extends OutputStream>
      extends FilterOutputStream {

    private long pos = 0L;

    TrackedOutputStream(T out) {
      super(out);
    }

    @SuppressWarnings("unchecked")
    public T getInner() {
      return (T) out;
    }

    @Override
    public void write(int b) throws IOException {
      out.write(b);
      ++pos;
    }

    @Override
    public void write(byte[] b) throws IOException {
      write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
      pos += len;
    }

    @Override
    public void flush() throws IOException {
      super.flush();
    }

    @Override
    public void close() throws IOException {
      super.close();
    }

  }

  /**
   * Configurable options for image generation mapping pluggable components.
   */
  public static class Options implements Configurable {

    public static final String START_INODE = "hdfs.image.writer.start.inode";
    public static final String CACHE_ENTRY = "hdfs.image.writer.cache.entries";
    public static final String UGI_CLASS   = "hdfs.image.writer.ugi.class";
    public static final String BLOCK_RESOLVER_CLASS =
        "hdfs.image.writer.blockresolver.class";

    private Path outdir;
    private Configuration conf;
    private OutputStream outStream;
    private int maxdircache;
    private long startBlock;
    private long startInode;
    private UGIResolver ugis;
    private Class<? extends UGIResolver> ugisClass;
    private BlockAliasMap<FileRegion> blocks;
    private String clusterID;
    private String blockPoolID;

    @SuppressWarnings("rawtypes")
    private Class<? extends BlockAliasMap> aliasMap;
    private BlockResolver blockIds;
    private Class<? extends BlockResolver> blockIdsClass;
    private FSImageCompression compress =
        FSImageCompression.createNoopCompression();

    protected Options() {
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      String def = new File("hdfs/name").toURI().toString();
      outdir = new Path(conf.get(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, def));
      startBlock = conf.getLong(FixedBlockResolver.START_BLOCK, (1L << 30) + 1);
      startInode = conf.getLong(START_INODE, (1L << 14) + 1);
      maxdircache = conf.getInt(CACHE_ENTRY, 100);
      ugisClass = conf.getClass(UGI_CLASS,
          SingleUGIResolver.class, UGIResolver.class);
      aliasMap = conf.getClass(
          DFSConfigKeys.DFS_PROVIDED_ALIASMAP_CLASS,
          NullBlockAliasMap.class, BlockAliasMap.class);
      blockIdsClass = conf.getClass(BLOCK_RESOLVER_CLASS,
          FixedBlockResolver.class, BlockResolver.class);
      clusterID = "";
      blockPoolID = "";
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    public Options output(String out) {
      this.outdir = new Path(out);
      return this;
    }

    public Options outStream(OutputStream outStream) {
      this.outStream = outStream;
      return this;
    }

    public Options codec(String codec) throws IOException {
      this.compress = FSImageCompression.createCompression(getConf(), codec);
      return this;
    }

    public Options cache(int nDirEntries) {
      this.maxdircache = nDirEntries;
      return this;
    }

    public Options ugi(UGIResolver ugis) {
      this.ugis = ugis;
      return this;
    }

    public Options ugi(Class<? extends UGIResolver> ugisClass) {
      this.ugisClass = ugisClass;
      return this;
    }

    public Options blockIds(BlockResolver blockIds) {
      this.blockIds = blockIds;
      return this;
    }

    public Options blockIds(Class<? extends BlockResolver> blockIdsClass) {
      this.blockIdsClass = blockIdsClass;
      return this;
    }

    public Options blocks(BlockAliasMap<FileRegion> blocks) {
      this.blocks = blocks;
      return this;
    }

    @SuppressWarnings("rawtypes")
    public Options blocks(Class<? extends BlockAliasMap> blocksClass) {
      this.aliasMap = blocksClass;
      return this;
    }

    public Options clusterID(String clusterID) {
      this.clusterID = clusterID;
      return this;
    }

    public Options blockPoolID(String blockPoolID) {
      this.blockPoolID = blockPoolID;
      return this;
    }
  }

}
