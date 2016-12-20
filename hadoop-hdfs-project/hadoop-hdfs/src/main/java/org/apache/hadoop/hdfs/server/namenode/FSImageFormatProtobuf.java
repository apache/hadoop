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

import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheDirectiveInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CachePoolInfoProto;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockIdManager;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.CacheManagerSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.NameSystemSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SecretManagerSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.StringTableSection;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FSImageFormatPBSnapshot;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.LimitInputStream;
import org.apache.hadoop.util.Time;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.CodedOutputStream;

/**
 * Utility class to read / write fsimage in protobuf format.
 */
@InterfaceAudience.Private
public final class FSImageFormatProtobuf {
  private static final Logger LOG = LoggerFactory
      .getLogger(FSImageFormatProtobuf.class);

  public static final class LoaderContext {
    private String[] stringTable;
    private final ArrayList<INodeReference> refList = Lists.newArrayList();

    public String[] getStringTable() {
      return stringTable;
    }

    public ArrayList<INodeReference> getRefList() {
      return refList;
    }
  }

  public static final class SaverContext {
    public static class DeduplicationMap<E> {
      private final Map<E, Integer> map = Maps.newHashMap();
      private DeduplicationMap() {}

      static <T> DeduplicationMap<T> newMap() {
        return new DeduplicationMap<T>();
      }

      int getId(E value) {
        if (value == null) {
          return 0;
        }
        Integer v = map.get(value);
        if (v == null) {
          int nv = map.size() + 1;
          map.put(value, nv);
          return nv;
        }
        return v;
      }

      int size() {
        return map.size();
      }

      Set<Entry<E, Integer>> entrySet() {
        return map.entrySet();
      }
    }
    private final ArrayList<INodeReference> refList = Lists.newArrayList();

    private final DeduplicationMap<String> stringMap = DeduplicationMap
        .newMap();

    public DeduplicationMap<String> getStringMap() {
      return stringMap;
    }

    public ArrayList<INodeReference> getRefList() {
      return refList;
    }
  }

  public static final class Loader implements FSImageFormat.AbstractLoader {
    static final int MINIMUM_FILE_LENGTH = 8;
    private final Configuration conf;
    private final FSNamesystem fsn;
    private final LoaderContext ctx;
    /** The MD5 sum of the loaded file */
    private MD5Hash imgDigest;
    /** The transaction ID of the last edit represented by the loaded file */
    private long imgTxId;
    /**
     * Whether the image's layout version must be the same with
     * {@link HdfsConstants#NAMENODE_LAYOUT_VERSION}. This is only set to true
     * when we're doing (rollingUpgrade rollback).
     */
    private final boolean requireSameLayoutVersion;

    Loader(Configuration conf, FSNamesystem fsn,
        boolean requireSameLayoutVersion) {
      this.conf = conf;
      this.fsn = fsn;
      this.ctx = new LoaderContext();
      this.requireSameLayoutVersion = requireSameLayoutVersion;
    }

    @Override
    public MD5Hash getLoadedImageMd5() {
      return imgDigest;
    }

    @Override
    public long getLoadedImageTxId() {
      return imgTxId;
    }

    public LoaderContext getLoaderContext() {
      return ctx;
    }

    void load(File file) throws IOException {
      long start = Time.monotonicNow();
      imgDigest = MD5FileUtils.computeMd5ForFile(file);
      RandomAccessFile raFile = new RandomAccessFile(file, "r");
      FileInputStream fin = new FileInputStream(file);
      try {
        loadInternal(raFile, fin);
        long end = Time.monotonicNow();
        LOG.info("Loaded FSImage in {} seconds.", (end - start) / 1000);
      } finally {
        fin.close();
        raFile.close();
      }
    }

    private void loadInternal(RandomAccessFile raFile, FileInputStream fin)
        throws IOException {
      if (!FSImageUtil.checkFileFormat(raFile)) {
        throw new IOException("Unrecognized file format");
      }
      FileSummary summary = FSImageUtil.loadSummary(raFile);
      if (requireSameLayoutVersion && summary.getLayoutVersion() !=
          HdfsConstants.NAMENODE_LAYOUT_VERSION) {
        throw new IOException("Image version " + summary.getLayoutVersion() +
            " is not equal to the software version " +
            HdfsConstants.NAMENODE_LAYOUT_VERSION);
      }

      FileChannel channel = fin.getChannel();

      FSImageFormatPBINode.Loader inodeLoader = new FSImageFormatPBINode.Loader(
          fsn, this);
      FSImageFormatPBSnapshot.Loader snapshotLoader = new FSImageFormatPBSnapshot.Loader(
          fsn, this);

      ArrayList<FileSummary.Section> sections = Lists.newArrayList(summary
          .getSectionsList());
      Collections.sort(sections, new Comparator<FileSummary.Section>() {
        @Override
        public int compare(FileSummary.Section s1, FileSummary.Section s2) {
          SectionName n1 = SectionName.fromString(s1.getName());
          SectionName n2 = SectionName.fromString(s2.getName());
          if (n1 == null) {
            return n2 == null ? 0 : -1;
          } else if (n2 == null) {
            return -1;
          } else {
            return n1.ordinal() - n2.ordinal();
          }
        }
      });

      StartupProgress prog = NameNode.getStartupProgress();
      /**
       * beginStep() and the endStep() calls do not match the boundary of the
       * sections. This is because that the current implementation only allows
       * a particular step to be started for once.
       */
      Step currentStep = null;

      for (FileSummary.Section s : sections) {
        channel.position(s.getOffset());
        InputStream in = new BufferedInputStream(new LimitInputStream(fin,
            s.getLength()));

        in = FSImageUtil.wrapInputStreamForCompression(conf,
            summary.getCodec(), in);

        String n = s.getName();

        switch (SectionName.fromString(n)) {
        case NS_INFO:
          loadNameSystemSection(in);
          break;
        case STRING_TABLE:
          loadStringTableSection(in);
          break;
        case INODE: {
          currentStep = new Step(StepType.INODES);
          prog.beginStep(Phase.LOADING_FSIMAGE, currentStep);
          inodeLoader.loadINodeSection(in);
        }
          break;
        case INODE_REFERENCE:
          snapshotLoader.loadINodeReferenceSection(in);
          break;
        case INODE_DIR:
          inodeLoader.loadINodeDirectorySection(in);
          break;
        case FILES_UNDERCONSTRUCTION:
          inodeLoader.loadFilesUnderConstructionSection(in);
          break;
        case SNAPSHOT:
          snapshotLoader.loadSnapshotSection(in);
          break;
        case SNAPSHOT_DIFF:
          snapshotLoader.loadSnapshotDiffSection(in);
          break;
        case SECRET_MANAGER: {
          prog.endStep(Phase.LOADING_FSIMAGE, currentStep);
          Step step = new Step(StepType.DELEGATION_TOKENS);
          prog.beginStep(Phase.LOADING_FSIMAGE, step);
          loadSecretManagerSection(in);
          prog.endStep(Phase.LOADING_FSIMAGE, step);
        }
          break;
        case CACHE_MANAGER: {
          Step step = new Step(StepType.CACHE_POOLS);
          prog.beginStep(Phase.LOADING_FSIMAGE, step);
          loadCacheManagerSection(in);
          prog.endStep(Phase.LOADING_FSIMAGE, step);
        }
          break;
        default:
          LOG.warn("Unrecognized section {}", n);
          break;
        }
      }
    }

    private void loadNameSystemSection(InputStream in) throws IOException {
      NameSystemSection s = NameSystemSection.parseDelimitedFrom(in);
      BlockIdManager blockIdManager = fsn.getBlockIdManager();
      blockIdManager.setGenerationStampV1(s.getGenstampV1());
      blockIdManager.setGenerationStampV2(s.getGenstampV2());
      blockIdManager.setGenerationStampV1Limit(s.getGenstampV1Limit());
      blockIdManager.setLastAllocatedBlockId(s.getLastAllocatedBlockId());
      imgTxId = s.getTransactionId();
      if (s.hasRollingUpgradeStartTime()
          && fsn.getFSImage().hasRollbackFSImage()) {
        // we set the rollingUpgradeInfo only when we make sure we have the
        // rollback image
        fsn.setRollingUpgradeInfo(true, s.getRollingUpgradeStartTime());
      }
    }

    private void loadStringTableSection(InputStream in) throws IOException {
      StringTableSection s = StringTableSection.parseDelimitedFrom(in);
      ctx.stringTable = new String[s.getNumEntry() + 1];
      for (int i = 0; i < s.getNumEntry(); ++i) {
        StringTableSection.Entry e = StringTableSection.Entry
            .parseDelimitedFrom(in);
        ctx.stringTable[e.getId()] = e.getStr();
      }
    }

    private void loadSecretManagerSection(InputStream in) throws IOException {
      SecretManagerSection s = SecretManagerSection.parseDelimitedFrom(in);
      int numKeys = s.getNumKeys(), numTokens = s.getNumTokens();
      ArrayList<SecretManagerSection.DelegationKey> keys = Lists
          .newArrayListWithCapacity(numKeys);
      ArrayList<SecretManagerSection.PersistToken> tokens = Lists
          .newArrayListWithCapacity(numTokens);

      for (int i = 0; i < numKeys; ++i)
        keys.add(SecretManagerSection.DelegationKey.parseDelimitedFrom(in));

      for (int i = 0; i < numTokens; ++i)
        tokens.add(SecretManagerSection.PersistToken.parseDelimitedFrom(in));

      fsn.loadSecretManagerState(s, keys, tokens);
    }

    private void loadCacheManagerSection(InputStream in) throws IOException {
      CacheManagerSection s = CacheManagerSection.parseDelimitedFrom(in);
      ArrayList<CachePoolInfoProto> pools = Lists.newArrayListWithCapacity(s
          .getNumPools());
      ArrayList<CacheDirectiveInfoProto> directives = Lists
          .newArrayListWithCapacity(s.getNumDirectives());
      for (int i = 0; i < s.getNumPools(); ++i)
        pools.add(CachePoolInfoProto.parseDelimitedFrom(in));
      for (int i = 0; i < s.getNumDirectives(); ++i)
        directives.add(CacheDirectiveInfoProto.parseDelimitedFrom(in));
      fsn.getCacheManager().loadState(
          new CacheManager.PersistState(s, pools, directives));
    }

  }

  public static final class Saver {
    public static final int CHECK_CANCEL_INTERVAL = 4096;

    private final SaveNamespaceContext context;
    private final SaverContext saverContext;
    private long currentOffset = FSImageUtil.MAGIC_HEADER.length;
    private MD5Hash savedDigest;

    private FileChannel fileChannel;
    // OutputStream for the section data
    private OutputStream sectionOutputStream;
    private CompressionCodec codec;
    private OutputStream underlyingOutputStream;

    Saver(SaveNamespaceContext context) {
      this.context = context;
      this.saverContext = new SaverContext();
    }

    public MD5Hash getSavedDigest() {
      return savedDigest;
    }

    public SaveNamespaceContext getContext() {
      return context;
    }

    public SaverContext getSaverContext() {
      return saverContext;
    }

    public void commitSection(FileSummary.Builder summary, SectionName name)
        throws IOException {
      long oldOffset = currentOffset;
      flushSectionOutputStream();

      if (codec != null) {
        sectionOutputStream = codec.createOutputStream(underlyingOutputStream);
      } else {
        sectionOutputStream = underlyingOutputStream;
      }
      long length = fileChannel.position() - oldOffset;
      summary.addSections(FileSummary.Section.newBuilder().setName(name.name)
          .setLength(length).setOffset(currentOffset));
      currentOffset += length;
    }

    private void flushSectionOutputStream() throws IOException {
      if (codec != null) {
        ((CompressionOutputStream) sectionOutputStream).finish();
      }
      sectionOutputStream.flush();
    }

    void save(File file, FSImageCompression compression) throws IOException {
      FileOutputStream fout = new FileOutputStream(file);
      fileChannel = fout.getChannel();
      try {
        LOG.info("Saving image file {} using {}", file, compression);
        long startTime = monotonicNow();
        saveInternal(fout, compression, file.getAbsolutePath());
        LOG.info("Image file {} of size {} bytes saved in {} seconds.", file,
            file.length(), (monotonicNow() - startTime) / 1000);
      } finally {
        fout.close();
      }
    }

    private static void saveFileSummary(OutputStream out, FileSummary summary)
        throws IOException {
      summary.writeDelimitedTo(out);
      int length = getOndiskTrunkSize(summary);
      byte[] lengthBytes = new byte[4];
      ByteBuffer.wrap(lengthBytes).asIntBuffer().put(length);
      out.write(lengthBytes);
    }

    private void saveInodes(FileSummary.Builder summary) throws IOException {
      FSImageFormatPBINode.Saver saver = new FSImageFormatPBINode.Saver(this,
          summary);

      saver.serializeINodeSection(sectionOutputStream);
      saver.serializeINodeDirectorySection(sectionOutputStream);
      saver.serializeFilesUCSection(sectionOutputStream);
    }

    private void saveSnapshots(FileSummary.Builder summary) throws IOException {
      FSImageFormatPBSnapshot.Saver snapshotSaver = new FSImageFormatPBSnapshot.Saver(
          this, summary, context, context.getSourceNamesystem());

      snapshotSaver.serializeSnapshotSection(sectionOutputStream);
      // Skip snapshot-related sections when there is no snapshot.
      if (context.getSourceNamesystem().getSnapshotManager()
          .getNumSnapshots() > 0) {
        snapshotSaver.serializeSnapshotDiffSection(sectionOutputStream);
      }
      snapshotSaver.serializeINodeReferenceSection(sectionOutputStream);
    }

    private void saveInternal(FileOutputStream fout,
        FSImageCompression compression, String filePath) throws IOException {
      StartupProgress prog = NameNode.getStartupProgress();
      MessageDigest digester = MD5Hash.getDigester();

      underlyingOutputStream = new DigestOutputStream(new BufferedOutputStream(
          fout), digester);
      underlyingOutputStream.write(FSImageUtil.MAGIC_HEADER);

      fileChannel = fout.getChannel();

      FileSummary.Builder b = FileSummary.newBuilder()
          .setOndiskVersion(FSImageUtil.FILE_VERSION)
          .setLayoutVersion(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);

      codec = compression.getImageCodec();
      if (codec != null) {
        b.setCodec(codec.getClass().getCanonicalName());
        sectionOutputStream = codec.createOutputStream(underlyingOutputStream);
      } else {
        sectionOutputStream = underlyingOutputStream;
      }

      saveNameSystemSection(b);
      // Check for cancellation right after serializing the name system section.
      // Some unit tests, such as TestSaveNamespace#testCancelSaveNameSpace
      // depends on this behavior.
      context.checkCancelled();

      Step step = new Step(StepType.INODES, filePath);
      prog.beginStep(Phase.SAVING_CHECKPOINT, step);
      saveInodes(b);
      saveSnapshots(b);
      prog.endStep(Phase.SAVING_CHECKPOINT, step);

      step = new Step(StepType.DELEGATION_TOKENS, filePath);
      prog.beginStep(Phase.SAVING_CHECKPOINT, step);
      saveSecretManagerSection(b);
      prog.endStep(Phase.SAVING_CHECKPOINT, step);

      step = new Step(StepType.CACHE_POOLS, filePath);
      prog.beginStep(Phase.SAVING_CHECKPOINT, step);
      saveCacheManagerSection(b);
      prog.endStep(Phase.SAVING_CHECKPOINT, step);

      saveStringTableSection(b);

      // We use the underlyingOutputStream to write the header. Therefore flush
      // the buffered stream (which is potentially compressed) first.
      flushSectionOutputStream();

      FileSummary summary = b.build();
      saveFileSummary(underlyingOutputStream, summary);
      underlyingOutputStream.close();
      savedDigest = new MD5Hash(digester.digest());
    }

    private void saveSecretManagerSection(FileSummary.Builder summary)
        throws IOException {
      final FSNamesystem fsn = context.getSourceNamesystem();
      DelegationTokenSecretManager.SecretManagerState state = fsn
          .saveSecretManagerState();
      state.section.writeDelimitedTo(sectionOutputStream);
      for (SecretManagerSection.DelegationKey k : state.keys)
        k.writeDelimitedTo(sectionOutputStream);

      for (SecretManagerSection.PersistToken t : state.tokens)
        t.writeDelimitedTo(sectionOutputStream);

      commitSection(summary, SectionName.SECRET_MANAGER);
    }

    private void saveCacheManagerSection(FileSummary.Builder summary)
        throws IOException {
      final FSNamesystem fsn = context.getSourceNamesystem();
      CacheManager.PersistState state = fsn.getCacheManager().saveState();
      state.section.writeDelimitedTo(sectionOutputStream);

      for (CachePoolInfoProto p : state.pools)
        p.writeDelimitedTo(sectionOutputStream);

      for (CacheDirectiveInfoProto p : state.directives)
        p.writeDelimitedTo(sectionOutputStream);

      commitSection(summary, SectionName.CACHE_MANAGER);
    }

    private void saveNameSystemSection(FileSummary.Builder summary)
        throws IOException {
      final FSNamesystem fsn = context.getSourceNamesystem();
      OutputStream out = sectionOutputStream;
      BlockIdManager blockIdManager = fsn.getBlockIdManager();
      NameSystemSection.Builder b = NameSystemSection.newBuilder()
          .setGenstampV1(blockIdManager.getGenerationStampV1())
          .setGenstampV1Limit(blockIdManager.getGenerationStampV1Limit())
          .setGenstampV2(blockIdManager.getGenerationStampV2())
          .setLastAllocatedBlockId(blockIdManager.getLastAllocatedBlockId())
          .setTransactionId(context.getTxId());

      // We use the non-locked version of getNamespaceInfo here since
      // the coordinating thread of saveNamespace already has read-locked
      // the namespace for us. If we attempt to take another readlock
      // from the actual saver thread, there's a potential of a
      // fairness-related deadlock. See the comments on HDFS-2223.
      b.setNamespaceId(fsn.unprotectedGetNamespaceInfo().getNamespaceID());
      if (fsn.isRollingUpgrade()) {
        b.setRollingUpgradeStartTime(fsn.getRollingUpgradeInfo().getStartTime());
      }
      NameSystemSection s = b.build();
      s.writeDelimitedTo(out);

      commitSection(summary, SectionName.NS_INFO);
    }

    private void saveStringTableSection(FileSummary.Builder summary)
        throws IOException {
      OutputStream out = sectionOutputStream;
      StringTableSection.Builder b = StringTableSection.newBuilder()
          .setNumEntry(saverContext.stringMap.size());
      b.build().writeDelimitedTo(out);
      for (Entry<String, Integer> e : saverContext.stringMap.entrySet()) {
        StringTableSection.Entry.Builder eb = StringTableSection.Entry
            .newBuilder().setId(e.getValue()).setStr(e.getKey());
        eb.build().writeDelimitedTo(out);
      }
      commitSection(summary, SectionName.STRING_TABLE);
    }
  }

  /**
   * Supported section name. The order of the enum determines the order of
   * loading.
   */
  public enum SectionName {
    NS_INFO("NS_INFO"),
    STRING_TABLE("STRING_TABLE"),
    EXTENDED_ACL("EXTENDED_ACL"),
    INODE("INODE"),
    INODE_REFERENCE("INODE_REFERENCE"),
    SNAPSHOT("SNAPSHOT"),
    INODE_DIR("INODE_DIR"),
    FILES_UNDERCONSTRUCTION("FILES_UNDERCONSTRUCTION"),
    SNAPSHOT_DIFF("SNAPSHOT_DIFF"),
    SECRET_MANAGER("SECRET_MANAGER"),
    CACHE_MANAGER("CACHE_MANAGER");

    private static final SectionName[] values = SectionName.values();

    public static SectionName fromString(String name) {
      for (SectionName n : values) {
        if (n.name.equals(name))
          return n;
      }
      return null;
    }

    private final String name;

    private SectionName(String name) {
      this.name = name;
    }
  }

  private static int getOndiskTrunkSize(com.google.protobuf.GeneratedMessage s) {
    return CodedOutputStream.computeRawVarint32Size(s.getSerializedSize())
        + s.getSerializedSize();
  }

  private FSImageFormatProtobuf() {
  }
}
