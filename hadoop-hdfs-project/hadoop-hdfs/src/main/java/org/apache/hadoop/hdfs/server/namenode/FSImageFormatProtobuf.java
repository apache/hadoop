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

import java.io.*;
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

import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.Table;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheDirectiveInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CachePoolInfoProto;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockIdManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.flatbuffer.*;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.CacheManagerSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.NameSystemSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SecretManagerSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.StringTableSection;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FSImageFormatPBSnapshot;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.AltFileInputStream;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressorStream;
import org.apache.hadoop.util.LimitInputStream;
import org.apache.hadoop.util.Time;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.CodedOutputStream;

import javax.xml.crypto.Data;

/**
 * Utility class to read / write fsimage in protobuf format.
 */
@InterfaceAudience.Private
public final class FSImageFormatProtobuf {
  private static final Log LOG = LogFactory.getLog(FSImageFormatProtobuf.class);

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
     * {@link HdfsServerConstants#NAMENODE_LAYOUT_VERSION}. This is only set to true
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
      AltFileInputStream fin = new AltFileInputStream(file);
      try {
        loadIntelInternal(raFile, fin);
        long end = Time.monotonicNow();
        LOG.info("Loaded FSImage in " + (end - start) / 1000 + " seconds.");
        /**
         * Number Of Blocks : 1043093
         * Block Size : 5MB
         * Load Time :
         *
         *          4482 Milliseconds
         *          4180 Milliseconds
         *          4599 Milliseconds
         *          4472 Milliseconds
         *          4223 Milliseconds
         *
         *  Average Load FSImage Time : 4391 Milliseconds
         */
        long time = end - start;
        File file1 = new File("/home/minglei/intervalTime.txt");
        FileOutputStream out = new FileOutputStream(file1);
        OutputStreamWriter osw  = new OutputStreamWriter(out);
        BufferedWriter bw = new BufferedWriter(osw);
        bw.write(String.valueOf(time) + " Milliseconds ");
        bw.flush();

      } finally {
        fin.close();
        raFile.close();
      }
    }

    private void loadIntelInternal(RandomAccessFile raFile, AltFileInputStream fin)
      throws IOException{
      if (!FSImageUtil.checkFileFormat(raFile)) {
        throw new IOException("Unrecongnized file format");
      }
      IntelFileSummary summary = FSImageUtil.loadIntelSummary(raFile);
      if (requireSameLayoutVersion &&
          summary.layoutVersion() != HdfsServerConstants.NAMENODE_LAYOUT_VERSION) {
        throw new IOException("Image version " + summary.layoutVersion() +
            "is not equal to the software version " +
            HdfsServerConstants.NAMENODE_LAYOUT_VERSION);
      }
      FileChannel channel = fin.getChannel();
      FSImageFormatPBINode.Loader inodeLoader = new FSImageFormatPBINode.Loader(
          fsn, this);
      FSImageFormatPBSnapshot.Loader snapshotLoader = new FSImageFormatPBSnapshot.Loader(
          fsn, this);

      ArrayList<IntelSection> sections = new ArrayList<IntelSection>();
      for (int i = 0; i < summary.sectionsLength();i++) {
        sections.add(summary.sections(i));
      }
      Collections.sort(sections, new Comparator<IntelSection>() {
        @Override
        public int compare(IntelSection s1, IntelSection s2) {
          SectionName n1 = SectionName.fromString(s1.name());
          SectionName n2 = SectionName.fromString(s2.name());
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
      Step currentStep = null;
      for (IntelSection s:sections) {
        channel.position(s.offset());
        InputStream in = new BufferedInputStream(new LimitInputStream(fin,
            s.length()));
        in = FSImageUtil.wrapInputStreamForCompression(conf,
            summary.codec(), in);

        String n = s.name();

        switch (SectionName.fromString(n)) {
          case NS_INFO:
            loadIntelNameSystemSection(in);   // success
            break;
          case STRING_TABLE:
            loadIntelStringTableSection(in); // success
            break;
          case INODE: {
            currentStep = new Step(StepType.INODES);
            prog.beginStep(Phase.LOADING_FSIMAGE, currentStep);
              inodeLoader.loadIntelINodeSection(in, prog, currentStep); // success
          }
          break;
          case INODE_REFERENCE:
            snapshotLoader.loadIntelINodeReferenceSection(in); // success
            break;
          case INODE_DIR:
            inodeLoader.loadIntelINodeDirectorySection(in); // success
            break;
          case FILES_UNDERCONSTRUCTION:
            inodeLoader.loadIntelFilesUnderConstructionSection(in); // success
            break;
          case SNAPSHOT:
            snapshotLoader.loadIntelSnapshotSection(in); // success
            break;
          case SNAPSHOT_DIFF:
            snapshotLoader.loadIntelSnapshotDiffSection(in); // succcess
            break;
          case SECRET_MANAGER: {
            prog.endStep(Phase.LOADING_FSIMAGE, currentStep);
            Step step = new Step(StepType.DELEGATION_TOKENS);
            prog.beginStep(Phase.LOADING_FSIMAGE, step);
            loadIntelSecretManagerSection(in, prog, step);  // success
            prog.endStep(Phase.LOADING_FSIMAGE, step);
          }
          break;
          case CACHE_MANAGER: {
            Step step = new Step(StepType.CACHE_POOLS);
            prog.beginStep(Phase.LOADING_FSIMAGE, step);
            loadIntelCacheManagerSection(in, prog, step); // success
            prog.endStep(Phase.LOADING_FSIMAGE, step);
          }
          break;
          default:
            LOG.warn("Unrecognized section " + n);
            break;
        }
      }
    }

    public static byte[] parseFrom(InputStream in) throws IOException {
      DataInputStream inputStream = new DataInputStream(in);
      int len = inputStream.readInt();
      byte[] data = new byte[len];
      inputStream.read(data);
      return data;
    }

    private void loadIntelNameSystemSection(InputStream in) throws IOException {
      ByteBuffer byteBuffer = ByteBuffer.wrap(parseFrom(in));
      IntelNameSystemSection intelNameSystemSection =
          IntelNameSystemSection.getRootAsIntelNameSystemSection(byteBuffer);
      BlockIdManager blockIdManager = fsn.getBlockIdManager();
      blockIdManager.setGenerationStampV1(intelNameSystemSection.genstampV1());
      blockIdManager.setGenerationStampV2(intelNameSystemSection.genstampV2());
      blockIdManager.setGenerationStampV1Limit(intelNameSystemSection.genstampV1Limit());
      blockIdManager.setLastAllocatedBlockId(intelNameSystemSection.lastAllocatedBlockId());
      imgTxId = intelNameSystemSection.transactionId();
//      long roll = intelNameSystemSection.rollingUpgradeStartTime();
//      boolean hasRoll = roll != 0;
      if (intelNameSystemSection.rollingUpgradeStartTime() != 0
          && fsn.getFSImage().hasRollbackFSImage()) {
        // we set the rollingUpgradeInfo only when we make sure we have the
        // rollback image
        fsn.setRollingUpgradeInfo(true, intelNameSystemSection.rollingUpgradeStartTime());
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

    private void loadIntelStringTableSection(InputStream in) throws IOException {
      ByteBuffer byteBuffer = ByteBuffer.wrap(parseFrom(in));
      IntelStringTableSection intelSts =
          IntelStringTableSection.getRootAsIntelStringTableSection(byteBuffer);
      ctx.stringTable = new String[(int)intelSts.numEntry() + 1];
      for (int i = 0; i < intelSts.numEntry(); ++i) {
        byte[] bytes1 = parseFrom(in);
        ByteBuffer byteBuffer1 = ByteBuffer.wrap(bytes1);
        IntelEntry intelEntry = IntelEntry.getRootAsIntelEntry(byteBuffer1);
        ctx.stringTable[(int)intelEntry.id()] = intelEntry.str();
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

    private void loadIntelSecretManagerSection(InputStream in, StartupProgress prog,
                                          Step currentStep) throws IOException {
      byte[] bytes = parseFrom(in);
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      IntelSecretManagerSection is = IntelSecretManagerSection.
          getRootAsIntelSecretManagerSection(byteBuffer);

      long numKeys = is.numKeys(), numTokens = is.numTokens();
      ArrayList<IntelDelegationKey> intelkeys = Lists
          .newArrayListWithCapacity((int)numKeys);
      ArrayList<IntelPersistToken> inteltokens = Lists
          .newArrayListWithCapacity((int)numTokens);

      for (int i = 0; i < numKeys; ++i) {
        byte[] bytes1 = parseFrom(in);
        ByteBuffer byteBuffer1 = ByteBuffer.wrap(bytes1);
        IntelDelegationKey intelDelegationKey = IntelDelegationKey.getRootAsIntelDelegationKey(byteBuffer1);
        intelkeys.add(intelDelegationKey);
      }

      prog.setTotal(Phase.LOADING_FSIMAGE, currentStep, numTokens);
      Counter counter = prog.getCounter(Phase.LOADING_FSIMAGE, currentStep);
      for (int i = 0; i < numTokens; ++i) {
        byte[] bytes2 = parseFrom(in);
        ByteBuffer byteBuffer2 = ByteBuffer.wrap(bytes2);
        IntelPersistToken intelPersistToken = IntelPersistToken.getRootAsIntelPersistToken(byteBuffer2);
        inteltokens.add(intelPersistToken);
        counter.increment();
      }

      fsn.loadSecretManagerState(null, is, null, intelkeys, inteltokens, null);
    }

    private void loadSecretManagerSection(InputStream in, StartupProgress prog,
        Step currentStep) throws IOException {
      SecretManagerSection s = SecretManagerSection.parseDelimitedFrom(in);
      int numKeys = s.getNumKeys(), numTokens = s.getNumTokens();
      ArrayList<SecretManagerSection.DelegationKey> keys = Lists
          .newArrayListWithCapacity(numKeys);
      ArrayList<SecretManagerSection.PersistToken> tokens = Lists
          .newArrayListWithCapacity(numTokens);

      for (int i = 0; i < numKeys; ++i)
        keys.add(SecretManagerSection.DelegationKey.parseDelimitedFrom(in));

      prog.setTotal(Phase.LOADING_FSIMAGE, currentStep, numTokens);
      Counter counter = prog.getCounter(Phase.LOADING_FSIMAGE, currentStep);
      for (int i = 0; i < numTokens; ++i) {
        tokens.add(SecretManagerSection.PersistToken.parseDelimitedFrom(in));
        counter.increment();
      }

      fsn.loadSecretManagerState(s, null, keys, null ,null, tokens);
    }


    private void loadIntelCacheManagerSection(InputStream in, StartupProgress prog,
                                         Step currentStep) throws IOException {
      byte[] bytes = parseFrom(in);
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      IntelCacheManagerSection cs =
          IntelCacheManagerSection.getRootAsIntelCacheManagerSection(byteBuffer);

      long numPools = cs.numPools();
      ArrayList<CachePoolInfoProto> pools = Lists
          .newArrayListWithCapacity((int)numPools);
      ArrayList<CacheDirectiveInfoProto> directives = Lists
          .newArrayListWithCapacity((int)cs.numDirectives());

      prog.setTotal(Phase.LOADING_FSIMAGE, currentStep, numPools);
      Counter counter = prog.getCounter(Phase.LOADING_FSIMAGE, currentStep);
      for (int i = 0; i < numPools; ++i) {
        pools.add(CachePoolInfoProto.parseDelimitedFrom(in));
        counter.increment();
      }
      for (int i = 0; i < cs.numDirectives(); ++i)
        directives.add(CacheDirectiveInfoProto.parseDelimitedFrom(in));
      fsn.getCacheManager().loadState(
          new CacheManager.PersistState(null, cs, pools, directives));
    }

    private void loadCacheManagerSection(InputStream in, StartupProgress prog,
        Step currentStep) throws IOException {
      CacheManagerSection s = CacheManagerSection.parseDelimitedFrom(in);
      int numPools = s.getNumPools();
      ArrayList<CachePoolInfoProto> pools = Lists
          .newArrayListWithCapacity(numPools);
      ArrayList<CacheDirectiveInfoProto> directives = Lists
          .newArrayListWithCapacity(s.getNumDirectives());
      prog.setTotal(Phase.LOADING_FSIMAGE, currentStep, numPools);
      Counter counter = prog.getCounter(Phase.LOADING_FSIMAGE, currentStep);
      for (int i = 0; i < numPools; ++i) {
        pools.add(CachePoolInfoProto.parseDelimitedFrom(in));
        counter.increment();
      }
      for (int i = 0; i < s.getNumDirectives(); ++i)
        directives.add(CacheDirectiveInfoProto.parseDelimitedFrom(in));
      fsn.getCacheManager().loadState(
          new CacheManager.PersistState(s, null, pools, directives));
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

    public int commitIntelSection(SectionName name, FlatBufferBuilder fbb) throws IOException {
      long oldOffset = currentOffset;
      flushSectionOutputStream();
      if (codec != null) {
        sectionOutputStream = codec.createOutputStream(underlyingOutputStream);
      } else {
        sectionOutputStream = underlyingOutputStream;
      }
      long length = fileChannel.position() - oldOffset;
      int sectionName = fbb.createString(name.name);
      long sectionOffset = currentOffset;
      currentOffset += length;
      return IntelSection.createIntelSection(fbb, sectionName, length, sectionOffset);
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
        ((CompressorStream) sectionOutputStream).finish();
      }
      sectionOutputStream.flush();
    }

    void save(File file, FSImageCompression compression) throws IOException {
      FileOutputStream fout = new FileOutputStream(file);
      fileChannel = fout.getChannel();
      try {
        saveIntelInternal(fout, compression, file.getAbsolutePath());
      } finally {
        fout.close();
      }
    }

    private static void saveIntelFileSummary(OutputStream out,
                                             int serializedLength, byte[] bytes)
      throws IOException{
      DataOutputStream dos = new DataOutputStream(out);
      dos.writeInt(serializedLength);
      dos.write(bytes);
      int length = serializedLength + 4;
      dos.writeInt(length);
      dos.flush();
    }

    private static void saveFileSummary(OutputStream out, FileSummary summary)
        throws IOException {
      summary.writeDelimitedTo(out);
      int length = getOndiskTrunkSize(summary);
      byte[] lengthBytes = new byte[4];
      ByteBuffer.wrap(lengthBytes).asIntBuffer().put(length);
      out.write(lengthBytes);
    }

    private void saveIntelInodes(FlatBufferBuilder fbb, ArrayList<Integer> list) throws IOException {
      FSImageFormatPBINode.Saver saver = new FSImageFormatPBINode.Saver(this, null, fbb);
      list.add(saver.serializeIntelINodeSection(sectionOutputStream, fbb));
      list.add(saver.serializeIntelINodeDirectorySection(sectionOutputStream, fbb));
      list.add(saver.serializeIntelFilesUCSection(sectionOutputStream, fbb));
    }

    private void saveIntelSnapshots(FlatBufferBuilder fbb, ArrayList<Integer> list)
        throws IOException {
      FSImageFormatPBSnapshot.Saver snapshotSaver = new FSImageFormatPBSnapshot.Saver(
          this, null, fbb,context, context.getSourceNamesystem());
      list.add(snapshotSaver.serializeIntelSnapshotSection(sectionOutputStream, fbb));
      list.add(snapshotSaver.serializeIntelSnapshotDiffSection(sectionOutputStream, fbb));
      list.add(snapshotSaver.serializeIntelINodeReferenceSection(sectionOutputStream, fbb));
    }

    // abandon method.
    private void saveInodes(FileSummary.Builder summary) throws IOException {
      FSImageFormatPBINode.Saver saver = new FSImageFormatPBINode.Saver(this, summary, null);
      saver.serializeINodeSection(sectionOutputStream);
      saver.serializeINodeDirectorySection(sectionOutputStream);
      saver.serializeFilesUCSection(sectionOutputStream);
    }
    // abandon method
    private void saveSnapshots(FileSummary.Builder summary) throws IOException {
      FSImageFormatPBSnapshot.Saver snapshotSaver = new FSImageFormatPBSnapshot.Saver(
          this, summary, null,context, context.getSourceNamesystem());

      snapshotSaver.serializeSnapshotSection(sectionOutputStream);
      snapshotSaver.serializeSnapshotDiffSection(sectionOutputStream);
      snapshotSaver.serializeINodeReferenceSection(sectionOutputStream);
    }

    private void saveIntelInternal(FileOutputStream fout, FSImageCompression compression, String filePath)
        throws IOException {

      ArrayList<Integer> listSection = new ArrayList<Integer>();

      StartupProgress prog = NameNode.getStartupProgress();
      MessageDigest digester = MD5Hash.getDigester();

      underlyingOutputStream = new DigestOutputStream(new BufferedOutputStream(
              fout), digester);
      underlyingOutputStream.write(FSImageUtil.MAGIC_HEADER);
      fileChannel = fout.getChannel();

      FlatBufferBuilder fbb = new FlatBufferBuilder();
      long disk_version = FSImageUtil.FILE_VERSION;
      long layout_version = context.getSourceNamesystem().getEffectiveLayoutVersion();
      int code = 0;
      codec = compression.getImageCodec();
      if (codec != null) {
         code = fbb.createString(codec.getClass().getCanonicalName());
        sectionOutputStream = codec.createOutputStream(underlyingOutputStream);
      } else {
        code = fbb.createString("");
        sectionOutputStream = underlyingOutputStream;
      }
      listSection.add(saveIntelNameSystemSection(fbb)); // success
      context.checkCancelled();
      Step step = new Step(StepType.INODES, filePath);
      prog.beginStep(Phase.SAVING_CHECKPOINT, step);
      saveIntelInodes(fbb, listSection); // success
      saveIntelSnapshots(fbb, listSection); // success
      prog.endStep(Phase.SAVING_CHECKPOINT, step);
      step = new Step(StepType.DELEGATION_TOKENS, filePath);
      prog.beginStep(Phase.SAVING_CHECKPOINT, step);
      listSection.add(saveIntelSecretManagerSection(fbb)); // success
      prog.endStep(Phase.SAVING_CHECKPOINT, step);
      step = new Step(StepType.CACHE_POOLS, filePath);
      prog.beginStep(Phase.SAVING_CHECKPOINT, step);
      listSection.add(saveIntelCacheManagerSection(fbb)); //success
      prog.endStep(Phase.SAVING_CHECKPOINT, step);
      listSection.add(saveIntelStringTableSection(fbb));  // success
      flushSectionOutputStream();

      Integer[] data = new Integer[listSection.size()];
      for (int i = 0; i < data.length ; i++) {
        data[i] = listSection.get(i);
      }
      /**
       * how to construct these data?  Use IntelSection.createIntelSection() will
       * return an int represent the current section.
       * construct a section use four param below. Now, see how protobuf op these
       * four param.
       * {
       *    FlatBufferBuilder builder,
       *    int name,
       *    long length,
       *    long offset
       * }
       *
       */
      int sections = IntelFileSummary.createSectionsVector(fbb, ArrayUtils.toPrimitive(data));
      int end = IntelFileSummary.createIntelFileSummary
                    (fbb, disk_version, layout_version, code, sections);
      IntelFileSummary.finishIntelFileSummaryBuffer(fbb, end);
      byte[] bytes = fbb.sizedByteArray();
      saveIntelFileSummary(underlyingOutputStream, bytes.length, bytes);
      underlyingOutputStream.close();
      savedDigest = new MD5Hash(digester.digest());
    }

    // saveImage main method
    private void saveInternal(FileOutputStream fout,
        FSImageCompression compression, String filePath) throws IOException {
      StartupProgress prog = NameNode.getStartupProgress();
      MessageDigest digester = MD5Hash.getDigester();

      underlyingOutputStream = new DigestOutputStream(new BufferedOutputStream(
          fout), digester);
      underlyingOutputStream.write(FSImageUtil.MAGIC_HEADER);

      fileChannel = fout.getChannel();

      // Will Revise
      FileSummary.Builder b = FileSummary.newBuilder()
          .setOndiskVersion(FSImageUtil.FILE_VERSION)
          .setLayoutVersion(
              context.getSourceNamesystem().getEffectiveLayoutVersion());

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

    private int saveIntelSecretManagerSection(FlatBufferBuilder fbb) throws IOException {
      final FSNamesystem fsn = context.getSourceNamesystem();
      DelegationTokenSecretManager.SecretManagerState state = fsn
          .saveSecretManagerState();

      ByteBuffer byteBuffer = state.intelSection.getByteBuffer();
      int size = byteBuffer.capacity() - byteBuffer.position();
      byte[] bytes = new byte[size];
      byteBuffer.get(bytes);
      writeTo(bytes, bytes.length, sectionOutputStream);

      for(IntelDelegationKey intelK : state.intelKeys) {
        ByteBuffer byteBuffer1 = intelK.keyAsByteBuffer(); // confusing...
        int size1 = byteBuffer1.capacity() - byteBuffer1.position();
        byte[] bytes1 = new byte[size1];
        byteBuffer1.get(bytes1);
        writeTo(bytes1, bytes1.length, sectionOutputStream);
      }

      for (IntelPersistToken intelT : state.intelTokens) {
        ByteBuffer byteBuffer2 = intelT.realUserAsByteBuffer(); // confus...
        int size2 = byteBuffer2.capacity() - byteBuffer2.position();
        byte[] bytes2 = new byte[size2];
        byteBuffer2.get(bytes2);
        writeTo(bytes2, bytes2.length, sectionOutputStream);
      }
     return commitIntelSection(SectionName.SECRET_MANAGER, fbb);
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

    private int saveIntelCacheManagerSection(FlatBufferBuilder fbb)
        throws IOException {
      final FSNamesystem fsn = context.getSourceNamesystem();
      CacheManager.PersistState state = fsn.getCacheManager().saveState();
      ByteBuffer byteBuffer = state.intelSection.getByteBuffer();
      int size = byteBuffer.capacity() - byteBuffer.position();
      byte[] bytes = new byte[size];
      byteBuffer.get(bytes);
      writeTo(bytes, bytes.length, sectionOutputStream);

      for (CachePoolInfoProto p : state.pools)
        p.writeDelimitedTo(sectionOutputStream);

      for (CacheDirectiveInfoProto p : state.directives)
        p.writeDelimitedTo(sectionOutputStream);

     return commitIntelSection(SectionName.CACHE_MANAGER, fbb);
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


    private int saveIntelNameSystemSection(FlatBufferBuilder fbb) throws IOException{
      final FSNamesystem fsn = context.getSourceNamesystem();
      BlockIdManager blockIdManager = fsn.getBlockIdManager();
      FlatBufferBuilder nsFbb = new FlatBufferBuilder();
      IntelNameSystemSection.startIntelNameSystemSection(nsFbb);
      IntelNameSystemSection.addGenstampV1(nsFbb, blockIdManager.getGenerationStampV1());
      IntelNameSystemSection.addGenstampV1Limit(nsFbb, blockIdManager.getGenerationStampV1Limit());
      IntelNameSystemSection.addGenstampV2(nsFbb, blockIdManager.getGenerationStampV2());
      IntelNameSystemSection.addLastAllocatedBlockId(nsFbb, blockIdManager.getLastAllocatedBlockId());
      IntelNameSystemSection.addTransactionId(nsFbb, context.getTxId());
      IntelNameSystemSection.addNamespaceId(nsFbb, fsn.unprotectedGetNamespaceInfo().getNamespaceID());
      if (fsn.isRollingUpgrade()) {
        IntelNameSystemSection.addRollingUpgradeStartTime(nsFbb, fsn.getRollingUpgradeInfo().getStartTime());
      }
      int offset = IntelNameSystemSection.endIntelNameSystemSection(nsFbb);
      IntelNameSystemSection.finishIntelNameSystemSectionBuffer(nsFbb, offset);
      byte[] bytes = nsFbb.sizedByteArray();
      writeTo(bytes, bytes.length, sectionOutputStream);
      return commitIntelSection(SectionName.NS_INFO ,fbb);
    }

    public static void writeTo(byte[] b, int serialLength , OutputStream outputStream) throws IOException {
      DataOutputStream dos = new DataOutputStream(outputStream);
      dos.writeInt(serialLength);
      dos.write(b);
      dos.flush();
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


    private int saveIntelStringTableSection(FlatBufferBuilder fbb)
        throws IOException {
      OutputStream out = sectionOutputStream;
      FlatBufferBuilder stsfbb = new FlatBufferBuilder();
      FlatBufferBuilder entryfbb = new FlatBufferBuilder();
      int inv = IntelStringTableSection.createIntelStringTableSection(stsfbb, saverContext.stringMap.size());
      IntelStringTableSection.finishIntelStringTableSectionBuffer(stsfbb, inv);
      byte[] bytes = stsfbb.sizedByteArray();
      writeTo(bytes, bytes.length, out);

      for (Entry<String, Integer> e : saverContext.stringMap.entrySet()) {
        int offset = IntelEntry.createIntelEntry(entryfbb, e.getValue(), entryfbb.createString(e.getKey()));
        IntelEntry.finishIntelEntryBuffer(entryfbb, offset);
        byte[] bytes1 = entryfbb.sizedByteArray();
        writeTo(bytes1, bytes1.length, out);
      }
      return commitIntelSection(SectionName.STRING_TABLE, fbb);
    }


    private int saveIntelStringTableSectionV2(FlatBufferBuilder fbb)
        throws IOException {
      OutputStream out = sectionOutputStream;

      int numEntry = saverContext.stringMap.size();
      IntelStringTableSection.createIntelStringTableSection(fbb, numEntry);

      ByteBuffer byteBuffer = fbb.dataBuffer();
      int serializedLength = byteBuffer.capacity() - byteBuffer.position();
      byte[] bytes = new byte[serializedLength];
      byteBuffer.get(bytes);
      DataOutputStream dos = new DataOutputStream(out);
      dos.write(serializedLength);
      dos.write(bytes);
      dos.flush();
      for (Entry<String, Integer> e : saverContext.stringMap.entrySet()) {
        IntelEntry.createIntelEntry(fbb, e.getValue(), fbb.createString(e.getKey()));
        ByteBuffer byteBuffer1 = fbb.dataBuffer();
        byteBuffer1 = fbb.dataBuffer();
        int serializedLength1 = byteBuffer1.capacity() - byteBuffer1.position();
        byte[] bytes1 = new byte[serializedLength1];
        byteBuffer1.get(bytes1);
        DataOutputStream dos1 = new DataOutputStream(out);
        dos1.write(serializedLength);
        dos1.write(bytes);
        dos1.flush();
      }
      return commitIntelSection(SectionName.STRING_TABLE, fbb);
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

  private static int getIntelOndiskTrunkSize(Table table) {
    return 4 + table.getByteBuffer().capacity() - table.getByteBuffer().position();
  }

  private static int getOndiskTrunkSize(com.google.protobuf.GeneratedMessage s) {
    return CodedOutputStream.computeRawVarint32Size(s.getSerializedSize())
        + s.getSerializedSize();
  }

  private FSImageFormatProtobuf() {
  }
}
