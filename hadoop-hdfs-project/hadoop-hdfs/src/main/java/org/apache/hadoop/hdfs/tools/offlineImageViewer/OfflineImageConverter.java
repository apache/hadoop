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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheDirectiveInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CachePoolInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.server.namenode.AclEntryStatusFormat;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.SectionName;
import org.apache.hadoop.hdfs.server.namenode.FSImageUtil;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.CacheManagerSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.ErasureCodingSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FilesUnderConstructionSection.FileUnderConstructionEntry;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.AclFeatureProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.NameSystemSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SecretManagerSection;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.namenode.SerialNumberManager;
import org.apache.hadoop.hdfs.server.namenode.XAttrFormat;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.io.CountingOutputStream;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Ints;
import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.util.LimitInputStream;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLStreamException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class OfflineImageConverter {
  public static final Logger LOG =
      LoggerFactory.getLogger(OfflineImageConverter.class);

  /**
   * The output stream.
   */
  private final CountingOutputStream out;

  /**
   * A map of section names to section handler objects.
   */
  private final HashMap<String, SectionProcessor> sections;

  /**
   * The offset of the start of the current section.
   */
  private long sectionStartOffset;

  /**
   * The FileSummary builder, where we gather information about each section
   * we wrote.
   */
  private final FileSummary.Builder fileSummaryBld =
      FileSummary.newBuilder();

  /**
   * The string table.  See registerStringId for details.
   */
  private final HashMap<String, Integer> contiguousIdStringMap = new HashMap<>();

  /**
   * The string table of input fsimage file.
   */
  private SerialNumberManager.StringTable stringTable;

  /**
   * The configuration object.
   */
  private final Configuration conf;

  /**
   * The latest string ID.  See registerStringId for details.
   */
  private int latestStringId = 1;

  /**
   * The layoutVersion of output fsimage file.
   */
  private int targetVersion;

  private static final String EMPTY_STRING = "";

  private OfflineImageConverter(Configuration conf, CountingOutputStream out,
      InputStreamReader reader, int targetVersion) throws XMLStreamException {
    this.conf = conf;
    this.out = out;
    this.targetVersion = targetVersion;
    this.sections = new HashMap<>();
    this.sections.put(StringTableProcessor.NAME, new StringTableProcessor());
    this.sections.put(NameSectionProcessor.NAME, new NameSectionProcessor());
    this.sections.put(ErasureCodingSectionProcessor.NAME,
        new ErasureCodingSectionProcessor());
    this.sections.put(INodeSectionProcessor.NAME, new INodeSectionProcessor());
    this.sections.put(SecretManagerSectionProcessor.NAME,
        new SecretManagerSectionProcessor());
    this.sections.put(CacheManagerSectionProcessor.NAME,
        new CacheManagerSectionProcessor());
    this.sections.put(SnapshotDiffSectionProcessor.NAME,
        new SnapshotDiffSectionProcessor());
    this.sections.put(INodeReferenceSectionProcessor.NAME,
        new INodeReferenceSectionProcessor());
    this.sections.put(INodeDirectorySectionProcessor.NAME,
        new INodeDirectorySectionProcessor());
    this.sections.put(FilesUnderConstructionSectionProcessor.NAME,
        new FilesUnderConstructionSectionProcessor());
    this.sections.put(SnapshotSectionProcessor.NAME,
        new SnapshotSectionProcessor());
  }


  /**
   * A processor for an FSImage Section.
   */
  private interface SectionProcessor {
    /**
     * Process this section.
     */
    void process(InputStream in) throws IOException;
  }

  private final class StringTableProcessor implements SectionProcessor {
    static final String NAME = "STRING_TABLE";

    @Override
    public void process(InputStream in) throws IOException {
      stringTable = FSImageLoader.loadStringTable(in);
    }
  }

  /**
   * Processes the NameSection containing last allocated block ID, etc.
   */
  private final class NameSectionProcessor implements SectionProcessor {
    static final String NAME = "NS_INFO";

    @Override
    public void process(InputStream in) throws IOException {
      NameSystemSection s = NameSystemSection.parseDelimitedFrom(in);
      if (LOG.isDebugEnabled()) {
        LOG.debug(SectionName.NS_INFO.name() + " writing header: {" +
            TextFormat.printToString(s) + "}");
      }
      s.writeDelimitedTo(out);
      recordSectionLength(SectionName.NS_INFO.name());
    }
  }

  private final class ErasureCodingSectionProcessor implements SectionProcessor {
    static final String NAME = "ERASURE_CODING";

    @Override
    public void process(InputStream in) throws IOException {
      ErasureCodingSection section = ErasureCodingSection.parseDelimitedFrom(in);
      section.writeDelimitedTo(out);
      recordSectionLength(SectionName.ERASURE_CODING.name());
    }
  }

  private final class INodeSectionProcessor implements SectionProcessor {
    static final String NAME = "INODE";

    @Override
    public void process(InputStream in) throws IOException {
      INodeSection s = INodeSection.parseDelimitedFrom(in);
      s.writeDelimitedTo(out);

      for (int i = 0; i < s.getNumInodes(); ++i) {
        INodeSection.INode p = INodeSection.INode.parseDelimitedFrom(in);
        INodeSection.INode.Builder inodeBld = processINode(p);
        inodeBld.build().writeDelimitedTo(out);
      }
      recordSectionLength(SectionName.INODE.name());
    }
  }

  private INodeSection.INode.Builder processINode(INodeSection.INode p)
      throws IOException {
    INodeSection.INode.Builder inodeBld = INodeSection.INode.newBuilder();
    inodeBld.setId(p.getId());
    inodeBld.setName(p.getName());

    if (p.hasFile()) {
      processFile(p.getFile(), inodeBld);
    } else if (p.hasDirectory()) {
      processDirectory(p.getDirectory(), inodeBld);
    } else if (p.hasSymlink()) {
      processSymlink(p.getSymlink(), inodeBld);
    }
    return inodeBld;
  }

  private void processFile(INodeSection.INodeFile f, INodeSection.INode.Builder inodeBld)
      throws IOException {
    inodeBld.setType(INodeSection.INode.Type.FILE);
    INodeSection.INodeFile.Builder bld = createINodeFileBuilder(f);
    inodeBld.setFile(bld);
  }

  private INodeSection.INodeFile.Builder createINodeFileBuilder(INodeSection.INodeFile f)
      throws IOException {
    INodeSection.INodeFile.Builder bld = INodeSection.INodeFile.newBuilder();

    // EC file shouldn't set replication factor
    if (!f.hasErasureCodingPolicyID()) {
      bld.setReplication(f.getReplication());
    }
    bld.setModificationTime(f.getModificationTime());
    bld.setAccessTime(f.getAccessTime());
    bld.setPreferredBlockSize(f.getPreferredBlockSize());
    bld.setPermission(processPermission(f.getPermission()));
    if (f.hasAcl()) {
      bld.setAcl(processAcl(f.getAcl()));
    }
    if (f.hasXAttrs()) {
      bld.setXAttrs(processXattrs(f.getXAttrs()));
    }

    if (f.getBlocksCount() > 0) {
      for (BlockProto b : f.getBlocksList()) {
        bld.addBlocks(createBlockBuilder(b));
      }
    }

    if (f.hasFileUC()) {
      INodeSection.FileUnderConstructionFeature.Builder fb =
          INodeSection.FileUnderConstructionFeature.newBuilder();
      INodeSection.FileUnderConstructionFeature u = f.getFileUC();
      fb.setClientName(u.getClientName());
      fb.setClientMachine(u.getClientMachine());
      bld.setFileUC(fb);
    }

    if (f.hasStoragePolicyID()) {
      bld.setStoragePolicyID(f.getStoragePolicyID());
    }

    String blockType = f.getBlockType().name();
    switch (blockType) {
    case "CONTIGUOUS":
      bld.setBlockType(HdfsProtos.BlockTypeProto.CONTIGUOUS);
      break;
    case "STRIPED":
      bld.setBlockType(HdfsProtos.BlockTypeProto.STRIPED);
      bld.setErasureCodingPolicyID(f.getErasureCodingPolicyID());
      break;
    default:
      throw new IOException("Unknown blockType: " + blockType);
    }
    return bld;
  }

  private BlockProto.Builder createBlockBuilder(BlockProto block)
      throws IOException {
    BlockProto.Builder blockBld = BlockProto.newBuilder();
    blockBld.setBlockId(block.getBlockId());
    blockBld.setGenStamp(block.getGenStamp());
    blockBld.setNumBytes(block.getNumBytes());
    return blockBld;
  }

  private void processDirectory(INodeSection.INodeDirectory d,
      INodeSection.INode.Builder inodeBld) throws IOException {
    inodeBld.setType(INodeSection.INode.Type.DIRECTORY);
    INodeSection.INodeDirectory.Builder bld =
        createINodeDirectoryBuilder(d);
    inodeBld.setDirectory(bld);
  }

  private INodeSection.INodeDirectory.Builder createINodeDirectoryBuilder(
      INodeSection.INodeDirectory d) throws IOException {
    INodeSection.INodeDirectory.Builder bld =
        INodeSection.INodeDirectory.newBuilder();

    bld.setModificationTime(d.getModificationTime());
    if (d.hasDsQuota() && d.hasNsQuota()) {
      bld.setNsQuota(d.getNsQuota());
      bld.setDsQuota(d.getDsQuota());
    }

    bld.setPermission(processPermission(d.getPermission()));
    if (d.hasAcl()) {
      bld.setAcl(processAcl(d.getAcl()));
    }
    if (d.hasXAttrs()) {
      bld.setXAttrs(processXattrs(d.getXAttrs()));
    }

    INodeSection.QuotaByStorageTypeFeatureProto.Builder qf =
        INodeSection.QuotaByStorageTypeFeatureProto.newBuilder();
    INodeSection.QuotaByStorageTypeFeatureProto typeQuotas =
        d.getTypeQuotas();
    if (typeQuotas != null) {
      for (INodeSection.QuotaByStorageTypeEntryProto entry: typeQuotas.getQuotasList()) {
        INodeSection.QuotaByStorageTypeEntryProto.Builder qbld =
            INodeSection.QuotaByStorageTypeEntryProto.newBuilder();
        qbld.setStorageType(entry.getStorageType());
        qbld.setQuota(entry.getQuota());
        qf.addQuotas(qbld);
      }
      bld.setTypeQuotas(qf);
    }
    return bld;
  }

  private void processSymlink(INodeSection.INodeSymlink s,
      INodeSection.INode.Builder inodeBld) throws IOException {
    inodeBld.setType(INodeSection.INode.Type.SYMLINK);
    INodeSection.INodeSymlink.Builder bld =
        INodeSection.INodeSymlink.newBuilder();

    bld.setPermission(processPermission(s.getPermission()));
    bld.setTarget(s.getTarget());
    bld.setModificationTime(s.getModificationTime());
    bld.setAccessTime(s.getAccessTime());

    inodeBld.setSymlink(bld);
  }

  private INodeSection.AclFeatureProto.Builder processAcl(AclFeatureProto aclFeatureProto)
          throws IOException {
    AclFeatureProto.Builder b = AclFeatureProto.newBuilder();
    ImmutableList<AclEntry> aclEntryList = FSImageFormatPBINode.Loader
        .loadAclEntries(aclFeatureProto, stringTable);
    if (aclEntryList.size() > 0) {
      for (AclEntry aclEntry : aclEntryList) {
        int nameId = registerStringId(aclEntry.getName() == null ? EMPTY_STRING
                : aclEntry.getName());
        long v = 0;
        v = AclEntryStatusFormat.NAME.getBitFormat().combine(nameId, v);
        v = AclEntryStatusFormat.TYPE.getBitFormat().combine(aclEntry.getType().ordinal(), v);
        v = AclEntryStatusFormat.SCOPE.getBitFormat().combine(aclEntry.getScope().ordinal(), v);
        v = AclEntryStatusFormat.PERMISSION.getBitFormat()
            .combine(aclEntry.getPermission().ordinal(), v);
        b.addEntries((int) v);
      }
    }
    return b;
  }

  private INodeSection.XAttrFeatureProto.Builder processXattrs(
      INodeSection.XAttrFeatureProto xattrs) throws IOException {
    INodeSection.XAttrFeatureProto.Builder bld =
        INodeSection.XAttrFeatureProto.newBuilder();
    List<XAttr> xattrList = FSImageFormatPBINode.Loader
        .loadXAttrs(xattrs, stringTable);

    for (XAttr xattr : xattrList) {
      INodeSection.XAttrCompactProto.Builder b =
          INodeSection.XAttrCompactProto.newBuilder();
      int v = XAttrFormat.toInt(xattr);
      v = (int) XAttrFormat.NAME.getBitFormat().combine(registerStringId(xattr.getName()), v);
      b.setName(v);
      b.setValue(ByteString.copyFrom(xattr.getValue()));
      bld.addXAttrs(b);
    }
    return bld;
  }

  private final class SecretManagerSectionProcessor implements SectionProcessor {
    static final String NAME = "SECRET_MANAGER";

    @Override
    public void process(InputStream in) throws IOException {
      SecretManagerSection s = SecretManagerSection.parseDelimitedFrom(in);

      int expectedNumDelegationKeys = s.getNumKeys();
      int expectedNumTokens = s.getNumTokens();
      s.writeDelimitedTo(out);

      for (int actualNumKeys = 0; actualNumKeys < expectedNumDelegationKeys; actualNumKeys++) {
        SecretManagerSection.DelegationKey dkey =
            SecretManagerSection.DelegationKey.parseDelimitedFrom(in);
        dkey.writeDelimitedTo(out);
      }

      for (int actualNumTokens = 0; actualNumTokens < expectedNumTokens; actualNumTokens++) {
        SecretManagerSection.PersistToken token =
            SecretManagerSection.PersistToken.parseDelimitedFrom(in);
        token.writeDelimitedTo(out);
      }
      recordSectionLength(SectionName.SECRET_MANAGER.name());
    }
  }

  private final class CacheManagerSectionProcessor implements SectionProcessor {
    static final String NAME = "CACHE_MANAGER";

    @Override
    public void process(InputStream in) throws IOException {
      CacheManagerSection s = CacheManagerSection.parseDelimitedFrom(in);

      int expectedNumPools = s.getNumPools();
      int expectedNumDirectives = s.getNumDirectives();
      s.writeDelimitedTo(out);

      for(int actualNumPools = 0; actualNumPools < expectedNumPools; ++actualNumPools){
        CachePoolInfoProto p = CachePoolInfoProto.parseDelimitedFrom(in);
        p.writeDelimitedTo(out);
      }

      for (int actualNumDirectives = 0; actualNumDirectives < expectedNumDirectives;
          ++actualNumDirectives) {
        CacheDirectiveInfoProto p = CacheDirectiveInfoProto.parseDelimitedFrom(in);
        p.writeDelimitedTo(out);
      }
      recordSectionLength(SectionName.CACHE_MANAGER.name());
    }
  }

  private final class INodeReferenceSectionProcessor implements SectionProcessor {
    static final String NAME = "INODE_REFERENCE";

    @Override
    public void process(InputStream in) throws IOException {
      while (true) {
        FsImageProto.INodeReferenceSection.INodeReference e = FsImageProto.INodeReferenceSection
            .INodeReference.parseDelimitedFrom(in);
        if (e == null) {
          break;
        }
        e.writeDelimitedTo(out);
      }
      recordSectionLength(SectionName.INODE_REFERENCE.name());
    }
  }

  private final class INodeDirectorySectionProcessor implements SectionProcessor {
    static final String NAME = "INODE_DIR";

    @Override
    public void process(InputStream in) throws IOException {
      while (true) {
        FsImageProto.INodeDirectorySection.DirEntry e = FsImageProto.INodeDirectorySection.DirEntry
            .parseDelimitedFrom(in);
        // note that in is a LimitedInputStream
        if (e == null) {
          break;
        }
        e.writeDelimitedTo(out);
      }
      recordSectionLength(SectionName.INODE_DIR.name());
    }
  }

  private final class FilesUnderConstructionSectionProcessor implements SectionProcessor {
    static final String NAME = "FILES_UNDERCONSTRUCTION";

    @Override
    public void process(InputStream in) throws IOException {
      while (true) {
        FileUnderConstructionEntry e = FileUnderConstructionEntry
            .parseDelimitedFrom(in);
        if (e == null) {
          break;
        }
        e.writeDelimitedTo(out);
      }
      recordSectionLength(SectionName.FILES_UNDERCONSTRUCTION.name());
    }
  }

  private final class SnapshotSectionProcessor implements SectionProcessor {
    static final String NAME = "SNAPSHOT";

    @Override
    public void process(InputStream in) throws IOException {
      FsImageProto.SnapshotSection s = FsImageProto.SnapshotSection.parseDelimitedFrom(in);
      s.writeDelimitedTo(out);

      int expectedNumSnapshots = s.getNumSnapshots();
      for(int actualNumSnapshots = 0; actualNumSnapshots < expectedNumSnapshots;
          ++actualNumSnapshots){
        FsImageProto.SnapshotSection.Snapshot.Builder bld =
            FsImageProto.SnapshotSection.Snapshot.newBuilder();
        FsImageProto.SnapshotSection.Snapshot pbs = FsImageProto.SnapshotSection.Snapshot
            .parseDelimitedFrom(in);

        bld.setSnapshotId(pbs.getSnapshotId());
        INodeSection.INode.Builder inodeBld = processINode(pbs.getRoot());
        bld.setRoot(inodeBld);
        bld.build().writeDelimitedTo(out);
      }
      recordSectionLength(SectionName.SNAPSHOT.name());
    }
  }

  private final class SnapshotDiffSectionProcessor implements SectionProcessor {
    static final String NAME = "SNAPSHOT_DIFF";

    @Override
    public void process(InputStream in) throws IOException {
      LOG.debug("Processing SnapshotDiffSection");
      while (true) {
        FsImageProto.SnapshotDiffSection.DiffEntry e = FsImageProto.SnapshotDiffSection.DiffEntry
            .parseDelimitedFrom(in);
        if (e == null) {
          break;
        }
        e.writeDelimitedTo(out);

        int expectedDiffs = e.getNumOfDiff();
        switch (e.getType()) {
        case FILEDIFF:
          processFileDiffEntry(in, expectedDiffs);
          break;
        case DIRECTORYDIFF:
          processDirDiffEntry(in, expectedDiffs);
          break;
        default:
          throw new IOException("unknown DiffEntry type " + e.getType());
        }
      }
      recordSectionLength(SectionName.SNAPSHOT_DIFF.name());
    }

    private void processFileDiffEntry(InputStream in, int expectedDiffs) throws IOException {
      LOG.debug("Processing fileDiffEntry");
      for (int actualDiffs = 0; actualDiffs < expectedDiffs; actualDiffs++) {
        FsImageProto.SnapshotDiffSection.FileDiff.Builder bld =
            FsImageProto.SnapshotDiffSection.FileDiff.newBuilder();
        FsImageProto.SnapshotDiffSection.FileDiff f = FsImageProto.SnapshotDiffSection.FileDiff
            .parseDelimitedFrom(in);
        bld.setSnapshotId(f.getSnapshotId());
        bld.setFileSize(f.getFileSize());
        bld.setName(f.getName());

        INodeSection.INodeFile snapshotCopy = f.getSnapshotCopy();
        if (snapshotCopy != null) {
          bld.setSnapshotCopy(createINodeFileBuilder(snapshotCopy));
        }

        if (f.getBlocksCount() > 0) {
          for (BlockProto b : f.getBlocksList()) {
            bld.addBlocks(createBlockBuilder(b));
          }
        }
        bld.build().writeDelimitedTo(out);
      }
    }

    private void processDirDiffEntry(InputStream in, int expectedDiffs) throws IOException {
      LOG.debug("Processing dirDiffEntry");
      for (int actualDiffs = 0; actualDiffs < expectedDiffs; actualDiffs++) {
        FsImageProto.SnapshotDiffSection.DirectoryDiff.Builder bld =
            FsImageProto.SnapshotDiffSection.DirectoryDiff.newBuilder();
        FsImageProto.SnapshotDiffSection.DirectoryDiff d = FsImageProto
            .SnapshotDiffSection.DirectoryDiff.parseDelimitedFrom(in);
        bld.setSnapshotId(d.getSnapshotId());
        bld.setIsSnapshotRoot(d.getIsSnapshotRoot());
        bld.setChildrenSize(d.getChildrenSize());
        bld.setName(d.getName());

        if (d.hasSnapshotCopy()) {
          bld.setSnapshotCopy(createINodeDirectoryBuilder(d.getSnapshotCopy()));
        }

        int expectedCreatedListSize = d.getCreatedListSize();
        bld.setCreatedListSize(expectedCreatedListSize);
        for (long did : d.getDeletedINodeList()) {
          bld.addDeletedINode(did);
        }
        for (int dRefid : d.getDeletedINodeRefList()) {
          bld.addDeletedINodeRef(dRefid);
        }
        bld.build().writeDelimitedTo(out);

        // process CreatedListEntry PBs
        for (int actualCreatedListSize = 0; actualCreatedListSize < d.getCreatedListSize();
            ++actualCreatedListSize) {
          FsImageProto.SnapshotDiffSection.CreatedListEntry ce =
              FsImageProto.SnapshotDiffSection.CreatedListEntry.parseDelimitedFrom(in);
          ce.writeDelimitedTo(out);
        }
      }
    }
  }

  private long processPermission(long permission) throws IOException {
    PermissionStatus permStatus = FSImageFormatPBINode.Loader.
        loadPermission(permission, stringTable);
    permission = INodeWithAdditionalFields.PermissionStatusFormat.USER.getBitFormat()
            .combine(registerStringId(permStatus.getUserName()), permission);
    permission = INodeWithAdditionalFields.PermissionStatusFormat.GROUP.getBitFormat()
            .combine(registerStringId(permStatus.getGroupName()), permission);
    return permission;
  }

  /**
   * The FSImage contains a string table which maps strings to IDs.
   * This is a simple form of compression which takes advantage of the
   * fact that the same strings tend to occur over and over again.
   * This function will return an ID which we can use to represent the
   * given string. If the string already exists in the string table, we
   * will use that ID; otherwise, we will allocate a new one.
   *
   * @param str           The string.
   * @return              The ID in the string table.
   * @throws IOException  If we run out of bits in the string table. We only
   *                      have 25 bits.
   */
  int registerStringId(String str) throws IOException {
    Integer id = contiguousIdStringMap.get(str);
    if (id != null) {
      return id;
    }
    int latestId = latestStringId;
    if (latestId >= 0x1ffffff) {
      throw new IOException("Cannot have more than 2**25 " +
          "strings in the fsimage, because of the limitation on " +
          "the size of string table IDs.");
    }
    contiguousIdStringMap.put(str, latestId);
    latestStringId++;
    return latestId;
  }

  /**
   * Record the length of a section of the FSImage in our FileSummary object.
   * The FileSummary appears at the end of the FSImage and acts as a table of
   * contents for the file.
   *
   * @param sectionNamePb  The name of the section as it should appear in
   *                       the fsimage.  (This is different than the XML
   *                       name.)
   * @throws IOException
   */
  void recordSectionLength(String sectionNamePb) throws IOException {
    long curSectionStartOffset = sectionStartOffset;
    long curPos = out.getCount();
    fileSummaryBld.addSections(FileSummary.Section.newBuilder().setName(sectionNamePb)
        .setLength(curPos - curSectionStartOffset).setOffset(curSectionStartOffset));
    sectionStartOffset = curPos;
  }

  /**
   * Read and check the version of fsimage.
   */
  private void readVersion(FileSummary summary) throws IOException {
    int onDiskVersion = summary.getOndiskVersion();
    int layoutVersion = summary.getLayoutVersion();

    if (layoutVersion != NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION
        && layoutVersion != targetVersion) {
      throw new IOException("Layout version mismatch. This oiv tool handles layout " +
          "version " + NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION + ", but the " +
          "fsimage file contains layout version " + layoutVersion + ".  Please " +
          "re-generate the fsimage file to be usable with this version of the oiv tool.");
    }

    if (targetVersion < NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION
        || targetVersion > NameNodeLayoutVersion.MINIMUM_COMPATIBLE_LAYOUT_VERSION) {
      throw new IOException("Layout version mismatch. This oiv tool handles layout " +
          "version " + NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION + ", but the " +
          "target layout version " + targetVersion + ".  Please adjust the target " +
          "layout version to be usable with this version of the oiv tool.");
    }

    fileSummaryBld.setOndiskVersion(onDiskVersion);

    if (targetVersion != 0){
      fileSummaryBld.setLayoutVersion(targetVersion);
    } else {
      fileSummaryBld.setLayoutVersion(layoutVersion);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Loaded fsimage with onDiskVersion=" + onDiskVersion +
          ", layoutVersion=" + layoutVersion + ".");
    }
  }

  /**
   * Write the string table to the fsimage.
   * @throws IOException
   */
  private void writeStringTableSection() throws IOException {
    FsImageProto.StringTableSection sectionHeader = FsImageProto.StringTableSection
        .newBuilder().setNumEntry(contiguousIdStringMap.size()).build();
    if (LOG.isDebugEnabled()) {
      LOG.debug(SectionName.STRING_TABLE.name() + " writing header: {"
          + TextFormat.printToString(sectionHeader) + "}");
    }
    sectionHeader.writeDelimitedTo(out);

    // The entries don't have to be in any particular order, so iterating
    // over the hash table is fine.
    for (Map.Entry<String, Integer> entry : contiguousIdStringMap.entrySet()) {
      FsImageProto.StringTableSection.Entry stEntry =
          FsImageProto.StringTableSection.Entry.newBuilder().
          setStr(entry.getKey()).
          setId(entry.getValue()).
          build();
      if (LOG.isTraceEnabled()) {
        LOG.trace("Writing string table entry: {" + TextFormat.printToString(stEntry) + "}");
      }
      stEntry.writeDelimitedTo(out);
    }
    recordSectionLength(SectionName.STRING_TABLE.name());
  }

  /**
   * Processes the high edition fsimage back into a low edition fsimage.
   * @param file  The input fsimage file.
   * @throws Exception
   */
  private void processImage(RandomAccessFile file) throws Exception {
    LOG.debug("Loading FSImage.");

    if (!FSImageUtil.checkFileFormat(file)) {
      throw new IOException("Unrecognized FSImage");
    }

    FileSummary summary = FSImageUtil.loadSummary(file);

    readVersion(summary);
    // Write the HDFSIMG1 magic number which begins the fsimage file.
    out.write(FSImageUtil.MAGIC_HEADER);
    // Write a series of fsimage sections.
    sectionStartOffset = FSImageUtil.MAGIC_HEADER.length;

    try (FileInputStream fin = new FileInputStream(file.getFD())) {
      ArrayList<FileSummary.Section> sectionsList = Lists.newArrayList(summary.getSectionsList());
      Collections.sort(sectionsList, new Comparator<FileSummary.Section>() {
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

      for (FileSummary.Section s : sectionsList) {
        fin.getChannel().position(s.getOffset());
        InputStream is = FSImageUtil.wrapInputStreamForCompression(conf, summary.getCodec(),
            new BufferedInputStream(new LimitInputStream(fin, s.getLength())));

        String sectionName = s.getName();
        if (!sections.containsKey(sectionName)) {
          continue;
        }

        SectionProcessor sectionProcessor = sections.get(sectionName);
        if (sectionProcessor == null) {
          throw new IOException("Unknown FSImage section " + sectionName +
              ". Valid section names are [" + StringUtils.join(", ", sections.keySet()) + "]");
        }
        sectionProcessor.process(is);
      }
    }

    // Write the StringTable section to disk.
    // This has to be done after the other sections, since some of them
    // add entries to the string table.
    writeStringTableSection();

    // Write the FileSummary section to disk.
    // This section is always last.
    long prevOffset = out.getCount();
    FileSummary fileSummary = fileSummaryBld.build();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Writing FileSummary: {" + TextFormat.printToString(fileSummary) + "}");
    }
    // Even though the last 4 bytes of the file gives the FileSummary length,
    // we still write a varint first that also contains the length.
    fileSummary.writeDelimitedTo(out);

    // Write the length of the FileSummary section as a fixed-size big
    // endian 4-byte quantity.
    int summaryLen = Ints.checkedCast(out.getCount() - prevOffset);
    byte[] summaryLenBytes = new byte[4];
    ByteBuffer.wrap(summaryLenBytes).asIntBuffer().put(summaryLen);
    out.write(summaryLenBytes);
  }

  /**
   * Run the OfflineImageConverter.
   * @param conf           Configuration to use.
   * @param inputPath      The input path to use.
   * @param outputPath     The output path to use.
   * @param targetVersion  The layoutVersion of output fsimage file.
   * @throws Exception  On error.
   */
  public static void run(Configuration conf, String inputPath, String outputPath, int targetVersion)
      throws Exception {
    MessageDigest digester = MD5Hash.getDigester();
    OutputStream fout = null;
    File foutHash = new File(outputPath + ".md5");
    Files.deleteIfExists(foutHash.toPath()); // delete any .md5 file that exists
    CountingOutputStream out = null;
    RandomAccessFile fin = null;
    InputStreamReader reader = null;
    try {
      Files.deleteIfExists(Paths.get(outputPath));
      fout = Files.newOutputStream(Paths.get(outputPath));
      fin = new RandomAccessFile(inputPath, "r");
      out = new CountingOutputStream(
          new DigestOutputStream(new BufferedOutputStream(fout), digester));
      OfflineImageConverter oic =
          new OfflineImageConverter(conf, out, reader, targetVersion);
      oic.processImage(fin);
    } finally {
      IOUtils.cleanupWithLogger(LOG, reader, fin, out, fout);
    }
    // Write the md5 file
    MD5FileUtils.saveMD5File(new File(outputPath), new MD5Hash(digester.digest()));
  }
}