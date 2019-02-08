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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import com.google.protobuf.ByteString;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheDirectiveInfoExpirationProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheDirectiveInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CachePoolInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.SectionName;
import org.apache.hadoop.hdfs.server.namenode.FSImageUtil;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.CacheManagerSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.ErasureCodingSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FilesUnderConstructionSection.FileUnderConstructionEntry;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeDirectorySection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.AclFeatureProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeSymlink;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeReferenceSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.NameSystemSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SecretManagerSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SnapshotDiffSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SnapshotSection;
import org.apache.hadoop.hdfs.server.namenode.SerialNumberManager;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.util.XMLUtils;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.util.LimitInputStream;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.util.VersionInfo;

import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.XATTR_NAMESPACE_MASK;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.XATTR_NAMESPACE_OFFSET;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.XATTR_NAMESPACE_EXT_MASK;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.XATTR_NAMESPACE_EXT_OFFSET;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.XATTR_NAME_OFFSET;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.XATTR_NAME_MASK;

/**
 * PBImageXmlWriter walks over an fsimage structure and writes out
 * an equivalent XML document that contains the fsimage's components.
 */
@InterfaceAudience.Private
public final class PBImageXmlWriter {
  public static final String NAME_SECTION_NAME = "NameSection";
  public static final String ERASURE_CODING_SECTION_NAME =
      "ErasureCodingSection";
  public static final String INODE_SECTION_NAME = "INodeSection";
  public static final String SECRET_MANAGER_SECTION_NAME =
      "SecretManagerSection";
  public static final String CACHE_MANAGER_SECTION_NAME = "CacheManagerSection";
  public static final String SNAPSHOT_DIFF_SECTION_NAME = "SnapshotDiffSection";
  public static final String INODE_REFERENCE_SECTION_NAME =
      "INodeReferenceSection";
  public static final String INODE_DIRECTORY_SECTION_NAME =
      "INodeDirectorySection";
  public static final String FILE_UNDER_CONSTRUCTION_SECTION_NAME =
      "FileUnderConstructionSection";
  public static final String SNAPSHOT_SECTION_NAME = "SnapshotSection";

  public static final String SECTION_ID = "id";
  public static final String SECTION_REPLICATION = "replication";
  public static final String SECTION_PATH = "path";
  public static final String SECTION_NAME = "name";

  public static final String NAME_SECTION_NAMESPACE_ID = "namespaceId";
  public static final String NAME_SECTION_GENSTAMPV1 = "genstampV1";
  public static final String NAME_SECTION_GENSTAMPV2 = "genstampV2";
  public static final String NAME_SECTION_GENSTAMPV1_LIMIT = "genstampV1Limit";
  public static final String NAME_SECTION_LAST_ALLOCATED_BLOCK_ID =
      "lastAllocatedBlockId";
  public static final String NAME_SECTION_TXID = "txid";
  public static final String NAME_SECTION_ROLLING_UPGRADE_START_TIME =
      "rollingUpgradeStartTime";
  public static final String NAME_SECTION_LAST_ALLOCATED_STRIPED_BLOCK_ID =
      "lastAllocatedStripedBlockId";

  public static final String ERASURE_CODING_SECTION_POLICY =
      "erasureCodingPolicy";
  public static final String ERASURE_CODING_SECTION_POLICY_ID =
      "policyId";
  public static final String ERASURE_CODING_SECTION_POLICY_NAME =
      "policyName";
  public static final String ERASURE_CODING_SECTION_POLICY_CELL_SIZE =
      "cellSize";
  public static final String ERASURE_CODING_SECTION_POLICY_STATE =
      "policyState";
  public static final String ERASURE_CODING_SECTION_SCHEMA =
      "ecSchema";
  public static final String ERASURE_CODING_SECTION_SCHEMA_CODEC_NAME =
      "codecName";
  public static final String ERASURE_CODING_SECTION_SCHEMA_DATA_UNITS =
      "dataUnits";
  public static final String ERASURE_CODING_SECTION_SCHEMA_PARITY_UNITS =
      "parityUnits";
  public static final String ERASURE_CODING_SECTION_SCHEMA_OPTIONS =
      "extraOptions";
  public static final String ERASURE_CODING_SECTION_SCHEMA_OPTION =
      "option";
  public static final String ERASURE_CODING_SECTION_SCHEMA_OPTION_KEY =
      "key";
  public static final String ERASURE_CODING_SECTION_SCHEMA_OPTION_VALUE =
      "value";

  public static final String INODE_SECTION_LAST_INODE_ID = "lastInodeId";
  public static final String INODE_SECTION_NUM_INODES = "numInodes";
  public static final String INODE_SECTION_TYPE = "type";
  public static final String INODE_SECTION_MTIME = "mtime";
  public static final String INODE_SECTION_ATIME = "atime";
  public static final String INODE_SECTION_PREFERRED_BLOCK_SIZE =
      "preferredBlockSize";
  public static final String INODE_SECTION_PERMISSION = "permission";
  public static final String INODE_SECTION_BLOCKS = "blocks";
  public static final String INODE_SECTION_BLOCK = "block";
  public static final String INODE_SECTION_GENSTAMP = "genstamp";
  public static final String INODE_SECTION_NUM_BYTES = "numBytes";
  public static final String INODE_SECTION_FILE_UNDER_CONSTRUCTION =
      "file-under-construction";
  public static final String INODE_SECTION_CLIENT_NAME = "clientName";
  public static final String INODE_SECTION_CLIENT_MACHINE = "clientMachine";
  public static final String INODE_SECTION_ACL = "acl";
  public static final String INODE_SECTION_ACLS = "acls";
  public static final String INODE_SECTION_XATTR = "xattr";
  public static final String INODE_SECTION_XATTRS = "xattrs";
  public static final String INODE_SECTION_STORAGE_POLICY_ID =
      "storagePolicyId";
  public static final String INODE_SECTION_BLOCK_TYPE = "blockType";
  public static final String INODE_SECTION_EC_POLICY_ID =
      "erasureCodingPolicyId";
  public static final String INODE_SECTION_NS_QUOTA = "nsquota";
  public static final String INODE_SECTION_DS_QUOTA = "dsquota";
  public static final String INODE_SECTION_TYPE_QUOTA = "typeQuota";
  public static final String INODE_SECTION_QUOTA = "quota";
  public static final String INODE_SECTION_TARGET = "target";
  public static final String INODE_SECTION_NS = "ns";
  public static final String INODE_SECTION_VAL = "val";
  public static final String INODE_SECTION_VAL_HEX = "valHex";
  public static final String INODE_SECTION_INODE = "inode";

  public static final String SECRET_MANAGER_SECTION_CURRENT_ID = "currentId";
  public static final String SECRET_MANAGER_SECTION_TOKEN_SEQUENCE_NUMBER =
      "tokenSequenceNumber";
  public static final String SECRET_MANAGER_SECTION_NUM_DELEGATION_KEYS =
      "numDelegationKeys";
  public static final String SECRET_MANAGER_SECTION_NUM_TOKENS = "numTokens";
  public static final String SECRET_MANAGER_SECTION_EXPIRY = "expiry";
  public static final String SECRET_MANAGER_SECTION_KEY = "key";
  public static final String SECRET_MANAGER_SECTION_DELEGATION_KEY =
      "delegationKey";
  public static final String SECRET_MANAGER_SECTION_VERSION = "version";
  public static final String SECRET_MANAGER_SECTION_OWNER = "owner";
  public static final String SECRET_MANAGER_SECTION_RENEWER = "renewer";
  public static final String SECRET_MANAGER_SECTION_REAL_USER = "realUser";
  public static final String SECRET_MANAGER_SECTION_ISSUE_DATE = "issueDate";
  public static final String SECRET_MANAGER_SECTION_MAX_DATE = "maxDate";
  public static final String SECRET_MANAGER_SECTION_SEQUENCE_NUMBER =
      "sequenceNumber";
  public static final String SECRET_MANAGER_SECTION_MASTER_KEY_ID =
      "masterKeyId";
  public static final String SECRET_MANAGER_SECTION_EXPIRY_DATE = "expiryDate";
  public static final String SECRET_MANAGER_SECTION_TOKEN = "token";

  public static final String CACHE_MANAGER_SECTION_NEXT_DIRECTIVE_ID =
      "nextDirectiveId";
  public static final String CACHE_MANAGER_SECTION_NUM_POOLS = "numPools";
  public static final String CACHE_MANAGER_SECTION_NUM_DIRECTIVES =
      "numDirectives";
  public static final String CACHE_MANAGER_SECTION_POOL_NAME = "poolName";
  public static final String CACHE_MANAGER_SECTION_OWNER_NAME = "ownerName";
  public static final String CACHE_MANAGER_SECTION_GROUP_NAME = "groupName";
  public static final String CACHE_MANAGER_SECTION_MODE = "mode";
  public static final String CACHE_MANAGER_SECTION_LIMIT = "limit";
  public static final String CACHE_MANAGER_SECTION_MAX_RELATIVE_EXPIRY =
      "maxRelativeExpiry";
  public static final String CACHE_MANAGER_SECTION_POOL = "pool";
  public static final String CACHE_MANAGER_SECTION_EXPIRATION = "expiration";
  public static final String CACHE_MANAGER_SECTION_MILLIS = "millis";
  public static final String CACHE_MANAGER_SECTION_RELATIVE = "relative";
  public static final String CACHE_MANAGER_SECTION_DIRECTIVE = "directive";

  public static final String SNAPSHOT_DIFF_SECTION_INODE_ID = "inodeId";
  public static final String SNAPSHOT_DIFF_SECTION_COUNT = "count";
  public static final String SNAPSHOT_DIFF_SECTION_SNAPSHOT_ID = "snapshotId";
  public static final String SNAPSHOT_DIFF_SECTION_CHILDREN_SIZE =
      "childrenSize";
  public static final String SNAPSHOT_DIFF_SECTION_IS_SNAPSHOT_ROOT =
      "isSnapshotRoot";
  public static final String SNAPSHOT_DIFF_SECTION_SNAPSHOT_COPY =
      "snapshotCopy";
  public static final String SNAPSHOT_DIFF_SECTION_CREATED_LIST_SIZE =
      "createdListSize";
  public static final String SNAPSHOT_DIFF_SECTION_DELETED_INODE =
      "deletedInode";
  public static final String SNAPSHOT_DIFF_SECTION_DELETED_INODE_REF =
      "deletedInoderef";
  public static final String SNAPSHOT_DIFF_SECTION_CREATED = "created";
  public static final String SNAPSHOT_DIFF_SECTION_SIZE = "size";
  public static final String SNAPSHOT_DIFF_SECTION_FILE_DIFF_ENTRY =
      "fileDiffEntry";
  public static final String SNAPSHOT_DIFF_SECTION_DIR_DIFF_ENTRY =
      "dirDiffEntry";
  public static final String SNAPSHOT_DIFF_SECTION_FILE_DIFF = "fileDiff";
  public static final String SNAPSHOT_DIFF_SECTION_DIR_DIFF = "dirDiff";

  public static final String INODE_REFERENCE_SECTION_REFERRED_ID = "referredId";
  public static final String INODE_REFERENCE_SECTION_DST_SNAPSHOT_ID =
      "dstSnapshotId";
  public static final String INODE_REFERENCE_SECTION_LAST_SNAPSHOT_ID =
      "lastSnapshotId";
  public static final String INODE_REFERENCE_SECTION_REF = "ref";

  public static final String INODE_DIRECTORY_SECTION_PARENT = "parent";
  public static final String INODE_DIRECTORY_SECTION_CHILD = "child";
  public static final String INODE_DIRECTORY_SECTION_REF_CHILD = "refChild";
  public static final String INODE_DIRECTORY_SECTION_DIRECTORY = "directory";

  public static final String SNAPSHOT_SECTION_SNAPSHOT_COUNTER =
      "snapshotCounter";
  public static final String SNAPSHOT_SECTION_NUM_SNAPSHOTS = "numSnapshots";
  public static final String SNAPSHOT_SECTION_SNAPSHOT_TABLE_DIR =
      "snapshottableDir";
  public static final String SNAPSHOT_SECTION_DIR = "dir";
  public static final String SNAPSHOT_SECTION_ROOT = "root";
  public static final String SNAPSHOT_SECTION_SNAPSHOT = "snapshot";

  private final Configuration conf;
  private final PrintStream out;
  private final SimpleDateFormat isoDateFormat;
  private SerialNumberManager.StringTable stringTable;

  public static SimpleDateFormat createSimpleDateFormat() {
    SimpleDateFormat format =
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    format.setTimeZone(TimeZone.getTimeZone("UTC"));
    return format;
  }

  public PBImageXmlWriter(Configuration conf, PrintStream out) {
    this.conf = conf;
    this.out = out;
    this.isoDateFormat = createSimpleDateFormat();
  }

  public void visit(RandomAccessFile file) throws IOException {
    if (!FSImageUtil.checkFileFormat(file)) {
      throw new IOException("Unrecognized FSImage");
    }

    FileSummary summary = FSImageUtil.loadSummary(file);
    try (FileInputStream fin = new FileInputStream(file.getFD())) {
      out.print("<?xml version=\"1.0\"?>\n<fsimage>");

      out.print("<version>");
      o("layoutVersion", summary.getLayoutVersion());
      o("onDiskVersion", summary.getOndiskVersion());
      // Output the version of OIV (which is not necessarily the version of
      // the fsimage file).  This could be helpful in the case where a bug
      // in OIV leads to information loss in the XML-- we can quickly tell
      // if a specific fsimage XML file is affected by this bug.
      o("oivRevision", VersionInfo.getRevision());
      out.print("</version>\n");

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

      for (FileSummary.Section s : sections) {
        fin.getChannel().position(s.getOffset());
        InputStream is = FSImageUtil.wrapInputStreamForCompression(conf,
            summary.getCodec(), new BufferedInputStream(new LimitInputStream(
                fin, s.getLength())));

        SectionName sectionName = SectionName.fromString(s.getName());
        if (sectionName == null) {
          throw new IOException("Unrecognized section " + s.getName());
        }
        switch (sectionName) {
        case NS_INFO:
          dumpNameSection(is);
          break;
        case STRING_TABLE:
          loadStringTable(is);
          break;
        case ERASURE_CODING:
          dumpErasureCodingSection(is);
          break;
        case INODE:
          dumpINodeSection(is);
          break;
        case INODE_REFERENCE:
          dumpINodeReferenceSection(is);
          break;
        case INODE_DIR:
          dumpINodeDirectorySection(is);
          break;
        case FILES_UNDERCONSTRUCTION:
          dumpFileUnderConstructionSection(is);
          break;
        case SNAPSHOT:
          dumpSnapshotSection(is);
          break;
        case SNAPSHOT_DIFF:
          dumpSnapshotDiffSection(is);
          break;
        case SECRET_MANAGER:
          dumpSecretManagerSection(is);
          break;
        case CACHE_MANAGER:
          dumpCacheManagerSection(is);
          break;
        default:
          break;
        }
      }
      out.print("</fsimage>\n");
    }
  }

  private void dumpCacheManagerSection(InputStream is) throws IOException {
    out.print("<" + CACHE_MANAGER_SECTION_NAME + ">");
    CacheManagerSection s = CacheManagerSection.parseDelimitedFrom(is);
    o(CACHE_MANAGER_SECTION_NEXT_DIRECTIVE_ID, s.getNextDirectiveId());
    o(CACHE_MANAGER_SECTION_NUM_DIRECTIVES, s.getNumDirectives());
    o(CACHE_MANAGER_SECTION_NUM_POOLS, s.getNumPools());
    for (int i = 0; i < s.getNumPools(); ++i) {
      CachePoolInfoProto p = CachePoolInfoProto.parseDelimitedFrom(is);
      out.print("<" + CACHE_MANAGER_SECTION_POOL +">");
      o(CACHE_MANAGER_SECTION_POOL_NAME, p.getPoolName()).
          o(CACHE_MANAGER_SECTION_OWNER_NAME, p.getOwnerName())
          .o(CACHE_MANAGER_SECTION_GROUP_NAME, p.getGroupName())
          .o(CACHE_MANAGER_SECTION_MODE, p.getMode())
          .o(CACHE_MANAGER_SECTION_LIMIT, p.getLimit())
          .o(CACHE_MANAGER_SECTION_MAX_RELATIVE_EXPIRY,
              p.getMaxRelativeExpiry());
      out.print("</" + CACHE_MANAGER_SECTION_POOL + ">\n");
    }
    for (int i = 0; i < s.getNumDirectives(); ++i) {
      CacheDirectiveInfoProto p = CacheDirectiveInfoProto
          .parseDelimitedFrom(is);
      out.print("<" + CACHE_MANAGER_SECTION_DIRECTIVE + ">");
      o(SECTION_ID, p.getId()).o(SECTION_PATH, p.getPath())
          .o(SECTION_REPLICATION, p.getReplication())
          .o(CACHE_MANAGER_SECTION_POOL, p.getPool());
      out.print("<" + CACHE_MANAGER_SECTION_EXPIRATION +">");
      CacheDirectiveInfoExpirationProto e = p.getExpiration();
      o(CACHE_MANAGER_SECTION_MILLIS, e.getMillis())
          .o(CACHE_MANAGER_SECTION_RELATIVE, e.getIsRelative());
      out.print("</" + CACHE_MANAGER_SECTION_EXPIRATION+ ">\n");
      out.print("</" + CACHE_MANAGER_SECTION_DIRECTIVE + ">\n");
    }
    out.print("</" + CACHE_MANAGER_SECTION_NAME + ">\n");

  }

  private void dumpFileUnderConstructionSection(InputStream in)
      throws IOException {
    out.print("<" + FILE_UNDER_CONSTRUCTION_SECTION_NAME + ">");
    while (true) {
      FileUnderConstructionEntry e = FileUnderConstructionEntry
          .parseDelimitedFrom(in);
      if (e == null) {
        break;
      }
      out.print("<" + INODE_SECTION_INODE + ">");
      o(SECTION_ID, e.getInodeId())
          .o(SECTION_PATH, e.getFullPath());
      out.print("</" + INODE_SECTION_INODE + ">\n");
    }
    out.print("</" + FILE_UNDER_CONSTRUCTION_SECTION_NAME + ">\n");
  }

  private void dumpXattrs(INodeSection.XAttrFeatureProto xattrs) {
    out.print("<" + INODE_SECTION_XATTRS + ">");
    for (INodeSection.XAttrCompactProto xattr : xattrs.getXAttrsList()) {
      out.print("<" + INODE_SECTION_XATTR + ">");
      int encodedName = xattr.getName();
      int ns = (XATTR_NAMESPACE_MASK & (encodedName >> XATTR_NAMESPACE_OFFSET)) |
          ((XATTR_NAMESPACE_EXT_MASK & (encodedName >> XATTR_NAMESPACE_EXT_OFFSET)) << 2);
      o(INODE_SECTION_NS, XAttrProtos.XAttrProto.
          XAttrNamespaceProto.valueOf(ns).toString());
      o(SECTION_NAME, SerialNumberManager.XATTR.getString(
          XATTR_NAME_MASK & (encodedName >> XATTR_NAME_OFFSET),
          stringTable));
      ByteString val = xattr.getValue();
      if (val.isValidUtf8()) {
        o(INODE_SECTION_VAL, val.toStringUtf8());
      } else {
        o(INODE_SECTION_VAL_HEX, Hex.encodeHexString(val.toByteArray()));
      }
      out.print("</" + INODE_SECTION_XATTR + ">");
    }
    out.print("</" + INODE_SECTION_XATTRS + ">");
  }

  private void dumpINodeDirectory(INodeDirectory d) {
    o(INODE_SECTION_MTIME, d.getModificationTime())
        .o(INODE_SECTION_PERMISSION, dumpPermission(d.getPermission()));
    if (d.hasXAttrs()) {
      dumpXattrs(d.getXAttrs());
    }
    dumpAcls(d.getAcl());
    if (d.hasDsQuota() && d.hasNsQuota()) {
      o(INODE_SECTION_NS_QUOTA, d.getNsQuota())
        .o(INODE_SECTION_DS_QUOTA, d.getDsQuota());
    }
    INodeSection.QuotaByStorageTypeFeatureProto typeQuotas =
      d.getTypeQuotas();
    if (typeQuotas != null) {
      for (INodeSection.QuotaByStorageTypeEntryProto entry:
            typeQuotas.getQuotasList()) {
        out.print("<" + INODE_SECTION_TYPE_QUOTA + ">");
        o(INODE_SECTION_TYPE, entry.getStorageType().toString());
        o(INODE_SECTION_QUOTA, entry.getQuota());
        out.print("</" + INODE_SECTION_TYPE_QUOTA + ">");
      }
    }
  }

  private void dumpINodeDirectorySection(InputStream in) throws IOException {
    out.print("<" + INODE_DIRECTORY_SECTION_NAME + ">");
    while (true) {
      INodeDirectorySection.DirEntry e = INodeDirectorySection.DirEntry
          .parseDelimitedFrom(in);
      // note that in is a LimitedInputStream
      if (e == null) {
        break;
      }
      out.print("<" + INODE_DIRECTORY_SECTION_DIRECTORY + ">");
      o(INODE_DIRECTORY_SECTION_PARENT, e.getParent());
      for (long id : e.getChildrenList()) {
        o(INODE_DIRECTORY_SECTION_CHILD, id);
      }
      for (int refId : e.getRefChildrenList()) {
        o(INODE_DIRECTORY_SECTION_REF_CHILD, refId);
      }
      out.print("</" + INODE_DIRECTORY_SECTION_DIRECTORY + ">\n");
    }
    out.print("</" + INODE_DIRECTORY_SECTION_NAME + ">\n");
  }

  private void dumpINodeReferenceSection(InputStream in) throws IOException {
    out.print("<" + INODE_REFERENCE_SECTION_NAME + ">");
    while (true) {
      INodeReferenceSection.INodeReference e = INodeReferenceSection
          .INodeReference.parseDelimitedFrom(in);
      if (e == null) {
        break;
      }
      dumpINodeReference(e);
    }
    out.print("</" + INODE_REFERENCE_SECTION_NAME + ">");
  }

  private void dumpINodeReference(INodeReferenceSection.INodeReference r) {
    out.print("<" + INODE_REFERENCE_SECTION_REF + ">");
    o(INODE_REFERENCE_SECTION_REFERRED_ID, r.getReferredId())
        .o(SECTION_NAME, r.getName().toStringUtf8())
            .o(INODE_REFERENCE_SECTION_DST_SNAPSHOT_ID, r.getDstSnapshotId())
            .o(INODE_REFERENCE_SECTION_LAST_SNAPSHOT_ID,
            r.getLastSnapshotId());
    out.print("</" + INODE_REFERENCE_SECTION_REF + ">\n");
  }

  private void dumpINodeFile(INodeSection.INodeFile f) {
    if (f.hasErasureCodingPolicyID()) {
      o(SECTION_REPLICATION, INodeFile.DEFAULT_REPL_FOR_STRIPED_BLOCKS);
    } else {
      o(SECTION_REPLICATION, f.getReplication());
    }
    o(INODE_SECTION_MTIME, f.getModificationTime())
        .o(INODE_SECTION_ATIME, f.getAccessTime())
        .o(INODE_SECTION_PREFERRED_BLOCK_SIZE, f.getPreferredBlockSize())
        .o(INODE_SECTION_PERMISSION, dumpPermission(f.getPermission()));
    if (f.hasXAttrs()) {
      dumpXattrs(f.getXAttrs());
    }
    dumpAcls(f.getAcl());
    if (f.getBlocksCount() > 0) {
      out.print("<" + INODE_SECTION_BLOCKS + ">");
      for (BlockProto b : f.getBlocksList()) {
        out.print("<" + INODE_SECTION_BLOCK + ">");
        o(SECTION_ID, b.getBlockId())
            .o(INODE_SECTION_GENSTAMP, b.getGenStamp())
            .o(INODE_SECTION_NUM_BYTES, b.getNumBytes());
        out.print("</" + INODE_SECTION_BLOCK + ">\n");
      }
      out.print("</" + INODE_SECTION_BLOCKS + ">\n");
    }
    if (f.hasStoragePolicyID()) {
      o(INODE_SECTION_STORAGE_POLICY_ID, f.getStoragePolicyID());
    }
    if (f.hasErasureCodingPolicyID()) {
      o(INODE_SECTION_BLOCK_TYPE, f.getBlockType().name());
      o(INODE_SECTION_EC_POLICY_ID, f.getErasureCodingPolicyID());
    }

    if (f.hasFileUC()) {
      INodeSection.FileUnderConstructionFeature u = f.getFileUC();
      out.print("<" + INODE_SECTION_FILE_UNDER_CONSTRUCTION + ">");
      o(INODE_SECTION_CLIENT_NAME, u.getClientName())
          .o(INODE_SECTION_CLIENT_MACHINE, u.getClientMachine());
      out.print("</" + INODE_SECTION_FILE_UNDER_CONSTRUCTION + ">\n");
    }
  }

  private void dumpAcls(AclFeatureProto aclFeatureProto) {
    ImmutableList<AclEntry> aclEntryList = FSImageFormatPBINode.Loader
        .loadAclEntries(aclFeatureProto, stringTable);
    if (aclEntryList.size() > 0) {
      out.print("<" + INODE_SECTION_ACLS + ">");
      for (AclEntry aclEntry : aclEntryList) {
        o(INODE_SECTION_ACL, aclEntry.toString());
      }
      out.print("</" + INODE_SECTION_ACLS + ">");
    }
  }

  private void dumpErasureCodingSection(InputStream in) throws IOException {
    ErasureCodingSection s = ErasureCodingSection.parseDelimitedFrom(in);
    if (s.getPoliciesCount() > 0) {
      out.println("<" + ERASURE_CODING_SECTION_NAME + ">");
      for (int i = 0; i < s.getPoliciesCount(); ++i) {
        HdfsProtos.ErasureCodingPolicyProto policy = s.getPolicies(i);
        dumpErasureCodingPolicy(PBHelperClient
            .convertErasureCodingPolicyInfo(policy));
      }
      out.println("</" + ERASURE_CODING_SECTION_NAME + ">\n");
    }
  }

  private void dumpErasureCodingPolicy(ErasureCodingPolicyInfo ecPolicyInfo) {
    ErasureCodingPolicy ecPolicy = ecPolicyInfo.getPolicy();
    out.println("<" + ERASURE_CODING_SECTION_POLICY + ">");
    o(ERASURE_CODING_SECTION_POLICY_ID, ecPolicy.getId());
    o(ERASURE_CODING_SECTION_POLICY_NAME, ecPolicy.getName());
    o(ERASURE_CODING_SECTION_POLICY_CELL_SIZE, ecPolicy.getCellSize());
    o(ERASURE_CODING_SECTION_POLICY_STATE, ecPolicyInfo.getState());
    out.println("<" + ERASURE_CODING_SECTION_SCHEMA + ">");
    ECSchema schema = ecPolicy.getSchema();
    o(ERASURE_CODING_SECTION_SCHEMA_CODEC_NAME, schema.getCodecName());
    o(ERASURE_CODING_SECTION_SCHEMA_DATA_UNITS, schema.getNumDataUnits());
    o(ERASURE_CODING_SECTION_SCHEMA_PARITY_UNITS,
        schema.getNumParityUnits());
    if (schema.getExtraOptions().size() > 0) {
      out.println("<" + ERASURE_CODING_SECTION_SCHEMA_OPTIONS + ">");
      for (Map.Entry<String, String> option :
          schema.getExtraOptions().entrySet()) {
        out.println("<" + ERASURE_CODING_SECTION_SCHEMA_OPTION + ">");
        o(ERASURE_CODING_SECTION_SCHEMA_OPTION_KEY, option.getKey());
        o(ERASURE_CODING_SECTION_SCHEMA_OPTION_VALUE, option.getValue());
        out.println("</" + ERASURE_CODING_SECTION_SCHEMA_OPTION + ">");
      }
      out.println("</" + ERASURE_CODING_SECTION_SCHEMA_OPTIONS + ">");
    }
    out.println("</" + ERASURE_CODING_SECTION_SCHEMA + ">");
    out.println("</" + ERASURE_CODING_SECTION_POLICY + ">\n");
  }

  private void dumpINodeSection(InputStream in) throws IOException {
    INodeSection s = INodeSection.parseDelimitedFrom(in);
    out.print("<" + INODE_SECTION_NAME + ">");
    o(INODE_SECTION_LAST_INODE_ID, s.getLastInodeId());
    o(INODE_SECTION_NUM_INODES, s.getNumInodes());
    for (int i = 0; i < s.getNumInodes(); ++i) {
      INodeSection.INode p = INodeSection.INode.parseDelimitedFrom(in);
      out.print("<" + INODE_SECTION_INODE + ">");
      dumpINodeFields(p);
      out.print("</" + INODE_SECTION_INODE + ">\n");
    }
    out.print("</" + INODE_SECTION_NAME + ">\n");
  }

  private void dumpINodeFields(INodeSection.INode p) {
    o(SECTION_ID, p.getId()).o(INODE_SECTION_TYPE, p.getType())
            .o(SECTION_NAME, p.getName().toStringUtf8());
    if (p.hasFile()) {
      dumpINodeFile(p.getFile());
    } else if (p.hasDirectory()) {
      dumpINodeDirectory(p.getDirectory());
    } else if (p.hasSymlink()) {
      dumpINodeSymlink(p.getSymlink());
    }
  }

  private void dumpINodeSymlink(INodeSymlink s) {
    o(INODE_SECTION_PERMISSION, dumpPermission(s.getPermission()))
        .o(INODE_SECTION_TARGET, s.getTarget().toStringUtf8())
        .o(INODE_SECTION_MTIME, s.getModificationTime())
        .o(INODE_SECTION_ATIME, s.getAccessTime());
  }

  private void dumpNameSection(InputStream in) throws IOException {
    NameSystemSection s = NameSystemSection.parseDelimitedFrom(in);
    out.print("<" + NAME_SECTION_NAME + ">");
    o(NAME_SECTION_NAMESPACE_ID, s.getNamespaceId());
    o(NAME_SECTION_GENSTAMPV1, s.getGenstampV1())
        .o(NAME_SECTION_GENSTAMPV2, s.getGenstampV2())
        .o(NAME_SECTION_GENSTAMPV1_LIMIT, s.getGenstampV1Limit())
        .o(NAME_SECTION_LAST_ALLOCATED_BLOCK_ID,
            s.getLastAllocatedBlockId())
        .o(NAME_SECTION_TXID, s.getTransactionId());
    out.print("</" + NAME_SECTION_NAME + ">\n");
  }

  private String dumpPermission(long permission) {
    PermissionStatus permStatus = FSImageFormatPBINode.Loader.
        loadPermission(permission, stringTable);
    return String.format("%s:%s:%04o", permStatus.getUserName(),
        permStatus.getGroupName(), permStatus.getPermission().toExtendedShort());
  }

  private void dumpSecretManagerSection(InputStream is) throws IOException {
    out.print("<" + SECRET_MANAGER_SECTION_NAME + ">");
    SecretManagerSection s = SecretManagerSection.parseDelimitedFrom(is);
    int expectedNumDelegationKeys = s.getNumKeys();
    int expectedNumTokens = s.getNumTokens();
    o(SECRET_MANAGER_SECTION_CURRENT_ID, s.getCurrentId())
        .o(SECRET_MANAGER_SECTION_TOKEN_SEQUENCE_NUMBER,
            s.getTokenSequenceNumber()).
        o(SECRET_MANAGER_SECTION_NUM_DELEGATION_KEYS,
            expectedNumDelegationKeys).
        o(SECRET_MANAGER_SECTION_NUM_TOKENS, expectedNumTokens);
    for (int i = 0; i < expectedNumDelegationKeys; i++) {
      SecretManagerSection.DelegationKey dkey =
          SecretManagerSection.DelegationKey.parseDelimitedFrom(is);
      out.print("<" + SECRET_MANAGER_SECTION_DELEGATION_KEY + ">");
      o(SECTION_ID, dkey.getId());
      o(SECRET_MANAGER_SECTION_KEY,
          Hex.encodeHexString(dkey.getKey().toByteArray()));
      if (dkey.hasExpiryDate()) {
        dumpDate(SECRET_MANAGER_SECTION_EXPIRY, dkey.getExpiryDate());
      }
      out.print("</" + SECRET_MANAGER_SECTION_DELEGATION_KEY + ">");
    }
    for (int i = 0; i < expectedNumTokens; i++) {
      SecretManagerSection.PersistToken token =
          SecretManagerSection.PersistToken.parseDelimitedFrom(is);
      out.print("<" + SECRET_MANAGER_SECTION_TOKEN + ">");
      if (token.hasVersion()) {
        o(SECRET_MANAGER_SECTION_VERSION, token.getVersion());
      }
      if (token.hasOwner()) {
        o(SECRET_MANAGER_SECTION_OWNER, token.getOwner());
      }
      if (token.hasRenewer()) {
        o(SECRET_MANAGER_SECTION_RENEWER, token.getRenewer());
      }
      if (token.hasRealUser()) {
        o(SECRET_MANAGER_SECTION_REAL_USER, token.getRealUser());
      }
      if (token.hasIssueDate()) {
        dumpDate(SECRET_MANAGER_SECTION_ISSUE_DATE, token.getIssueDate());
      }
      if (token.hasMaxDate()) {
        dumpDate(SECRET_MANAGER_SECTION_MAX_DATE, token.getMaxDate());
      }
      if (token.hasSequenceNumber()) {
        o(SECRET_MANAGER_SECTION_SEQUENCE_NUMBER,
            token.getSequenceNumber());
      }
      if (token.hasMasterKeyId()) {
        o(SECRET_MANAGER_SECTION_MASTER_KEY_ID, token.getMasterKeyId());
      }
      if (token.hasExpiryDate()) {
        dumpDate(SECRET_MANAGER_SECTION_EXPIRY_DATE, token.getExpiryDate());
      }
      out.print("</" + SECRET_MANAGER_SECTION_TOKEN + ">");
    }
    out.print("</" + SECRET_MANAGER_SECTION_NAME + ">");
  }

  private void dumpDate(String tag, long date) {
    out.print("<" + tag + ">" +
      isoDateFormat.format(new Date(date)) + "</" + tag + ">");
  }

  private void dumpSnapshotDiffSection(InputStream in) throws IOException {
    out.print("<" + SNAPSHOT_DIFF_SECTION_NAME + ">");
    while (true) {
      SnapshotDiffSection.DiffEntry e = SnapshotDiffSection.DiffEntry
          .parseDelimitedFrom(in);
      if (e == null) {
        break;
      }
      switch (e.getType()) {
      case FILEDIFF:
        out.print("<" + SNAPSHOT_DIFF_SECTION_FILE_DIFF_ENTRY + ">");
        break;
      case DIRECTORYDIFF:
        out.print("<" + SNAPSHOT_DIFF_SECTION_DIR_DIFF_ENTRY + ">");
        break;
      default:
        throw new IOException("unknown DiffEntry type " + e.getType());
      }
      o(SNAPSHOT_DIFF_SECTION_INODE_ID, e.getInodeId());
      o(SNAPSHOT_DIFF_SECTION_COUNT, e.getNumOfDiff());
      switch (e.getType()) {
      case FILEDIFF: {
        for (int i = 0; i < e.getNumOfDiff(); ++i) {
          out.print("<" + SNAPSHOT_DIFF_SECTION_FILE_DIFF + ">");
          SnapshotDiffSection.FileDiff f = SnapshotDiffSection.FileDiff
              .parseDelimitedFrom(in);
          o(SNAPSHOT_DIFF_SECTION_SNAPSHOT_ID, f.getSnapshotId())
              .o(SNAPSHOT_DIFF_SECTION_SIZE, f.getFileSize())
              .o(SECTION_NAME, f.getName().toStringUtf8());
          INodeSection.INodeFile snapshotCopy = f.getSnapshotCopy();
          if (snapshotCopy != null) {
            out.print("<" + SNAPSHOT_DIFF_SECTION_SNAPSHOT_COPY + ">");
            dumpINodeFile(snapshotCopy);
            out.print("</" + SNAPSHOT_DIFF_SECTION_SNAPSHOT_COPY + ">\n");
          }
          if (f.getBlocksCount() > 0) {
            out.print("<" + INODE_SECTION_BLOCKS + ">");
            for (BlockProto b : f.getBlocksList()) {
              out.print("<" + INODE_SECTION_BLOCK + ">");
              o(SECTION_ID, b.getBlockId())
                  .o(INODE_SECTION_GENSTAMP, b.getGenStamp())
                  .o(INODE_SECTION_NUM_BYTES, b.getNumBytes());
              out.print("</" + INODE_SECTION_BLOCK + ">\n");
            }
            out.print("</" + INODE_SECTION_BLOCKS + ">\n");
          }
          out.print("</" + SNAPSHOT_DIFF_SECTION_FILE_DIFF + ">\n");
        }
      }
        break;
      case DIRECTORYDIFF: {
        for (int i = 0; i < e.getNumOfDiff(); ++i) {
          out.print("<" + SNAPSHOT_DIFF_SECTION_DIR_DIFF + ">");
          SnapshotDiffSection.DirectoryDiff d = SnapshotDiffSection.DirectoryDiff
              .parseDelimitedFrom(in);
          o(SNAPSHOT_DIFF_SECTION_SNAPSHOT_ID, d.getSnapshotId())
              .o(SNAPSHOT_DIFF_SECTION_CHILDREN_SIZE, d.getChildrenSize())
              .o(SNAPSHOT_DIFF_SECTION_IS_SNAPSHOT_ROOT, d.getIsSnapshotRoot())
              .o(SECTION_NAME, d.getName().toStringUtf8());
          if (d.hasSnapshotCopy()) {
            out.print("<" + SNAPSHOT_DIFF_SECTION_SNAPSHOT_COPY + ">");
            dumpINodeDirectory(d.getSnapshotCopy());
            out.print("</" + SNAPSHOT_DIFF_SECTION_SNAPSHOT_COPY + ">\n");
          }
          o(SNAPSHOT_DIFF_SECTION_CREATED_LIST_SIZE, d.getCreatedListSize());
          for (long did : d.getDeletedINodeList()) {
            o(SNAPSHOT_DIFF_SECTION_DELETED_INODE, did);
          }
          for (int dRefid : d.getDeletedINodeRefList()) {
            o(SNAPSHOT_DIFF_SECTION_DELETED_INODE_REF, dRefid);
          }
          for (int j = 0; j < d.getCreatedListSize(); ++j) {
            SnapshotDiffSection.CreatedListEntry ce = SnapshotDiffSection.CreatedListEntry
                .parseDelimitedFrom(in);
            out.print("<" + SNAPSHOT_DIFF_SECTION_CREATED + ">");
            o(SECTION_NAME, ce.getName().toStringUtf8());
            out.print("</" + SNAPSHOT_DIFF_SECTION_CREATED + ">\n");
          }
          out.print("</" + SNAPSHOT_DIFF_SECTION_DIR_DIFF + ">\n");
        }
        break;
      }
      default:
        break;
      }
      switch (e.getType()) {
      case FILEDIFF:
        out.print("</" + SNAPSHOT_DIFF_SECTION_FILE_DIFF_ENTRY + ">");
        break;
      case DIRECTORYDIFF:
        out.print("</" + SNAPSHOT_DIFF_SECTION_DIR_DIFF_ENTRY + ">");
        break;
      default:
        throw new IOException("unknown DiffEntry type " + e.getType());
      }
    }
    out.print("</" + SNAPSHOT_DIFF_SECTION_NAME + ">\n");
  }

  private void dumpSnapshotSection(InputStream in) throws IOException {
    out.print("<" + SNAPSHOT_SECTION_NAME + ">");
    SnapshotSection s = SnapshotSection.parseDelimitedFrom(in);
    o(SNAPSHOT_SECTION_SNAPSHOT_COUNTER, s.getSnapshotCounter());
    o(SNAPSHOT_SECTION_NUM_SNAPSHOTS, s.getNumSnapshots());
    if (s.getSnapshottableDirCount() > 0) {
      out.print("<" + SNAPSHOT_SECTION_SNAPSHOT_TABLE_DIR + ">");
      for (long id : s.getSnapshottableDirList()) {
        o(SNAPSHOT_SECTION_DIR, id);
      }
      out.print("</" + SNAPSHOT_SECTION_SNAPSHOT_TABLE_DIR + ">\n");
    }
    for (int i = 0; i < s.getNumSnapshots(); ++i) {
      SnapshotSection.Snapshot pbs = SnapshotSection.Snapshot
          .parseDelimitedFrom(in);
      out.print("<" + SNAPSHOT_SECTION_SNAPSHOT + ">");
      o(SECTION_ID, pbs.getSnapshotId());
      out.print("<" + SNAPSHOT_SECTION_ROOT + ">");
      dumpINodeFields(pbs.getRoot());
      out.print("</" + SNAPSHOT_SECTION_ROOT + ">");
      out.print("</" + SNAPSHOT_SECTION_SNAPSHOT + ">");
    }
    out.print("</" + SNAPSHOT_SECTION_NAME + ">\n");
  }

  private void loadStringTable(InputStream in) throws IOException {
    stringTable = FSImageLoader.loadStringTable(in);
  }

  private PBImageXmlWriter o(final String e, final Object v) {
    if (v instanceof Boolean) {
      // For booleans, the presence of the element indicates true, and its
      // absence indicates false.
      if ((Boolean)v != false) {
        out.print("<" + e + "/>");
      }
      return this;
    }
    out.print("<" + e + ">" +
        XMLUtils.mangleXmlString(v.toString(), true) + "</" + e + ">");
    return this;
  }
}
