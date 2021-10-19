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

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.ACL_ENTRY_NAME_MASK;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.ACL_ENTRY_NAME_OFFSET;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.ACL_ENTRY_SCOPE_OFFSET;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.ACL_ENTRY_TYPE_OFFSET;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.XATTR_NAMESPACE_MASK;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.XATTR_NAME_OFFSET;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.XATTR_NAMESPACE_OFFSET;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.XATTR_NAMESPACE_EXT_OFFSET;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.XATTR_NAMESPACE_EXT_MASK;
import static org.apache.hadoop.hdfs.tools.offlineImageViewer.PBImageXmlWriter.*;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

import org.apache.hadoop.thirdparty.com.google.common.io.CountingOutputStream;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Ints;
import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ECSchemaProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ErasureCodingPolicyProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheDirectiveInfoExpirationProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheDirectiveInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CachePoolInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos;
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
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SnapshotDiffSection.DiffEntry;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.hdfs.util.XMLUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.StringUtils;

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class OfflineImageReconstructor {
  public static final Logger LOG =
      LoggerFactory.getLogger(OfflineImageReconstructor.class);

  /**
   * The output stream.
   */
  private final CountingOutputStream out;

  /**
   * A source of XML events based on the input file.
   */
  private final XMLEventReader events;

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
  private final HashMap<String, Integer> stringTable = new HashMap<>();

  /**
   * The date formatter to use with fsimage XML files.
   */
  private final SimpleDateFormat isoDateFormat;

  /**
   * The latest string ID.  See registerStringId for details.
   */
  private int latestStringId = 0;

  private static final String EMPTY_STRING = "";

  private OfflineImageReconstructor(CountingOutputStream out,
      InputStreamReader reader) throws XMLStreamException {
    this.out = out;
    XMLInputFactory factory = XMLInputFactory.newInstance();
    this.events = factory.createXMLEventReader(reader);
    this.sections = new HashMap<>();
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
    this.isoDateFormat = PBImageXmlWriter.createSimpleDateFormat();
  }

  /**
   * Read the next tag start or end event.
   *
   * @param expected     The name of the next tag we expect.
   *                     We will validate that the tag has this name,
   *                     unless this string is enclosed in braces.
   * @param allowEnd     If true, we will also end events.
   *                     If false, end events cause an exception.
   *
   * @return             The next tag start or end event.
   */
  private XMLEvent expectTag(String expected, boolean allowEnd)
      throws IOException {
    XMLEvent ev = null;
    while (true) {
      try {
        ev = events.nextEvent();
      } catch (XMLStreamException e) {
        throw new IOException("Expecting " + expected +
            ", but got XMLStreamException", e);
      }
      switch (ev.getEventType()) {
      case XMLEvent.ATTRIBUTE:
        throw new IOException("Got unexpected attribute: " + ev);
      case XMLEvent.CHARACTERS:
        if (!ev.asCharacters().isWhiteSpace()) {
          throw new IOException("Got unxpected characters while " +
              "looking for " + expected + ": " +
              ev.asCharacters().getData());
        }
        break;
      case XMLEvent.END_ELEMENT:
        if (!allowEnd) {
          throw new IOException("Got unexpected end event " +
              "while looking for " + expected);
        }
        return ev;
      case XMLEvent.START_ELEMENT:
        if (!expected.startsWith("[")) {
          if (!ev.asStartElement().getName().getLocalPart().
                equals(expected)) {
            throw new IOException("Failed to find <" + expected + ">; " +
                "got " + ev.asStartElement().getName().getLocalPart() +
                " instead.");
          }
        }
        return ev;
      default:
        // Ignore other event types like comment, etc.
        if (LOG.isTraceEnabled()) {
          LOG.trace("Skipping XMLEvent of type " +
              ev.getEventType() + "(" +  ev + ")");
        }
        break;
      }
    }
  }

  private void expectTagEnd(String expected) throws IOException {
    XMLEvent ev = expectTag(expected, true);
    if (ev.getEventType() != XMLStreamConstants.END_ELEMENT) {
      throw new IOException("Expected tag end event for " + expected +
            ", but got: " + ev);
    }
    if (!expected.startsWith("[")) {
      String tag = ev.asEndElement().getName().getLocalPart();
      if (!tag.equals(expected)) {
        throw new IOException("Expected tag end event for " + expected +
        ", but got tag end event for " + tag);
      }
    }
  }

  private static class Node {
    private static final String EMPTY = "";
    HashMap<String, LinkedList<Node>> children;
    String val = EMPTY;

    void addChild(String key, Node node) {
      if (children == null) {
        children = new HashMap<>();
      }
      LinkedList<Node> cur = children.get(key);
      if (cur == null) {
        cur = new LinkedList<>();
        children.put(key, cur);
      }
      cur.add(node);
    }

    Node removeChild(String key) {
      if (children == null) {
        return null;
      }
      LinkedList<Node> cur = children.get(key);
      if (cur == null) {
        return null;
      }
      Node node = cur.remove();
      if ((node == null) || cur.isEmpty()) {
        children.remove(key);
      }
      return node;
    }

    String removeChildStr(String key) {
      Node child = removeChild(key);
      if (child == null) {
        return null;
      }
      if ((child.children != null) && (!child.children.isEmpty())) {
        throw new RuntimeException("Node " + key + " contains children " +
            "of its own.");
      }
      return child.getVal();
    }

    Integer removeChildInt(String key) throws IOException {
      String str = removeChildStr(key);
      if (str == null) {
        return null;
      }
      return Integer.valueOf(str);
    }

    Long removeChildLong(String key) throws IOException {
      String str = removeChildStr(key);
      if (str == null) {
        return null;
      }
      return Long.valueOf(str);
    }

    boolean removeChildBool(String key) throws IOException {
      String str = removeChildStr(key);
      if (str == null) {
        return false;
      }
      return true;
    }

    String getRemainingKeyNames() {
      if (children == null) {
        return "";
      }
      return StringUtils.join(", ", children.keySet());
    }

    void verifyNoRemainingKeys(String sectionName) throws IOException {
      String remainingKeyNames = getRemainingKeyNames();
      if (!remainingKeyNames.isEmpty()) {
        throw new IOException("Found unknown XML keys in " +
            sectionName + ": " + remainingKeyNames);
      }
    }

    void setVal(String val) {
      this.val = val;
    }

    String getVal() {
      return val;
    }

    String dump() {
      StringBuilder bld = new StringBuilder();
      if ((children != null) && (!children.isEmpty())) {
        bld.append("{");
      }
      if (val != null) {
        bld.append("[").append(val).append("]");
      }
      if ((children != null) && (!children.isEmpty())) {
        String prefix = "";
        for (Map.Entry<String, LinkedList<Node>> entry : children.entrySet()) {
          for (Node n : entry.getValue()) {
            bld.append(prefix);
            bld.append(entry.getKey()).append(": ");
            bld.append(n.dump());
            prefix = ", ";
          }
        }
        bld.append("}");
      }
      return bld.toString();
    }
  }

  private void loadNodeChildrenHelper(Node parent, String expected,
                                String terminators[]) throws IOException {
    XMLEvent ev = null;
    while (true) {
      try {
        ev = events.peek();
        switch (ev.getEventType()) {
        case XMLEvent.END_ELEMENT:
          if (terminators.length != 0) {
            return;
          }
          events.nextEvent();
          return;
        case XMLEvent.START_ELEMENT:
          String key = ev.asStartElement().getName().getLocalPart();
          for (String terminator : terminators) {
            if (terminator.equals(key)) {
              return;
            }
          }
          events.nextEvent();
          Node node = new Node();
          parent.addChild(key, node);
          loadNodeChildrenHelper(node, expected, new String[0]);
          break;
        case XMLEvent.CHARACTERS:
          String val = XMLUtils.
              unmangleXmlString(ev.asCharacters().getData(), false);
          parent.setVal(parent.getVal() + val);
          events.nextEvent();
          break;
        case XMLEvent.ATTRIBUTE:
          throw new IOException("Unexpected XML event " + ev);
        default:
          // Ignore other event types like comment, etc.
          if (LOG.isTraceEnabled()) {
            LOG.trace("Skipping XMLEvent " + ev);
          }
          events.nextEvent();
          break;
        }
      } catch (XMLStreamException e) {
        throw new IOException("Expecting " + expected +
            ", but got XMLStreamException", e);
      }
    }
  }

  /**
   * Load a subtree of the XML into a Node structure.
   * We will keep consuming XML events until we exit the current subtree.
   * If there are any terminators specified, we will always leave the
   * terminating end tag event in the stream.
   *
   * @param parent         The node to fill in.
   * @param expected       A string to display in exceptions.
   * @param terminators    Entering any one of these XML tags terminates our
   *                       traversal.
   * @throws IOException
   */
  private void loadNodeChildren(Node parent, String expected,
                                String... terminators) throws IOException {
    loadNodeChildrenHelper(parent, expected, terminators);
    if (LOG.isTraceEnabled()) {
      LOG.trace("loadNodeChildren(expected=" + expected +
          ", terminators=[" + StringUtils.join(",", terminators) +
          "]):" + parent.dump());
    }
  }

  /**
   * A processor for an FSImage XML section.
   */
  private interface SectionProcessor {
    /**
     * Process this section.
     */
    void process() throws IOException;
  }

  /**
   * Processes the NameSection containing last allocated block ID, etc.
   */
  private class NameSectionProcessor implements SectionProcessor {
    static final String NAME = "NameSection";

    @Override
    public void process() throws IOException {
      Node node = new Node();
      loadNodeChildren(node, "NameSection fields");
      NameSystemSection.Builder b =  NameSystemSection.newBuilder();
      Integer namespaceId = node.removeChildInt(NAME_SECTION_NAMESPACE_ID);
      if (namespaceId == null)  {
        throw new IOException("<NameSection> is missing <namespaceId>");
      }
      b.setNamespaceId(namespaceId);
      Long lval = node.removeChildLong(NAME_SECTION_GENSTAMPV1);
      if (lval != null)  {
        b.setGenstampV1(lval);
      }
      lval = node.removeChildLong(NAME_SECTION_GENSTAMPV2);
      if (lval != null)  {
        b.setGenstampV2(lval);
      }
      lval = node.removeChildLong(NAME_SECTION_GENSTAMPV1_LIMIT);
      if (lval != null)  {
        b.setGenstampV1Limit(lval);
      }
      lval = node.removeChildLong(NAME_SECTION_LAST_ALLOCATED_BLOCK_ID);
      if (lval != null)  {
        b.setLastAllocatedBlockId(lval);
      }
      lval = node.removeChildLong(NAME_SECTION_TXID);
      if (lval != null)  {
        b.setTransactionId(lval);
      }
      lval = node.removeChildLong(
          NAME_SECTION_ROLLING_UPGRADE_START_TIME);
      if (lval != null)  {
        b.setRollingUpgradeStartTime(lval);
      }
      lval = node.removeChildLong(
          NAME_SECTION_LAST_ALLOCATED_STRIPED_BLOCK_ID);
      if (lval != null)  {
        b.setLastAllocatedStripedBlockId(lval);
      }
      node.verifyNoRemainingKeys("NameSection");
      NameSystemSection s = b.build();
      if (LOG.isDebugEnabled()) {
        LOG.debug(SectionName.NS_INFO.name() + " writing header: {" +
            TextFormat.printToString(s) + "}");
      }
      s.writeDelimitedTo(out);
      recordSectionLength(SectionName.NS_INFO.name());
    }
  }

  private class ErasureCodingSectionProcessor implements SectionProcessor {
    static final String NAME = "ErasureCodingSection";

    @Override
    public void process() throws IOException {
      Node node = new Node();
      loadNodeChildren(node, "ErasureCodingSection fields");
      ErasureCodingSection.Builder builder = ErasureCodingSection.newBuilder();
      while (true) {
        ErasureCodingPolicyProto.Builder policyBuilder =
            ErasureCodingPolicyProto.newBuilder();
        Node ec = node.removeChild(ERASURE_CODING_SECTION_POLICY);
        if (ec == null) {
          break;
        }
        int policyId = ec.removeChildInt(ERASURE_CODING_SECTION_POLICY_ID);
        policyBuilder.setId(policyId);
        String name = ec.removeChildStr(ERASURE_CODING_SECTION_POLICY_NAME);
        policyBuilder.setName(name);
        Integer cellSize =
            ec.removeChildInt(ERASURE_CODING_SECTION_POLICY_CELL_SIZE);
        policyBuilder.setCellSize(cellSize);
        String policyState =
            ec.removeChildStr(ERASURE_CODING_SECTION_POLICY_STATE);
        if (policyState != null) {
          policyBuilder.setState(
              HdfsProtos.ErasureCodingPolicyState.valueOf(policyState));
        }

        Node schema = ec.removeChild(ERASURE_CODING_SECTION_SCHEMA);
        Preconditions.checkNotNull(schema);

        ECSchemaProto.Builder schemaBuilder = ECSchemaProto.newBuilder();
        String codecName =
            schema.removeChildStr(ERASURE_CODING_SECTION_SCHEMA_CODEC_NAME);
        schemaBuilder.setCodecName(codecName);
        Integer dataUnits =
            schema.removeChildInt(ERASURE_CODING_SECTION_SCHEMA_DATA_UNITS);
        schemaBuilder.setDataUnits(dataUnits);
        Integer parityUnits = schema.
            removeChildInt(ERASURE_CODING_SECTION_SCHEMA_PARITY_UNITS);
        schemaBuilder.setParityUnits(parityUnits);
        Node options = schema
            .removeChild(ERASURE_CODING_SECTION_SCHEMA_OPTIONS);
        if (options != null) {
          while (true) {
            Node option =
                options.removeChild(ERASURE_CODING_SECTION_SCHEMA_OPTION);
            if (option == null) {
              break;
            }
            String key = option
                .removeChildStr(ERASURE_CODING_SECTION_SCHEMA_OPTION_KEY);
            String value = option
                .removeChildStr(ERASURE_CODING_SECTION_SCHEMA_OPTION_VALUE);
            schemaBuilder.addOptions(HdfsProtos.ECSchemaOptionEntryProto
                .newBuilder().setKey(key).setValue(value).build());
          }
        }
        policyBuilder.setSchema(schemaBuilder.build());

        builder.addPolicies(policyBuilder.build());
      }
      ErasureCodingSection section = builder.build();
      section.writeDelimitedTo(out);
      node.verifyNoRemainingKeys("ErasureCodingSection");
      recordSectionLength(SectionName.ERASURE_CODING.name());
    }
  }

  private class INodeSectionProcessor implements SectionProcessor {
    static final String NAME = "INodeSection";

    @Override
    public void process() throws IOException {
      Node headerNode = new Node();
      loadNodeChildren(headerNode, "INodeSection fields", "inode");
      INodeSection.Builder b =  INodeSection.newBuilder();
      Long lval = headerNode.removeChildLong(INODE_SECTION_LAST_INODE_ID);
      if (lval != null)  {
        b.setLastInodeId(lval);
      }
      Integer expectedNumINodes =
          headerNode.removeChildInt(INODE_SECTION_NUM_INODES);
      if (expectedNumINodes == null) {
        throw new IOException("Failed to find <numInodes> in INodeSection.");
      }
      b.setNumInodes(expectedNumINodes);
      INodeSection s = b.build();
      s.writeDelimitedTo(out);
      headerNode.verifyNoRemainingKeys("INodeSection");
      int actualNumINodes = 0;
      while (actualNumINodes < expectedNumINodes) {
        try {
          expectTag(INODE_SECTION_INODE, false);
        } catch (IOException e) {
          throw new IOException("Only found " + actualNumINodes +
              " <inode> entries out of " + expectedNumINodes, e);
        }
        actualNumINodes++;
        Node inode = new Node();
        loadNodeChildren(inode, "INode fields");
        INodeSection.INode.Builder inodeBld = processINodeXml(inode);
        inodeBld.build().writeDelimitedTo(out);
      }
      expectTagEnd(INODE_SECTION_NAME);
      recordSectionLength(SectionName.INODE.name());
    }
  }

  private INodeSection.INode.Builder processINodeXml(Node node)
      throws IOException {
    String type = node.removeChildStr(INODE_SECTION_TYPE);
    if (type == null) {
      throw new IOException("INode XML found with no <type> tag.");
    }
    INodeSection.INode.Builder inodeBld = INodeSection.INode.newBuilder();
    Long id = node.removeChildLong(SECTION_ID);
    if (id == null) {
      throw new IOException("<inode> found without <id>");
    }
    inodeBld.setId(id);
    String name = node.removeChildStr(SECTION_NAME);
    if (name != null) {
      inodeBld.setName(ByteString.copyFrom(name, "UTF8"));
    }
    switch (type) {
    case "FILE":
      processFileXml(node, inodeBld);
      break;
    case "DIRECTORY":
      processDirectoryXml(node, inodeBld);
      break;
    case "SYMLINK":
      processSymlinkXml(node, inodeBld);
      break;
    default:
      throw new IOException("INode XML found with unknown <type> " +
          "tag " + type);
    }
    node.verifyNoRemainingKeys("inode");
    return inodeBld;
  }

  private void processFileXml(Node node, INodeSection.INode.Builder inodeBld)
      throws IOException {
    inodeBld.setType(INodeSection.INode.Type.FILE);
    INodeSection.INodeFile.Builder bld = createINodeFileBuilder(node);
    inodeBld.setFile(bld);
    // Will check remaining keys and serialize in processINodeXml
  }

  private INodeSection.INodeFile.Builder createINodeFileBuilder(Node node)
      throws IOException {
    INodeSection.INodeFile.Builder bld = INodeSection.INodeFile.newBuilder();
    Integer ival = node.removeChildInt(SECTION_REPLICATION);
    if (ival != null) {
      bld.setReplication(ival);
    }
    Long lval = node.removeChildLong(INODE_SECTION_MTIME);
    if (lval != null) {
      bld.setModificationTime(lval);
    }
    lval = node.removeChildLong(INODE_SECTION_ATIME);
    if (lval != null) {
      bld.setAccessTime(lval);
    }
    lval = node.removeChildLong(INODE_SECTION_PREFERRED_BLOCK_SIZE);
    if (lval != null) {
      bld.setPreferredBlockSize(lval);
    }
    String perm = node.removeChildStr(INODE_SECTION_PERMISSION);
    if (perm != null) {
      bld.setPermission(permissionXmlToU64(perm));
    }
    Node blocks = node.removeChild(INODE_SECTION_BLOCKS);
    if (blocks != null) {
      while (true) {
        Node block = blocks.removeChild(INODE_SECTION_BLOCK);
        if (block == null) {
          break;
        }
        bld.addBlocks(createBlockBuilder(block));
      }
    }
    Node fileUnderConstruction =
        node.removeChild(INODE_SECTION_FILE_UNDER_CONSTRUCTION);
    if (fileUnderConstruction != null) {
      INodeSection.FileUnderConstructionFeature.Builder fb =
          INodeSection.FileUnderConstructionFeature.newBuilder();
      String clientName =
          fileUnderConstruction.removeChildStr(INODE_SECTION_CLIENT_NAME);
      if (clientName == null) {
        throw new IOException("<file-under-construction> found without " +
            "<clientName>");
      }
      fb.setClientName(clientName);
      String clientMachine =
          fileUnderConstruction
                  .removeChildStr(INODE_SECTION_CLIENT_MACHINE);
      if (clientMachine == null) {
        throw new IOException("<file-under-construction> found without " +
            "<clientMachine>");
      }
      fb.setClientMachine(clientMachine);
      bld.setFileUC(fb);
    }
    Node acls = node.removeChild(INODE_SECTION_ACLS);
    if (acls != null) {
      bld.setAcl(aclXmlToProto(acls));
    }
    Node xattrs = node.removeChild(INODE_SECTION_XATTRS);
    if (xattrs != null) {
      bld.setXAttrs(xattrsXmlToProto(xattrs));
    }
    ival = node.removeChildInt(INODE_SECTION_STORAGE_POLICY_ID);
    if (ival != null) {
      bld.setStoragePolicyID(ival);
    }
    String blockType = node.removeChildStr(INODE_SECTION_BLOCK_TYPE);
    if(blockType != null) {
      switch (blockType) {
      case "CONTIGUOUS":
        bld.setBlockType(HdfsProtos.BlockTypeProto.CONTIGUOUS);
        break;
      case "STRIPED":
        bld.setBlockType(HdfsProtos.BlockTypeProto.STRIPED);
        ival = node.removeChildInt(INODE_SECTION_EC_POLICY_ID);
        if (ival != null) {
          bld.setErasureCodingPolicyID(ival);
        }
        break;
      default:
        throw new IOException("INode XML found with unknown <blocktype> " +
            blockType);
      }
    }
    return bld;
  }

  private HdfsProtos.BlockProto.Builder createBlockBuilder(Node block)
      throws IOException {
    HdfsProtos.BlockProto.Builder blockBld =
        HdfsProtos.BlockProto.newBuilder();
    Long id = block.removeChildLong(SECTION_ID);
    if (id == null) {
      throw new IOException("<block> found without <id>");
    }
    blockBld.setBlockId(id);
    Long genstamp = block.removeChildLong(INODE_SECTION_GENSTAMP);
    if (genstamp == null) {
      throw new IOException("<block> found without <genstamp>");
    }
    blockBld.setGenStamp(genstamp);
    Long numBytes = block.removeChildLong(INODE_SECTION_NUM_BYTES);
    if (numBytes == null) {
      throw new IOException("<block> found without <numBytes>");
    }
    blockBld.setNumBytes(numBytes);
    return blockBld;
  }

  private void processDirectoryXml(Node node,
          INodeSection.INode.Builder inodeBld) throws IOException {
    inodeBld.setType(INodeSection.INode.Type.DIRECTORY);
    INodeSection.INodeDirectory.Builder bld =
        createINodeDirectoryBuilder(node);
    inodeBld.setDirectory(bld);
    // Will check remaining keys and serialize in processINodeXml
  }

  private INodeSection.INodeDirectory.Builder
      createINodeDirectoryBuilder(Node node) throws IOException {
    INodeSection.INodeDirectory.Builder bld =
        INodeSection.INodeDirectory.newBuilder();
    Long lval = node.removeChildLong(INODE_SECTION_MTIME);
    if (lval != null) {
      bld.setModificationTime(lval);
    }
    lval = node.removeChildLong(INODE_SECTION_NS_QUOTA);
    if (lval != null) {
      bld.setNsQuota(lval);
    }
    lval = node.removeChildLong(INODE_SECTION_DS_QUOTA);
    if (lval != null) {
      bld.setDsQuota(lval);
    }
    String perm = node.removeChildStr(INODE_SECTION_PERMISSION);
    if (perm != null) {
      bld.setPermission(permissionXmlToU64(perm));
    }
    Node acls = node.removeChild(INODE_SECTION_ACLS);
    if (acls != null) {
      bld.setAcl(aclXmlToProto(acls));
    }
    Node xattrs = node.removeChild(INODE_SECTION_XATTRS);
    if (xattrs != null) {
      bld.setXAttrs(xattrsXmlToProto(xattrs));
    }
    INodeSection.QuotaByStorageTypeFeatureProto.Builder qf =
        INodeSection.QuotaByStorageTypeFeatureProto.newBuilder();
    while (true) {
      Node typeQuota = node.removeChild(INODE_SECTION_TYPE_QUOTA);
      if (typeQuota == null) {
        break;
      }
      INodeSection.QuotaByStorageTypeEntryProto.Builder qbld =
          INodeSection.QuotaByStorageTypeEntryProto.newBuilder();
      String type = typeQuota.removeChildStr(INODE_SECTION_TYPE);
      if (type == null) {
        throw new IOException("<typeQuota> was missing <type>");
      }
      HdfsProtos.StorageTypeProto storageType =
          HdfsProtos.StorageTypeProto.valueOf(type);
      if (storageType == null) {
        throw new IOException("<typeQuota> had unknown <type> " + type);
      }
      qbld.setStorageType(storageType);
      Long quota = typeQuota.removeChildLong(INODE_SECTION_QUOTA);
      if (quota == null) {
        throw new IOException("<typeQuota> was missing <quota>");
      }
      qbld.setQuota(quota);
      qf.addQuotas(qbld);
    }
    bld.setTypeQuotas(qf);
    return bld;
  }

  private void processSymlinkXml(Node node,
                                 INodeSection.INode.Builder inodeBld) throws IOException {
    inodeBld.setType(INodeSection.INode.Type.SYMLINK);
    INodeSection.INodeSymlink.Builder bld =
        INodeSection.INodeSymlink.newBuilder();
    String perm = node.removeChildStr(INODE_SECTION_PERMISSION);
    if (perm != null) {
      bld.setPermission(permissionXmlToU64(perm));
    }
    String target = node.removeChildStr(INODE_SECTION_TARGET);
    if (target != null) {
      bld.setTarget(ByteString.copyFrom(target, "UTF8"));
    }
    Long lval = node.removeChildLong(INODE_SECTION_MTIME);
    if (lval != null) {
      bld.setModificationTime(lval);
    }
    lval = node.removeChildLong(INODE_SECTION_ATIME);
    if (lval != null) {
      bld.setAccessTime(lval);
    }
    inodeBld.setSymlink(bld);
    // Will check remaining keys and serialize in processINodeXml
  }

  private INodeSection.AclFeatureProto.Builder aclXmlToProto(Node acls)
      throws IOException {
    AclFeatureProto.Builder b = AclFeatureProto.newBuilder();
    while (true) {
      Node acl = acls.removeChild(INODE_SECTION_ACL);
      if (acl == null) {
        break;
      }
      String val = acl.getVal();
      AclEntry entry = AclEntry.parseAclEntry(val, true);
      int nameId = registerStringId(entry.getName() == null ? EMPTY_STRING
          : entry.getName());
      int v = ((nameId & ACL_ENTRY_NAME_MASK) << ACL_ENTRY_NAME_OFFSET)
          | (entry.getType().ordinal() << ACL_ENTRY_TYPE_OFFSET)
          | (entry.getScope().ordinal() << ACL_ENTRY_SCOPE_OFFSET)
          | (entry.getPermission().ordinal());
      b.addEntries(v);
    }
    return b;
  }

  private INodeSection.XAttrFeatureProto.Builder xattrsXmlToProto(Node xattrs)
      throws IOException {
    INodeSection.XAttrFeatureProto.Builder bld =
        INodeSection.XAttrFeatureProto.newBuilder();
    while (true) {
      Node xattr = xattrs.removeChild(INODE_SECTION_XATTR);
      if (xattr == null) {
        break;
      }
      INodeSection.XAttrCompactProto.Builder b =
          INodeSection.XAttrCompactProto.newBuilder();
      String ns = xattr.removeChildStr(INODE_SECTION_NS);
      if (ns == null) {
        throw new IOException("<xattr> had no <ns> entry.");
      }
      int nsIdx = XAttrProtos.XAttrProto.
          XAttrNamespaceProto.valueOf(ns).ordinal();
      String name = xattr.removeChildStr(SECTION_NAME);
      String valStr = xattr.removeChildStr(INODE_SECTION_VAL);
      byte[] val = null;
      if (valStr == null) {
        String valHex = xattr.removeChildStr(INODE_SECTION_VAL_HEX);
        if (valHex == null) {
          throw new IOException("<xattr> had no <val> or <valHex> entry.");
        }
        val = new HexBinaryAdapter().unmarshal(valHex);
      } else {
        val = valStr.getBytes("UTF8");
      }
      b.setValue(ByteString.copyFrom(val));

      // The XAttrCompactProto name field uses a fairly complex format
      // to encode both the string table ID of the xattr name and the
      // namespace ID.  See the protobuf file for details.
      int nameId = registerStringId(name);
      int encodedName = (nameId << XATTR_NAME_OFFSET) |
          ((nsIdx & XATTR_NAMESPACE_MASK) << XATTR_NAMESPACE_OFFSET) |
          (((nsIdx >> 2) & XATTR_NAMESPACE_EXT_MASK)
              << XATTR_NAMESPACE_EXT_OFFSET);
      b.setName(encodedName);
      xattr.verifyNoRemainingKeys("xattr");
      bld.addXAttrs(b);
    }
    xattrs.verifyNoRemainingKeys("xattrs");
    return bld;
  }

  private class SecretManagerSectionProcessor implements SectionProcessor {
    static final String NAME = "SecretManagerSection";

    @Override
    public void process() throws IOException {
      Node secretHeader = new Node();
      loadNodeChildren(secretHeader, "SecretManager fields",
          "delegationKey", "token");
      SecretManagerSection.Builder b =  SecretManagerSection.newBuilder();
      Integer currentId =
          secretHeader.removeChildInt(SECRET_MANAGER_SECTION_CURRENT_ID);
      if (currentId == null) {
        throw new IOException("SecretManager section had no <currentId>");
      }
      b.setCurrentId(currentId);
      Integer tokenSequenceNumber = secretHeader.removeChildInt(
          SECRET_MANAGER_SECTION_TOKEN_SEQUENCE_NUMBER);
      if (tokenSequenceNumber == null) {
        throw new IOException("SecretManager section had no " +
            "<tokenSequenceNumber>");
      }
      b.setTokenSequenceNumber(tokenSequenceNumber);
      Integer expectedNumKeys = secretHeader.removeChildInt(
          SECRET_MANAGER_SECTION_NUM_DELEGATION_KEYS);
      if (expectedNumKeys == null) {
        throw new IOException("SecretManager section had no " +
            "<numDelegationKeys>");
      }
      b.setNumKeys(expectedNumKeys);
      Integer expectedNumTokens =
          secretHeader.removeChildInt(SECRET_MANAGER_SECTION_NUM_TOKENS);
      if (expectedNumTokens == null) {
        throw new IOException("SecretManager section had no " +
            "<numTokens>");
      }
      b.setNumTokens(expectedNumTokens);
      secretHeader.verifyNoRemainingKeys("SecretManager");
      b.build().writeDelimitedTo(out);
      for (int actualNumKeys = 0; actualNumKeys < expectedNumKeys;
           actualNumKeys++) {
        try {
          expectTag(SECRET_MANAGER_SECTION_DELEGATION_KEY, false);
        } catch (IOException e) {
          throw new IOException("Only read " + actualNumKeys +
              " delegation keys out of " + expectedNumKeys, e);
        }
        SecretManagerSection.DelegationKey.Builder dbld =
            SecretManagerSection.DelegationKey.newBuilder();
        Node dkey = new Node();
        loadNodeChildren(dkey, "Delegation key fields");
        Integer id = dkey.removeChildInt(SECTION_ID);
        if (id == null) {
          throw new IOException("Delegation key stanza <delegationKey> " +
              "lacked an <id> field.");
        }
        dbld.setId(id);
        String expiry = dkey.removeChildStr(SECRET_MANAGER_SECTION_EXPIRY);
        if (expiry == null) {
          throw new IOException("Delegation key stanza <delegationKey> " +
              "lacked an <expiry> field.");
        }
        dbld.setExpiryDate(dateStrToLong(expiry));
        String keyHex = dkey.removeChildStr(SECRET_MANAGER_SECTION_KEY);
        if (keyHex == null) {
          throw new IOException("Delegation key stanza <delegationKey> " +
              "lacked a <key> field.");
        }
        byte[] key = new HexBinaryAdapter().unmarshal(keyHex);
        dkey.verifyNoRemainingKeys(SECRET_MANAGER_SECTION_DELEGATION_KEY);
        dbld.setKey(ByteString.copyFrom(key));
        dbld.build().writeDelimitedTo(out);
      }
      for (int actualNumTokens = 0; actualNumTokens < expectedNumTokens;
           actualNumTokens++) {
        try {
          expectTag(SECRET_MANAGER_SECTION_TOKEN, false);
        } catch (IOException e) {
          throw new IOException("Only read " + actualNumTokens +
              " tokens out of " + expectedNumTokens, e);
        }
        SecretManagerSection.PersistToken.Builder tbld =
            SecretManagerSection.PersistToken.newBuilder();
        Node token = new Node();
        loadNodeChildren(token, "PersistToken key fields");
        Integer version =
            token.removeChildInt(SECRET_MANAGER_SECTION_VERSION);
        if (version != null) {
          tbld.setVersion(version);
        }
        String owner = token.removeChildStr(SECRET_MANAGER_SECTION_OWNER);
        if (owner != null) {
          tbld.setOwner(owner);
        }
        String renewer =
            token.removeChildStr(SECRET_MANAGER_SECTION_RENEWER);
        if (renewer != null) {
          tbld.setRenewer(renewer);
        }
        String realUser =
            token.removeChildStr(SECRET_MANAGER_SECTION_REAL_USER);
        if (realUser != null) {
          tbld.setRealUser(realUser);
        }
        String issueDateStr =
            token.removeChildStr(SECRET_MANAGER_SECTION_ISSUE_DATE);
        if (issueDateStr != null) {
          tbld.setIssueDate(dateStrToLong(issueDateStr));
        }
        String maxDateStr =
            token.removeChildStr(SECRET_MANAGER_SECTION_MAX_DATE);
        if (maxDateStr != null) {
          tbld.setMaxDate(dateStrToLong(maxDateStr));
        }
        Integer seqNo =
            token.removeChildInt(SECRET_MANAGER_SECTION_SEQUENCE_NUMBER);
        if (seqNo != null) {
          tbld.setSequenceNumber(seqNo);
        }
        Integer masterKeyId =
            token.removeChildInt(SECRET_MANAGER_SECTION_MASTER_KEY_ID);
        if (masterKeyId != null) {
          tbld.setMasterKeyId(masterKeyId);
        }
        String expiryDateStr =
            token.removeChildStr(SECRET_MANAGER_SECTION_EXPIRY_DATE);
        if (expiryDateStr != null) {
          tbld.setExpiryDate(dateStrToLong(expiryDateStr));
        }
        token.verifyNoRemainingKeys("token");
        tbld.build().writeDelimitedTo(out);
      }
      expectTagEnd(SECRET_MANAGER_SECTION_NAME);
      recordSectionLength(SectionName.SECRET_MANAGER.name());
    }

    private long dateStrToLong(String dateStr) throws IOException {
      try {
        Date date = isoDateFormat.parse(dateStr);
        return date.getTime();
      } catch (ParseException e) {
        throw new IOException("Failed to parse ISO date string " + dateStr, e);
      }
    }
  }

  private class CacheManagerSectionProcessor implements SectionProcessor {
    static final String NAME = "CacheManagerSection";

    @Override
    public void process() throws IOException {
      Node node = new Node();
      loadNodeChildren(node, "CacheManager fields", "pool", "directive");
      CacheManagerSection.Builder b =  CacheManagerSection.newBuilder();
      Long nextDirectiveId =
          node.removeChildLong(CACHE_MANAGER_SECTION_NEXT_DIRECTIVE_ID);
      if (nextDirectiveId == null) {
        throw new IOException("CacheManager section had no <nextDirectiveId>");
      }
      b.setNextDirectiveId(nextDirectiveId);
      Integer expectedNumPools =
          node.removeChildInt(CACHE_MANAGER_SECTION_NUM_POOLS);
      if (expectedNumPools == null) {
        throw new IOException("CacheManager section had no <numPools>");
      }
      b.setNumPools(expectedNumPools);
      Integer expectedNumDirectives =
          node.removeChildInt(CACHE_MANAGER_SECTION_NUM_DIRECTIVES);
      if (expectedNumDirectives == null) {
        throw new IOException("CacheManager section had no <numDirectives>");
      }
      b.setNumDirectives(expectedNumDirectives);
      b.build().writeDelimitedTo(out);
      long actualNumPools = 0;
      while (actualNumPools < expectedNumPools) {
        try {
          expectTag(CACHE_MANAGER_SECTION_POOL, false);
        } catch (IOException e) {
          throw new IOException("Only read " + actualNumPools +
              " cache pools out of " + expectedNumPools, e);
        }
        actualNumPools++;
        Node pool = new Node();
        loadNodeChildren(pool, "pool fields", "");
        processPoolXml(node);
      }
      long actualNumDirectives = 0;
      while (actualNumDirectives < expectedNumDirectives) {
        try {
          expectTag(CACHE_MANAGER_SECTION_DIRECTIVE, false);
        } catch (IOException e) {
          throw new IOException("Only read " + actualNumDirectives +
              " cache pools out of " + expectedNumDirectives, e);
        }
        actualNumDirectives++;
        Node pool = new Node();
        loadNodeChildren(pool, "directive fields", "");
        processDirectiveXml(node);
      }
      expectTagEnd(CACHE_MANAGER_SECTION_NAME);
      recordSectionLength(SectionName.CACHE_MANAGER.name());
    }

    private void processPoolXml(Node pool) throws IOException {
      CachePoolInfoProto.Builder bld = CachePoolInfoProto.newBuilder();
      String poolName =
          pool.removeChildStr(CACHE_MANAGER_SECTION_POOL_NAME);
      if (poolName == null) {
        throw new IOException("<pool> found without <poolName>");
      }
      bld.setPoolName(poolName);
      String ownerName =
          pool.removeChildStr(CACHE_MANAGER_SECTION_OWNER_NAME);
      if (ownerName == null) {
        throw new IOException("<pool> found without <ownerName>");
      }
      bld.setOwnerName(ownerName);
      String groupName =
          pool.removeChildStr(CACHE_MANAGER_SECTION_GROUP_NAME);
      if (groupName == null) {
        throw new IOException("<pool> found without <groupName>");
      }
      bld.setGroupName(groupName);
      Integer mode = pool.removeChildInt(CACHE_MANAGER_SECTION_MODE);
      if (mode == null) {
        throw new IOException("<pool> found without <mode>");
      }
      bld.setMode(mode);
      Long limit = pool.removeChildLong(CACHE_MANAGER_SECTION_LIMIT);
      if (limit == null) {
        throw new IOException("<pool> found without <limit>");
      }
      bld.setLimit(limit);
      Long maxRelativeExpiry =
          pool.removeChildLong(CACHE_MANAGER_SECTION_MAX_RELATIVE_EXPIRY);
      if (maxRelativeExpiry == null) {
        throw new IOException("<pool> found without <maxRelativeExpiry>");
      }
      bld.setMaxRelativeExpiry(maxRelativeExpiry);
      pool.verifyNoRemainingKeys("pool");
      bld.build().writeDelimitedTo(out);
    }

    private void processDirectiveXml(Node directive) throws IOException {
      CacheDirectiveInfoProto.Builder bld =
          CacheDirectiveInfoProto.newBuilder();
      Long id = directive.removeChildLong(SECTION_ID);
      if (id == null) {
        throw new IOException("<directive> found without <id>");
      }
      bld.setId(id);
      String path = directive.removeChildStr(SECTION_PATH);
      if (path == null) {
        throw new IOException("<directive> found without <path>");
      }
      bld.setPath(path);
      Integer replication = directive.removeChildInt(SECTION_REPLICATION);
      if (replication == null) {
        throw new IOException("<directive> found without <replication>");
      }
      bld.setReplication(replication);
      String pool = directive.removeChildStr(CACHE_MANAGER_SECTION_POOL);
      if (path == null) {
        throw new IOException("<directive> found without <pool>");
      }
      bld.setPool(pool);
      Node expiration =
          directive.removeChild(CACHE_MANAGER_SECTION_EXPIRATION);
      if (expiration != null) {
        CacheDirectiveInfoExpirationProto.Builder ebld =
            CacheDirectiveInfoExpirationProto.newBuilder();
        Long millis =
            expiration.removeChildLong(CACHE_MANAGER_SECTION_MILLIS);
        if (millis == null) {
          throw new IOException("cache directive <expiration> found " +
              "without <millis>");
        }
        ebld.setMillis(millis);
        if (expiration.removeChildBool(CACHE_MANAGER_SECTION_RELATIVE)) {
          ebld.setIsRelative(true);
        } else {
          ebld.setIsRelative(false);
        }
        bld.setExpiration(ebld);
      }
      directive.verifyNoRemainingKeys("directive");
      bld.build().writeDelimitedTo(out);
    }
  }

  private class INodeReferenceSectionProcessor implements SectionProcessor {
    static final String NAME = "INodeReferenceSection";

    @Override
    public void process() throws IOException {
      // There is no header for this section.
      // We process the repeated <ref> elements.
      while (true) {
        XMLEvent ev = expectTag(INODE_REFERENCE_SECTION_REF, true);
        if (ev.isEndElement()) {
          break;
        }
        Node inodeRef = new Node();
        FsImageProto.INodeReferenceSection.INodeReference.Builder bld =
            FsImageProto.INodeReferenceSection.INodeReference.newBuilder();
        loadNodeChildren(inodeRef, "INodeReference");
        Long referredId =
            inodeRef.removeChildLong(INODE_REFERENCE_SECTION_REFERRED_ID);
        if (referredId != null) {
          bld.setReferredId(referredId);
        }
        String name = inodeRef.removeChildStr("name");
        if (name != null) {
          bld.setName(ByteString.copyFrom(name, "UTF8"));
        }
        Integer dstSnapshotId = inodeRef.removeChildInt(
            INODE_REFERENCE_SECTION_DST_SNAPSHOT_ID);
        if (dstSnapshotId != null) {
          bld.setDstSnapshotId(dstSnapshotId);
        }
        Integer lastSnapshotId = inodeRef.removeChildInt(
            INODE_REFERENCE_SECTION_LAST_SNAPSHOT_ID);
        if (lastSnapshotId != null) {
          bld.setLastSnapshotId(lastSnapshotId);
        }
        inodeRef.verifyNoRemainingKeys("ref");
        bld.build().writeDelimitedTo(out);
      }
      recordSectionLength(SectionName.INODE_REFERENCE.name());
    }
  }

  private class INodeDirectorySectionProcessor implements SectionProcessor {
    static final String NAME = "INodeDirectorySection";

    @Override
    public void process() throws IOException {
      // No header for this section
      // Process the repeated <directory> elements.
      while (true) {
        XMLEvent ev = expectTag(INODE_DIRECTORY_SECTION_DIRECTORY, true);
        if (ev.isEndElement()) {
          break;
        }
        Node directory = new Node();
        FsImageProto.INodeDirectorySection.DirEntry.Builder bld =
            FsImageProto.INodeDirectorySection.DirEntry.newBuilder();
        loadNodeChildren(directory, "directory");
        Long parent = directory.removeChildLong(
            INODE_DIRECTORY_SECTION_PARENT);
        if (parent != null) {
          bld.setParent(parent);
        }
        while (true) {
          Node child = directory.removeChild(
              INODE_DIRECTORY_SECTION_CHILD);
          if (child == null) {
            break;
          }
          bld.addChildren(Long.parseLong(child.getVal()));
        }
        while (true) {
          Node refChild = directory.removeChild(
              INODE_DIRECTORY_SECTION_REF_CHILD);
          if (refChild == null) {
            break;
          }
          bld.addRefChildren(Integer.parseInt(refChild.getVal()));
        }
        directory.verifyNoRemainingKeys("directory");
        bld.build().writeDelimitedTo(out);
      }
      recordSectionLength(SectionName.INODE_DIR.name());
    }
  }

  private class FilesUnderConstructionSectionProcessor
      implements SectionProcessor {
    static final String NAME = "FileUnderConstructionSection";

    @Override
    public void process() throws IOException {
      // No header for this section type.
      // Process the repeated files under construction elements.
      while (true) {
        XMLEvent ev = expectTag(INODE_SECTION_INODE, true);
        if (ev.isEndElement()) {
          break;
        }
        Node fileUnderConstruction = new Node();
        loadNodeChildren(fileUnderConstruction, "file under construction");
        FileUnderConstructionEntry.Builder bld =
            FileUnderConstructionEntry.newBuilder();
        Long id = fileUnderConstruction.removeChildLong(SECTION_ID);
        if (id != null) {
          bld.setInodeId(id);
        }
        String fullpath =
            fileUnderConstruction.removeChildStr(SECTION_PATH);
        if (fullpath != null) {
          bld.setFullPath(fullpath);
        }
        fileUnderConstruction.verifyNoRemainingKeys("inode");
        bld.build().writeDelimitedTo(out);
      }
      recordSectionLength(SectionName.FILES_UNDERCONSTRUCTION.name());
    }
  }

  private class SnapshotSectionProcessor implements SectionProcessor {
    static final String NAME = "SnapshotSection";

    @Override
    public void process() throws IOException {
      FsImageProto.SnapshotSection.Builder bld =
          FsImageProto.SnapshotSection.newBuilder();
      Node header = new Node();
      loadNodeChildren(header, "SnapshotSection fields", "snapshot");
      Integer snapshotCounter = header.removeChildInt(
          SNAPSHOT_SECTION_SNAPSHOT_COUNTER);
      if (snapshotCounter == null) {
        throw new IOException("No <snapshotCounter> entry found in " +
            "SnapshotSection header");
      }
      bld.setSnapshotCounter(snapshotCounter);
      Integer expectedNumSnapshots = header.removeChildInt(
          SNAPSHOT_SECTION_NUM_SNAPSHOTS);
      if (expectedNumSnapshots == null) {
        throw new IOException("No <numSnapshots> entry found in " +
            "SnapshotSection header");
      }
      bld.setNumSnapshots(expectedNumSnapshots);
      while (true) {
        Node sd = header.removeChild(SNAPSHOT_SECTION_SNAPSHOT_TABLE_DIR);
        if (sd == null) {
          break;
        }
        Long dir;
        while ((dir = sd.removeChildLong(SNAPSHOT_SECTION_DIR)) != null) {
          // Add all snapshottable directories, one by one
          bld.addSnapshottableDir(dir);
        }
      }
      header.verifyNoRemainingKeys("SnapshotSection");
      bld.build().writeDelimitedTo(out);
      int actualNumSnapshots = 0;
      while (actualNumSnapshots < expectedNumSnapshots) {
        try {
          expectTag(SNAPSHOT_SECTION_SNAPSHOT, false);
        } catch (IOException e) {
          throw new IOException("Only read " + actualNumSnapshots +
              " <snapshot> entries out of " + expectedNumSnapshots, e);
        }
        actualNumSnapshots++;
        Node snapshot = new Node();
        loadNodeChildren(snapshot, "snapshot fields");
        FsImageProto.SnapshotSection.Snapshot.Builder s =
            FsImageProto.SnapshotSection.Snapshot.newBuilder();
        Integer snapshotId = snapshot.removeChildInt(SECTION_ID);
        if (snapshotId == null) {
          throw new IOException("<snapshot> section was missing <id>");
        }
        s.setSnapshotId(snapshotId);
        Node snapshotRoot = snapshot.removeChild(SNAPSHOT_SECTION_ROOT);
        INodeSection.INode.Builder inodeBld = processINodeXml(snapshotRoot);
        s.setRoot(inodeBld);
        s.build().writeDelimitedTo(out);
      }
      expectTagEnd(SNAPSHOT_SECTION_NAME);
      recordSectionLength(SectionName.SNAPSHOT.name());
    }
  }

  private class SnapshotDiffSectionProcessor implements SectionProcessor {
    static final String NAME = "SnapshotDiffSection";

    @Override
    public void process() throws IOException {
      // No header for this section type.
      LOG.debug("Processing SnapshotDiffSection");
      while (true) {
        XMLEvent ev = expectTag("[diff start tag]", true);
        if (ev.isEndElement()) {
          String name = ev.asEndElement().getName().getLocalPart();
          if (name.equals(SNAPSHOT_DIFF_SECTION_NAME)) {
            break;
          }
          throw new IOException("Got unexpected end tag for " + name);
        }
        String tagName = ev.asStartElement().getName().getLocalPart();
        if (tagName.equals(SNAPSHOT_DIFF_SECTION_DIR_DIFF_ENTRY)) {
          processDirDiffEntry();
        } else if (tagName.equals(SNAPSHOT_DIFF_SECTION_FILE_DIFF_ENTRY)) {
          processFileDiffEntry();
        } else {
          throw new IOException("SnapshotDiffSection contained unexpected " +
              "tag " + tagName);
        }
      }
      recordSectionLength(SectionName.SNAPSHOT_DIFF.name());
    }

    private void processDirDiffEntry() throws IOException {
      LOG.debug("Processing dirDiffEntry");
      DiffEntry.Builder headerBld = DiffEntry.newBuilder();
      headerBld.setType(DiffEntry.Type.DIRECTORYDIFF);
      Node dirDiffHeader = new Node();
      loadNodeChildren(dirDiffHeader, "dirDiffEntry fields", "dirDiff");
      Long inodeId = dirDiffHeader.removeChildLong(
          SNAPSHOT_DIFF_SECTION_INODE_ID);
      if (inodeId == null) {
        throw new IOException("<dirDiffEntry> contained no <inodeId> entry.");
      }
      headerBld.setInodeId(inodeId);
      Integer expectedDiffs = dirDiffHeader.removeChildInt(
          SNAPSHOT_DIFF_SECTION_COUNT);
      if (expectedDiffs == null) {
        throw new IOException("<dirDiffEntry> contained no <count> entry.");
      }
      headerBld.setNumOfDiff(expectedDiffs);
      dirDiffHeader.verifyNoRemainingKeys("dirDiffEntry");
      headerBld.build().writeDelimitedTo(out);
      for (int actualDiffs = 0; actualDiffs < expectedDiffs; actualDiffs++) {
        try {
          expectTag(SNAPSHOT_DIFF_SECTION_DIR_DIFF, false);
        } catch (IOException e) {
          throw new IOException("Only read " + (actualDiffs + 1) +
              " diffs out of " + expectedDiffs, e);
        }
        Node dirDiff = new Node();
        loadNodeChildren(dirDiff, "dirDiff fields");
        FsImageProto.SnapshotDiffSection.DirectoryDiff.Builder bld =
            FsImageProto.SnapshotDiffSection.DirectoryDiff.newBuilder();
        Integer snapshotId = dirDiff.removeChildInt(
            SNAPSHOT_DIFF_SECTION_SNAPSHOT_ID);
        if (snapshotId != null) {
          bld.setSnapshotId(snapshotId);
        }
        Integer childrenSize = dirDiff.removeChildInt(
            SNAPSHOT_DIFF_SECTION_CHILDREN_SIZE);
        if (childrenSize == null) {
          throw new IOException("Expected to find <childrenSize> in " +
              "<dirDiff> section.");
        }
        bld.setIsSnapshotRoot(dirDiff.removeChildBool(
            SNAPSHOT_DIFF_SECTION_IS_SNAPSHOT_ROOT));
        bld.setChildrenSize(childrenSize);
        String name = dirDiff.removeChildStr(SECTION_NAME);
        if (name != null) {
          bld.setName(ByteString.copyFrom(name, "UTF8"));
        }
        Node snapshotCopy = dirDiff.removeChild(
            SNAPSHOT_DIFF_SECTION_SNAPSHOT_COPY);
        if (snapshotCopy != null) {
          bld.setSnapshotCopy(createINodeDirectoryBuilder(snapshotCopy));
        }
        Integer expectedCreatedListSize = dirDiff.removeChildInt(
            SNAPSHOT_DIFF_SECTION_CREATED_LIST_SIZE);
        if (expectedCreatedListSize == null) {
          throw new IOException("Expected to find <createdListSize> in " +
              "<dirDiff> section.");
        }
        bld.setCreatedListSize(expectedCreatedListSize);
        while (true) {
          Node deleted = dirDiff.removeChild(
              SNAPSHOT_DIFF_SECTION_DELETED_INODE);
          if (deleted == null){
            break;
          }
          bld.addDeletedINode(Long.parseLong(deleted.getVal()));
        }
        while (true) {
          Node deleted = dirDiff.removeChild(
              SNAPSHOT_DIFF_SECTION_DELETED_INODE_REF);
          if (deleted == null){
            break;
          }
          bld.addDeletedINodeRef(Integer.parseInt(deleted.getVal()));
        }
        bld.build().writeDelimitedTo(out);
        // After the DirectoryDiff header comes a list of CreatedListEntry PBs.
        int actualCreatedListSize = 0;
        while (true) {
          Node created = dirDiff.removeChild(
              SNAPSHOT_DIFF_SECTION_CREATED);
          if (created == null){
            break;
          }
          String cleName = created.removeChildStr(SECTION_NAME);
          if (cleName == null) {
            throw new IOException("Expected <created> entry to have " +
                "a <name> field");
          }
          created.verifyNoRemainingKeys("created");
          FsImageProto.SnapshotDiffSection.CreatedListEntry.newBuilder().
              setName(ByteString.copyFrom(cleName, "UTF8")).
              build().writeDelimitedTo(out);
          actualCreatedListSize++;
        }
        if (actualCreatedListSize != expectedCreatedListSize) {
          throw new IOException("<createdListSize> was " +
              expectedCreatedListSize +", but there were " +
              actualCreatedListSize + " <created> entries.");
        }
        dirDiff.verifyNoRemainingKeys("dirDiff");
      }
      expectTagEnd(SNAPSHOT_DIFF_SECTION_DIR_DIFF_ENTRY);
    }

    private void processFileDiffEntry() throws IOException {
      LOG.debug("Processing fileDiffEntry");
      DiffEntry.Builder headerBld = DiffEntry.newBuilder();
      headerBld.setType(DiffEntry.Type.FILEDIFF);
      Node fileDiffHeader = new Node();
      loadNodeChildren(fileDiffHeader, "fileDiffEntry fields", "fileDiff");
      Long inodeId = fileDiffHeader.removeChildLong(
          SNAPSHOT_DIFF_SECTION_INODE_ID);
      if (inodeId == null) {
        throw new IOException("<fileDiffEntry> contained no <inodeid> entry.");
      }
      headerBld.setInodeId(inodeId);
      Integer expectedDiffs = fileDiffHeader.removeChildInt(
          SNAPSHOT_DIFF_SECTION_COUNT);
      if (expectedDiffs == null) {
        throw new IOException("<fileDiffEntry> contained no <count> entry.");
      }
      headerBld.setNumOfDiff(expectedDiffs);
      fileDiffHeader.verifyNoRemainingKeys("fileDiffEntry");
      headerBld.build().writeDelimitedTo(out);
      for (int actualDiffs = 0; actualDiffs < expectedDiffs; actualDiffs++) {
        try {
          expectTag(SNAPSHOT_DIFF_SECTION_FILE_DIFF, false);
        } catch (IOException e) {
          throw new IOException("Only read " + (actualDiffs + 1) +
              " diffs out of " + expectedDiffs, e);
        }
        Node fileDiff = new Node();
        loadNodeChildren(fileDiff, "fileDiff fields");
        FsImageProto.SnapshotDiffSection.FileDiff.Builder bld =
            FsImageProto.SnapshotDiffSection.FileDiff.newBuilder();
        Integer snapshotId = fileDiff.removeChildInt(
            SNAPSHOT_DIFF_SECTION_SNAPSHOT_ID);
        if (snapshotId != null) {
          bld.setSnapshotId(snapshotId);
        }
        Long size = fileDiff.removeChildLong(
            SNAPSHOT_DIFF_SECTION_SIZE);
        if (size != null) {
          bld.setFileSize(size);
        }
        String name = fileDiff.removeChildStr(SECTION_NAME);
        if (name != null) {
          bld.setName(ByteString.copyFrom(name, "UTF8"));
        }
        Node snapshotCopy = fileDiff.removeChild(
            SNAPSHOT_DIFF_SECTION_SNAPSHOT_COPY);
        if (snapshotCopy != null) {
          bld.setSnapshotCopy(createINodeFileBuilder(snapshotCopy));
        }
        Node blocks = fileDiff.removeChild(INODE_SECTION_BLOCKS);
        if (blocks != null) {
          while (true) {
            Node block = blocks.removeChild(INODE_SECTION_BLOCK);
            if (block == null) {
              break;
            }
            bld.addBlocks(createBlockBuilder(block));
          }
        }
        fileDiff.verifyNoRemainingKeys("fileDiff");
        bld.build().writeDelimitedTo(out);
      }
      expectTagEnd(SNAPSHOT_DIFF_SECTION_FILE_DIFF_ENTRY);
    }
  }

  /**
   * Permission is serialized as a 64-bit long. [0:24):[25:48):[48:64)
   * (in Big Endian).  The first and the second parts are the string ids
   * of the user and group name, and the last 16 bits are the permission bits.
   *
   * @param perm           The permission string from the XML.
   * @return               The 64-bit value to use in the fsimage for permission.
   * @throws IOException   If we run out of string IDs in the string table.
   */
  private long permissionXmlToU64(String perm) throws IOException {
    String components[] = perm.split(":");
    if (components.length != 3) {
      throw new IOException("Unable to parse permission string " + perm +
          ": expected 3 components, but only had " + components.length);
    }
    String userName = components[0];
    String groupName = components[1];
    String modeString = components[2];
    long userNameId = registerStringId(userName);
    long groupNameId = registerStringId(groupName);
    long mode = new FsPermission(modeString).toShort();
    return (userNameId << 40) | (groupNameId << 16) | mode;
  }

  /**
   * The FSImage contains a string table which maps strings to IDs.
   * This is a simple form of compression which takes advantage of the fact
   * that the same strings tend to occur over and over again.
   * This function will return an ID which we can use to represent the given
   * string.  If the string already exists in the string table, we will use
   * that ID; otherwise, we will allocate a new one.
   *
   * @param str           The string.
   * @return              The ID in the string table.
   * @throws IOException  If we run out of bits in the string table.  We only
   *                      have 25 bits.
   */
  int registerStringId(String str) throws IOException {
    Integer id = stringTable.get(str);
    if (id != null) {
      return id;
    }
    int latestId = latestStringId;
    if (latestId >= 0x1ffffff) {
      throw new IOException("Cannot have more than 2**25 " +
          "strings in the fsimage, because of the limitation on " +
          "the size of string table IDs.");
    }
    stringTable.put(str, latestId);
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
    //if (sectionNamePb.equals(SectionName.STRING_TABLE.name())) {
      fileSummaryBld.addSections(FileSummary.Section.newBuilder().
          setName(sectionNamePb).
          setLength(curPos - curSectionStartOffset).
          setOffset(curSectionStartOffset));
    //}
    sectionStartOffset = curPos;
  }

  /**
   * Read the version tag which starts the XML file.
   */
  private void readVersion() throws IOException {
    try {
      expectTag("version", false);
    } catch (IOException e) {
      // Handle the case where <version> does not exist.
      // Note: fsimage XML files which are missing <version> are also missing
      // many other fields that oiv needs to accurately reconstruct the
      // fsimage.
      throw new IOException("No <version> section found at the top of " +
          "the fsimage XML.  This XML file is too old to be processed " +
          "by oiv.", e);
    }
    Node version = new Node();
    loadNodeChildren(version, "version fields");
    Integer onDiskVersion = version.removeChildInt("onDiskVersion");
    if (onDiskVersion == null) {
      throw new IOException("The <version> section doesn't contain " +
          "the onDiskVersion.");
    }
    Integer layoutVersion = version.removeChildInt("layoutVersion");
    if (layoutVersion == null) {
      throw new IOException("The <version> section doesn't contain " +
          "the layoutVersion.");
    }
    if (layoutVersion.intValue() !=
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION) {
      throw new IOException("Layout version mismatch.  This oiv tool " +
          "handles layout version " +
          NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION + ", but the " +
          "XML file has <layoutVersion> " + layoutVersion + ".  Please " +
          "either re-generate the XML file with the proper layout version, " +
          "or manually edit the XML file to be usable with this version " +
          "of the oiv tool.");
    }
    fileSummaryBld.setOndiskVersion(onDiskVersion);
    fileSummaryBld.setLayoutVersion(layoutVersion);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Loaded <version> with onDiskVersion=" + onDiskVersion +
          ", layoutVersion=" + layoutVersion + ".");
    }
  }

  /**
   * Write the string table to the fsimage.
   * @throws IOException
   */
  private void writeStringTableSection() throws IOException {
    FsImageProto.StringTableSection sectionHeader =
        FsImageProto.StringTableSection.newBuilder().
        setNumEntry(stringTable.size()).build();
    if (LOG.isDebugEnabled()) {
      LOG.debug(SectionName.STRING_TABLE.name() + " writing header: {" +
            TextFormat.printToString(sectionHeader) + "}");
    }
    sectionHeader.writeDelimitedTo(out);

    // The entries don't have to be in any particular order, so iterating
    // over the hash table is fine.
    for (Map.Entry<String, Integer> entry : stringTable.entrySet()) {
      FsImageProto.StringTableSection.Entry stEntry =
          FsImageProto.StringTableSection.Entry.newBuilder().
          setStr(entry.getKey()).
          setId(entry.getValue()).
          build();
      if (LOG.isTraceEnabled()) {
        LOG.trace("Writing string table entry: {" +
            TextFormat.printToString(stEntry) + "}");
      }
      stEntry.writeDelimitedTo(out);
    }
    recordSectionLength(SectionName.STRING_TABLE.name());
  }

  /**
   * Processes the XML file back into an fsimage.
   */
  private void processXml() throws Exception {
    LOG.debug("Loading <fsimage>.");
    expectTag("fsimage", false);
    // Read the <version> tag.
    readVersion();
    // Write the HDFSIMG1 magic number which begins the fsimage file.
    out.write(FSImageUtil.MAGIC_HEADER);
    // Write a series of fsimage sections.
    sectionStartOffset = FSImageUtil.MAGIC_HEADER.length;
    final HashSet<String> unprocessedSections =
        new HashSet<>(sections.keySet());
    while (!unprocessedSections.isEmpty()) {
      XMLEvent ev = expectTag("[section header]", true);
      if (ev.getEventType() == XMLStreamConstants.END_ELEMENT) {
        if (ev.asEndElement().getName().getLocalPart().equals("fsimage")) {
          throw new IOException("FSImage XML ended prematurely, without " +
              "including section(s) " + StringUtils.join(", ",
              unprocessedSections));
        }
        throw new IOException("Got unexpected tag end event for " +
            ev.asEndElement().getName().getLocalPart() + " while looking " +
            "for section header tag.");
      } else if (ev.getEventType() != XMLStreamConstants.START_ELEMENT) {
        throw new IOException("Expected section header START_ELEMENT; " +
            "got event of type " + ev.getEventType());
      }
      String sectionName = ev.asStartElement().getName().getLocalPart();
      if (!unprocessedSections.contains(sectionName)) {
        throw new IOException("Unknown or duplicate section found for " +
            sectionName);
      }
      SectionProcessor sectionProcessor = sections.get(sectionName);
      if (sectionProcessor == null) {
        throw new IOException("Unknown FSImage section " + sectionName +
            ".  Valid section names are [" +
            StringUtils.join(", ", sections.keySet()) + "]");
      }
      unprocessedSections.remove(sectionName);
      sectionProcessor.process();
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
      LOG.debug("Writing FileSummary: {" +
          TextFormat.printToString(fileSummary) + "}");
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
   * Run the OfflineImageReconstructor.
   *
   * @param inputPath         The input path to use.
   * @param outputPath        The output path to use.
   *
   * @throws Exception        On error.
   */
  public static void run(String inputPath, String outputPath)
      throws Exception {
    MessageDigest digester = MD5Hash.getDigester();
    OutputStream fout = null;
    File foutHash = new File(outputPath + ".md5");
    Files.deleteIfExists(foutHash.toPath()); // delete any .md5 file that exists
    CountingOutputStream out = null;
    InputStream fis = null;
    InputStreamReader reader = null;
    try {
      Files.deleteIfExists(Paths.get(outputPath));
      fout = Files.newOutputStream(Paths.get(outputPath));
      fis = Files.newInputStream(Paths.get(inputPath));
      reader = new InputStreamReader(fis, Charset.forName("UTF-8"));
      out = new CountingOutputStream(
          new DigestOutputStream(
              new BufferedOutputStream(fout), digester));
      OfflineImageReconstructor oir =
          new OfflineImageReconstructor(out, reader);
      oir.processXml();
    } finally {
      IOUtils.cleanupWithLogger(LOG, reader, fis, out, fout);
    }
    // Write the md5 file
    MD5FileUtils.saveMD5File(new File(outputPath),
        new MD5Hash(digester.digest()));
  }
}
