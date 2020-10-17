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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.protobuf.CodedInputStream;
import org.apache.hadoop.thirdparty.protobuf.InvalidProtocolBufferException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf;
import org.apache.hadoop.hdfs.server.namenode.FSImageUtil;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.hdfs.server.namenode.SerialNumberManager;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.resources.XAttrEncodingParam;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.LimitInputStream;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

/**
 * FSImageLoader loads fsimage and provide methods to return JSON formatted
 * file status of the namespace of the fsimage.
 */
class FSImageLoader {
  public static final Logger LOG =
      LoggerFactory.getLogger(FSImageHandler.class);

  private final SerialNumberManager.StringTable stringTable;
  // byte representation of inodes, sorted by id
  private final byte[][] inodes;
  private final Map<Long, long[]> dirmap;
  private static final Comparator<byte[]> INODE_BYTES_COMPARATOR = new
          Comparator<byte[]>() {
    @Override
    public int compare(byte[] o1, byte[] o2) {
      try {
        final FsImageProto.INodeSection.INode l = FsImageProto.INodeSection
                .INode.parseFrom(o1);
        final FsImageProto.INodeSection.INode r = FsImageProto.INodeSection
                .INode.parseFrom(o2);
        if (l.getId() < r.getId()) {
          return -1;
        } else if (l.getId() > r.getId()) {
          return 1;
        } else {
          return 0;
        }
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
  };

  private FSImageLoader(SerialNumberManager.StringTable stringTable,
                        byte[][] inodes, Map<Long, long[]> dirmap) {
    this.stringTable = stringTable;
    this.inodes = inodes;
    this.dirmap = dirmap;
  }

  /**
   * Load fsimage into the memory.
   * @param inputFile the filepath of the fsimage to load.
   * @return FSImageLoader
   * @throws IOException if failed to load fsimage.
   */
  static FSImageLoader load(String inputFile) throws IOException {
    Configuration conf = new Configuration();
    RandomAccessFile file = new RandomAccessFile(inputFile, "r");
    if (!FSImageUtil.checkFileFormat(file)) {
      throw new IOException("Unrecognized FSImage");
    }

    FsImageProto.FileSummary summary = FSImageUtil.loadSummary(file);


    try (FileInputStream fin = new FileInputStream(file.getFD())) {
      // Map to record INodeReference to the referred id
      ImmutableList<Long> refIdList = null;
      SerialNumberManager.StringTable stringTable = null;
      byte[][] inodes = null;
      Map<Long, long[]> dirmap = null;

      ArrayList<FsImageProto.FileSummary.Section> sections =
          Lists.newArrayList(summary.getSectionsList());
      Collections.sort(sections,
          new Comparator<FsImageProto.FileSummary.Section>() {
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

      for (FsImageProto.FileSummary.Section s : sections) {
        fin.getChannel().position(s.getOffset());
        InputStream is = FSImageUtil.wrapInputStreamForCompression(conf,
            summary.getCodec(), new BufferedInputStream(new LimitInputStream(
            fin, s.getLength())));

        if (LOG.isDebugEnabled()) {
          LOG.debug("Loading section " + s.getName() + " length: " + s.getLength
              ());
        }

        FSImageFormatProtobuf.SectionName sectionName
            = FSImageFormatProtobuf.SectionName.fromString(s.getName());
        if (sectionName == null) {
          throw new IOException("Unrecognized section " + s.getName());
        }
        switch (sectionName) {
          case STRING_TABLE:
            stringTable = loadStringTable(is);
            break;
          case INODE:
            inodes = loadINodeSection(is);
            break;
          case INODE_REFERENCE:
            refIdList = loadINodeReferenceSection(is);
            break;
          case INODE_DIR:
            dirmap = loadINodeDirectorySection(is, refIdList);
            break;
          default:
            break;
        }
      }
      return new FSImageLoader(stringTable, inodes, dirmap);
    }
  }

  private static Map<Long, long[]> loadINodeDirectorySection
          (InputStream in, List<Long> refIdList)
      throws IOException {
    LOG.info("Loading inode directory section");
    Map<Long, long[]> dirs = Maps.newHashMap();
    long counter = 0;
    while (true) {
      FsImageProto.INodeDirectorySection.DirEntry e =
          FsImageProto.INodeDirectorySection.DirEntry.parseDelimitedFrom(in);
      // note that in is a LimitedInputStream
      if (e == null) {
        break;
      }
      ++counter;

      long[] l = new long[e.getChildrenCount() + e.getRefChildrenCount()];
      for (int i = 0; i < e.getChildrenCount(); ++i) {
        l[i] = e.getChildren(i);
      }
      for (int i = e.getChildrenCount(); i < l.length; i++) {
        int refId = e.getRefChildren(i - e.getChildrenCount());
        l[i] = refIdList.get(refId);
      }
      dirs.put(e.getParent(), l);
    }
    LOG.info("Loaded " + counter + " directories");
    return dirs;
  }

  static ImmutableList<Long> loadINodeReferenceSection(InputStream in)
      throws IOException {
    LOG.info("Loading inode references");
    ImmutableList.Builder<Long> builder = ImmutableList.builder();
    long counter = 0;
    while (true) {
      FsImageProto.INodeReferenceSection.INodeReference e =
          FsImageProto.INodeReferenceSection.INodeReference
              .parseDelimitedFrom(in);
      if (e == null) {
        break;
      }
      ++counter;
      builder.add(e.getReferredId());
    }
    LOG.info("Loaded " + counter + " inode references");
    return builder.build();
  }

  private static byte[][] loadINodeSection(InputStream in)
          throws IOException {
    FsImageProto.INodeSection s = FsImageProto.INodeSection
        .parseDelimitedFrom(in);
    LOG.info("Loading " + s.getNumInodes() + " inodes.");
    final byte[][] inodes = new byte[(int) s.getNumInodes()][];

    for (int i = 0; i < s.getNumInodes(); ++i) {
      int size = CodedInputStream.readRawVarint32(in.read(), in);
      byte[] bytes = new byte[size];
      IOUtils.readFully(in, bytes, 0, size);
      inodes[i] = bytes;
    }
    LOG.debug("Sorting inodes");
    Arrays.sort(inodes, INODE_BYTES_COMPARATOR);
    LOG.debug("Finished sorting inodes");
    return inodes;
  }

  static SerialNumberManager.StringTable loadStringTable(InputStream in)
        throws IOException {
    FsImageProto.StringTableSection s = FsImageProto.StringTableSection
        .parseDelimitedFrom(in);
    LOG.info("Loading " + s.getNumEntry() + " strings");
    SerialNumberManager.StringTable stringTable =
        SerialNumberManager.newStringTable(s.getNumEntry(), s.getMaskBits());
    for (int i = 0; i < s.getNumEntry(); ++i) {
      FsImageProto.StringTableSection.Entry e = FsImageProto
          .StringTableSection.Entry.parseDelimitedFrom(in);
      stringTable.put(e.getId(), e.getStr());
    }
    return stringTable;
  }

  /**
   * Return the JSON formatted FileStatus of the specified file.
   * @param path a path specifies a file
   * @return JSON formatted FileStatus
   * @throws IOException if failed to serialize fileStatus to JSON.
   */
  String getFileStatus(String path) throws IOException {
    FsImageProto.INodeSection.INode inode = fromINodeId(lookup(path));
    return "{\"FileStatus\":\n"
        + JsonUtil.toJsonString(getFileStatus(inode, false)) + "\n}\n";
  }

  /**
   * Return the JSON formatted list of the files in the specified directory.
   * @param path a path specifies a directory to list
   * @return JSON formatted file list in the directory
   * @throws IOException if failed to serialize fileStatus to JSON.
   */
  String listStatus(String path) throws IOException {
    StringBuilder sb = new StringBuilder();
    List<Map<String, Object>> fileStatusList = getFileStatusList(path);
    sb.append("{\"FileStatuses\":{\"FileStatus\":[\n");
    int i = 0;
    for (Map<String, Object> fileStatusMap : fileStatusList) {
      if (i++ != 0) {
        sb.append(',');
      }
      sb.append(JsonUtil.toJsonString(fileStatusMap));
    }
    sb.append("\n]}}\n");
    return sb.toString();
  }

  private List<Map<String, Object>> getFileStatusList(String path)
          throws IOException {
    List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
    long id = lookup(path);
    FsImageProto.INodeSection.INode inode = fromINodeId(id);
    if (inode.getType() == FsImageProto.INodeSection.INode.Type.DIRECTORY) {
      if (!dirmap.containsKey(id)) {
        // if the directory is empty, return empty list
        return list;
      }
      long[] children = dirmap.get(id);
      for (long cid : children) {
        list.add(getFileStatus(fromINodeId(cid), true));
      }
    } else {
      list.add(getFileStatus(inode, false));
    }
    return list;
  }

  /**
   * Return the JSON formatted ContentSummary of the specified path.
   * @param path a path specifies a file or directory
   * @return JSON formatted ContentSummary
   * @throws IOException if failed to serialize ContentSummary to JSON.
   */
  String getContentSummary(String path) throws IOException {
    return "{\"ContentSummary\":\n"
        + JsonUtil.toJsonString(getContentSummaryMap(path)) + "\n}\n";
  }

  private Map<String, Object> getContentSummaryMap(String path)
      throws IOException {
    long id = lookup(path);
    INode inode = fromINodeId(id);
    long spaceQuota = 0;
    long nsQuota = 0;
    long[] data = new long[4];
    FsImageProto.INodeSection.INodeFile f = inode.getFile();
    switch (inode.getType()) {
    case FILE:
      data[0] = 0;
      data[1] = 1;
      data[2] = getFileSize(f);
      nsQuota = -1;
      data[3] = data[2] * f.getReplication();
      spaceQuota = -1;
      return fillSummaryMap(spaceQuota, nsQuota, data);
    case DIRECTORY:
      fillDirSummary(id, data);
      nsQuota = inode.getDirectory().getNsQuota();
      spaceQuota = inode.getDirectory().getDsQuota();
      return fillSummaryMap(spaceQuota, nsQuota, data);
    case SYMLINK:
      data[0] = 0;
      data[1] = 1;
      data[2] = 0;
      nsQuota = -1;
      data[3] = 0;
      spaceQuota = -1;
      return fillSummaryMap(spaceQuota, nsQuota, data);
    default:
      return null;
    }

  }

  private Map<String, Object> fillSummaryMap(long spaceQuota,
      long nsQuota, long[] data) {
    Map<String, Object> map = Maps.newHashMap();
    map.put("directoryCount", data[0]);
    map.put("fileCount", data[1]);
    map.put("length", data[2]);
    map.put("quota", nsQuota);
    map.put("spaceConsumed", data[3]);
    map.put("spaceQuota", spaceQuota);
    return map;
  }

  private void fillDirSummary(long id, long[] data) throws IOException {
    data[0]++;
    long[] children = dirmap.get(id);
    if (children == null) {
      return;
    }

    for (long cid : children) {
      INode node = fromINodeId(cid);
      switch (node.getType()) {
      case DIRECTORY:
        fillDirSummary(cid, data);
        break;
      case FILE:
        FsImageProto.INodeSection.INodeFile f = node.getFile();
        long curLength = getFileSize(f);
        data[1]++;
        data[2] += curLength;
        data[3] += (curLength) * (f.getReplication());
        break;
      case SYMLINK:
        data[1]++;
        break;
      default:
        break;
      }
    }
  }

  /**
   * Return the JSON formatted XAttrNames of the specified file.
   *
   * @param path
   *          a path specifies a file
   * @return JSON formatted XAttrNames
   * @throws IOException
   *           if failed to serialize fileStatus to JSON.
   */
  String listXAttrs(String path) throws IOException {
    return JsonUtil.toJsonString(getXAttrList(path));
  }

  /**
   * Return the JSON formatted XAttrs of the specified file.
   *
   * @param path
   *          a path specifies a file
   * @return JSON formatted XAttrs
   * @throws IOException
   *           if failed to serialize fileStatus to JSON.
   */
  String getXAttrs(String path, List<String> names, String encoder)
      throws IOException {

    List<XAttr> xAttrs = getXAttrList(path);
    List<XAttr> filtered;
    if (names == null || names.size() == 0) {
      filtered = xAttrs;
    } else {
      filtered = Lists.newArrayListWithCapacity(names.size());
      for (String name : names) {
        XAttr search = XAttrHelper.buildXAttr(name);

        boolean found = false;
        for (XAttr aXAttr : xAttrs) {
          if (aXAttr.getNameSpace() == search.getNameSpace()
              && aXAttr.getName().equals(search.getName())) {

            filtered.add(aXAttr);
            found = true;
            break;
          }
        }

        if (!found) {
          throw new IOException(
              "At least one of the attributes provided was not found.");
        }
      }

    }
    return JsonUtil.toJsonString(filtered,
        new XAttrEncodingParam(encoder).getEncoding());
  }

  private List<XAttr> getXAttrList(String path) throws IOException {
    long id = lookup(path);
    FsImageProto.INodeSection.INode inode = fromINodeId(id);
    switch (inode.getType()) {
    case FILE:
      return FSImageFormatPBINode.Loader.loadXAttrs(
          inode.getFile().getXAttrs(), stringTable);
    case DIRECTORY:
      return FSImageFormatPBINode.Loader.loadXAttrs(inode.getDirectory()
          .getXAttrs(), stringTable);
    default:
      return null;
    }
  }

  /**
   * Return the JSON formatted ACL status of the specified file.
   * @param path a path specifies a file
   * @return JSON formatted AclStatus
   * @throws IOException if failed to serialize fileStatus to JSON.
   */
  String getAclStatus(String path) throws IOException {
    PermissionStatus p = getPermissionStatus(path);
    List<AclEntry> aclEntryList = getAclEntryList(path);
    FsPermission permission = p.getPermission();
    AclStatus.Builder builder = new AclStatus.Builder();
    builder.owner(p.getUserName()).group(p.getGroupName())
        .addEntries(aclEntryList).setPermission(permission)
        .stickyBit(permission.getStickyBit());
    AclStatus aclStatus = builder.build();
    return JsonUtil.toJsonString(aclStatus);
  }

  private List<AclEntry> getAclEntryList(String path) throws IOException {
    long id = lookup(path);
    FsImageProto.INodeSection.INode inode = fromINodeId(id);
    switch (inode.getType()) {
      case FILE: {
        FsImageProto.INodeSection.INodeFile f = inode.getFile();
        return FSImageFormatPBINode.Loader.loadAclEntries(
            f.getAcl(), stringTable);
      }
      case DIRECTORY: {
        FsImageProto.INodeSection.INodeDirectory d = inode.getDirectory();
        return FSImageFormatPBINode.Loader.loadAclEntries(
            d.getAcl(), stringTable);
      }
      default: {
        return new ArrayList<AclEntry>();
      }
    }
  }

  private PermissionStatus getPermissionStatus(String path) throws IOException {
    long id = lookup(path);
    FsImageProto.INodeSection.INode inode = fromINodeId(id);
    switch (inode.getType()) {
      case FILE: {
        FsImageProto.INodeSection.INodeFile f = inode.getFile();
        return FSImageFormatPBINode.Loader.loadPermission(
            f.getPermission(), stringTable);
      }
      case DIRECTORY: {
        FsImageProto.INodeSection.INodeDirectory d = inode.getDirectory();
        return FSImageFormatPBINode.Loader.loadPermission(
            d.getPermission(), stringTable);
      }
      case SYMLINK: {
        FsImageProto.INodeSection.INodeSymlink s = inode.getSymlink();
        return FSImageFormatPBINode.Loader.loadPermission(
            s.getPermission(), stringTable);
      }
      default: {
        return null;
      }
    }
  }

  /**
   * Return the INodeId of the specified path.
   */
  private long lookup(String path) throws IOException {
    Preconditions.checkArgument(path.startsWith("/"));
    long id = INodeId.ROOT_INODE_ID;
    for (int offset = 0, next; offset < path.length(); offset = next) {
      next = path.indexOf('/', offset + 1);
      if (next == -1) {
        next = path.length();
      }
      if (offset + 1 > next) {
        break;
      }

      final String component = path.substring(offset + 1, next);

      if (component.isEmpty()) {
        continue;
      }

      final long[] children = dirmap.get(id);
      if (children == null) {
        throw new FileNotFoundException(path);
      }

      boolean found = false;
      for (long cid : children) {
        FsImageProto.INodeSection.INode child = fromINodeId(cid);
        if (component.equals(child.getName().toStringUtf8())) {
          found = true;
          id = child.getId();
          break;
        }
      }
      if (!found) {
        throw new FileNotFoundException(path);
      }
    }
    return id;
  }

  private Map<String, Object> getFileStatus
      (FsImageProto.INodeSection.INode inode, boolean printSuffix){
    Map<String, Object> map = Maps.newHashMap();
    switch (inode.getType()) {
      case FILE: {
        FsImageProto.INodeSection.INodeFile f = inode.getFile();
        PermissionStatus p = FSImageFormatPBINode.Loader.loadPermission(
            f.getPermission(), stringTable);
        map.put("accessTime", f.getAccessTime());
        map.put("blockSize", f.getPreferredBlockSize());
        map.put("group", p.getGroupName());
        map.put("length", getFileSize(f));
        map.put("modificationTime", f.getModificationTime());
        map.put("owner", p.getUserName());
        map.put("pathSuffix",
            printSuffix ? inode.getName().toStringUtf8() : "");
        map.put("permission", toString(p.getPermission()));
        if (f.hasErasureCodingPolicyID()) {
          map.put("replication", INodeFile.DEFAULT_REPL_FOR_STRIPED_BLOCKS);
        } else {
          map.put("replication", f.getReplication());
        }
        map.put("type", inode.getType());
        map.put("fileId", inode.getId());
        map.put("childrenNum", 0);
        return map;
      }
      case DIRECTORY: {
        FsImageProto.INodeSection.INodeDirectory d = inode.getDirectory();
        PermissionStatus p = FSImageFormatPBINode.Loader.loadPermission(
            d.getPermission(), stringTable);
        map.put("accessTime", 0);
        map.put("blockSize", 0);
        map.put("group", p.getGroupName());
        map.put("length", 0);
        map.put("modificationTime", d.getModificationTime());
        map.put("owner", p.getUserName());
        map.put("pathSuffix",
            printSuffix ? inode.getName().toStringUtf8() : "");
        map.put("permission", toString(p.getPermission()));
        map.put("replication", 0);
        map.put("type", inode.getType());
        map.put("fileId", inode.getId());
        map.put("childrenNum", dirmap.containsKey(inode.getId()) ?
            dirmap.get(inode.getId()).length : 0);
        return map;
      }
      case SYMLINK: {
        FsImageProto.INodeSection.INodeSymlink d = inode.getSymlink();
        PermissionStatus p = FSImageFormatPBINode.Loader.loadPermission(
            d.getPermission(), stringTable);
        map.put("accessTime", d.getAccessTime());
        map.put("blockSize", 0);
        map.put("group", p.getGroupName());
        map.put("length", 0);
        map.put("modificationTime", d.getModificationTime());
        map.put("owner", p.getUserName());
        map.put("pathSuffix",
            printSuffix ? inode.getName().toStringUtf8() : "");
        map.put("permission", toString(p.getPermission()));
        map.put("replication", 0);
        map.put("type", inode.getType());
        map.put("symlink", d.getTarget().toStringUtf8());
        map.put("fileId", inode.getId());
        map.put("childrenNum", 0);
        return map;
      }
      default:
        return null;
    }
  }

  static long getFileSize(FsImageProto.INodeSection.INodeFile f) {
    long size = 0;
    for (HdfsProtos.BlockProto p : f.getBlocksList()) {
      size += p.getNumBytes();
    }
    return size;
  }

  private String toString(FsPermission permission) {
    return String.format("%o", permission.toShort());
  }

  private FsImageProto.INodeSection.INode fromINodeId(final long id)
          throws IOException {
    int l = 0, r = inodes.length;
    while (l < r) {
      int mid = l + (r - l) / 2;
      FsImageProto.INodeSection.INode n = FsImageProto.INodeSection.INode
              .parseFrom(inodes[mid]);
      long nid = n.getId();
      if (id > nid) {
        l = mid + 1;
      } else if (id < nid) {
        r = mid;
      } else {
        return n;
      }
    }
    return null;
  }
}
