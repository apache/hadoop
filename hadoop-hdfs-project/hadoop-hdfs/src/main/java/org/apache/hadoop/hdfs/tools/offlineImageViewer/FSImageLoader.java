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
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf;
import org.apache.hadoop.hdfs.server.namenode.FSImageUtil;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.LimitInputStream;

/**
 * FSImageLoader loads fsimage and provide methods to return JSON formatted
 * file status of the namespace of the fsimage.
 */
class FSImageLoader {
  public static final Log LOG = LogFactory.getLog(FSImageHandler.class);

  private static String[] stringTable;
  private static Map<Long, FsImageProto.INodeSection.INode> inodes =
      Maps.newHashMap();
  private static Map<Long, long[]> dirmap = Maps.newHashMap();
  private static List<FsImageProto.INodeReferenceSection.INodeReference>
      refList = Lists.newArrayList();

  private FSImageLoader() {}

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
    FileInputStream fin = null;
    try {
      fin = new FileInputStream(file.getFD());

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

        switch (FSImageFormatProtobuf.SectionName.fromString(s.getName())) {
          case STRING_TABLE:
            loadStringTable(is);
            break;
          case INODE:
            loadINodeSection(is);
            break;
          case INODE_REFERENCE:
            loadINodeReferenceSection(is);
            break;
          case INODE_DIR:
            loadINodeDirectorySection(is);
            break;
          default:
            break;
        }
      }
    } finally {
      IOUtils.cleanup(null, fin);
    }
    return new FSImageLoader();
  }

  private static void loadINodeDirectorySection(InputStream in)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Loading directory section");
    }
    while (true) {
      FsImageProto.INodeDirectorySection.DirEntry e =
          FsImageProto.INodeDirectorySection.DirEntry.parseDelimitedFrom(in);
      // note that in is a LimitedInputStream
      if (e == null) {
        break;
      }
      long[] l = new long[e.getChildrenCount() + e.getRefChildrenCount()];
      for (int i = 0; i < e.getChildrenCount(); ++i) {
        l[i] = e.getChildren(i);
      }
      for (int i = e.getChildrenCount(); i < l.length; i++) {
        int refId = e.getRefChildren(i - e.getChildrenCount());
        l[i] = refList.get(refId).getReferredId();
      }
      dirmap.put(e.getParent(), l);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Loaded directory (parent " + e.getParent()
            + ") with " + e.getChildrenCount() + " children and "
            + e.getRefChildrenCount() + " reference children");
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Loaded " + dirmap.size() + " directories");
    }
  }

  private static void loadINodeReferenceSection(InputStream in)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Loading inode reference section");
    }
    while (true) {
      FsImageProto.INodeReferenceSection.INodeReference e =
          FsImageProto.INodeReferenceSection.INodeReference
              .parseDelimitedFrom(in);
      if (e == null) {
        break;
      }
      refList.add(e);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Loaded inode reference named '" + e.getName()
            + "' referring to id " + e.getReferredId() + "");
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Loaded " + refList.size() + " inode references");
    }
  }

  private static void loadINodeSection(InputStream in) throws IOException {
    FsImageProto.INodeSection s = FsImageProto.INodeSection
        .parseDelimitedFrom(in);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Found " + s.getNumInodes() + " inodes in inode section");
    }
    for (int i = 0; i < s.getNumInodes(); ++i) {
      FsImageProto.INodeSection.INode p = FsImageProto.INodeSection.INode
          .parseDelimitedFrom(in);
      inodes.put(p.getId(), p);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Loaded inode id " + p.getId() + " type " + p.getType()
            + " name '" + p.getName().toStringUtf8() + "'");
      }
    }
  }

  private static void loadStringTable(InputStream in) throws IOException {
    FsImageProto.StringTableSection s = FsImageProto.StringTableSection
        .parseDelimitedFrom(in);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Found " + s.getNumEntry() + " strings in string section");
    }
    stringTable = new String[s.getNumEntry() + 1];
    for (int i = 0; i < s.getNumEntry(); ++i) {
      FsImageProto.StringTableSection.Entry e = FsImageProto
          .StringTableSection.Entry.parseDelimitedFrom(in);
      stringTable[e.getId()] = e.getStr();
      if (LOG.isTraceEnabled()) {
        LOG.trace("Loaded string " + e.getStr());
      }
    }
  }

  /**
   * Return the JSON formatted FileStatus of the specified file.
   * @param path a path specifies a file
   * @return JSON formatted FileStatus
   * @throws IOException if failed to serialize fileStatus to JSON.
   */
  String getFileStatus(String path) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    FsImageProto.INodeSection.INode inode = inodes.get(getINodeId(path));
    return "{\"FileStatus\":\n"
        + mapper.writeValueAsString(getFileStatus(inode, false)) + "\n}\n";
  }

  /**
   * Return the JSON formatted list of the files in the specified directory.
   * @param path a path specifies a directory to list
   * @return JSON formatted file list in the directory
   * @throws IOException if failed to serialize fileStatus to JSON.
   */
  String listStatus(String path) throws IOException {
    StringBuilder sb = new StringBuilder();
    ObjectMapper mapper = new ObjectMapper();
    List<Map<String, Object>> fileStatusList = getFileStatusList(path);
    sb.append("{\"FileStatuses\":{\"FileStatus\":[\n");
    int i = 0;
    for (Map<String, Object> fileStatusMap : fileStatusList) {
      if (i++ != 0) {
        sb.append(',');
      }
      sb.append(mapper.writeValueAsString(fileStatusMap));
    }
    sb.append("\n]}}\n");
    return sb.toString();
  }

  private List<Map<String, Object>> getFileStatusList(String path) {
    List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
    long id = getINodeId(path);
    FsImageProto.INodeSection.INode inode = inodes.get(id);
    if (inode.getType() == FsImageProto.INodeSection.INode.Type.DIRECTORY) {
      if (!dirmap.containsKey(id)) {
        // if the directory is empty, return empty list
        return list;
      }
      long[] children = dirmap.get(id);
      for (long cid : children) {
        list.add(getFileStatus(inodes.get(cid), true));
      }
    } else {
      list.add(getFileStatus(inode, false));
    }
    return list;
  }

  /**
   * Return the JSON formatted ACL status of the specified file.
   * @param path a path specifies a file
   * @return JSON formatted AclStatus
   * @throws IOException if failed to serialize fileStatus to JSON.
   */
  String getAclStatus(String path) throws IOException {
    StringBuilder sb = new StringBuilder();
    List<AclEntry> aclEntryList = getAclEntryList(path);
    PermissionStatus p = getPermissionStatus(path);
    sb.append("{\"AclStatus\":{\"entries\":[");
    int i = 0;
    for (AclEntry aclEntry : aclEntryList) {
      if (i++ != 0) {
        sb.append(',');
      }
      sb.append('"');
      sb.append(aclEntry.toString());
      sb.append('"');
    }
    sb.append("],\"group\": \"");
    sb.append(p.getGroupName());
    sb.append("\",\"owner\": \"");
    sb.append(p.getUserName());
    sb.append("\",\"stickyBit\": ");
    sb.append(p.getPermission().getStickyBit());
    sb.append("}}\n");
    return sb.toString();
  }

  private List<AclEntry> getAclEntryList(String path) {
    long id = getINodeId(path);
    FsImageProto.INodeSection.INode inode = inodes.get(id);
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

  private PermissionStatus getPermissionStatus(String path) {
    long id = getINodeId(path);
    FsImageProto.INodeSection.INode inode = inodes.get(id);
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
  private long getINodeId(String strPath) {
    if (strPath.equals("/")) {
      return INodeId.ROOT_INODE_ID;
    }

    String[] nameList = strPath.split("/");
    Preconditions.checkArgument(nameList.length > 1,
                                "Illegal path: " + strPath);
    long id = INodeId.ROOT_INODE_ID;
    for (int i = 1; i < nameList.length; i++) {
      long[] children = dirmap.get(id);
      Preconditions.checkNotNull(children, "File: " +
          strPath + " is not found in the fsimage.");
      String cName = nameList[i];
      boolean findChildren = false;
      for (long cid : children) {
        if (cName.equals(inodes.get(cid).getName().toStringUtf8())) {
          id = cid;
          findChildren = true;
          break;
        }
      }
      Preconditions.checkArgument(findChildren, "File: " +
          strPath + " is not found in the fsimage.");
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
        map.put("replication", f.getReplication());
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

  private long getFileSize(FsImageProto.INodeSection.INodeFile f) {
    long size = 0;
    for (HdfsProtos.BlockProto p : f.getBlocksList()) {
      size += p.getNumBytes();
    }
    return size;
  }

  private String toString(FsPermission permission) {
    return String.format("%o", permission.toShort());
  }
}
