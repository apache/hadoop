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
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.SectionName;
import org.apache.hadoop.hdfs.server.namenode.FSImageUtil;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeDirectorySection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeReferenceSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INode;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeSymlink;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.StringTableSection;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.io.IOUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.LimitInputStream;

/**
 * LsrPBImage displays the blocks of the namespace in a format very similar
 * to the output of ls/lsr.  Entries are marked as directories or not,
 * permissions listed, replication, username and groupname, along with size,
 * modification date and full path.
 *
 * Note: A significant difference between the output of the lsr command
 * and this image visitor is that this class cannot sort the file entries;
 * they are listed in the order they are stored within the fsimage file. 
 * Therefore, the output of this class cannot be directly compared to the
 * output of the lsr command.
 */
final class LsrPBImage {
  private final Configuration conf;
  private final PrintWriter out;
  private String[] stringTable;
  private HashMap<Long, INodeSection.INode> inodes = Maps.newHashMap();
  private HashMap<Long, long[]> dirmap = Maps.newHashMap();
  private ArrayList<INodeReferenceSection.INodeReference> refList = Lists.newArrayList();

  public LsrPBImage(Configuration conf, PrintWriter out) {
    this.conf = conf;
    this.out = out;
  }

  public void visit(RandomAccessFile file) throws IOException {
    if (!FSImageUtil.checkFileFormat(file)) {
      throw new IOException("Unrecognized FSImage");
    }

    FileSummary summary = FSImageUtil.loadSummary(file);
    FileInputStream fin = null;
    try {
      fin = new FileInputStream(file.getFD());

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

        switch (SectionName.fromString(s.getName())) {
        case STRING_TABLE:
          loadStringTable(is);
          break;
        case INODE:
          loadINodeSection(is);
          break;
        case INODE_REFRENCE:
          loadINodeReferenceSection(is);
          break;
        case INODE_DIR:
          loadINodeDirectorySection(is);
          break;
        default:
          break;
        }
      }
      list("", INodeId.ROOT_INODE_ID);
    } finally {
      IOUtils.cleanup(null, fin);
    }
  }

  private void list(String parent, long dirId) {
    INode inode = inodes.get(dirId);
    listINode(parent.isEmpty() ? "/" : parent, inode);
    long[] children = dirmap.get(dirId);
    if (children == null) {
      return;
    }
    String newParent = parent + inode.getName().toStringUtf8() + "/";
    for (long cid : children) {
      list(newParent, cid);
    }
  }

  private void listINode(String parent, INode inode) {
    switch (inode.getType()) {
    case FILE: {
      INodeFile f = inode.getFile();
      PermissionStatus p = FSImageFormatPBINode.Loader.loadPermission(
          f.getPermission(), stringTable);
      out.print(String.format("-%s %2s %8s %10s %10s %10d %s%s\n", p
          .getPermission().toString(), f.getReplication(), p.getUserName(), p
          .getGroupName(), f.getModificationTime(), getFileSize(f), parent,
          inode.getName().toStringUtf8()));
    }
      break;
    case DIRECTORY: {
      INodeDirectory d = inode.getDirectory();
      PermissionStatus p = FSImageFormatPBINode.Loader.loadPermission(
          d.getPermission(), stringTable);
      out.print(String.format("d%s  - %8s %10s %10s %10d %s%s\n", p
          .getPermission().toString(), p.getUserName(), p.getGroupName(), d
          .getModificationTime(), 0, parent, inode.getName().toStringUtf8()));
    }
      break;
    case SYMLINK: {
      INodeSymlink d = inode.getSymlink();
      PermissionStatus p = FSImageFormatPBINode.Loader.loadPermission(
          d.getPermission(), stringTable);
      out.print(String.format("-%s  - %8s %10s %10s %10d %s%s -> %s\n", p
          .getPermission().toString(), p.getUserName(), p.getGroupName(), 0, 0,
          parent, inode.getName().toStringUtf8(), d.getTarget().toStringUtf8()));
    }
      break;
    default:
      break;
    }
  }

  private long getFileSize(INodeFile f) {
    long size = 0;
    for (BlockProto p : f.getBlocksList()) {
      size += p.getNumBytes();
    }
    return size;
  }

  private void loadINodeDirectorySection(InputStream in) throws IOException {
    while (true) {
      INodeDirectorySection.DirEntry e = INodeDirectorySection.DirEntry
          .parseDelimitedFrom(in);
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
    }
  }

  private void loadINodeReferenceSection(InputStream in) throws IOException {
    while (true) {
      INodeReferenceSection.INodeReference e = INodeReferenceSection
          .INodeReference.parseDelimitedFrom(in);
      if (e == null) {
        break;
      }
      refList.add(e);
    }
  }

  private void loadINodeSection(InputStream in) throws IOException {
    INodeSection s = INodeSection.parseDelimitedFrom(in);
    for (int i = 0; i < s.getNumInodes(); ++i) {
      INodeSection.INode p = INodeSection.INode.parseDelimitedFrom(in);
      inodes.put(p.getId(), p);
    }
  }

  private void loadStringTable(InputStream in) throws IOException {
    StringTableSection s = StringTableSection.parseDelimitedFrom(in);
    stringTable = new String[s.getNumEntry() + 1];
    for (int i = 0; i < s.getNumEntry(); ++i) {
      StringTableSection.Entry e = StringTableSection.Entry
          .parseDelimitedFrom(in);
      stringTable[e.getId()] = e.getStr();
    }
  }
}
