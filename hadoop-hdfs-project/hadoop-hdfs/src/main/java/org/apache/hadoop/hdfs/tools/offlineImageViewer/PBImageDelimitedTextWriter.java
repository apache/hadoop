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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INode;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeSymlink;

import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;

/**
 * A PBImageDelimitedTextWriter generates a text representation of the PB fsimage,
 * with each element separated by a delimiter string.  All of the elements
 * common to both inodes and inodes-under-construction are included. When
 * processing an fsimage with a layout version that did not include an
 * element, such as AccessTime, the output file will include a column
 * for the value, but no value will be included.
 *
 * Individual block information for each file is not currently included.
 *
 * The default delimiter is tab, as this is an unlikely value to be included in
 * an inode path or other text metadata. The delimiter value can be via the
 * constructor.
 */
public class PBImageDelimitedTextWriter extends PBImageTextWriter {
  private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm";
  private boolean printStoragePolicy;

  static class OutputEntryBuilder {
    private final SimpleDateFormat dateFormatter =
        new SimpleDateFormat(DATE_FORMAT);

    private PBImageDelimitedTextWriter writer;
    private Path path;
    private int replication = 0;
    private long modificationTime;
    private long accessTime = 0;
    private long preferredBlockSize = 0;
    private int blocksCount = 0;
    private long fileSize = 0;
    private long nsQuota = 0;
    private long dsQuota = 0;
    private int storagePolicy = 0;

    private String dirPermission = "-";
    private PermissionStatus permissionStatus;
    private String aclPermission = "";

    OutputEntryBuilder(PBImageDelimitedTextWriter writer, INode inode) {
      this.writer = writer;
      switch (inode.getType()) {
      case FILE:
        INodeFile file = inode.getFile();
        replication = file.getReplication();
        modificationTime = file.getModificationTime();
        accessTime = file.getAccessTime();
        preferredBlockSize = file.getPreferredBlockSize();
        blocksCount = file.getBlocksCount();
        fileSize = FSImageLoader.getFileSize(file);
        permissionStatus = writer.getPermission(file.getPermission());
        if (file.hasAcl() && file.getAcl().getEntriesCount() > 0){
          aclPermission = "+";
        }
        storagePolicy = file.getStoragePolicyID();
        break;
      case DIRECTORY:
        INodeDirectory dir = inode.getDirectory();
        modificationTime = dir.getModificationTime();
        nsQuota = dir.getNsQuota();
        dsQuota = dir.getDsQuota();
        dirPermission = "d";
        permissionStatus = writer.getPermission(dir.getPermission());
        if (dir.hasAcl() && dir.getAcl().getEntriesCount() > 0) {
          aclPermission = "+";
        }
        storagePolicy = writer.getStoragePolicy(dir.getXAttrs());
        break;
      case SYMLINK:
        INodeSymlink s = inode.getSymlink();
        modificationTime = s.getModificationTime();
        accessTime = s.getAccessTime();
        permissionStatus = writer.getPermission(s.getPermission());
        storagePolicy = HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
        break;
      default:
        break;
      }
    }

    void setPath(Path path) {
      this.path = path;
    }

    public String build() {
      assert permissionStatus != null : "The PermissionStatus is null!";
      assert permissionStatus.getUserName() != null : "User name is null!";
      assert permissionStatus.getGroupName() != null : "Group name is null!";

      StringBuffer buffer = new StringBuffer();
      writer.append(buffer, path.toString());
      writer.append(buffer, replication);
      writer.append(buffer, dateFormatter.format(modificationTime));
      writer.append(buffer, dateFormatter.format(accessTime));
      writer.append(buffer, preferredBlockSize);
      writer.append(buffer, blocksCount);
      writer.append(buffer, fileSize);
      writer.append(buffer, nsQuota);
      writer.append(buffer, dsQuota);
      writer.append(buffer, dirPermission +
          permissionStatus.getPermission().toString() + aclPermission);
      writer.append(buffer, permissionStatus.getUserName());
      writer.append(buffer, permissionStatus.getGroupName());
      if (writer.printStoragePolicy) {
        writer.append(buffer, storagePolicy);
      }
      return buffer.substring(1);
    }
  }

  PBImageDelimitedTextWriter(PrintStream out, String delimiter, String tempPath)
      throws IOException {
    this(out, delimiter, tempPath, false);
  }

  PBImageDelimitedTextWriter(PrintStream out, String delimiter,
                             String tempPath, boolean printStoragePolicy)
      throws IOException {
    super(out, delimiter, tempPath);
    this.printStoragePolicy = printStoragePolicy;
  }

  @Override
  public String getEntry(String parent, INode inode) {
    OutputEntryBuilder entryBuilder =
        new OutputEntryBuilder(this, inode);

    String inodeName = inode.getName().toStringUtf8();
    Path path = new Path(parent.isEmpty() ? "/" : parent,
      inodeName.isEmpty() ? "/" : inodeName);
    entryBuilder.setPath(path);

    return entryBuilder.build();
  }

  @Override
  public String getHeader() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("Path");
    append(buffer, "Replication");
    append(buffer, "ModificationTime");
    append(buffer, "AccessTime");
    append(buffer, "PreferredBlockSize");
    append(buffer, "BlocksCount");
    append(buffer, "FileSize");
    append(buffer, "NSQUOTA");
    append(buffer, "DSQUOTA");
    append(buffer, "Permission");
    append(buffer, "UserName");
    append(buffer, "GroupName");
    if (printStoragePolicy) {
      append(buffer, "StoragePolicyId");
    }
    return buffer.toString();
  }

  @Override
  public void afterOutput() {
    // do nothing
  }
}
