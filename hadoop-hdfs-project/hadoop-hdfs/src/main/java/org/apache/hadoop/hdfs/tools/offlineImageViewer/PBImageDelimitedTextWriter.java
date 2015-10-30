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
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INode;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeSymlink;

import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;

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
  static final String DEFAULT_DELIMITER = "\t";
  private static final String DATE_FORMAT="yyyy-MM-dd HH:mm";
  private final SimpleDateFormat dateFormatter =
      new SimpleDateFormat(DATE_FORMAT);

  private final String delimiter;

  PBImageDelimitedTextWriter(PrintStream out, String delimiter, String tempPath)
      throws IOException {
    super(out, tempPath);
    this.delimiter = delimiter;
  }

  private String formatDate(long date) {
    return dateFormatter.format(new Date(date));
  }

  private void append(StringBuffer buffer, int field) {
    buffer.append(delimiter);
    buffer.append(field);
  }

  private void append(StringBuffer buffer, long field) {
    buffer.append(delimiter);
    buffer.append(field);
  }

  private void append(StringBuffer buffer, String field) {
    buffer.append(delimiter);
    buffer.append(field);
  }

  @Override
  public String getEntry(String parent, INode inode) {
    StringBuffer buffer = new StringBuffer();
    String inodeName = inode.getName().toStringUtf8();
    Path path = new Path(parent.isEmpty() ? "/" : parent,
      inodeName.isEmpty() ? "/" : inodeName);
    buffer.append(path.toString());
    PermissionStatus p = null;

    switch (inode.getType()) {
    case FILE:
      INodeFile file = inode.getFile();
      p = getPermission(file.getPermission());
      append(buffer, file.getReplication());
      append(buffer, formatDate(file.getModificationTime()));
      append(buffer, formatDate(file.getAccessTime()));
      append(buffer, file.getPreferredBlockSize());
      append(buffer, file.getBlocksCount());
      append(buffer, FSImageLoader.getFileSize(file));
      append(buffer, 0);  // NS_QUOTA
      append(buffer, 0);  // DS_QUOTA
      break;
    case DIRECTORY:
      INodeDirectory dir = inode.getDirectory();
      p = getPermission(dir.getPermission());
      append(buffer, 0);  // Replication
      append(buffer, formatDate(dir.getModificationTime()));
      append(buffer, formatDate(0));  // Access time.
      append(buffer, 0);  // Block size.
      append(buffer, 0);  // Num blocks.
      append(buffer, 0);  // Num bytes.
      append(buffer, dir.getNsQuota());
      append(buffer, dir.getDsQuota());
      break;
    case SYMLINK:
      INodeSymlink s = inode.getSymlink();
      p = getPermission(s.getPermission());
      append(buffer, 0);  // Replication
      append(buffer, formatDate(s.getModificationTime()));
      append(buffer, formatDate(s.getAccessTime()));
      append(buffer, 0);  // Block size.
      append(buffer, 0);  // Num blocks.
      append(buffer, 0);  // Num bytes.
      append(buffer, 0);  // NS_QUOTA
      append(buffer, 0);  // DS_QUOTA
      break;
    default:
      break;
    }
    assert p != null;
    append(buffer, p.getPermission().toString());
    append(buffer, p.getUserName());
    append(buffer, p.getGroupName());
    return buffer.toString();
  }
}
