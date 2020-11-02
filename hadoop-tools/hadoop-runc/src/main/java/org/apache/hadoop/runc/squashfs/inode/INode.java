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

package org.apache.hadoop.runc.squashfs.inode;

import org.apache.hadoop.runc.squashfs.SquashFsException;
import org.apache.hadoop.runc.squashfs.metadata.MetadataWriter;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;

import java.io.DataInput;
import java.io.IOException;

public interface INode {

  int XATTR_NOT_PRESENT = 0xffff_ffff;

  static INode read(SuperBlock sb, DataInput in)
      throws SquashFsException, IOException {
    INodeType inodeType = INodeType.fromValue(in.readShort());
    INode inode = inodeType.create();
    inode.readData(sb, in);
    return inode;
  }

  INodeType getInodeType();

  int getSerializedSize();

  short getPermissions();

  void setPermissions(short permissions);

  short getUidIdx();

  void setUidIdx(short uidIdx);

  short getGidIdx();

  void setGidIdx(short uidIdx);

  int getModifiedTime();

  void setModifiedTime(int modifiedTime);

  int getInodeNumber();

  void setInodeNumber(int inodeNumber);

  int getNlink();

  void copyTo(INode dest);

  INode simplify();

  void readData(SuperBlock sb, DataInput in)
      throws SquashFsException, IOException;

  void writeData(MetadataWriter out) throws IOException;

}
