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

import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.DECIMAL;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.UNSIGNED;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.dumpBin;

public class ExtendedSymlinkINode extends AbstractINode
    implements SymlinkINode {

  private static final byte[] EMPTY = new byte[0];

  private int nlink = 1;
  private byte[] targetPath = EMPTY;
  private int xattrIndex = XATTR_NOT_PRESENT;

  @Override
  public int getNlink() {
    return nlink;
  }

  @Override
  public void setNlink(int nlink) {
    this.nlink = nlink;
  }

  @Override
  public byte[] getTargetPath() {
    return targetPath;
  }

  @Override
  public void setTargetPath(byte[] targetPath) {
    this.targetPath = (targetPath == null) ? EMPTY : targetPath;
  }

  @Override
  public int getXattrIndex() {
    return xattrIndex;
  }

  @Override
  public void setXattrIndex(int xattrIndex) {
    this.xattrIndex = xattrIndex;
  }

  @Override
  public boolean isXattrPresent() {
    return xattrIndex != XATTR_NOT_PRESENT;
  }

  @Override
  protected int getChildSerializedSize() {
    return 12 + targetPath.length;
  }

  @Override
  protected String getName() {
    return "extended-symlink-inode";
  }

  @Override
  public INodeType getInodeType() {
    return INodeType.EXTENDED_SYMLINK;
  }

  @Override
  protected void readExtraData(SuperBlock sb, DataInput in)
      throws SquashFsException, IOException {
    nlink = in.readInt();
    int targetSize = in.readInt();
    targetPath = new byte[targetSize];
    in.readFully(targetPath);
    xattrIndex = in.readInt();
  }

  @Override
  protected void writeExtraData(MetadataWriter out) throws IOException {
    out.writeInt(nlink);
    out.writeInt(targetPath.length);
    out.write(targetPath);
    out.writeInt(xattrIndex);
  }

  @Override
  protected int getPreferredDumpWidth() {
    return 12;
  }

  @Override
  protected void dumpProperties(StringBuilder buf, int width) {
    dumpBin(buf, width, "nlink", nlink, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "targetSize", targetPath.length, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "targetPath", targetPath, 0, targetPath.length, 16, 2);
    dumpBin(buf, width, "xattrIndex", xattrIndex, DECIMAL, UNSIGNED);
  }

  public SymlinkINode simplify() {
    return BasicSymlinkINode.simplify(this);
  }
}
