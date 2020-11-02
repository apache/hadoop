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

package org.apache.hadoop.runc.squashfs;

import org.apache.hadoop.runc.squashfs.data.DataBlock;
import org.apache.hadoop.runc.squashfs.inode.FileINode;
import org.apache.hadoop.runc.squashfs.inode.INode;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

abstract public class AbstractSquashFsReader implements SquashFsReader {

  protected static int compareBytes(byte[] left, byte[] right) {
    for (int i = 0; i < left.length && i < right.length; i++) {
      int a = (left[i] & 0xff);
      int b = (right[i] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return left.length - right.length;
  }

  protected byte[] createSparseBlock(SuperBlock sb) {
    return new byte[sb.getBlockSize()];
  }

  abstract protected byte[] getSparseBlock();

  abstract protected DataBlock readBlock(
      FileINode fileInode,
      int blockNumber,
      boolean cache) throws IOException, SquashFsException;

  abstract protected DataBlock readFragment(
      FileINode fileInode,
      int fragmentSize,
      boolean cache) throws IOException, SquashFsException;

  @Override
  public long writeFileStream(INode inode, OutputStream out)
      throws IOException, SquashFsException {

    return writeFileOut(inode, (out instanceof DataOutput)
        ? (DataOutput) out
        : new DataOutputStream(out));
  }

  @Override
  public long writeFileOut(INode inode, DataOutput out)
      throws IOException, SquashFsException {

    if (!(inode instanceof FileINode)) {
      throw new IllegalArgumentException("Inode is not a file");
    }

    FileINode fileInode = (FileINode) inode;

    long fileSize = fileInode.getFileSize();
    int blockSize = getSuperBlock().getBlockSize();
    int blockCount = fileInode.getBlockSizes().length;
    boolean hasFragment = fileInode.isFragmentPresent();

    long bytesRead = 0L;

    for (int i = 0; i < blockCount; i++) {
      DataBlock data = readBlock(fileInode, i, false);

      if (i == (blockCount - 1) && !hasFragment) {
        if (data.getLogicalSize() > blockSize) {
          throw new SquashFsException(
              String.format(
                  "Error during block read: expected max %d bytes, got %d",
                  blockSize, data.getLogicalSize()));
        }
        writeBlock(getSparseBlock(), out, data);
        bytesRead += data.getLogicalSize();
      } else {
        if (data.getLogicalSize() != blockSize) {
          throw new SquashFsException(
              String.format("Error during file read: expected %d bytes, got %d",
                  blockSize, data.getLogicalSize()));
        }
        writeBlock(getSparseBlock(), out, data);
        bytesRead += data.getLogicalSize();
      }
    }

    if (hasFragment) {
      DataBlock data =
          readFragment(fileInode, (int) (fileSize - bytesRead), true);

      if (data.getLogicalSize() > blockSize) {
        throw new SquashFsException(
            String.format(
                "Error during fragment read: expected max %d bytes, got %d",
                blockSize, data.getLogicalSize()));
      }
      writeBlock(getSparseBlock(), out, data);
      bytesRead += data.getLogicalSize();
    }
    if (bytesRead != fileSize) {
      throw new SquashFsException(
          String.format(
              "Error during final block read: expected %d total bytes, got %d",
              fileSize, bytesRead));
    }

    return bytesRead;
  }

  @Override
  public int read(INode inode, long fileOffset, byte[] buf, int off, int len)
      throws IOException, SquashFsException {

    if (!(inode instanceof FileINode)) {
      throw new IllegalArgumentException("Inode is not a file");
    }

    FileINode fileInode = (FileINode) inode;

    long fileSize = fileInode.getFileSize();
    int blockSize = getSuperBlock().getBlockSize();
    int blockCount = fileInode.getBlockSizes().length;
    boolean hasFragment = fileInode.isFragmentPresent();

    int blockRelative = (int) (fileOffset % (long) blockSize);
    int blockNumber = (int) ((fileOffset - blockRelative) / (long) blockSize);

    int bytesToRead = Math.max(0, Math.min(len, blockSize - blockRelative));

    if (blockNumber < blockCount) {
      // read the block
      DataBlock data = readBlock(fileInode, blockNumber, true);

      if (blockNumber == (blockCount - 1) && !hasFragment) {
        if (data.getLogicalSize() > blockSize) {
          throw new SquashFsException(
              String.format(
                  "Error during block read: expected max %d bytes, got %d",
                  blockSize, data.getLogicalSize()));
        }

        int bytesCopied =
            copyData(getSparseBlock(), blockRelative, buf, off, bytesToRead,
                data);
        if (bytesCopied == 0) {
          bytesCopied = -1;
        }
        return bytesCopied;

      } else {
        if (data.getLogicalSize() != blockSize) {
          throw new SquashFsException(
              String.format("Error during file read: expected %d bytes, got %d",
                  blockSize, data.getLogicalSize()));
        }
        return copyData(getSparseBlock(), blockRelative, buf, off, bytesToRead,
            data);
      }
    } else if (blockNumber == blockCount && hasFragment) {
      int fragmentSize = (int) (fileSize % (long) blockSize);

      // read fragment
      DataBlock data = readFragment(fileInode, fragmentSize, true);

      if (data.getLogicalSize() > blockSize) {
        throw new SquashFsException(
            String.format(
                "Error during fragment read: expected max %d bytes, got %d",
                blockSize, data.getLogicalSize()));
      }
      int bytesCopied =
          copyData(getSparseBlock(), blockRelative, buf, off, bytesToRead,
              data);
      if (bytesCopied == 0) {
        bytesCopied = -1;
      }
      return bytesCopied;

    } else {
      // EOF
      return -1;
    }
  }

  protected int copyData(
      byte[] sparseBlock,
      int blockOffset,
      byte[] data,
      int off,
      int len,
      DataBlock block) {
    if (block.getLogicalSize() == 0) {
      return 0;
    }

    int bytesToCopy =
        Math.max(0, Math.min(len, block.getLogicalSize() - blockOffset));
    if (bytesToCopy == 0) {
      return 0;
    }

    if (block.isSparse()) {
      System.arraycopy(sparseBlock, 0, data, off, bytesToCopy);
      return bytesToCopy;
    }
    System.arraycopy(block.getData(), blockOffset, data, off, bytesToCopy);
    return bytesToCopy;
  }

  protected void writeBlock(byte[] sparseBlock, DataOutput out, DataBlock block)
      throws IOException {
    if (block.getLogicalSize() == 0) {
      return;
    }

    if (block.isSparse()) {
      out.write(sparseBlock, 0, block.getLogicalSize());
      return;
    }

    out.write(block.getData(), 0, block.getLogicalSize());
  }

}
