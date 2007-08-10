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
package org.apache.hadoop.fs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/** Interface that represents the client side information for a file.
 */
public class FileStatus implements Writable {

  private Path path;
  private long length;
  private boolean isdir;
  private short block_replication;
  private long blocksize;
  private long modification_time;

  public FileStatus() { this(0, false, 0, 0, 0, null); }

  public FileStatus(long length, boolean isdir, int block_replication,
             long blocksize, long modification_time, Path path) {
    this(length, isdir, (short)block_replication, blocksize,
         modification_time, path);
  }

  public FileStatus(long length, boolean isdir, short block_replication,
             long blocksize, long modification_time, Path path) {
    this.length = length;
    this.isdir = isdir;
    this.block_replication = block_replication;
    this.blocksize = blocksize;
    this.modification_time = modification_time;
    this.path = path;
  }

  /* 
   * @return the length of this file, in blocks
   */
  public long getLen() {
    return length;
  }

  /**
   * Is this a directory?
   * @return true if this is a directory
   */
  public boolean isDir() {
    return isdir;
  }

  /**
   * Get the block size of the file.
   * @return the number of bytes
   */
  public long getBlockSize() {
    return blocksize;
  }

  /**
   * Get the replication factor of a file.
   * @return the replication factor of a file.
   */
  public short getReplication() {
    return block_replication;
  }

  /**
   * Get the modification time of the file.
   * @return the modification time of file in milliseconds since January 1, 1970 UTC.
   */
  public long getModificationTime() {
    return modification_time;
  }

  public Path getPath() {
    return path;
  }

  //////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, getPath().toString());
    out.writeLong(length);
    out.writeBoolean(isdir);
    out.writeShort(block_replication);
    out.writeLong(blocksize);
    out.writeLong(modification_time);
  }

  public void readFields(DataInput in) throws IOException {
    String strPath = Text.readString(in);
    this.path = new Path(strPath);
    this.length = in.readLong();
    this.isdir = in.readBoolean();
    this.block_replication = in.readShort();
    blocksize = in.readLong();
    modification_time = in.readLong();
  }

}
