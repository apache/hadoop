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
package org.apache.hadoop.hdfs.server.common;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

/**
 * This class is used to represent provided blocks that are file regions,
 * i.e., can be described using (path, offset, length).
 */
public class FileRegion implements BlockAlias {

  private final Path path;
  private final long offset;
  private final long length;
  private final long blockId;
  private final String bpid;
  private final long genStamp;

  public FileRegion(long blockId, Path path, long offset,
      long length, String bpid, long genStamp) {
    this.path = path;
    this.offset = offset;
    this.length = length;
    this.blockId = blockId;
    this.bpid = bpid;
    this.genStamp = genStamp;
  }

  public FileRegion(long blockId, Path path, long offset,
      long length, String bpid) {
    this(blockId, path, offset, length, bpid,
        HdfsConstants.GRANDFATHER_GENERATION_STAMP);

  }

  public FileRegion(long blockId, Path path, long offset,
      long length, long genStamp) {
    this(blockId, path, offset, length, null, genStamp);

  }

  public FileRegion(long blockId, Path path, long offset, long length) {
    this(blockId, path, offset, length, null);
  }

  @Override
  public Block getBlock() {
    return new Block(blockId, length, genStamp);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof FileRegion)) {
      return false;
    }
    FileRegion o = (FileRegion) other;
    return blockId == o.blockId
      && offset == o.offset
      && length == o.length
      && genStamp == o.genStamp
      && path.equals(o.path);
  }

  @Override
  public int hashCode() {
    return (int)(blockId & Integer.MIN_VALUE);
  }

  public Path getPath() {
    return path;
  }

  public long getOffset() {
    return offset;
  }

  public long getLength() {
    return length;
  }

  public long getGenerationStamp() {
    return genStamp;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{ block=\"").append(getBlock()).append("\"");
    sb.append(", path=\"").append(getPath()).append("\"");
    sb.append(", off=\"").append(getOffset()).append("\"");
    sb.append(", len=\"").append(getBlock().getNumBytes()).append("\"");
    sb.append(", genStamp=\"").append(getBlock()
        .getGenerationStamp()).append("\"");
    sb.append(", bpid=\"").append(bpid).append("\"");
    sb.append(" }");
    return sb.toString();
  }

  public String getBlockPoolId() {
    return this.bpid;
  }

}
