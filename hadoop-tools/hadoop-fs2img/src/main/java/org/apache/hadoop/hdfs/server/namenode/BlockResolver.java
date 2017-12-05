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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;

/**
 * Given an external reference, create a sequence of blocks and associated
 * metadata.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class BlockResolver {

  protected BlockProto buildBlock(long blockId, long bytes) {
    return buildBlock(blockId, bytes, 1001);
  }

  protected BlockProto buildBlock(long blockId, long bytes, long genstamp) {
    BlockProto.Builder b = BlockProto.newBuilder()
        .setBlockId(blockId)
        .setNumBytes(bytes)
        .setGenStamp(genstamp);
    return b.build();
  }

  /**
   * @param s the external reference.
   * @return sequence of blocks that make up the reference.
   */
  public Iterable<BlockProto> resolve(FileStatus s) {
    List<Long> lengths = blockLengths(s);
    ArrayList<BlockProto> ret = new ArrayList<>(lengths.size());
    long tot = 0;
    for (long l : lengths) {
      tot += l;
      ret.add(buildBlock(nextId(), l));
    }
    if (tot != s.getLen()) {
      // log a warning?
      throw new IllegalStateException(
          "Expected " + s.getLen() + " found " + tot);
    }
    return ret;
  }

  /**
   * @return the next block id.
   */
  public abstract long nextId();

  /**
   * @return the maximum sequentially allocated block ID for this filesystem.
   */
  protected abstract long lastId();

  /**
   * @param status the external reference.
   * @return the lengths of the resultant blocks.
   */
  protected abstract List<Long> blockLengths(FileStatus status);


  /**
   * @param status the external reference.
   * @return the block size to assign to this external reference.
   */
  public long preferredBlockSize(FileStatus status) {
    return status.getBlockSize();
  }

  /**
   * @param status the external reference.
   * @return the replication to assign to this external reference.
   */
  public abstract int getReplication(FileStatus status);

}
