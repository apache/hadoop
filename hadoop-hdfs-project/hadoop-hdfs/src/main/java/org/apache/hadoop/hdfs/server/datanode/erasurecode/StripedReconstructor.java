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
package org.apache.hadoop.hdfs.server.datanode.erasurecode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.BlockReadStats;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.concurrent.CompletionService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * StripedReconstructor reconstruct one or more missed striped block in the
 * striped block group, the minimum number of live striped blocks should be
 * no less than data block number.
 *
 * | <- Striped Block Group -> |
 *  blk_0      blk_1       blk_2(*)   blk_3   ...   <- A striped block group
 *    |          |           |          |
 *    v          v           v          v
 * +------+   +------+   +------+   +------+
 * |cell_0|   |cell_1|   |cell_2|   |cell_3|  ...
 * +------+   +------+   +------+   +------+
 * |cell_4|   |cell_5|   |cell_6|   |cell_7|  ...
 * +------+   +------+   +------+   +------+
 * |cell_8|   |cell_9|   |cell10|   |cell11|  ...
 * +------+   +------+   +------+   +------+
 *  ...         ...       ...         ...
 *
 *
 * We use following steps to reconstruct striped block group, in each round, we
 * reconstruct <code>bufferSize</code> data until finish, the
 * <code>bufferSize</code> is configurable and may be less or larger than
 * cell size:
 * step1: read <code>bufferSize</code> data from minimum number of sources
 *        required by reconstruction.
 * step2: decode data for targets.
 * step3: transfer data to targets.
 *
 * In step1, try to read <code>bufferSize</code> data from minimum number
 * of sources , if there is corrupt or stale sources, read from new source
 * will be scheduled. The best sources are remembered for next round and
 * may be updated in each round.
 *
 * In step2, typically if source blocks we read are all data blocks, we
 * need to call encode, and if there is one parity block, we need to call
 * decode. Notice we only read once and reconstruct all missed striped block
 * if they are more than one.
 *
 * In step3, send the reconstructed data to targets by constructing packet
 * and send them directly. Same as continuous block replication, we
 * don't check the packet ack. Since the datanode doing the reconstruction work
 * are one of the source datanodes, so the reconstructed data are sent
 * remotely.
 *
 * There are some points we can do further improvements in next phase:
 * 1. we can read the block file directly on the local datanode,
 *    currently we use remote block reader. (Notice short-circuit is not
 *    a good choice, see inline comments).
 * 2. We need to check the packet ack for EC reconstruction? Since EC
 *    reconstruction is more expensive than continuous block replication,
 *    it needs to read from several other datanodes, should we make sure the
 *    reconstructed result received by targets?
 */
@InterfaceAudience.Private
abstract class StripedReconstructor {
  protected static final Logger LOG = DataNode.LOG;

  private final Configuration conf;
  private final DataNode datanode;
  private final ErasureCodingPolicy ecPolicy;
  private RawErasureDecoder decoder;
  private final ExtendedBlock blockGroup;
  private static final ByteBufferPool BUFFER_POOL = new ElasticByteBufferPool();

  // position in striped internal block
  private long positionInBlock;
  private StripedReader stripedReader;
  private ErasureCodingWorker erasureCodingWorker;
  private final CachingStrategy cachingStrategy;
  private long maxTargetLength = 0L;
  private final BitSet liveBitSet;

  // metrics
  private AtomicLong bytesRead = new AtomicLong(0);
  private AtomicLong bytesWritten = new AtomicLong(0);
  private AtomicLong remoteBytesRead = new AtomicLong(0);

  StripedReconstructor(ErasureCodingWorker worker,
      StripedReconstructionInfo stripedReconInfo) {
    this.erasureCodingWorker = worker;
    this.datanode = worker.getDatanode();
    this.conf = worker.getConf();
    this.ecPolicy = stripedReconInfo.getEcPolicy();
    liveBitSet = new BitSet(
        ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits());
    for (int i = 0; i < stripedReconInfo.getLiveIndices().length; i++) {
      liveBitSet.set(stripedReconInfo.getLiveIndices()[i]);
    }
    blockGroup = stripedReconInfo.getBlockGroup();
    stripedReader = new StripedReader(this, datanode, conf, stripedReconInfo);
    cachingStrategy = CachingStrategy.newDefaultStrategy();

    positionInBlock = 0L;
  }

  public void incrBytesRead(boolean local, long delta) {
    if (local) {
      bytesRead.addAndGet(delta);
    } else {
      bytesRead.addAndGet(delta);
      remoteBytesRead.addAndGet(delta);
    }
  }

  public void incrBytesWritten(long delta) {
    bytesWritten.addAndGet(delta);
  }

  public long getBytesRead() {
    return bytesRead.get();
  }

  public long getRemoteBytesRead() {
    return remoteBytesRead.get();
  }

  public long getBytesWritten() {
    return bytesWritten.get();
  }

  /**
   * Reconstruct one or more missed striped block in the striped block group,
   * the minimum number of live striped blocks should be no less than data
   * block number.
   *
   * @throws IOException
   */
  abstract void reconstruct() throws IOException;

  boolean useDirectBuffer() {
    return decoder.preferDirectBuffer();
  }

  ByteBuffer allocateBuffer(int length) {
    return BUFFER_POOL.getBuffer(useDirectBuffer(), length);
  }

  void freeBuffer(ByteBuffer buffer) {
    BUFFER_POOL.putBuffer(buffer);
  }

  ExtendedBlock getBlock(int i) {
    return StripedBlockUtil.constructInternalBlock(blockGroup, ecPolicy, i);
  }

  long getBlockLen(int i) {
    return StripedBlockUtil.getInternalBlockLength(blockGroup.getNumBytes(),
        ecPolicy, i);
  }

  // Initialize decoder
  protected void initDecoderIfNecessary() {
    if (decoder == null) {
      ErasureCoderOptions coderOptions = new ErasureCoderOptions(
          ecPolicy.getNumDataUnits(), ecPolicy.getNumParityUnits());
      decoder = CodecUtil.createRawDecoder(conf, ecPolicy.getCodecName(),
          coderOptions);
    }
  }

  long getPositionInBlock() {
    return positionInBlock;
  }

  InetSocketAddress getSocketAddress4Transfer(DatanodeInfo dnInfo) {
    return NetUtils.createSocketAddr(dnInfo.getXferAddr(
        datanode.getDnConf().getConnectToDnViaHostname()));
  }

  int getBufferSize() {
    return stripedReader.getBufferSize();
  }

  public DataChecksum getChecksum() {
    return stripedReader.getChecksum();
  }

  CachingStrategy getCachingStrategy() {
    return cachingStrategy;
  }

  CompletionService<BlockReadStats> createReadService() {
    return erasureCodingWorker.createReadService();
  }

  ExtendedBlock getBlockGroup() {
    return blockGroup;
  }

  /**
   * Get the xmits that _will_ be used for this reconstruction task.
   */
  int getXmits() {
    return stripedReader.getXmits();
  }

  BitSet getLiveBitSet() {
    return liveBitSet;
  }

  long getMaxTargetLength() {
    return maxTargetLength;
  }

  void setMaxTargetLength(long maxTargetLength) {
    this.maxTargetLength = maxTargetLength;
  }

  void updatePositionInBlock(long positionInBlockArg) {
    this.positionInBlock += positionInBlockArg;
  }

  RawErasureDecoder getDecoder() {
    return decoder;
  }

  void cleanup() {
    if (decoder != null) {
      decoder.release();
    }
  }

  StripedReader getStripedReader() {
    return stripedReader;
  }

  Configuration getConf() {
    return conf;
  }

  DataNode getDatanode() {
    return datanode;
  }
}
