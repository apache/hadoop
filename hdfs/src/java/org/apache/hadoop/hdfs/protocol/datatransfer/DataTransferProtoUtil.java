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
package org.apache.hadoop.hdfs.protocol.datatransfer;


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsProtoUtil;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BaseHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientOperationHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpWriteBlockProto;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;


/**
 * Static utilities for dealing with the protocol buffers used by the
 * Data Transfer Protocol.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
abstract class DataTransferProtoUtil {

  static BlockConstructionStage fromProto(
      OpWriteBlockProto.BlockConstructionStage stage) {
    return BlockConstructionStage.valueOf(BlockConstructionStage.class,
        stage.name());
  }

  static OpWriteBlockProto.BlockConstructionStage toProto(
      BlockConstructionStage stage) {
    return OpWriteBlockProto.BlockConstructionStage.valueOf(
        stage.name());
  }

  static ClientOperationHeaderProto buildClientHeader(ExtendedBlock blk,
      String client, Token<BlockTokenIdentifier> blockToken) {
    ClientOperationHeaderProto header =
      ClientOperationHeaderProto.newBuilder()
        .setBaseHeader(buildBaseHeader(blk, blockToken))
        .setClientName(client)
        .build();
    return header;
  }

  static BaseHeaderProto buildBaseHeader(ExtendedBlock blk,
      Token<BlockTokenIdentifier> blockToken) {
    return BaseHeaderProto.newBuilder()
      .setBlock(HdfsProtoUtil.toProto(blk))
      .setToken(HdfsProtoUtil.toProto(blockToken))
      .build();
  }
}
