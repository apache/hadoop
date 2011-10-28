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
package org.apache.hadoop.hdfs.protocol;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.util.ExactSizeInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;

/**
 * Utilities for converting to and from protocol buffers used in the
 * HDFS wire protocol, as well as some generic utilities useful
 * for dealing with protocol buffers.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class HdfsProtoUtil {
  
  //// Block Token ////
  
  public static HdfsProtos.BlockTokenIdentifierProto toProto(Token<?> blockToken) {
    return HdfsProtos.BlockTokenIdentifierProto.newBuilder()
      .setIdentifier(ByteString.copyFrom(blockToken.getIdentifier()))
      .setPassword(ByteString.copyFrom(blockToken.getPassword()))
      .setKind(blockToken.getKind().toString())
      .setService(blockToken.getService().toString())
      .build();
  }

  public static Token<BlockTokenIdentifier> fromProto(HdfsProtos.BlockTokenIdentifierProto proto) {
    return new Token<BlockTokenIdentifier>(proto.getIdentifier().toByteArray(),
        proto.getPassword().toByteArray(),
        new Text(proto.getKind()),
        new Text(proto.getService()));
  }

  //// Extended Block ////
  
  public static HdfsProtos.ExtendedBlockProto toProto(ExtendedBlock block) {
    return HdfsProtos.ExtendedBlockProto.newBuilder()
      .setBlockId(block.getBlockId())
      .setPoolId(block.getBlockPoolId())
      .setNumBytes(block.getNumBytes())
      .setGenerationStamp(block.getGenerationStamp())
      .build();
  }
    
  public static ExtendedBlock fromProto(HdfsProtos.ExtendedBlockProto proto) {
    return new ExtendedBlock(
        proto.getPoolId(), proto.getBlockId(),
        proto.getNumBytes(), proto.getGenerationStamp());
  }

  //// DatanodeID ////
  
  private static HdfsProtos.DatanodeIDProto toProto(
      DatanodeID dni) {
    return HdfsProtos.DatanodeIDProto.newBuilder()
      .setName(dni.getName())
      .setStorageID(dni.getStorageID())
      .setInfoPort(dni.getInfoPort())
      .setIpcPort(dni.getIpcPort())
      .build();
  }
  
  private static DatanodeID fromProto(HdfsProtos.DatanodeIDProto idProto) {
    return new DatanodeID(
        idProto.getName(),
        idProto.getStorageID(),
        idProto.getInfoPort(),
        idProto.getIpcPort());
  }
  
  //// DatanodeInfo ////
  
  public static HdfsProtos.DatanodeInfoProto toProto(DatanodeInfo dni) {
    return HdfsProtos.DatanodeInfoProto.newBuilder()
      .setId(toProto((DatanodeID)dni))
      .setCapacity(dni.getCapacity())
      .setDfsUsed(dni.getDfsUsed())
      .setRemaining(dni.getRemaining())
      .setBlockPoolUsed(dni.getBlockPoolUsed())
      .setLastUpdate(dni.getLastUpdate())
      .setXceiverCount(dni.getXceiverCount())
      .setLocation(dni.getNetworkLocation())
      .setHostName(dni.getHostName())
      .setAdminState(HdfsProtos.DatanodeInfoProto.AdminState.valueOf(
          dni.getAdminState().name()))
      .build();
  }

  public static DatanodeInfo fromProto(HdfsProtos.DatanodeInfoProto dniProto) {
    DatanodeInfo dniObj = new DatanodeInfo(fromProto(dniProto.getId()),
        dniProto.getLocation(), dniProto.getHostName());

    dniObj.setCapacity(dniProto.getCapacity());
    dniObj.setDfsUsed(dniProto.getDfsUsed());
    dniObj.setRemaining(dniProto.getRemaining());
    dniObj.setBlockPoolUsed(dniProto.getBlockPoolUsed());
    dniObj.setLastUpdate(dniProto.getLastUpdate());
    dniObj.setXceiverCount(dniProto.getXceiverCount());
    dniObj.setAdminState(DatanodeInfo.AdminStates.valueOf(
        dniProto.getAdminState().name()));
    return dniObj;
  }
  
  public static ArrayList<? extends HdfsProtos.DatanodeInfoProto> toProtos(
      DatanodeInfo[] dnInfos, int startIdx) {
    ArrayList<HdfsProtos.DatanodeInfoProto> protos =
      Lists.newArrayListWithCapacity(dnInfos.length);
    for (int i = startIdx; i < dnInfos.length; i++) {
      protos.add(toProto(dnInfos[i]));
    }
    return protos;
  }
  
  public static DatanodeInfo[] fromProtos(
      List<HdfsProtos.DatanodeInfoProto> targetsList) {
    DatanodeInfo[] ret = new DatanodeInfo[targetsList.size()];
    int i = 0;
    for (HdfsProtos.DatanodeInfoProto proto : targetsList) {
      ret[i++] = fromProto(proto);
    }
    return ret;
  }

  public static InputStream vintPrefixed(final InputStream input)
  throws IOException {
    final int firstByte = input.read();
    if (firstByte == -1) {
      throw new EOFException("Premature EOF: no length prefix available");
    }
    
    int size = CodedInputStream.readRawVarint32(firstByte, input);
    assert size >= 0;
  
    return new ExactSizeInputStream(input, size);
  }
}