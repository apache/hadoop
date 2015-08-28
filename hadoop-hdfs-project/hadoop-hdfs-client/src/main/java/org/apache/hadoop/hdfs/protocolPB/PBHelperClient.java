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
package org.apache.hadoop.hdfs.protocolPB;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeLocalInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ShortCircuitShmIdProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ShortCircuitShmSlotProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeIDProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeLocalInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ExtendedBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.StorageTypeProto;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.ShmId;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.SlotId;
import org.apache.hadoop.hdfs.util.ExactSizeInputStream;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for converting protobuf classes to and from implementation classes
 * and other helper utilities to help in dealing with protobuf.
 *
 * Note that when converting from an internal type to protobuf type, the
 * converter never return null for protobuf type. The check for internal type
 * being null must be done before calling the convert() method.
 */
public class PBHelperClient {
  private PBHelperClient() {
    /** Hidden constructor */
  }

  public static ByteString getByteString(byte[] bytes) {
    return ByteString.copyFrom(bytes);
  }

  public static ShmId convert(ShortCircuitShmIdProto shmId) {
    return new ShmId(shmId.getHi(), shmId.getLo());
  }

  public static DataChecksum.Type convert(HdfsProtos.ChecksumTypeProto type) {
    return DataChecksum.Type.valueOf(type.getNumber());
  }

  public static HdfsProtos.ChecksumTypeProto convert(DataChecksum.Type type) {
    return HdfsProtos.ChecksumTypeProto.valueOf(type.id);
  }

  public static ExtendedBlockProto convert(final ExtendedBlock b) {
    if (b == null) return null;
    return ExtendedBlockProto.newBuilder().
      setPoolId(b.getBlockPoolId()).
      setBlockId(b.getBlockId()).
      setNumBytes(b.getNumBytes()).
      setGenerationStamp(b.getGenerationStamp()).
      build();
  }

  public static TokenProto convert(Token<?> tok) {
    return TokenProto.newBuilder().
      setIdentifier(ByteString.copyFrom(tok.getIdentifier())).
      setPassword(ByteString.copyFrom(tok.getPassword())).
      setKind(tok.getKind().toString()).
      setService(tok.getService().toString()).build();
  }

  public static ShortCircuitShmIdProto convert(ShmId shmId) {
    return ShortCircuitShmIdProto.newBuilder().
      setHi(shmId.getHi()).
      setLo(shmId.getLo()).
      build();

  }

  public static ShortCircuitShmSlotProto convert(SlotId slotId) {
    return ShortCircuitShmSlotProto.newBuilder().
      setShmId(convert(slotId.getShmId())).
      setSlotIdx(slotId.getSlotIdx()).
      build();
  }

  public static DatanodeIDProto convert(DatanodeID dn) {
    // For wire compatibility with older versions we transmit the StorageID
    // which is the same as the DatanodeUuid. Since StorageID is a required
    // field we pass the empty string if the DatanodeUuid is not yet known.
    return DatanodeIDProto.newBuilder()
      .setIpAddr(dn.getIpAddr())
      .setHostName(dn.getHostName())
      .setXferPort(dn.getXferPort())
      .setDatanodeUuid(dn.getDatanodeUuid() != null ? dn.getDatanodeUuid() : "")
      .setInfoPort(dn.getInfoPort())
      .setInfoSecurePort(dn.getInfoSecurePort())
      .setIpcPort(dn.getIpcPort()).build();
  }

  public static DatanodeInfoProto.AdminState convert(
    final DatanodeInfo.AdminStates inAs) {
    switch (inAs) {
      case NORMAL: return  DatanodeInfoProto.AdminState.NORMAL;
      case DECOMMISSION_INPROGRESS:
        return DatanodeInfoProto.AdminState.DECOMMISSION_INPROGRESS;
      case DECOMMISSIONED: return DatanodeInfoProto.AdminState.DECOMMISSIONED;
      default: return DatanodeInfoProto.AdminState.NORMAL;
    }
  }

  public static DatanodeInfoProto convert(DatanodeInfo info) {
    DatanodeInfoProto.Builder builder = DatanodeInfoProto.newBuilder();
    if (info.getNetworkLocation() != null) {
      builder.setLocation(info.getNetworkLocation());
    }
    builder
      .setId(convert((DatanodeID) info))
      .setCapacity(info.getCapacity())
      .setDfsUsed(info.getDfsUsed())
      .setRemaining(info.getRemaining())
      .setBlockPoolUsed(info.getBlockPoolUsed())
      .setCacheCapacity(info.getCacheCapacity())
      .setCacheUsed(info.getCacheUsed())
      .setLastUpdate(info.getLastUpdate())
      .setLastUpdateMonotonic(info.getLastUpdateMonotonic())
      .setXceiverCount(info.getXceiverCount())
      .setAdminState(convert(info.getAdminState()))
      .build();
    return builder.build();
  }

  public static List<? extends HdfsProtos.DatanodeInfoProto> convert(
    DatanodeInfo[] dnInfos) {
    return convert(dnInfos, 0);
  }

  /**
   * Copy from {@code dnInfos} to a target of list of same size starting at
   * {@code startIdx}.
   */
  public static List<? extends HdfsProtos.DatanodeInfoProto> convert(
    DatanodeInfo[] dnInfos, int startIdx) {
    if (dnInfos == null)
      return null;
    ArrayList<HdfsProtos.DatanodeInfoProto> protos = Lists
      .newArrayListWithCapacity(dnInfos.length);
    for (int i = startIdx; i < dnInfos.length; i++) {
      protos.add(convert(dnInfos[i]));
    }
    return protos;
  }

  public static List<Boolean> convert(boolean[] targetPinnings, int idx) {
    List<Boolean> pinnings = new ArrayList<>();
    if (targetPinnings == null) {
      pinnings.add(Boolean.FALSE);
    } else {
      for (; idx < targetPinnings.length; ++idx) {
        pinnings.add(targetPinnings[idx]);
      }
    }
    return pinnings;
  }

  public static ExtendedBlock convert(ExtendedBlockProto eb) {
    if (eb == null) return null;
    return new ExtendedBlock( eb.getPoolId(), eb.getBlockId(), eb.getNumBytes(),
        eb.getGenerationStamp());
  }

  public static DatanodeLocalInfo convert(DatanodeLocalInfoProto proto) {
    return new DatanodeLocalInfo(proto.getSoftwareVersion(),
        proto.getConfigVersion(), proto.getUptime());
  }

  static public DatanodeInfoProto convertDatanodeInfo(DatanodeInfo di) {
    if (di == null) return null;
    return convert(di);
  }

  public static StorageTypeProto convertStorageType(StorageType type) {
    switch(type) {
      case DISK:
        return StorageTypeProto.DISK;
      case SSD:
        return StorageTypeProto.SSD;
      case ARCHIVE:
        return StorageTypeProto.ARCHIVE;
      case RAM_DISK:
        return StorageTypeProto.RAM_DISK;
      default:
        throw new IllegalStateException(
          "BUG: StorageType not found, type=" + type);
    }
  }

  public static StorageType convertStorageType(StorageTypeProto type) {
    switch(type) {
      case DISK:
        return StorageType.DISK;
      case SSD:
        return StorageType.SSD;
      case ARCHIVE:
        return StorageType.ARCHIVE;
      case RAM_DISK:
        return StorageType.RAM_DISK;
      default:
        throw new IllegalStateException(
          "BUG: StorageTypeProto not found, type=" + type);
    }
  }

  public static List<StorageTypeProto> convertStorageTypes(
    StorageType[] types) {
    return convertStorageTypes(types, 0);
  }

  public static List<StorageTypeProto> convertStorageTypes(
    StorageType[] types, int startIdx) {
    if (types == null) {
      return null;
    }
    final List<StorageTypeProto> protos = new ArrayList<>(
      types.length);
    for (int i = startIdx; i < types.length; ++i) {
      protos.add(PBHelperClient.convertStorageType(types[i]));
    }
    return protos;
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
