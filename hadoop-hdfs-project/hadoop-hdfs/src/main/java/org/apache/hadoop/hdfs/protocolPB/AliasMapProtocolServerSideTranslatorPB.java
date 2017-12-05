/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.protocolPB;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ProvidedStorageLocation;
import org.apache.hadoop.hdfs.protocol.proto.AliasMapProtocolProtos.KeyValueProto;
import org.apache.hadoop.hdfs.protocol.proto.AliasMapProtocolProtos.ReadResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.AliasMapProtocolProtos.WriteRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AliasMapProtocolProtos.WriteResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMapProtocol;
import org.apache.hadoop.hdfs.server.common.FileRegion;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdfs.protocol.proto.AliasMapProtocolProtos.*;
import static org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMap.*;

/**
 * AliasMapProtocolServerSideTranslatorPB is responsible for translating RPC
 * calls and forwarding them to the internal InMemoryAliasMap.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class AliasMapProtocolServerSideTranslatorPB
    implements AliasMapProtocolPB {

  private final InMemoryAliasMapProtocol aliasMap;

  public AliasMapProtocolServerSideTranslatorPB(
      InMemoryAliasMapProtocol aliasMap) {
    this.aliasMap = aliasMap;
  }

  private static final WriteResponseProto VOID_WRITE_RESPONSE =
      WriteResponseProto.newBuilder().build();

  @Override
  public WriteResponseProto write(RpcController controller,
      WriteRequestProto request) throws ServiceException {
    try {
      FileRegion toWrite =
          PBHelper.convert(request.getKeyValuePair());

      aliasMap.write(toWrite.getBlock(), toWrite.getProvidedStorageLocation());
      return VOID_WRITE_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ReadResponseProto read(RpcController controller,
      ReadRequestProto request) throws ServiceException {
    try {
      Block toRead =  PBHelperClient.convert(request.getKey());

      Optional<ProvidedStorageLocation> optionalResult =
          aliasMap.read(toRead);

      ReadResponseProto.Builder builder = ReadResponseProto.newBuilder();
      if (optionalResult.isPresent()) {
        ProvidedStorageLocation providedStorageLocation = optionalResult.get();
        builder.setValue(PBHelperClient.convert(providedStorageLocation));
      }

      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ListResponseProto list(RpcController controller,
      ListRequestProto request) throws ServiceException {
    try {
      BlockProto marker = request.getMarker();
      IterationResult iterationResult;
      if (marker.isInitialized()) {
        iterationResult =
            aliasMap.list(Optional.of(PBHelperClient.convert(marker)));
      } else {
        iterationResult = aliasMap.list(Optional.empty());
      }
      ListResponseProto.Builder responseBuilder =
          ListResponseProto.newBuilder();
      List<FileRegion> fileRegions = iterationResult.getFileRegions();

      List<KeyValueProto> keyValueProtos = fileRegions.stream()
          .map(PBHelper::convert).collect(Collectors.toList());
      responseBuilder.addAllFileRegions(keyValueProtos);
      Optional<Block> nextMarker = iterationResult.getNextBlock();
      nextMarker
          .map(m -> responseBuilder.setNextMarker(PBHelperClient.convert(m)));

      return responseBuilder.build();

    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  public BlockPoolResponseProto getBlockPoolId(RpcController controller,
      BlockPoolRequestProto req) throws ServiceException {
    try {
      String bpid = aliasMap.getBlockPoolId();
      return BlockPoolResponseProto.newBuilder().setBlockPoolId(bpid).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
