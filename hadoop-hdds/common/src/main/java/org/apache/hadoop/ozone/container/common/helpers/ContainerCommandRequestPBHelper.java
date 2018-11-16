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
package org.apache.hadoop.ozone.container.common.helpers;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.
    ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.ozone.audit.DNAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Utilities for converting protobuf classes to Java classes.
 */
public final class ContainerCommandRequestPBHelper {

  static final Logger LOG =
      LoggerFactory.getLogger(ContainerCommandRequestPBHelper.class);

  private ContainerCommandRequestPBHelper() {
  }

  public static Map<String, String> getAuditParams(
      ContainerCommandRequestProto msg) {
    Map<String, String> auditParams = new TreeMap<>();
    Type cmdType = msg.getCmdType();
    String containerID = String.valueOf(msg.getContainerID());
    switch(cmdType) {
    case CreateContainer:
      auditParams.put("containerID", containerID);
      auditParams.put("containerType",
          msg.getCreateContainer().getContainerType().toString());
      return auditParams;

    case ReadContainer:
      auditParams.put("containerID", containerID);
      return auditParams;

    case UpdateContainer:
      auditParams.put("containerID", containerID);
      auditParams.put("forceUpdate",
          String.valueOf(msg.getUpdateContainer().getForceUpdate()));
      return auditParams;

    case DeleteContainer:
      auditParams.put("containerID", containerID);
      auditParams.put("forceDelete",
          String.valueOf(msg.getDeleteContainer().getForceDelete()));
      return auditParams;

    case ListContainer:
      auditParams.put("startContainerID", containerID);
      auditParams.put("count",
          String.valueOf(msg.getListContainer().getCount()));
      return auditParams;

    case PutBlock:
      try{
        auditParams.put("blockData",
            BlockData.getFromProtoBuf(msg.getPutBlock().getBlockData())
                .toString());
      }catch (IOException ex){
        LOG.trace("Encountered error parsing BlockData from protobuf:"
            + ex.getMessage());
        return null;
      }
      return auditParams;

    case GetBlock:
      auditParams.put("blockData",
          BlockID.getFromProtobuf(msg.getGetBlock().getBlockID()).toString());
      return auditParams;

    case DeleteBlock:
      auditParams.put("blockData",
          BlockID.getFromProtobuf(msg.getDeleteBlock().getBlockID())
              .toString());
      return auditParams;

    case ListBlock:
      auditParams.put("startLocalID",
          String.valueOf(msg.getListBlock().getStartLocalID()));
      auditParams.put("count", String.valueOf(msg.getListBlock().getCount()));
      return auditParams;

    case ReadChunk:
      auditParams.put("blockData",
          BlockID.getFromProtobuf(msg.getReadChunk().getBlockID()).toString());
      return auditParams;

    case DeleteChunk:
      auditParams.put("blockData",
          BlockID.getFromProtobuf(msg.getDeleteChunk().getBlockID())
              .toString());
      return auditParams;

    case WriteChunk:
      auditParams.put("blockData",
          BlockID.getFromProtobuf(msg.getWriteChunk().getBlockID())
              .toString());
      return auditParams;

    case ListChunk:
      auditParams.put("blockData",
          BlockID.getFromProtobuf(msg.getListChunk().getBlockID()).toString());
      auditParams.put("prevChunkName", msg.getListChunk().getPrevChunkName());
      auditParams.put("count", String.valueOf(msg.getListChunk().getCount()));
      return auditParams;

    case CompactChunk: return null; //CompactChunk operation

    case PutSmallFile:
      try{
        auditParams.put("blockData",
            BlockData.getFromProtoBuf(msg.getPutSmallFile()
                .getBlock().getBlockData()).toString());
      }catch (IOException ex){
        LOG.trace("Encountered error parsing BlockData from protobuf:"
            + ex.getMessage());
      }
      return auditParams;

    case GetSmallFile:
      auditParams.put("blockData",
          BlockID.getFromProtobuf(msg.getGetSmallFile().getBlock().getBlockID())
              .toString());
      return auditParams;

    case CloseContainer:
      auditParams.put("containerID", containerID);
      return auditParams;

    case GetCommittedBlockLength:
      auditParams.put("blockData",
          BlockID.getFromProtobuf(msg.getGetCommittedBlockLength().getBlockID())
              .toString());
      return auditParams;

    default :
      LOG.debug("Invalid command type - " + cmdType);
      return null;
    }

  }

  public static DNAction getAuditAction(Type cmdType) {
    switch (cmdType) {
    case CreateContainer  : return DNAction.CREATE_CONTAINER;
    case ReadContainer    : return DNAction.READ_CONTAINER;
    case UpdateContainer  : return DNAction.UPDATE_CONTAINER;
    case DeleteContainer  : return DNAction.DELETE_CONTAINER;
    case ListContainer    : return DNAction.LIST_CONTAINER;
    case PutBlock         : return DNAction.PUT_BLOCK;
    case GetBlock         : return DNAction.GET_BLOCK;
    case DeleteBlock      : return DNAction.DELETE_BLOCK;
    case ListBlock        : return DNAction.LIST_BLOCK;
    case ReadChunk        : return DNAction.READ_CHUNK;
    case DeleteChunk      : return DNAction.DELETE_CHUNK;
    case WriteChunk       : return DNAction.WRITE_CHUNK;
    case ListChunk        : return DNAction.LIST_CHUNK;
    case CompactChunk     : return DNAction.COMPACT_CHUNK;
    case PutSmallFile     : return DNAction.PUT_SMALL_FILE;
    case GetSmallFile     : return DNAction.GET_SMALL_FILE;
    case CloseContainer   : return DNAction.CLOSE_CONTAINER;
    case GetCommittedBlockLength : return DNAction.GET_COMMITTED_BLOCK_LENGTH;
    default :
      LOG.debug("Invalid command type - " + cmdType);
      return null;
    }
  }

}
