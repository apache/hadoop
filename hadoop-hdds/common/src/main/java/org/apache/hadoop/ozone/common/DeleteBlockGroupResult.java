/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.common;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.container.common.helpers.DeleteBlockResult;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos
    .DeleteScmBlockResult;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos
    .DeleteScmBlockResult.Result;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Result to delete a group of blocks.
 */
public class DeleteBlockGroupResult {
  private String objectKey;
  private List<DeleteBlockResult> blockResultList;
  public DeleteBlockGroupResult(String objectKey,
      List<DeleteBlockResult> blockResultList) {
    this.objectKey = objectKey;
    this.blockResultList = blockResultList;
  }

  public String getObjectKey() {
    return objectKey;
  }

  public List<DeleteBlockResult> getBlockResultList() {
    return blockResultList;
  }

  public List<DeleteScmBlockResult> getBlockResultProtoList() {
    List<DeleteScmBlockResult> resultProtoList =
        new ArrayList<>(blockResultList.size());
    for (DeleteBlockResult result : blockResultList) {
      DeleteScmBlockResult proto = DeleteScmBlockResult.newBuilder()
          .setBlockID(result.getBlockID().getProtobuf())
          .setResult(result.getResult()).build();
      resultProtoList.add(proto);
    }
    return resultProtoList;
  }

  public static List<DeleteBlockResult> convertBlockResultProto(
      List<DeleteScmBlockResult> results) {
    List<DeleteBlockResult> protoResults = new ArrayList<>(results.size());
    for (DeleteScmBlockResult result : results) {
      protoResults.add(new DeleteBlockResult(BlockID.getFromProtobuf(
          result.getBlockID()), result.getResult()));
    }
    return protoResults;
  }

  /**
   * Only if all blocks are successfully deleted, this group is considered
   * to be successfully executed.
   *
   * @return true if all blocks are successfully deleted, false otherwise.
   */
  public boolean isSuccess() {
    for (DeleteBlockResult result : blockResultList) {
      if (result.getResult() != Result.success) {
        return false;
      }
    }
    return true;
  }

  /**
   * @return A list of deletion failed block IDs.
   */
  public List<BlockID> getFailedBlocks() {
    List<BlockID> failedBlocks = blockResultList.stream()
        .filter(result -> result.getResult() != Result.success)
        .map(DeleteBlockResult::getBlockID).collect(Collectors.toList());
    return failedBlocks;
  }
}
