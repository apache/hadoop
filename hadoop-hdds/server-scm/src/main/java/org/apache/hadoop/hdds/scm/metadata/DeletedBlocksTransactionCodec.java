/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.scm.metadata;


import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.utils.db.Codec;

/**
 * Codec for Persisting the DeletedBlocks.
 */
public class DeletedBlocksTransactionCodec
    implements Codec<DeletedBlocksTransaction> {
  @Override
  public byte[] toPersistedFormat(DeletedBlocksTransaction object)
      throws IOException {
    return object.toByteArray();
  }

  @Override
  public DeletedBlocksTransaction fromPersistedFormat(byte[] rawData)
      throws IOException {
    try {
      return DeletedBlocksTransaction.parseFrom(rawData);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(
          "Can't convert rawBytes to DeletedBlocksTransaction.", e);
    }
  }
}
