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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.codec.binary.Base64;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.BLOCK_ID_LENGTH;

/**
 * Represents a block in Azure Blob Storage used by Azure Data Lake Storage (ADLS).
 *
 * <p>Extends {@link AbfsBlock} and provides functionality specific to Azure Blob Storage blocks.
 * Each block is identified by a unique block ID generated based on the offset and stream ID.</p>
 */
public class AbfsBlobBlock extends AbfsBlock {

  private final String blockId;

  /**
   * Gets the activeBlock and the blockId.
   *
   * @param outputStream AbfsOutputStream Instance.
   * @param offset       Used to generate blockId based on offset.
   * @throws IOException exception is thrown.
   */
  AbfsBlobBlock(AbfsOutputStream outputStream, long offset) throws IOException {
    super(outputStream, offset);
    this.blockId = generateBlockId(offset);
  }

  /**
   * Helper method that generates blockId.
   * @param position The offset needed to generate blockId.
   * @return String representing the block ID generated.
   */
  private String generateBlockId(long position) {
    String streamId = getOutputStream().getStreamID();
    String streamIdHash = Integer.toString(streamId.hashCode());
    String blockId = String.format("%d_%s", position, streamIdHash);
    byte[] blockIdByteArray = new byte[BLOCK_ID_LENGTH];
    System.arraycopy(blockId.getBytes(StandardCharsets.UTF_8), 0, blockIdByteArray, 0, Math.min(BLOCK_ID_LENGTH, blockId.length()));
    return new String(Base64.encodeBase64(blockIdByteArray), StandardCharsets.UTF_8);
  }

  /**
   * Returns blockId for the block.
   * @return blockId.
   */
  public String getBlockId() {
    return blockId;
  }
}

