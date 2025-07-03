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
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;

/**
 * Represents a block in Azure Blob Storage used by Azure Data Lake Storage (ADLS).
 *
 * <p>Extends {@link AbfsBlock} and provides functionality specific to Azure Blob Storage blocks.
 * Each block is identified by a unique block ID generated based on the offset and stream ID.</p>
 */
public class AbfsBlobBlock extends AbfsBlock {

  private final String blockId;
  private final long blockIndex;

  /**
   * Gets the activeBlock and the blockId.
   *
   * @param outputStream AbfsOutputStream Instance.
   * @param offset       Used to generate blockId based on offset.
   * @throws IOException exception is thrown.
   */
  AbfsBlobBlock(AbfsOutputStream outputStream, long offset, int blockIdLength, long blockIndex) throws IOException {
    super(outputStream, offset);
    this.blockIndex = blockIndex;
    String streamId = getOutputStream().getStreamID();
    UUID streamIdGuid = UUID.nameUUIDFromBytes(streamId.getBytes(StandardCharsets.UTF_8));
    this.blockId = generateBlockId(streamIdGuid, blockIdLength);
  }

  /**
   * Generates a Base64-encoded block ID string based on the given position, stream ID, and desired raw length.
   * The block ID is composed using the stream UUID and the block index, which is derived from
   * the given position divided by the output stream's buffer size. The resulting string is
   * optionally adjusted to match the specified raw length, padded or trimmed as needed, and
   * then Base64-encoded.
   *
   * @param streamId   The UUID representing the stream, used as a prefix in the block ID.
   * @param rawLength  The desired length of the raw block ID string before Base64 encoding.
   *                   If 0, no length adjustment is made.
   * @return A Base64-encoded block ID string suitable for use in block-based storage APIs.
   */
  private String generateBlockId(UUID streamId, int rawLength) {
    String rawBlockId = String.format("%s-%06d", streamId, blockIndex);

    if (rawLength != 0) {
      // Adjust to match expected decoded length
      if (rawBlockId.length() < rawLength) {
        rawBlockId = String.format("%-" + rawLength + "s", rawBlockId)
            .replace(' ', '_');
      } else if (rawBlockId.length() > rawLength) {
        rawBlockId = rawBlockId.substring(0, rawLength);
      }
    }

    return Base64.encodeBase64String(rawBlockId.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Returns blockId for the block.
   * @return blockId.
   */
  public String getBlockId() {
    return blockId;
  }
}

