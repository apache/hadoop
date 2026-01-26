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

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.BLOCK_ID_FORMAT;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.PADDING_CHARACTER;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.PADDING_FORMAT;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.SPACE_CHARACTER;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.STRING_SUFFIX;

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
   * @param blockIdLength  the expected length of the generated block ID.
   * @param blockIndex     the index of the block; used in block ID generation.
   * @throws IOException exception is thrown.
   */
  AbfsBlobBlock(AbfsOutputStream outputStream, long offset, int blockIdLength, long blockIndex) throws IOException {
    super(outputStream, offset);
    this.blockIndex = blockIndex;
    String streamId = outputStream.getStreamID();
    UUID streamIdGuid = UUID.nameUUIDFromBytes(streamId.getBytes(StandardCharsets.UTF_8));
    this.blockId = generateBlockId(streamIdGuid, blockIdLength);
  }

  /**
   * Generates a Base64-encoded block ID string using the given stream UUID and block index.
   * The block ID is first created as a raw string using a format with the stream ID and block index.
   * If a non-zero rawLength is provided, the raw block ID is padded or trimmed to match the length.
   * The final string is then Base64-encoded and returned.
   *
   * @param streamId   the UUID of the stream used to generate the block ID.
   * @param rawLength  the desired length of the raw block ID string before encoding.
   *                   If 0, no length adjustment is done.
   * @return the Base64-encoded block ID string.
   */
  private String generateBlockId(UUID streamId, int rawLength) {
    String rawBlockId = String.format(BLOCK_ID_FORMAT, streamId, blockIndex);

    if (rawLength != 0) {
      // Adjust to match expected decoded length
      if (rawBlockId.length() < rawLength) {
        rawBlockId = String.format(PADDING_FORMAT + rawLength + STRING_SUFFIX, rawBlockId)
            .replace(SPACE_CHARACTER, PADDING_CHARACTER);
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

