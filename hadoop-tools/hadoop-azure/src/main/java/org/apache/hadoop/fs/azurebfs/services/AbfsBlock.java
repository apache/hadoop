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

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.store.DataBlocks;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.BLOCK_ID_LENGTH;

/**
 * Return activeBlock with blockId.
 */
public class AbfsBlock {

    DataBlocks.DataBlock activeBlock;
    String blockId;

    /**
     * Gets the activeBlock and the blockId.
     * @param outputStream AbfsOutputStream Instance.
     * @param offset Used to generate blockId based on offset.
     * @throws IOException
     */
    AbfsBlock(AbfsOutputStream outputStream, long offset) throws IOException {
        DataBlocks.BlockFactory blockFactory = outputStream.getBlockFactory();
        long blockCount = outputStream.getBlockCount();
        int blockSize = outputStream.getBlockSize();
        AbfsOutputStreamStatistics outputStreamStatistics = outputStream.getOutputStreamStatistics();
        this.activeBlock = blockFactory.create(blockCount, blockSize, outputStreamStatistics);
        this.blockId = generateBlockId(offset);
    }

    /**
     * Helper method that generates blockId.
     * @param position The offset needed to generate blockId.
     * @return String representing the block ID generated.
     */
    private String generateBlockId(long position) {
        String blockId = String.format("%d", position);
        byte[] blockIdByteArray = new byte[BLOCK_ID_LENGTH];
        System.arraycopy(blockId.getBytes(), 0, blockIdByteArray, 0, Math.min(BLOCK_ID_LENGTH, blockId.length()));
        return new String(Base64.encodeBase64(blockIdByteArray), StandardCharsets.UTF_8);
    }

    /**
     * Returns activeBlock.
     * @return activeBlock.
     */
    public DataBlocks.DataBlock getActiveBlock() {
        return activeBlock;
    }

    /**
     * Returns blockId for the block.
     * @return blockId.
     */
    public String getBlockId() {
        return blockId;
    }

    /**
     * Returns datasize for the block.
     * @return datasize.
     */
    public int dataSize() {
        return activeBlock.dataSize();
    }

    /**
     * Return instance of BlockUploadData.
     * @return instance of BlockUploadData.
     * @throws IOException
     */
    public DataBlocks.BlockUploadData startUpload() throws IOException {
        return activeBlock.startUpload();
    }

    /**
     * Return the block has data or not.
     * @return block has data or not.
     */
    public boolean hasData() {
        return activeBlock.hasData();
    }

    /**
     * Write a series of bytes from the buffer, from the offset. Returns the number of bytes written.
     * Only valid in the state Writing. Base class verifies the state but does no writing.
     * @param buffer buffer.
     * @param offset offset.
     * @param length length.
     * @return number of bytes written.
     * @throws IOException
     */
    public int write(byte[] buffer, int offset, int length) throws IOException {
        return activeBlock.write(buffer, offset, length);
    }

    /**
     * Returns remainingCapacity.
     * @return remainingCapacity.
     */
    public int remainingCapacity() {
        return activeBlock.remainingCapacity();
    }
}
