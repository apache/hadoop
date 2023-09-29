/*
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

package org.apache.hadoop.fs.store;

import java.io.IOException;
import java.util.Random;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.fs.store.DataBlocks.DATA_BLOCKS_BUFFER_ARRAY;
import static org.apache.hadoop.fs.store.DataBlocks.DATA_BLOCKS_BUFFER_DISK;
import static org.apache.hadoop.fs.store.DataBlocks.DATA_BLOCKS_BYTEBUFFER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * UTs to test {@link DataBlocks} functionalities.
 */
public class TestDataBlocks {
  private final Configuration configuration = new Configuration();
  private static final int ONE_KB = 1024;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDataBlocks.class);

  /**
   * Test to verify different DataBlocks factories, different operations.
   */
  @Test
  public void testDataBlocksFactory() throws Exception {
    testCreateFactory(DATA_BLOCKS_BUFFER_DISK);
    testCreateFactory(DATA_BLOCKS_BUFFER_ARRAY);
    testCreateFactory(DATA_BLOCKS_BYTEBUFFER);
  }

  /**
   * Verify creation of a data block factory and its operations.
   *
   * @param nameOfFactory Name of the DataBlock factory to be created.
   * @throws IOException Throw IOE in case of failure while creating a block.
   */
  public void testCreateFactory(String nameOfFactory) throws Exception {
    LOG.info("Testing: {}", nameOfFactory);
    DataBlocks.BlockFactory blockFactory =
        DataBlocks.createFactory("Dir", configuration, nameOfFactory);

    DataBlocks.DataBlock dataBlock = blockFactory.create(0, ONE_KB, null);
    assertWriteBlock(dataBlock);
    assertToByteArray(dataBlock);
    assertCloseBlock(dataBlock);
  }

  /**
   * Verify Writing of a dataBlock.
   *
   * @param dataBlock DataBlock to be tested.
   * @throws IOException Throw Exception in case of failures.
   */
  private void assertWriteBlock(DataBlocks.DataBlock dataBlock)
      throws IOException {
    byte[] oneKbBuff = new byte[ONE_KB];
    new Random().nextBytes(oneKbBuff);
    dataBlock.write(oneKbBuff, 0, ONE_KB);
    // Verify DataBlock state is at Writing.
    dataBlock.verifyState(DataBlocks.DataBlock.DestState.Writing);
    // Verify that the DataBlock has data written.
    assertTrue("Expected Data block to have data", dataBlock.hasData());
    // Verify the size of data.
    assertEquals("Mismatch in data size in block", ONE_KB,
        dataBlock.dataSize());
    // Verify that no capacity is left in the data block to write more.
    assertFalse("Expected the data block to have no capacity to write 1 byte "
        + "of data", dataBlock.hasCapacity(1));
  }

  /**
   * Verify the Conversion of Data blocks into byte[].
   *
   * @param dataBlock data block to be tested.
   * @throws Exception Throw Exception in case of failures.
   */
  private void assertToByteArray(DataBlocks.DataBlock dataBlock)
      throws Exception {
    DataBlocks.BlockUploadData blockUploadData = dataBlock.startUpload();
    // Verify that the current state is in upload.
    dataBlock.verifyState(DataBlocks.DataBlock.DestState.Upload);
    // Convert the DataBlock upload to byteArray.
    byte[] bytesWritten = blockUploadData.toByteArray();
    // Verify that we can call toByteArray() more than once and gives the
    // same byte[].
    assertEquals("Mismatch in byteArray provided by toByteArray() the second "
        + "time", bytesWritten, blockUploadData.toByteArray());
    IOUtils.close(blockUploadData);
    // Verify that after closing blockUploadData, we can't call toByteArray().
    LambdaTestUtils.intercept(IllegalStateException.class,
        "Block is closed",
        "Expected to throw IllegalStateException.java after closing "
            + "blockUploadData and trying to call toByteArray()",
        () -> {
          blockUploadData.toByteArray();
        });
  }

  /**
   * Verify the close() of data blocks.
   *
   * @param dataBlock data block to be tested.
   * @throws IOException Throw Exception in case of failures.
   */
  private void assertCloseBlock(DataBlocks.DataBlock dataBlock)
      throws IOException {
    dataBlock.close();
    // Verify that the current state is in Closed.
    dataBlock.verifyState(DataBlocks.DataBlock.DestState.Closed);
  }
}
