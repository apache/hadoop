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

package org.apache.hadoop.fs.azurebfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.store.BlockUploadStatistics;
import org.apache.hadoop.fs.store.DataBlocks;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.DATA_BLOCKS_BUFFER;
import static org.apache.hadoop.fs.store.DataBlocks.DATA_BLOCKS_BUFFER_ARRAY;
import static org.apache.hadoop.fs.store.DataBlocks.DATA_BLOCKS_BUFFER_DISK;
import static org.apache.hadoop.fs.store.DataBlocks.DATA_BLOCKS_BYTEBUFFER;
import static org.apache.hadoop.fs.store.DataBlocks.DataBlock.DestState.Closed;
import static org.apache.hadoop.fs.store.DataBlocks.DataBlock.DestState.Writing;

/**
 * Test append operations.
 */
public class ITestAzureBlobFileSystemAppend extends
    AbstractAbfsIntegrationTest {
  private static final String TEST_FILE_PATH = "testfile";
  private static final String TEST_FOLDER_PATH = "testFolder";

  public ITestAzureBlobFileSystemAppend() throws Exception {
    super();
  }

  @Test(expected = FileNotFoundException.class)
  public void testAppendDirShouldFail() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path filePath = path(TEST_FILE_PATH);
    fs.mkdirs(filePath);
    fs.append(filePath, 0).close();
  }

  @Test
  public void testAppendWithLength0() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    try(FSDataOutputStream stream = fs.create(path(TEST_FILE_PATH))) {
      final byte[] b = new byte[1024];
      new Random().nextBytes(b);
      stream.write(b, 1000, 0);
      assertEquals(0, stream.getPos());
    }
  }


  @Test(expected = FileNotFoundException.class)
  public void testAppendFileAfterDelete() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path filePath = path(TEST_FILE_PATH);
    ContractTestUtils.touch(fs, filePath);
    fs.delete(filePath, false);

    fs.append(filePath).close();
  }

  @Test(expected = FileNotFoundException.class)
  public void testAppendDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path folderPath = path(TEST_FOLDER_PATH);
    fs.mkdirs(folderPath);
    fs.append(folderPath).close();
  }

  @Test
  public void testTracingForAppend() throws IOException {
    AzureBlobFileSystem fs = getFileSystem();
    Path testPath = path(TEST_FILE_PATH);
    fs.create(testPath).close();
    fs.registerListener(new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.APPEND, false, 0));
    fs.append(testPath, 10);
  }

  @Test
  public void testCloseOfDataBlockOnAppendComplete() throws Exception {
    Set<String> blockBufferTypes = new HashSet<>();
    blockBufferTypes.add(DATA_BLOCKS_BUFFER_DISK);
    blockBufferTypes.add(DATA_BLOCKS_BYTEBUFFER);
    blockBufferTypes.add(DATA_BLOCKS_BUFFER_ARRAY);
    for (String blockBufferType : blockBufferTypes) {
      Configuration configuration = new Configuration(getRawConfiguration());
      configuration.set(DATA_BLOCKS_BUFFER, blockBufferType);
      AzureBlobFileSystem fs = Mockito.spy(
          (AzureBlobFileSystem) FileSystem.newInstance(configuration));
      AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
      Mockito.doReturn(store).when(fs).getAbfsStore();
      DataBlocks.DataBlock[] dataBlock = new DataBlocks.DataBlock[1];
      Mockito.doAnswer(getBlobFactoryInvocation -> {
        DataBlocks.BlockFactory factory = Mockito.spy(
            (DataBlocks.BlockFactory) getBlobFactoryInvocation.callRealMethod());
        Mockito.doAnswer(factoryCreateInvocation -> {
              dataBlock[0] = Mockito.spy(
                  (DataBlocks.DataBlock) factoryCreateInvocation.callRealMethod());
              return dataBlock[0];
            })
            .when(factory)
            .create(Mockito.anyLong(), Mockito.anyInt(), Mockito.any(
                BlockUploadStatistics.class));
        return factory;
      }).when(store).getBlockFactory();
      try (OutputStream os = fs.create(
          new Path(getMethodName() + "_" + blockBufferType))) {
        os.write(new byte[1]);
        Assertions.assertThat(dataBlock[0].getState())
            .describedAs(
                "On write of data in outputStream, state should become Writing")
            .isEqualTo(Writing);
        os.close();
        Mockito.verify(dataBlock[0], Mockito.times(1)).close();
        Assertions.assertThat(dataBlock[0].getState())
            .describedAs("On close of outputStream, state should become Closed")
            .isEqualTo(Closed);
      }
    }
  }
}
