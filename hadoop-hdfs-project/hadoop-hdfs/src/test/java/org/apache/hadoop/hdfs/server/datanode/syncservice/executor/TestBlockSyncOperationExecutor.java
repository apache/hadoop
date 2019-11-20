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
package org.apache.hadoop.hdfs.server.datanode.syncservice.executor;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BBPartHandle;
import org.apache.hadoop.fs.MultipartUploader;
import org.apache.hadoop.fs.PartHandle;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.protocol.BlockSyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionResult;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestBlockSyncOperationExecutor {

  @Mock
  private BlockReader blockReaderMock;

  @Mock
  private MultipartUploader multipartUploaderMock;

  @Test
  public void executeMultipartPutFileSyncTask() throws Exception {
    long blockLength = 42L;
    Configuration conf = new Configuration();
    BlockSyncOperationExecutor blockSyncOperationExecutor =
        new BlockSyncOperationExecutor(conf,
            (locatedBlock, config) -> blockReaderMock,
            fs -> multipartUploaderMock);
    String uploadHandleStr = "uploadHandle";
    ByteBuffer uploadHandle = ByteBuffer.wrap(uploadHandleStr.getBytes());
    PartHandle partHandle = BBPartHandle.from(uploadHandle);
    when(multipartUploaderMock.putPart(any(), any(), anyInt(), any(),
        anyLong())).thenReturn(partHandle);
    UUID syncTaskId = UUID.randomUUID();
    URI remoteUri = new URI("remoteUri");
    String syncMountId = "syncMountId";
    Block block = new Block(42L, blockLength, 44L);
    ExtendedBlock extendedBlock1 = new ExtendedBlock("poolId", block);
    LocatedBlock locatedBlock = new LocatedBlock(extendedBlock1, null);
    List<LocatedBlock> locatedBlocks = Lists.newArrayList(locatedBlock);
    Integer partNumber = 85;
    final int offset = 0;
    final long length = locatedBlock.getBlockSize();


    BlockSyncTask blockSyncTask = new BlockSyncTask(syncTaskId, remoteUri,
        locatedBlocks, partNumber, uploadHandle, offset, length, syncMountId);

    SyncTaskExecutionResult result =
        blockSyncOperationExecutor.execute(blockSyncTask);

    assertThat(result).isNotNull();
    Long actualLength = result.getNumberOfBytes();
    assertThat(actualLength).isEqualTo(blockLength);
    assertThat(result.getResult()).isEqualTo(partHandle.bytes());
  }
}