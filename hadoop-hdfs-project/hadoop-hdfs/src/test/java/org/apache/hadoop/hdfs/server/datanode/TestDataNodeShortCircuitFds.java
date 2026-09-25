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
package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.FileInputStream;
import java.io.IOException;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.junit.jupiter.api.Test;

public class TestDataNodeShortCircuitFds {

  @Test
  @SuppressWarnings("unchecked")
  public void testClosesBlockStreamWhenMetadataStreamFails() throws Exception {
    // A failure opening the metadata stream must not leak the already-opened block stream:
    // requestShortCircuitFdsForRead() must close fis[0] before propagating the IOException.
    ExtendedBlock blk = new ExtendedBlock("bp", 1L);
    FileInputStream blockStream = mock(FileInputStream.class);
    IOException failure = new IOException("metadata stream");

    FsDatasetSpi<FsVolumeSpi> data = mock(FsDatasetSpi.class);
    when(data.getBlockInputStream(any(ExtendedBlock.class), anyLong())).thenReturn(blockStream);
    when(data.getMetaDataInputStream(any(ExtendedBlock.class))).thenThrow(failure);

    DataNode dn = mock(DataNode.class, CALLS_REAL_METHODS);
    dn.data = data;
    dn.metrics = mock(DataNodeMetrics.class);

    IOException thrown = assertThrows(IOException.class,
        () -> dn.requestShortCircuitFdsForRead(blk, null, DataNode.CURRENT_BLOCK_FORMAT_VERSION));

    assertSame(failure, thrown);
    verify(blockStream).close();
  }
}
