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
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FSDirWriteFileOp.ValidateAddBlockResult;
import org.apache.hadoop.net.Node;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class TestFSDirWriteFileOp {

  @Test
  @SuppressWarnings("unchecked")
  public void testIgnoreClientLocality() throws IOException {
    ValidateAddBlockResult addBlockResult =
        new ValidateAddBlockResult(1024L, 3, (byte) 0x01, null, null, null);

    EnumSet<AddBlockFlag> addBlockFlags =
        EnumSet.of(AddBlockFlag.IGNORE_CLIENT_LOCALITY);

    BlockManager bmMock = mock(BlockManager.class);

    ArgumentCaptor<Node> nodeCaptor = ArgumentCaptor.forClass(Node.class);

    when(bmMock.chooseTarget4NewBlock(anyString(), anyInt(), any(), anySet(),
        anyLong(), anyList(), anyByte(), any(), any(), any())).thenReturn(null);

    FSDirWriteFileOp.chooseTargetForNewBlock(bmMock, "localhost", null, null,
        addBlockFlags, addBlockResult);

    // There should be no other interactions with the block manager when the
    // IGNORE_CLIENT_LOCALITY is passed in because there is no need to discover
    // the local node requesting the new block
    verify(bmMock, times(1)).chooseTarget4NewBlock(anyString(), anyInt(),
        nodeCaptor.capture(), anySet(), anyLong(), anyList(), anyByte(), any(),
        any(), any());

    verifyNoMoreInteractions(bmMock);

    assertNull(
        "Source node was assigned a value. Expected 'null' value because "
            + "chooseTarget was flagged to ignore source node locality",
        nodeCaptor.getValue());
  }
}
