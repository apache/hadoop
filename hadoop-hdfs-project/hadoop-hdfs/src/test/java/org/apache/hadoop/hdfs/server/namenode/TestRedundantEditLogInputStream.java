/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.Test;

import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.MkdirOp;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRedundantEditLogInputStream {
  private static final String FAKE_EDIT_STREAM_NAME = "FAKE_STREAM";

  @Test
  public void testNextOp() throws IOException {
    EditLogInputStream fakeStream1 = mock(EditLogInputStream.class);
    EditLogInputStream fakeStream2 = mock(EditLogInputStream.class);
    ArrayList<EditLogInputStream> list = new ArrayList();
    list.add(fakeStream1);
    list.add(fakeStream2);
    for (int i = 0; i < list.size(); i++) {
      EditLogInputStream stream = list.get(i);
      when(stream.getName()).thenReturn(FAKE_EDIT_STREAM_NAME + i);
      when(stream.getFirstTxId()).thenReturn(1L);
      when(stream.getLastTxId()).thenReturn(2L);
      when(stream.length()).thenReturn(1L);
    }
    when(fakeStream1.skipUntil(1)).thenThrow(new IOException("skipUntil failed."));
    when(fakeStream2.skipUntil(1)).thenReturn(true);
    FSEditLogOp op = new MkdirOp();
    op.setTransactionId(100);
    when(fakeStream2.readOp()).thenReturn(op);

    LogCapturer capture = LogCapturer.captureLogs(RedundantEditLogInputStream.LOG);
    RedundantEditLogInputStream redundantEditLogInputStream =
        new RedundantEditLogInputStream(list, 1);

    FSEditLogOp returnOp = redundantEditLogInputStream.nextOp();
    String log = capture.getOutput();
    assertTrue(log.contains("Got error skipUntil edit log input stream FAKE_STREAM0"));
    assertTrue(log.contains("Got error reading edit log input stream FAKE_STREAM0; "
        + "failing over to edit log FAKE_STREAM1"));
    assertEquals(op, returnOp);
  }
}
