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

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import com.google.common.base.Joiner;

public class TestFileJournalManager {

  @Test
  public void testGetRemoteEditLog() throws IOException {
    StorageDirectory sd = FSImageTestUtil.mockStorageDirectory(
        NameNodeDirType.EDITS, false,
        NNStorage.getFinalizedEditsFileName(1, 100),
        NNStorage.getFinalizedEditsFileName(101, 200),
        NNStorage.getInProgressEditsFileName(201),
        NNStorage.getFinalizedEditsFileName(1001, 1100));
        
    FileJournalManager fjm = new FileJournalManager(sd);
    assertEquals("[1,100],[101,200],[1001,1100]", getLogsAsString(fjm, 1));
    assertEquals("[101,200],[1001,1100]", getLogsAsString(fjm, 101));
    assertEquals("[1001,1100]", getLogsAsString(fjm, 201));
    try {
      assertEquals("[]", getLogsAsString(fjm, 150));
      fail("Did not throw when asking for a txn in the middle of a log");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "150 which is in the middle", ioe);
    }
    assertEquals("Asking for a newer log than exists should return empty list",
        "", getLogsAsString(fjm, 9999));
  }

  private static String getLogsAsString(
      FileJournalManager fjm, long firstTxId) throws IOException {
    return Joiner.on(",").join(fjm.getRemoteEditLogs(firstTxId));
  }
  
}
