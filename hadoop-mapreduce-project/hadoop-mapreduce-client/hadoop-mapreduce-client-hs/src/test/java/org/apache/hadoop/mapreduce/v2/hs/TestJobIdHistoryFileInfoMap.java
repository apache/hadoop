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
package org.apache.hadoop.mapreduce.v2.hs;

import java.util.Collection;
import java.util.NavigableSet;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager.HistoryFileInfo;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager.JobIdHistoryFileInfoMap;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestJobIdHistoryFileInfoMap {

  private boolean checkSize(JobIdHistoryFileInfoMap map, int size)
      throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      if (map.size() != size)
        Thread.sleep(20);
      else
        return true;
    }
    return false;
  }

  /**
   * Trivial test case that verifies basic functionality of {@link
   * JobIdHistoryFileInfoMap}
   */
  @Test(timeout = 2000)
  public void testWithSingleElement() throws InterruptedException {
    JobIdHistoryFileInfoMap mapWithSize = new JobIdHistoryFileInfoMap();

    JobId jobId = MRBuilderUtils.newJobId(1, 1, 1);
    HistoryFileInfo fileInfo1 = Mockito.mock(HistoryFileInfo.class);
    Mockito.when(fileInfo1.getJobId()).thenReturn(jobId);

    // add it twice
    assertEquals("Incorrect return on putIfAbsent()",
        null, mapWithSize.putIfAbsent(jobId, fileInfo1));
    assertEquals("Incorrect return on putIfAbsent()",
        fileInfo1, mapWithSize.putIfAbsent(jobId, fileInfo1));

    // check get()
    assertEquals("Incorrect get()", fileInfo1, mapWithSize.get(jobId));
    assertTrue("Incorrect size()", checkSize(mapWithSize, 1));

    // check navigableKeySet()
    NavigableSet<JobId> set = mapWithSize.navigableKeySet();
    assertEquals("Incorrect navigableKeySet()", 1, set.size());
    assertTrue("Incorrect navigableKeySet()", set.contains(jobId));

    // check values()
    Collection<HistoryFileInfo> values = mapWithSize.values();
    assertEquals("Incorrect values()", 1, values.size());
    assertTrue("Incorrect values()", values.contains(fileInfo1));
  }
}
