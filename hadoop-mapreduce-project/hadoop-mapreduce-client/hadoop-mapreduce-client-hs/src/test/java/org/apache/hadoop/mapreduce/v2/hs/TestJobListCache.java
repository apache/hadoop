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

import java.lang.InterruptedException;
import java.util.Collection;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager.HistoryFileInfo;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager.JobListCache;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;

public class TestJobListCache {

  @Test (timeout = 1000)
  public void testAddExisting() {
    JobListCache cache = new JobListCache(2, 1000);

    JobId jobId = MRBuilderUtils.newJobId(1, 1, 1);
    HistoryFileInfo fileInfo = Mockito.mock(HistoryFileInfo.class);
    Mockito.when(fileInfo.getJobId()).thenReturn(jobId);

    cache.addIfAbsent(fileInfo);
    cache.addIfAbsent(fileInfo);
    assertEquals("Incorrect number of cache entries", 1,
        cache.values().size());
  }

  @Test (timeout = 5000)
  public void testEviction() throws InterruptedException {
    int maxSize = 2;
    JobListCache cache = new JobListCache(maxSize, 1000);

    JobId jobId1 = MRBuilderUtils.newJobId(1, 1, 1);
    HistoryFileInfo fileInfo1 = Mockito.mock(HistoryFileInfo.class);
    Mockito.when(fileInfo1.getJobId()).thenReturn(jobId1);

    JobId jobId2 = MRBuilderUtils.newJobId(2, 2, 2);
    HistoryFileInfo fileInfo2 = Mockito.mock(HistoryFileInfo.class);
    Mockito.when(fileInfo2.getJobId()).thenReturn(jobId2);

    JobId jobId3 = MRBuilderUtils.newJobId(3, 3, 3);
    HistoryFileInfo fileInfo3 = Mockito.mock(HistoryFileInfo.class);
    Mockito.when(fileInfo3.getJobId()).thenReturn(jobId3);

    cache.addIfAbsent(fileInfo1);
    cache.addIfAbsent(fileInfo2);
    cache.addIfAbsent(fileInfo3);

    Collection <HistoryFileInfo> values;
    for (int i = 0; i < 9; i++) {
      values = cache.values();
      if (values.size() > maxSize) {
        Thread.sleep(100);
      } else {
        assertFalse("fileInfo1 should have been evicted",
          values.contains(fileInfo1));
        return;
      }
    }
    fail("JobListCache didn't delete the extra entry");
  }
}
