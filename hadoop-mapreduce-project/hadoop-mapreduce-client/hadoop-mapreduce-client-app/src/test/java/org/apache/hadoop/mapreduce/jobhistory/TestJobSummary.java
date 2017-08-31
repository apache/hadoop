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

package org.apache.hadoop.mapreduce.jobhistory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestJobSummary {

  private static final Log LOG = LogFactory.getLog(TestJobSummary.class);
  private JobSummary summary = new JobSummary();

  @Before
  public void before() {
    JobId mockJobId = mock(JobId.class);
    when(mockJobId.toString()).thenReturn("testJobId");
    summary.setJobId(mockJobId);
    summary.setJobSubmitTime(2L);
    summary.setJobLaunchTime(3L);
    summary.setFirstMapTaskLaunchTime(4L);
    summary.setFirstReduceTaskLaunchTime(5L);
    summary.setJobFinishTime(6L);
    summary.setNumSucceededMaps(1);
    summary.setNumFailedMaps(0);
    summary.setNumSucceededReduces(1);
    summary.setNumFailedReduces(0);
    summary.setNumKilledMaps(0);
    summary.setNumKilledReduces(0);
    summary.setUser("testUser");
    summary.setQueue("testQueue");
    summary.setJobStatus("testJobStatus");
    summary.setMapSlotSeconds(7);
    summary.setReduceSlotSeconds(8);
    summary.setJobName("testName");
  }

  @Test
  public void testEscapeJobSummary() {
    // verify newlines are escaped
    summary.setJobName("aa\rbb\ncc\r\ndd");
    String out = summary.getJobSummaryString();
    LOG.info("summary: " + out);
    Assert.assertFalse(out.contains("\r"));
    Assert.assertFalse(out.contains("\n"));
    Assert.assertTrue(out.contains("aa\\rbb\\ncc\\r\\ndd"));
  }
}
