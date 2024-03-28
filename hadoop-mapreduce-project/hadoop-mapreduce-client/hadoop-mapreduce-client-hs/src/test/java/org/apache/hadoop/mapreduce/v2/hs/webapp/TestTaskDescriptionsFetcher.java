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

package org.apache.hadoop.mapreduce.v2.hs.webapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.task.TaskDescriptions;
import org.apache.hadoop.mapreduce.v2.hs.JobHistory;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.app.AppState;
import org.apache.hadoop.yarn.app.SimpleAppInfo;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestTaskDescriptionsFetcher {

  @Test
  public void testBuildMRAppMasterWebServiceUrl() {
    String trackUrl = "http://ip-test.test:9046/proxy/application_1426670112915_0602/";
    String jobId = "job_1426670112915_0602";
    TaskDescriptionsFetcher fetcher =
        new TaskDescriptionsFetcher(mock(JobHistory.class), mock(RestClient.class));
    String url = fetcher.buildMRAppMasterWebServiceUrl(trackUrl, jobId);
    assertEquals(
        "http://ip-test.test:9046"
            + "/proxy/application_1426670112915_0602/"
            + "ws/v1/mapreduce/jobs/job_1426670112915_0602/taskDescriptions",
        url);
  }

  @Test
  public void testFetch() {
    JobHistory ctx = mock(JobHistory.class);
    when(ctx.getConfig()).thenReturn(new Configuration());
    RestClient client = mock(RestClient.class);
    SimpleAppInfo app = new SimpleAppInfo();
    app.setId(386);
    app.setState(AppState.RUNNING);
    app.setTrackingUrl("http://ip-test.test:9046/proxy/application_1426670112915_0602/");
    when(client.fetchAs(anyString(), eq(SimpleAppInfo.class))).thenReturn(app);
    TaskDescriptions expected = new TaskDescriptions();
    expected.setFound(true);
    expected.setSuccessful(true);
    expected.setErrorMsg(null);
    expected.setTaskDescriptionList(null);
    when(client.fetchAs(anyString(), eq(TaskDescriptions.class))).thenReturn(expected);
    TaskDescriptionsFetcher fetcher = new TaskDescriptionsFetcher(ctx, client);
    TaskDescriptions fetched = fetcher.fetch(MRApps.toJobID("job_1426670112915_0602"));
    assertNotNull(fetched);
    assertTrue(fetched.isFound());
    assertTrue(fetched.isSuccessful());
    assertNull(fetched.getTaskDescriptionList());
    assertNull(fetched.getErrorMsg());
  }
}
