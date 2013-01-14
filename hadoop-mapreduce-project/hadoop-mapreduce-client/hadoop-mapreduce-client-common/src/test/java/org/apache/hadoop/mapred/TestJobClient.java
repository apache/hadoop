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
package org.apache.hadoop.mapred;

import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ClusterStatus.BlackListInfo;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.junit.Assert;
import org.junit.Test;

public class TestJobClient {
  @Test
  public void testGetClusterStatusWithLocalJobRunner() throws Exception {
    Configuration conf = new Configuration();
    conf.set(JTConfig.JT_IPC_ADDRESS, MRConfig.LOCAL_FRAMEWORK_NAME);
    conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
    JobClient client = new JobClient(conf);
    ClusterStatus clusterStatus = client.getClusterStatus(true);
    Collection<String> activeTrackerNames = clusterStatus
        .getActiveTrackerNames();
    Assert.assertEquals(0, activeTrackerNames.size());
    int blacklistedTrackers = clusterStatus.getBlacklistedTrackers();
    Assert.assertEquals(0, blacklistedTrackers);
    Collection<BlackListInfo> blackListedTrackersInfo = clusterStatus
        .getBlackListedTrackersInfo();
    Assert.assertEquals(0, blackListedTrackersInfo.size());
  }
}
