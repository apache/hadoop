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

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestJobLocalizer {

  @Test(timeout = 1000)
  public void testConcurrentJobLocalizers() throws IOException {
    final String LOCAL_DIR = "/tmp/mapred/local";
    JobConf conf = new JobConf(new Configuration());
    
    JobLocalizer localizer1 = new JobLocalizer(conf, "user1", "jobid1",
        LOCAL_DIR);
    JobLocalizer localizer2 = new JobLocalizer(conf, "user2", "jobid2",
        LOCAL_DIR);
    assertTrue("Localizer 1 job local dirs should have user1",
        localizer1.ttConf.get(JobLocalizer.JOB_LOCAL_CTXT).contains("user1"));
    assertTrue("Localizer 2 job local dirs should have user2",
        localizer2.ttConf.get(JobLocalizer.JOB_LOCAL_CTXT).contains("user2"));
  }
}
