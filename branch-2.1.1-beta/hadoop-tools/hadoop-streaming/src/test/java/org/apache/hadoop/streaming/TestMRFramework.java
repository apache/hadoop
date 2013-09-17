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
package org.apache.hadoop.streaming;

import static org.junit.Assert.*;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.junit.Test;

public class TestMRFramework {

  @Test
  public void testFramework() {
    JobConf jobConf = new JobConf();
    jobConf.set(JTConfig.JT_IPC_ADDRESS, MRConfig.LOCAL_FRAMEWORK_NAME);
    jobConf.set(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_FRAMEWORK_NAME);
    assertFalse("Expected 'isLocal' to be false", 
        StreamUtil.isLocalJobTracker(jobConf));
    
    jobConf.set(JTConfig.JT_IPC_ADDRESS, MRConfig.LOCAL_FRAMEWORK_NAME);
    jobConf.set(MRConfig.FRAMEWORK_NAME, MRConfig.CLASSIC_FRAMEWORK_NAME);
    assertFalse("Expected 'isLocal' to be false", 
        StreamUtil.isLocalJobTracker(jobConf));
    
    jobConf.set(JTConfig.JT_IPC_ADDRESS, "jthost:9090");
    jobConf.set(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
    assertTrue("Expected 'isLocal' to be true", 
        StreamUtil.isLocalJobTracker(jobConf));
  }

}
