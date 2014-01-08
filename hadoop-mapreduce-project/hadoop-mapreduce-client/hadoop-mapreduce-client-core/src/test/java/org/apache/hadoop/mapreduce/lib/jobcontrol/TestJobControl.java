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
package org.apache.hadoop.mapreduce.lib.jobcontrol;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestJobControl {

  /*
   * Tests for the circular dependency between the jobs in job control. If there
   * is a circular dependency, an IllegalArgumentException will be thrown
   */
  @Test
  public void testCircularDependency() throws IOException {
    ControlledJob job1 = new ControlledJob(new Configuration());
    job1.setJobName("job1");
    ControlledJob job2 = new ControlledJob(new Configuration());
    job2.setJobName("job2");
    ControlledJob job3 = new ControlledJob(new Configuration());
    job3.setJobName("job3");
    job1.addDependingJob(job2);
    job2.addDependingJob(job3);
    job3.addDependingJob(job1);
    JobControl jobControl = new JobControl("test");
    jobControl.addJob(job1);
    jobControl.addJob(job2);
    jobControl.addJob(job3);

    try {
      jobControl.run();
    } catch (Exception e) {
      assertTrue(e instanceof IllegalArgumentException);
    }

  }
}

