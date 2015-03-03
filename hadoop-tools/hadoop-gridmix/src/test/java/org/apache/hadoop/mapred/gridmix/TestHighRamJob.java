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
package org.apache.hadoop.mapred.gridmix;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.gridmix.DebugJobProducer.MockJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.rumen.JobStory;
import org.junit.Test;

/**
 * Test if Gridmix correctly configures the simulated job's configuration for
 * high ram job properties.
 */
public class TestHighRamJob {
  /**
   * A dummy {@link GridmixJob} that opens up the simulated job for testing.
   */
  protected static class DummyGridmixJob extends GridmixJob {
    public DummyGridmixJob(Configuration conf, JobStory desc) 
    throws IOException {
      super(conf, System.currentTimeMillis(), desc, new Path("test"), 
            UserGroupInformation.getCurrentUser(), -1);
    }
    
    /**
     * Do nothing since this is a dummy gridmix job.
     */
    @Override
    public Job call() throws Exception {
      return null;
    }
    
    @Override
    protected boolean canEmulateCompression() {
      // return false as we don't need compression
      return false;
    }
    
    protected Job getJob() {
      // open the simulated job for testing
      return job;
    }
  }
  
  private static void testHighRamConfig(long jobMapMB, long jobReduceMB, 
      long clusterMapMB, long clusterReduceMB, long simulatedClusterMapMB, 
      long simulatedClusterReduceMB, long expectedMapMB, long expectedReduceMB, 
      Configuration gConf) 
  throws IOException {
    Configuration simulatedJobConf = new Configuration(gConf);
    simulatedJobConf.setLong(MRConfig.MAPMEMORY_MB, simulatedClusterMapMB);
    simulatedJobConf.setLong(MRConfig.REDUCEMEMORY_MB, 
                             simulatedClusterReduceMB);
    
    // define a source conf
    Configuration sourceConf = new Configuration();
    
    // configure the original job
    sourceConf.setLong(MRJobConfig.MAP_MEMORY_MB, jobMapMB);
    sourceConf.setLong(MRConfig.MAPMEMORY_MB, clusterMapMB);
    sourceConf.setLong(MRJobConfig.REDUCE_MEMORY_MB, jobReduceMB);
    sourceConf.setLong(MRConfig.REDUCEMEMORY_MB, clusterReduceMB);
    
    // define a mock job
    MockJob story = new MockJob(sourceConf);
    
    GridmixJob job = new DummyGridmixJob(simulatedJobConf, story);
    Job simulatedJob = job.getJob();
    JobConf simulatedConf = (JobConf)simulatedJob.getConfiguration();
    
    // check if the high ram properties are not set
    assertEquals(expectedMapMB, 
                 simulatedConf.getMemoryRequired(TaskType.MAP));
    assertEquals(expectedReduceMB, 
                 simulatedConf.getMemoryRequired(TaskType.REDUCE));
  }
  
  /**
   * Tests high ram job properties configuration.
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testHighRamFeatureEmulation() throws IOException {
    // define the gridmix conf
    Configuration gridmixConf = new Configuration();
    
    // test : check high ram emulation disabled
    gridmixConf.setBoolean(GridmixJob.GRIDMIX_HIGHRAM_EMULATION_ENABLE, false);
    testHighRamConfig(10, 20, 5, 10, MRJobConfig.DEFAULT_MAP_MEMORY_MB, 
                      MRJobConfig.DEFAULT_REDUCE_MEMORY_MB, 
                      MRJobConfig.DEFAULT_MAP_MEMORY_MB, 
                      MRJobConfig.DEFAULT_REDUCE_MEMORY_MB, gridmixConf);
    
    // test : check with high ram enabled (default) and no scaling
    gridmixConf = new Configuration();
    // set the deprecated max memory limit
    gridmixConf.setLong(JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY, 
                        20*1024*1024);
    testHighRamConfig(10, 20, 5, 10, 5, 10, 10, 20, gridmixConf);
    
    // test : check with high ram enabled and scaling
    gridmixConf = new Configuration();
    // set the new max map/reduce memory limits
    gridmixConf.setLong(JTConfig.JT_MAX_MAPMEMORY_MB, 100);
    gridmixConf.setLong(JTConfig.JT_MAX_REDUCEMEMORY_MB, 300);
    testHighRamConfig(10, 45, 5, 15, 50, 100, 100, 300, gridmixConf);
    
    // test : check with high ram enabled and map memory scaling mismatch 
    //        (deprecated)
    gridmixConf = new Configuration();
    gridmixConf.setLong(JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY, 
                        70*1024*1024);
    Boolean failed = null;
    try {
      testHighRamConfig(10, 45, 5, 15, 50, 100, 100, 300, gridmixConf);
      failed = false;
    } catch (Exception e) {
      failed = true;
    }
    assertNotNull(failed);
    assertTrue("Exception expected for exceeding map memory limit "
               + "(deprecation)!", failed);
    
    // test : check with high ram enabled and reduce memory scaling mismatch 
    //        (deprecated)
    gridmixConf = new Configuration();
    gridmixConf.setLong(JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY, 
                        150*1024*1024);
    failed = null;
    try {
      testHighRamConfig(10, 45, 5, 15, 50, 100, 100, 300, gridmixConf);
      failed = false;
    } catch (Exception e) {
      failed = true;
    }
    assertNotNull(failed);
    assertTrue("Exception expected for exceeding reduce memory limit "
               + "(deprecation)!", failed);
    
    // test : check with high ram enabled and scaling mismatch on map limits
    gridmixConf = new Configuration();
    gridmixConf.setLong(JTConfig.JT_MAX_MAPMEMORY_MB, 70);
    failed = null;
    try {
      testHighRamConfig(10, 45, 5, 15, 50, 100, 100, 300, gridmixConf);
      failed = false;
    } catch (Exception e) {
      failed = true;
    }
    assertNotNull(failed);
    assertTrue("Exception expected for exceeding map memory limit!", failed);
    
    // test : check with high ram enabled and scaling mismatch on reduce 
    //        limits
    gridmixConf = new Configuration();
    gridmixConf.setLong(JTConfig.JT_MAX_REDUCEMEMORY_MB, 200);
    failed = null;
    try {
      testHighRamConfig(10, 45, 5, 15, 50, 100, 100, 300, gridmixConf);
      failed = false;
    } catch (Exception e) {
      failed = true;
    }
    assertNotNull(failed);
    assertTrue("Exception expected for exceeding reduce memory limit!", failed);
  }
}
