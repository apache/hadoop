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
package org.apache.hadoop.conf;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;

public class TestJobConf {

  @Test
  public void testProfileParamsDefaults() {
    JobConf configuration = new JobConf();
    String result = configuration.getProfileParams();
    Assert.assertNotNull(result);
    Assert.assertTrue(result.contains("file=%s"));
    Assert.assertTrue(result.startsWith("-agentlib:hprof"));
  }

  @Test
  public void testProfileParamsSetter() {
    JobConf configuration = new JobConf();

    configuration.setProfileParams("test");
    Assert.assertEquals("test", configuration.get(MRJobConfig.TASK_PROFILE_PARAMS));
  }

  @Test
  public void testProfileParamsGetter() {
    JobConf configuration = new JobConf();

    configuration.set(MRJobConfig.TASK_PROFILE_PARAMS, "test");
    Assert.assertEquals("test", configuration.getProfileParams());
  }

  /**
   * Testing mapred.task.maxvmem replacement with new values
   *
   */
  @Test
  public void testMemoryConfigForMapOrReduceTask(){
    JobConf configuration = new JobConf();
    configuration.set(MRJobConfig.MAP_MEMORY_MB,String.valueOf(300));
    configuration.set(MRJobConfig.REDUCE_MEMORY_MB,String.valueOf(300));
    Assert.assertEquals(configuration.getMemoryForMapTask(),300);
    Assert.assertEquals(configuration.getMemoryForReduceTask(),300);

    configuration.set("mapred.task.maxvmem" , String.valueOf(2*1024 * 1024));
    configuration.set(MRJobConfig.MAP_MEMORY_MB,String.valueOf(300));
    configuration.set(MRJobConfig.REDUCE_MEMORY_MB,String.valueOf(300));
    Assert.assertEquals(configuration.getMemoryForMapTask(),2);
    Assert.assertEquals(configuration.getMemoryForReduceTask(),2);

    configuration = new JobConf();
    configuration.set("mapred.task.maxvmem" , "-1");
    configuration.set(MRJobConfig.MAP_MEMORY_MB,String.valueOf(300));
    configuration.set(MRJobConfig.REDUCE_MEMORY_MB,String.valueOf(400));
    Assert.assertEquals(configuration.getMemoryForMapTask(), 300);
    Assert.assertEquals(configuration.getMemoryForReduceTask(), 400);

    configuration = new JobConf();
    configuration.set("mapred.task.maxvmem" , String.valueOf(2*1024 * 1024));
    configuration.set(MRJobConfig.MAP_MEMORY_MB,"-1");
    configuration.set(MRJobConfig.REDUCE_MEMORY_MB,"-1");
    Assert.assertEquals(configuration.getMemoryForMapTask(),2);
    Assert.assertEquals(configuration.getMemoryForReduceTask(),2);

    configuration = new JobConf();
    configuration.set("mapred.task.maxvmem" , String.valueOf(-1));
    configuration.set(MRJobConfig.MAP_MEMORY_MB,"-1");
    configuration.set(MRJobConfig.REDUCE_MEMORY_MB,"-1");
    Assert.assertEquals(configuration.getMemoryForMapTask(),-1);
    Assert.assertEquals(configuration.getMemoryForReduceTask(),-1);    

    configuration = new JobConf();
    configuration.set("mapred.task.maxvmem" , String.valueOf(2*1024 * 1024));
    configuration.set(MRJobConfig.MAP_MEMORY_MB, "3");
    configuration.set(MRJobConfig.REDUCE_MEMORY_MB, "3");
    Assert.assertEquals(configuration.getMemoryForMapTask(),2);
    Assert.assertEquals(configuration.getMemoryForReduceTask(),2);
    
  }

  /**
   * Test that negative values for MAPRED_TASK_MAXVMEM_PROPERTY cause
   * new configuration keys' values to be used.
   */
  @Test
  public void testNegativeValueForTaskVmem() {
    JobConf configuration = new JobConf();
    
    configuration.set(JobConf.MAPRED_TASK_MAXVMEM_PROPERTY, "-3");
    Assert.assertEquals(MRJobConfig.DEFAULT_MAP_MEMORY_MB,
        configuration.getMemoryForMapTask());
    Assert.assertEquals(MRJobConfig.DEFAULT_REDUCE_MEMORY_MB,
        configuration.getMemoryForReduceTask());
    
    configuration.set(MRJobConfig.MAP_MEMORY_MB, "4");
    configuration.set(MRJobConfig.REDUCE_MEMORY_MB, "5");
    Assert.assertEquals(4, configuration.getMemoryForMapTask());
    Assert.assertEquals(5, configuration.getMemoryForReduceTask());
    
  }
  
  /**
   * Test that negative values for new configuration keys get passed through.
   */
  @Test
  public void testNegativeValuesForMemoryParams() {
    JobConf configuration = new JobConf();
        
    configuration.set(MRJobConfig.MAP_MEMORY_MB, "-5");
    configuration.set(MRJobConfig.REDUCE_MEMORY_MB, "-6");
    Assert.assertEquals(-5, configuration.getMemoryForMapTask());
    Assert.assertEquals(-6, configuration.getMemoryForReduceTask());
  }
  
  /**
   *   Test deprecated accessor and mutator method for mapred.task.maxvmem
   */
  @Test
  public void testMaxVirtualMemoryForTask() {
    JobConf configuration = new JobConf();

    //get test case
    configuration.set(MRJobConfig.MAP_MEMORY_MB, String.valueOf(300));
    configuration.set(MRJobConfig.REDUCE_MEMORY_MB, String.valueOf(-1));
    Assert.assertEquals(
      configuration.getMaxVirtualMemoryForTask(), 300 * 1024 * 1024);

    configuration = new JobConf();
    configuration.set(MRJobConfig.MAP_MEMORY_MB, String.valueOf(-1));
    configuration.set(MRJobConfig.REDUCE_MEMORY_MB, String.valueOf(200));
    Assert.assertEquals(
      configuration.getMaxVirtualMemoryForTask(), 200 * 1024 * 1024);

    configuration = new JobConf();
    configuration.set(MRJobConfig.MAP_MEMORY_MB, String.valueOf(-1));
    configuration.set(MRJobConfig.REDUCE_MEMORY_MB, String.valueOf(-1));
    configuration.set("mapred.task.maxvmem", String.valueOf(1 * 1024 * 1024));
    Assert.assertEquals(
      configuration.getMaxVirtualMemoryForTask(), 1 * 1024 * 1024);

    configuration = new JobConf();
    configuration.set("mapred.task.maxvmem", String.valueOf(1 * 1024 * 1024));
    Assert.assertEquals(
      configuration.getMaxVirtualMemoryForTask(), 1 * 1024 * 1024);

    //set test case

    configuration = new JobConf();
    configuration.setMaxVirtualMemoryForTask(2 * 1024 * 1024);
    Assert.assertEquals(configuration.getMemoryForMapTask(), 2);
    Assert.assertEquals(configuration.getMemoryForReduceTask(), 2);

    configuration = new JobConf();   
    configuration.set(MRJobConfig.MAP_MEMORY_MB, String.valueOf(300));
    configuration.set(MRJobConfig.REDUCE_MEMORY_MB, String.valueOf(400));
    configuration.setMaxVirtualMemoryForTask(2 * 1024 * 1024);
    Assert.assertEquals(configuration.getMemoryForMapTask(), 2);
    Assert.assertEquals(configuration.getMemoryForReduceTask(), 2);
    
    
  }

  /**
   * Ensure that by default JobContext.MAX_TASK_FAILURES_PER_TRACKER is less
   * JobContext.MAP_MAX_ATTEMPTS and JobContext.REDUCE_MAX_ATTEMPTS so that
   * failed tasks will be retried on other nodes
   */
  @Test
  public void testMaxTaskFailuresPerTracker() {
    JobConf jobConf = new JobConf(true);
    Assert.assertTrue("By default JobContext.MAX_TASK_FAILURES_PER_TRACKER was "
      + "not less than JobContext.MAP_MAX_ATTEMPTS and REDUCE_MAX_ATTEMPTS"
      ,jobConf.getMaxTaskFailuresPerTracker() < jobConf.getMaxMapAttempts() &&
      jobConf.getMaxTaskFailuresPerTracker() < jobConf.getMaxReduceAttempts()
      );
  }
}
