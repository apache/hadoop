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

import java.util.regex.Pattern;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.junit.Assert;
import org.junit.Test;

/**
 * test JobConf
 * 
 */
public class TestJobConf {

  /**
   * test getters and setters of JobConf
   */
  @SuppressWarnings("deprecation")
  @Test (timeout=5000)
  public void testJobConf() {
    JobConf conf = new JobConf();
    // test default value
    Pattern pattern = conf.getJarUnpackPattern();
    assertEquals(Pattern.compile("(?:classes/|lib/).*").toString(),
        pattern.toString());
    // default value
    assertFalse(conf.getKeepFailedTaskFiles());
    conf.setKeepFailedTaskFiles(true);
    assertTrue(conf.getKeepFailedTaskFiles());

    // default value
    assertNull(conf.getKeepTaskFilesPattern());
    conf.setKeepTaskFilesPattern("123454");
    assertEquals("123454", conf.getKeepTaskFilesPattern());

    // default value
    assertNotNull(conf.getWorkingDirectory());
    conf.setWorkingDirectory(new Path("test"));
    assertTrue(conf.getWorkingDirectory().toString().endsWith("test"));

    // default value
    assertEquals(1, conf.getNumTasksToExecutePerJvm());

    // default value
    assertNull(conf.getKeyFieldComparatorOption());
    conf.setKeyFieldComparatorOptions("keySpec");
    assertEquals("keySpec", conf.getKeyFieldComparatorOption());

    // default value
    assertFalse(conf.getUseNewReducer());
    conf.setUseNewReducer(true);
    assertTrue(conf.getUseNewReducer());

    // default
    assertTrue(conf.getMapSpeculativeExecution());
    assertTrue(conf.getReduceSpeculativeExecution());
    assertTrue(conf.getSpeculativeExecution());
    conf.setReduceSpeculativeExecution(false);
    assertTrue(conf.getSpeculativeExecution());

    conf.setMapSpeculativeExecution(false);
    assertFalse(conf.getSpeculativeExecution());
    assertFalse(conf.getMapSpeculativeExecution());
    assertFalse(conf.getReduceSpeculativeExecution());

    conf.setSessionId("ses");
    assertEquals("ses", conf.getSessionId());

    assertEquals(3, conf.getMaxTaskFailuresPerTracker());
    conf.setMaxTaskFailuresPerTracker(2);
    assertEquals(2, conf.getMaxTaskFailuresPerTracker());

    assertEquals(0, conf.getMaxMapTaskFailuresPercent());
    conf.setMaxMapTaskFailuresPercent(50);
    assertEquals(50, conf.getMaxMapTaskFailuresPercent());

    assertEquals(0, conf.getMaxReduceTaskFailuresPercent());
    conf.setMaxReduceTaskFailuresPercent(70);
    assertEquals(70, conf.getMaxReduceTaskFailuresPercent());

    // by default
    assertThat(conf.getJobPriority()).isEqualTo(JobPriority.DEFAULT);
    conf.setJobPriority(JobPriority.HIGH);
    assertThat(conf.getJobPriority()).isEqualTo(JobPriority.HIGH);

    assertNull(conf.getJobSubmitHostName());
    conf.setJobSubmitHostName("hostname");
    assertEquals("hostname", conf.getJobSubmitHostName());

    // default
    assertNull(conf.getJobSubmitHostAddress());
    conf.setJobSubmitHostAddress("ww");
    assertEquals("ww", conf.getJobSubmitHostAddress());

    // default value
    assertFalse(conf.getProfileEnabled());
    conf.setProfileEnabled(true);
    assertTrue(conf.getProfileEnabled());

    // default value
    assertEquals(conf.getProfileTaskRange(true).toString(), "0-2");
    assertEquals(conf.getProfileTaskRange(false).toString(), "0-2");
    conf.setProfileTaskRange(true, "0-3");
    assertEquals(conf.getProfileTaskRange(false).toString(), "0-2");
    assertEquals(conf.getProfileTaskRange(true).toString(), "0-3");

    // default value
    assertNull(conf.getMapDebugScript());
    conf.setMapDebugScript("mDbgScript");
    assertEquals("mDbgScript", conf.getMapDebugScript());

    // default value
    assertNull(conf.getReduceDebugScript());
    conf.setReduceDebugScript("rDbgScript");
    assertEquals("rDbgScript", conf.getReduceDebugScript());

    // default value
    assertNull(conf.getJobLocalDir());

    assertEquals("default", conf.getQueueName());
    conf.setQueueName("qname");
    assertEquals("qname", conf.getQueueName());

    conf.setMemoryForMapTask(100 * 1000);
    assertEquals(100 * 1000, conf.getMemoryForMapTask());
    conf.setMemoryForReduceTask(1000 * 1000);
    assertEquals(1000 * 1000, conf.getMemoryForReduceTask());

    assertEquals(-1, conf.getMaxPhysicalMemoryForTask());
    assertEquals("The variable key is no longer used.",
        JobConf.deprecatedString("key"));
    
    // make sure mapreduce.map|reduce.java.opts are not set by default
    // so that they won't override mapred.child.java.opts
    assertNull("mapreduce.map.java.opts should not be set by default",
        conf.get(JobConf.MAPRED_MAP_TASK_JAVA_OPTS));
    assertNull("mapreduce.reduce.java.opts should not be set by default",
        conf.get(JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS));
  }

  /**
   * Ensure that M/R 1.x applications can get and set task virtual memory with
   * old property names
   */
  @SuppressWarnings("deprecation")
  @Test (timeout = 10000)
  public void testDeprecatedPropertyNameForTaskVmem() {
    JobConf configuration = new JobConf();

    configuration.setLong(JobConf.MAPRED_JOB_MAP_MEMORY_MB_PROPERTY, 1024);
    configuration.setLong(JobConf.MAPRED_JOB_REDUCE_MEMORY_MB_PROPERTY, 1024);
    Assert.assertEquals(1024, configuration.getMemoryForMapTask());
    Assert.assertEquals(1024, configuration.getMemoryForReduceTask());
    // Make sure new property names aren't broken by the old ones
    configuration.setLong(JobConf.MAPREDUCE_JOB_MAP_MEMORY_MB_PROPERTY, 1025);
    configuration.setLong(JobConf.MAPREDUCE_JOB_REDUCE_MEMORY_MB_PROPERTY, 1025);
    Assert.assertEquals(1025, configuration.getMemoryForMapTask());
    Assert.assertEquals(1025, configuration.getMemoryForReduceTask());

    configuration.setMemoryForMapTask(2048);
    configuration.setMemoryForReduceTask(2048);
    Assert.assertEquals(2048, configuration.getLong(
        JobConf.MAPRED_JOB_MAP_MEMORY_MB_PROPERTY, -1));
    Assert.assertEquals(2048, configuration.getLong(
        JobConf.MAPRED_JOB_REDUCE_MEMORY_MB_PROPERTY, -1));
    // Make sure new property names aren't broken by the old ones
    Assert.assertEquals(2048, configuration.getLong(
        JobConf.MAPREDUCE_JOB_MAP_MEMORY_MB_PROPERTY, -1));
    Assert.assertEquals(2048, configuration.getLong(
        JobConf.MAPREDUCE_JOB_REDUCE_MEMORY_MB_PROPERTY, -1));
  }


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
    assertThat(configuration.getMemoryForMapTask()).isEqualTo(300);
    assertThat(configuration.getMemoryForReduceTask()).isEqualTo(300);

    configuration.set("mapred.task.maxvmem" , String.valueOf(2*1024 * 1024));
    configuration.set(MRJobConfig.MAP_MEMORY_MB,String.valueOf(300));
    configuration.set(MRJobConfig.REDUCE_MEMORY_MB,String.valueOf(300));
    assertThat(configuration.getMemoryForMapTask()).isEqualTo(2);
    assertThat(configuration.getMemoryForReduceTask()).isEqualTo(2);

    configuration = new JobConf();
    configuration.set("mapred.task.maxvmem" , "-1");
    configuration.set(MRJobConfig.MAP_MEMORY_MB,String.valueOf(300));
    configuration.set(MRJobConfig.REDUCE_MEMORY_MB,String.valueOf(400));
    assertThat(configuration.getMemoryForMapTask()).isEqualTo(300);
    assertThat(configuration.getMemoryForReduceTask()).isEqualTo(400);

    configuration = new JobConf();
    configuration.set("mapred.task.maxvmem" , String.valueOf(2*1024 * 1024));
    configuration.set(MRJobConfig.MAP_MEMORY_MB,"-1");
    configuration.set(MRJobConfig.REDUCE_MEMORY_MB,"-1");
    assertThat(configuration.getMemoryForMapTask()).isEqualTo(2);
    assertThat(configuration.getMemoryForReduceTask()).isEqualTo(2);

    configuration = new JobConf();
    configuration.set("mapred.task.maxvmem" , String.valueOf(-1));
    configuration.set(MRJobConfig.MAP_MEMORY_MB,"-1");
    configuration.set(MRJobConfig.REDUCE_MEMORY_MB,"-1");
    assertThat(configuration.getMemoryForMapTask()).isEqualTo(
        MRJobConfig.DEFAULT_MAP_MEMORY_MB);
    assertThat(configuration.getMemoryForReduceTask()).isEqualTo(
        MRJobConfig.DEFAULT_REDUCE_MEMORY_MB);

    configuration = new JobConf();
    configuration.set("mapred.task.maxvmem" , String.valueOf(2*1024 * 1024));
    configuration.set(MRJobConfig.MAP_MEMORY_MB, "3");
    configuration.set(MRJobConfig.REDUCE_MEMORY_MB, "3");
    assertThat(configuration.getMemoryForMapTask()).isEqualTo(2);
    assertThat(configuration.getMemoryForReduceTask()).isEqualTo(2);
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
    Assert.assertEquals(MRJobConfig.DEFAULT_MAP_MEMORY_MB,
        configuration.getMemoryForMapTask());
    Assert.assertEquals(MRJobConfig.DEFAULT_REDUCE_MEMORY_MB,
        configuration.getMemoryForReduceTask());
  }

  /**
   *   Test deprecated accessor and mutator method for mapred.task.maxvmem
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testMaxVirtualMemoryForTask() {
    JobConf configuration = new JobConf();

    //get test case
    configuration.set(MRJobConfig.MAP_MEMORY_MB, String.valueOf(300));
    configuration.set(MRJobConfig.REDUCE_MEMORY_MB, String.valueOf(-1));
    assertThat(configuration.getMaxVirtualMemoryForTask())
        .isEqualTo(1024 * 1024 * 1024);

    configuration = new JobConf();
    configuration.set(MRJobConfig.MAP_MEMORY_MB, String.valueOf(-1));
    configuration.set(MRJobConfig.REDUCE_MEMORY_MB, String.valueOf(200));
    assertThat(configuration.getMaxVirtualMemoryForTask())
        .isEqualTo(1024 * 1024 * 1024);

    configuration = new JobConf();
    configuration.set(MRJobConfig.MAP_MEMORY_MB, String.valueOf(-1));
    configuration.set(MRJobConfig.REDUCE_MEMORY_MB, String.valueOf(-1));
    configuration.set("mapred.task.maxvmem", String.valueOf(1 * 1024 * 1024));
    assertThat(configuration.getMaxVirtualMemoryForTask())
        .isEqualTo(1 * 1024 * 1024);

    configuration = new JobConf();
    configuration.set("mapred.task.maxvmem", String.valueOf(1 * 1024 * 1024));
    assertThat(configuration.getMaxVirtualMemoryForTask())
        .isEqualTo(1 * 1024 * 1024);

    //set test case

    configuration = new JobConf();
    configuration.setMaxVirtualMemoryForTask(2 * 1024 * 1024);
    assertThat(configuration.getMemoryForMapTask()).isEqualTo(2);
    assertThat(configuration.getMemoryForReduceTask()).isEqualTo(2);

    configuration = new JobConf();
    configuration.set(MRJobConfig.MAP_MEMORY_MB, String.valueOf(300));
    configuration.set(MRJobConfig.REDUCE_MEMORY_MB, String.valueOf(400));
    configuration.setMaxVirtualMemoryForTask(2 * 1024 * 1024);
    assertThat(configuration.getMemoryForMapTask()).isEqualTo(2);
    assertThat(configuration.getMemoryForReduceTask()).isEqualTo(2);
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

  /**
   * Test parsing various types of Java heap options.
   */
  @Test
  public void testParseMaximumHeapSizeMB() {
    // happy cases
    Assert.assertEquals(4096, JobConf.parseMaximumHeapSizeMB("-Xmx4294967296"));
    Assert.assertEquals(4096, JobConf.parseMaximumHeapSizeMB("-Xmx4194304k"));
    Assert.assertEquals(4096, JobConf.parseMaximumHeapSizeMB("-Xmx4096m"));
    Assert.assertEquals(4096, JobConf.parseMaximumHeapSizeMB("-Xmx4g"));

    // sad cases
    Assert.assertEquals(-1, JobConf.parseMaximumHeapSizeMB("-Xmx4?"));
    Assert.assertEquals(-1, JobConf.parseMaximumHeapSizeMB(""));
  }

  /**
   * Test various Job Priority
   */
  @Test
  public void testJobPriorityConf() {
    JobConf conf = new JobConf();

    // by default
    assertThat(conf.getJobPriority()).isEqualTo(JobPriority.DEFAULT);
    assertEquals(0, conf.getJobPriorityAsInteger());
    // Set JobPriority.LOW using old API, and verify output from both getter
    conf.setJobPriority(JobPriority.LOW);
    assertThat(conf.getJobPriority()).isEqualTo(JobPriority.LOW);
    assertEquals(2, conf.getJobPriorityAsInteger());

    // Set JobPriority.VERY_HIGH using old API, and verify output
    conf.setJobPriority(JobPriority.VERY_HIGH);
    assertThat(conf.getJobPriority()).isEqualTo(JobPriority.VERY_HIGH);
    assertEquals(5, conf.getJobPriorityAsInteger());

    // Set 3 as priority using new API, and verify output from both getter
    conf.setJobPriorityAsInteger(3);
    assertThat(conf.getJobPriority()).isEqualTo(JobPriority.NORMAL);
    assertEquals(3, conf.getJobPriorityAsInteger());

    // Set 4 as priority using new API, and verify output
    conf.setJobPriorityAsInteger(4);
    assertThat(conf.getJobPriority()).isEqualTo(JobPriority.HIGH);
    assertEquals(4, conf.getJobPriorityAsInteger());
    // Now set some high integer values and verify output from old api
    conf.setJobPriorityAsInteger(57);
    assertThat(conf.getJobPriority()).isEqualTo(JobPriority.UNDEFINED_PRIORITY);
    assertEquals(57, conf.getJobPriorityAsInteger());

    // Error case where UNDEFINED_PRIORITY is set explicitly
    conf.setJobPriority(JobPriority.UNDEFINED_PRIORITY);
    assertThat(conf.getJobPriority()).isEqualTo(JobPriority.UNDEFINED_PRIORITY);

    // As UNDEFINED_PRIORITY cannot be mapped to any integer value, resetting
    // to default as 0.
    assertEquals(0, conf.getJobPriorityAsInteger());
  }
}
