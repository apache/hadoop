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
import static org.junit.Assert.*;

import org.apache.hadoop.fs.Path;
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
    assertEquals(JobPriority.NORMAL.name(), conf.getJobPriority().name());
    conf.setJobPriority(JobPriority.HIGH);
    assertEquals(JobPriority.HIGH.name(), conf.getJobPriority().name());

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
    assertEquals("mapreduce.map.java.opts should not be set by default",
        null, conf.get(JobConf.MAPRED_MAP_TASK_JAVA_OPTS));
    assertEquals("mapreduce.reduce.java.opts should not be set by default",
        null, conf.get(JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS));
  }

  /**
   * Ensure that M/R 1.x applications can get and set task virtual memory with
   * old property names
   */
  @SuppressWarnings("deprecation")
  @Test (timeout = 1000)
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
}
