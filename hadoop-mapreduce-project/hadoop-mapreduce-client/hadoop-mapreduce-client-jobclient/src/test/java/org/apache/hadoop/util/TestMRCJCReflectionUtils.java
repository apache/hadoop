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

package org.apache.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for the JobConf-related parts of common's ReflectionUtils
 * class.
 */
public class TestMRCJCReflectionUtils {
  @Before
  public void setUp() {
    ReflectionUtils.clearCache();
  }

  /**
   * This is to test backward compatibility of ReflectionUtils for 
   * JobConfigurable objects. 
   * This should be made deprecated along with the mapred package HADOOP-1230. 
   * Should be removed when mapred package is removed.
   */
  @Test
  public void testSetConf() {
    JobConfigurableOb ob = new JobConfigurableOb();
    ReflectionUtils.setConf(ob, new Configuration());
    assertFalse(ob.configured);
    ReflectionUtils.setConf(ob, new JobConf());
    assertTrue(ob.configured);
  }
  
  private static class JobConfigurableOb implements JobConfigurable {
    boolean configured;
    public void configure(JobConf job) {
      configured = true;
    }
  }
}
