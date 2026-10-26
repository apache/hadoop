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
package org.apache.hadoop.mapreduce.v2.app.security.authorize;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTaskLevelSecurityEnforcer extends AbstractHadoopTestBase {

  @Test
  public void testServiceDisabled() {
    JobConf conf = new JobConf();
    assertPass(conf);
  }

  @Test
  public void testServiceEnabled() {
    JobConf conf = new JobConf();
    conf.setBoolean(MRConfig.MAPREDUCE_TASK_SECURITY_ENABLED, true);
    assertPass(conf);
  }

  @Test
  public void testDeniedPackage() {
    JobConf conf = new JobConf();
    conf.setBoolean(MRConfig.MAPREDUCE_TASK_SECURITY_ENABLED, true);
    conf.setStrings(MRConfig.SECURITY_DENIED_TASKS, "org.apache.hadoop.streaming");
    conf.set(MRJobConfig.MAP_CLASS_ATTR, "org.apache.hadoop.streaming.PipeMapper");
    assertDenied(conf);
  }

  @Test
  public void testDeniedClass() {
    JobConf conf = new JobConf();
    conf.setBoolean(MRConfig.MAPREDUCE_TASK_SECURITY_ENABLED, true);
    conf.setStrings(MRConfig.SECURITY_DENIED_TASKS,
        "org.apache.hadoop.streaming",
        "org.apache.hadoop.examples.QuasiMonteCarlo$QmcReducer");
    conf.set(MRJobConfig.REDUCE_CLASS_ATTR,
        "org.apache.hadoop.examples.QuasiMonteCarlo$QmcReducer");
    assertDenied(conf);
  }

  @Test
  public void testIgnoreReducer() {
    JobConf conf = new JobConf();
    conf.setBoolean(MRConfig.MAPREDUCE_TASK_SECURITY_ENABLED, true);
    conf.setStrings(MRConfig.SECURITY_PROPERTY_DOMAIN,
        MRJobConfig.MAP_CLASS_ATTR,
        MRJobConfig.COMBINE_CLASS_ATTR);
    conf.setStrings(MRConfig.SECURITY_DENIED_TASKS,
        "org.apache.hadoop.streaming",
        "org.apache.hadoop.examples.QuasiMonteCarlo$QmcReducer");
    conf.set(MRJobConfig.REDUCE_CLASS_ATTR,
        "org.apache.hadoop.examples.QuasiMonteCarlo$QmcReducer");
    assertPass(conf);
  }

  @Test
  public void testDeniedUser() {
    JobConf conf = new JobConf();
    conf.setBoolean(MRConfig.MAPREDUCE_TASK_SECURITY_ENABLED, true);
    conf.setStrings(MRConfig.SECURITY_DENIED_TASKS, "org.apache.hadoop.streaming");
    conf.setStrings(MRConfig.SECURITY_ALLOWED_USERS, "alice");
    conf.set(MRJobConfig.MAP_CLASS_ATTR, "org.apache.hadoop.streaming.PipeMapper");
    conf.set(MRJobConfig.USER_NAME, "bob");
    assertDenied(conf);
  }

  @Test
  public void testAllowedUser() {
    JobConf conf = new JobConf();
    conf.setBoolean(MRConfig.MAPREDUCE_TASK_SECURITY_ENABLED, true);
    conf.setStrings(MRConfig.SECURITY_DENIED_TASKS, "org.apache.hadoop.streaming");
    conf.setStrings(MRConfig.SECURITY_ALLOWED_USERS, "alice", "bob");
    conf.set(MRJobConfig.MAP_CLASS_ATTR, "org.apache.hadoop.streaming.PipeMapper");
    conf.set(MRJobConfig.USER_NAME, "bob");
    assertPass(conf);
  }

  @Test
  public void testTurnOff() {
    JobConf conf = new JobConf();
    conf.setBoolean(MRConfig.MAPREDUCE_TASK_SECURITY_ENABLED, false);
    conf.setStrings(MRConfig.SECURITY_DENIED_TASKS, "org.apache.hadoop.streaming");
    conf.setStrings(MRConfig.SECURITY_ALLOWED_USERS, "alice");
    conf.set(MRJobConfig.MAP_CLASS_ATTR, "org.apache.hadoop.streaming.PipeMapper");
    conf.set(MRJobConfig.USER_NAME, "bob");
    assertPass(conf);
  }

  @Test
  public void testJobConfigCanNotOverwriteMapreduceConfig() {
    JobConf mapreduceConf = new JobConf();
    mapreduceConf.setBoolean(MRConfig.MAPREDUCE_TASK_SECURITY_ENABLED, true);
    mapreduceConf.setStrings(MRConfig.SECURITY_DENIED_TASKS, "org.apache.hadoop.streaming");
    mapreduceConf.setStrings(MRConfig.SECURITY_ALLOWED_USERS, "alice");

    JobConf jobConf = new JobConf();
    jobConf.setStrings(MRConfig.SECURITY_ALLOWED_USERS, "bob");
    jobConf.set(MRJobConfig.MAP_CLASS_ATTR, "org.apache.hadoop.streaming.PipeMapper");
    jobConf.set(MRJobConfig.USER_NAME, "bob");

    mapreduceConf.addResource(jobConf);
    assertDenied(mapreduceConf);
  }

  private void assertPass(JobConf conf) {
    assertThatCode(() ->
        TaskLevelSecurityEnforcer.validate(conf, UserGroupInformation.getCurrentUser()))
        .doesNotThrowAnyException();
  }

  private void assertDenied(JobConf conf) {
    assertThatThrownBy(() ->
        TaskLevelSecurityEnforcer.validate(conf, UserGroupInformation.getCurrentUser()))
        .isInstanceOf(TaskLevelSecurityException.class);
  }
}
