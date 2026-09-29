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

package org.apache.hadoop.mapreduce.v2.app.webapp.jsonprovider;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.mapreduce.v2.app.webapp.dao.AMAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.AMAttemptsInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.AppInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.ConfEntryInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.CounterGroupInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.CounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobTaskAttemptCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobTaskAttemptState;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobTaskCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobsInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.MapTaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.ReduceTaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptsInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskCounterGroupInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TasksInfo;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.webapp.RemoteExceptionData;

public class ClassSerializationConfig {

  private static final Set<Class<?>> CONST_WRAPPED_CLASSES = Sets.newHashSet(
      AMAttemptInfo.class, AMAttemptsInfo.class,
      AppInfo.class, CounterInfo.class, JobTaskAttemptCounterInfo.class,
      JobTaskCounterInfo.class, TaskCounterGroupInfo.class,
      org.apache.hadoop.mapreduce.v2.app.webapp.dao.ConfInfo.class,
      JobCounterInfo.class, TaskCounterInfo.class, CounterGroupInfo.class,
      JobInfo.class, JobsInfo.class, MapTaskAttemptInfo.class, ReduceTaskAttemptInfo.class,
      TaskInfo.class, TasksInfo.class, TaskAttemptInfo.class, TaskAttemptsInfo.class,
      ConfEntryInfo.class, RemoteExceptionData.class
  );

  private static final Set<Class<?>> CONST_UNWRAPPED_CLASSES =
      Sets.newHashSet(JobTaskAttemptState.class);

  private final Set<Class<?>> wrappedClasses;
  private final Set<Class<?>> unWrappedClasses;


  public ClassSerializationConfig() {
    wrappedClasses = new HashSet<>(CONST_WRAPPED_CLASSES);
    unWrappedClasses = new HashSet<>(CONST_UNWRAPPED_CLASSES);
  }

  public Set<Class<?>> getWrappedClasses() {
    return wrappedClasses;
  }

  public Set<Class<?>> getUnWrappedClasses() {
    return unWrappedClasses;
  }

}
