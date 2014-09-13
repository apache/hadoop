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
package org.apache.hadoop.mapred.nativetask;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.mapred.TaskAttemptID;

@InterfaceAudience.Private
public class TaskContext {
  private final JobConf conf;
  private Class<?> iKClass;
  private Class<?> iVClass;
  private Class<?> oKClass;
  private Class<?> oVClass;
  private final TaskReporter reporter;
  private final TaskAttemptID taskAttemptID;

  public TaskContext(JobConf conf, Class<?> iKClass, Class<?> iVClass,
      Class<?> oKClass, Class<?> oVClass, TaskReporter reporter,
      TaskAttemptID id) {
    this.conf = conf;
    this.iKClass = iKClass;
    this.iVClass = iVClass;
    this.oKClass = oKClass;
    this.oVClass = oVClass;
    this.reporter = reporter;
    this.taskAttemptID = id;
  }

  public Class<?> getInputKeyClass() {
    return iKClass;
  }

  public void setInputKeyClass(Class<?> klass) {
    this.iKClass = klass;
  }

  public Class<?> getInputValueClass() {
    return iVClass;
  }

  public void setInputValueClass(Class<?> klass) {
    this.iVClass = klass;
  }

  public Class<?> getOutputKeyClass() {
    return this.oKClass;
  }

  public void setOutputKeyClass(Class<?> klass) {
    this.oKClass = klass;
  }

  public Class<?> getOutputValueClass() {
    return this.oVClass;
  }

  public void setOutputValueClass(Class<?> klass) {
    this.oVClass = klass;
  }

  public TaskReporter getTaskReporter() {
    return this.reporter;
  }

  public TaskAttemptID getTaskAttemptId() {
    return this.taskAttemptID;
  }

  public JobConf getConf() {
    return this.conf;
  }

  public TaskContext copyOf() {
    return new TaskContext(conf, iKClass, iVClass, oKClass, oVClass, reporter, taskAttemptID);
  }
}
