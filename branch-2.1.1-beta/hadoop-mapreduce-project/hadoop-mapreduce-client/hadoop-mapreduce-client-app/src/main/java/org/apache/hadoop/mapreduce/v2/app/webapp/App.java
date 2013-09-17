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

package org.apache.hadoop.mapreduce.v2.app.webapp;

import com.google.inject.Inject;
import com.google.inject.servlet.RequestScoped;

import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;

@RequestScoped
public class App {
  final AppContext context;
  private Job job;
  private Task task;

  @Inject
  App(AppContext ctx) {
    context = ctx;
  }

  void setJob(Job job) {
    this.job = job;
  }

  public Job getJob() {
    return job;
  }

  void setTask(Task task) {
    this.task = task;
  }

  public Task getTask() {
    return task;
  }
}
