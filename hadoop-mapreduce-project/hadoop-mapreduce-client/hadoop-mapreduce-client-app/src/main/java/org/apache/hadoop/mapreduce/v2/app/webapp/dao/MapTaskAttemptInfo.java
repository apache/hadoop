/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.v2.app.webapp.dao;

import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "taskAttempt")
public class MapTaskAttemptInfo extends TaskAttemptInfo {

  public MapTaskAttemptInfo() {
  }

  public MapTaskAttemptInfo(TaskAttempt ta) {
    this(ta, false);
  }

  public MapTaskAttemptInfo(TaskAttempt ta, Boolean isRunning) {
    super(ta, TaskType.MAP, isRunning);
  }
}