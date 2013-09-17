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

package org.apache.hadoop.mapreduce.v2.api.protocolrecords;

import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;

public interface GetTaskAttemptCompletionEventsResponse {
  public abstract List<TaskAttemptCompletionEvent> getCompletionEventList();
  public abstract TaskAttemptCompletionEvent getCompletionEvent(int index);
  public abstract int getCompletionEventCount();
  
  public abstract void addAllCompletionEvents(List<TaskAttemptCompletionEvent> eventList);
  public abstract void addCompletionEvent(TaskAttemptCompletionEvent event);
  public abstract void removeCompletionEvent(int index);
  public abstract void clearCompletionEvents();  
}
