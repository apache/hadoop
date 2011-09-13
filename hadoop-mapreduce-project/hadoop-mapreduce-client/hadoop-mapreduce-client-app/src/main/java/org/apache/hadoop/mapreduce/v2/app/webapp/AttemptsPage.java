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

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_TYPE;
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.ATTEMPT_STATE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRApps.TaskAttemptStateUI;
import org.apache.hadoop.yarn.webapp.SubView;

import com.google.inject.Inject;

public class AttemptsPage extends TaskPage {
  static class FewAttemptsBlock extends TaskPage.AttemptsBlock {
    @Inject
    FewAttemptsBlock(App ctx) {
      super(ctx);
    }

    @Override
    protected boolean isValidRequest() {
      return true;
    }

    @Override
    protected Collection<TaskAttempt> getTaskAttempts() {
      List<TaskAttempt> fewTaskAttemps = new ArrayList<TaskAttempt>();
      String taskTypeStr = $(TASK_TYPE);
      TaskType taskType = MRApps.taskType(taskTypeStr);
      String attemptStateStr = $(ATTEMPT_STATE);
      TaskAttemptStateUI neededState = MRApps
          .taskAttemptState(attemptStateStr);
      for (Task task : super.app.getJob().getTasks(taskType).values()) {
        Map<TaskAttemptId, TaskAttempt> attempts = task.getAttempts();
        for (TaskAttempt attempt : attempts.values()) {
          if (neededState.correspondsTo(attempt.getState())) {
            fewTaskAttemps.add(attempt);
          }
        }
      }
      return fewTaskAttemps;
    }
  }

  @Override
  protected Class<? extends SubView> content() {
    return FewAttemptsBlock.class;
  }
}
