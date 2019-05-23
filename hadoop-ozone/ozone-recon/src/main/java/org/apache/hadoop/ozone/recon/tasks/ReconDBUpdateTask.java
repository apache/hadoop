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

package org.apache.hadoop.ozone.recon.tasks;

import java.util.Collection;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.OMMetadataManager;

/**
 * Abstract class used to denote a Recon task that needs to act on OM DB events.
 */
public abstract class ReconDBUpdateTask {

  private String taskName;

  protected ReconDBUpdateTask(String taskName) {
    this.taskName = taskName;
  }

  /**
   * Return task name.
   * @return task name
   */
  public String getTaskName() {
    return taskName;
  }

  /**
   * Return the list of tables that the task is listening on.
   * Empty list means the task is NOT listening on any tables.
   * @return Collection of Tables.
   */
  protected abstract Collection<String> getTaskTables();

  /**
   * Process a set of OM events on tables that the task is listening on.
   * @param events Set of events to be processed by the task.
   * @return Pair of task name -> task success.
   */
  abstract Pair<String, Boolean> process(OMUpdateEventBatch events);

  /**
   * Process a  on tables that the task is listening on.
   * @param omMetadataManager OM Metadata manager instance.
   * @return Pair of task name -> task success.
   */
  abstract Pair<String, Boolean> reprocess(OMMetadataManager omMetadataManager);

}
