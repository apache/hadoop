/*
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
package org.apache.hadoop.chukwa.inputtools;

import java.io.File;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.chukwa.datacollection.controller.ChukwaAgentController;
import org.apache.hadoop.mapred.*;

/**
 * An instrumentation plugin for Hadoop, to trigger Chukwa-based task logfile collection.
 * 
 * WARNING:  This code depends on hadoop features that have not yet been committed.
 *   To allow it to compile, the key lines have been commented out, and marked with
 *   'PENDING'.
 *
 */
public class ChukwaTTInstru 
extends TaskTrackerMetricsInst  //PENDING on getting new metrics code into Hadoop
{

  private Map<TaskAttemptID, Long> stdOutAdaptors;
  private Map<TaskAttemptID, Long> stdErrAdaptors;
  private ChukwaAgentController chukwa;
  
  public ChukwaTTInstru(TaskTracker t) {
    super(t);  //PENDING
    stdOutAdaptors = new HashMap<TaskAttemptID, Long>();
    stdErrAdaptors = new HashMap<TaskAttemptID, Long>();
    chukwa = new ChukwaAgentController();
  }
  
  public void reportTaskLaunch(TaskAttemptID taskid, File stdout, File stderr)  {
    long stdoutID = chukwa.addFile("unknown-userdata", stdout.getAbsolutePath());
    long stderrID = chukwa.addFile("unknown-userdata", stderr.getAbsolutePath());
    stdOutAdaptors.put(taskid, stdoutID);
    stdErrAdaptors.put(taskid, stderrID);
  }
  
  public void reportTaskEnd(TaskAttemptID taskid) {
    try {
      Long id = stdOutAdaptors.remove(taskid);
      if(id != null)
        chukwa.remove(id);
      
      id = stdErrAdaptors.remove(taskid);
      if(id != null)
        chukwa.remove(id);
    } catch(java.io.IOException e) {
      //failed to talk to chukwa.  Not much to be done.
    }
  }
}
