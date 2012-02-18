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

package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.test.system.JobInfo;

/**
 * Concrete implementation of the JobInfo interface which is exposed to the
 * clients.
 * Look at {@link JobInfo} for further details.
 */
class JobInfoImpl implements JobInfo {

  private List<String> blackListedTracker;
  private String historyUrl;
  private JobID id;
  private boolean setupLaunched;
  private boolean setupFinished;
  private boolean cleanupLaunched;
  private JobStatus status;
  private int runningMaps;
  private int runningReduces;
  private int waitingMaps;
  private int waitingReduces;
  private int finishedMaps;
  private int finishedReduces;
  private int numMaps;
  private int numReduces;
  private boolean historyCopied;

  public JobInfoImpl() {
    id = new JobID();
    status = new JobStatus();
    blackListedTracker = new LinkedList<String>();
    historyUrl = "";
  }
  
  public JobInfoImpl(
      JobID id, boolean setupLaunched, boolean setupFinished,
      boolean cleanupLaunched, int runningMaps, int runningReduces,
      int waitingMaps, int waitingReduces, int finishedMaps,
      int finishedReduces, JobStatus status, String historyUrl,
      List<String> blackListedTracker, boolean isComplete, int numMaps,
      int numReduces, boolean historyCopied) {
    super();
    this.blackListedTracker = blackListedTracker;
    this.historyUrl = historyUrl;
    this.id = id;
    this.setupLaunched = setupLaunched;
    this.setupFinished = setupFinished;
    this.cleanupLaunched = cleanupLaunched;
    this.status = status;
    this.runningMaps = runningMaps;
    this.runningReduces = runningReduces;
    this.waitingMaps = waitingMaps;
    this.waitingReduces = waitingReduces;
    this.finishedMaps = finishedMaps;
    this.finishedReduces = finishedReduces;
    this.numMaps = numMaps;
    this.numReduces = numReduces;
    this.historyCopied = historyCopied;
  }

  @Override
  public List<String> getBlackListedTrackers() {
    return blackListedTracker;
  }

  @Override
  public String getHistoryUrl() {
    return historyUrl;
  }

  @Override
  public JobID getID() {
    return id;
  }

  @Override
  public JobStatus getStatus() {
    return status;
  }

  @Override
  public boolean isCleanupLaunched() {
    return cleanupLaunched;
  }

  @Override
  public boolean isSetupLaunched() {
    return setupLaunched;
  }

  @Override
  public boolean isSetupFinished() {
    return setupFinished;
  }

  @Override
  public int runningMaps() {
    return runningMaps;
  }

  @Override
  public int runningReduces() {
    return runningReduces;
  }

  @Override
  public int waitingMaps() {
    return waitingMaps;
  }

  @Override
  public int waitingReduces() {
    return waitingReduces;
  }
 
  @Override
  public int finishedMaps() {
    return finishedMaps;
  }

  @Override
  public int finishedReduces() {
    return finishedReduces;
  }
  
  @Override
  public int numMaps() {
    return numMaps;
  }
  
  @Override
  public int numReduces() {
    return numReduces;
  }
  
  @Override
  public boolean isHistoryFileCopied() {
    return historyCopied;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    id.readFields(in);
    setupLaunched = in.readBoolean();
    setupFinished = in.readBoolean();
    cleanupLaunched = in.readBoolean();
    status.readFields(in);
    runningMaps = in.readInt();
    runningReduces = in.readInt();
    waitingMaps = in.readInt();
    waitingReduces = in.readInt();
    historyUrl = in.readUTF();
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      blackListedTracker.add(in.readUTF());
    }
    finishedMaps = in.readInt();
    finishedReduces = in.readInt();
    numMaps = in.readInt();
    numReduces = in.readInt();
    historyCopied = in.readBoolean();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    id.write(out);
    out.writeBoolean(setupLaunched);
    out.writeBoolean(setupFinished);
    out.writeBoolean(cleanupLaunched);
    status.write(out);
    out.writeInt(runningMaps);
    out.writeInt(runningReduces);
    out.writeInt(waitingMaps);
    out.writeInt(waitingReduces);
    out.writeUTF(historyUrl);
    out.writeInt(blackListedTracker.size());
    for (String str : blackListedTracker) {
      out.writeUTF(str);
    }
    out.writeInt(finishedMaps);
    out.writeInt(finishedReduces);
    out.writeInt(numMaps);
    out.writeInt(numReduces);
    out.writeBoolean(historyCopied);
  }


}
