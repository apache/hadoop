/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.hadoop.io.*;

import java.io.*;

/**************************************************
 * Describes the current status of a task.  This is
 * not intended to be a comprehensive piece of data.
 *
 * @author Mike Cafarella
 **************************************************/
class TaskStatus implements Writable {
    public static final int RUNNING = 0;
    public static final int SUCCEEDED = 1;
    public static final int FAILED = 2;
    public static final int UNASSIGNED = 3;
    
    private String taskid;
    private boolean isMap;
    private float progress;
    private int runState;
    private String diagnosticInfo;
    private String stateString;

    public TaskStatus() {}

    public TaskStatus(String taskid, boolean isMap, float progress, int runState, String diagnosticInfo, String stateString) {
        this.taskid = taskid;
        this.isMap = isMap;
        this.progress = progress;
        this.runState = runState;
        this.diagnosticInfo = diagnosticInfo;
        this.stateString = stateString;
    }
    
    public String getTaskId() { return taskid; }
    public boolean getIsMap() { return isMap; }
    public float getProgress() { return progress; }
    public void setProgress(float progress) { this.progress = progress; } 
    public int getRunState() { return runState; }
    public void setRunState(int runState) { this.runState = runState; }
    public String getDiagnosticInfo() { return diagnosticInfo; }
    public void setDiagnosticInfo(String info) { this.diagnosticInfo = info; }
    public String getStateString() { return stateString; }
    public void setStateString(String stateString) { this.stateString = stateString; }

    //////////////////////////////////////////////
    // Writable
    //////////////////////////////////////////////
    public void write(DataOutput out) throws IOException {
        UTF8.writeString(out, taskid);
        out.writeBoolean(isMap);
        out.writeFloat(progress);
        out.writeInt(runState);
        UTF8.writeString(out, diagnosticInfo);
        UTF8.writeString(out, stateString);
    }

    public void readFields(DataInput in) throws IOException {
        this.taskid = UTF8.readString(in);
        this.isMap = in.readBoolean();
        this.progress = in.readFloat();
        this.runState = in.readInt();
        this.diagnosticInfo = UTF8.readString(in);
        this.stateString = UTF8.readString(in);
    }
}
