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
 * Describes the current status of a job.  This is
 * not intended to be a comprehensive piece of data.
 * For that, look at JobProfile.
 *
 * @author Mike Cafarella
 **************************************************/
class JobStatus implements Writable {

    static {                                      // register a ctor
      WritableFactories.setFactory
        (JobStatus.class,
         new WritableFactory() {
           public Writable newInstance() { return new JobStatus(); }
         });
    }

    public static final int RUNNING = 1;
    public static final int SUCCEEDED = 2;
    public static final int FAILED = 3;
    public static final int PREP = 4;

    String jobid;
    float mapProgress;
    float reduceProgress;
    int runState;

    /**
     */
    public JobStatus() {
    }

    /**
     */
    public JobStatus(String jobid, float mapProgress, float reduceProgress, int runState) {
        this.jobid = jobid;
        this.mapProgress = mapProgress;
        this.reduceProgress = reduceProgress;
        this.runState = runState;
    }

    /**
     */
    public String getJobId() { return jobid; }
    public float mapProgress() { return mapProgress; }
    public void setMapProgress(float p) { this.mapProgress = p; }
    public float reduceProgress() { return reduceProgress; }
    public void setReduceProgress(float p) { this.reduceProgress = p; }
    public int getRunState() { return runState; }

    ///////////////////////////////////////
    // Writable
    ///////////////////////////////////////
    public void write(DataOutput out) throws IOException {
        UTF8.writeString(out, jobid);
        out.writeFloat(mapProgress);
        out.writeFloat(reduceProgress);
        out.writeInt(runState);
    }
    public void readFields(DataInput in) throws IOException {
        this.jobid = UTF8.readString(in);
        this.mapProgress = in.readFloat();
        this.reduceProgress = in.readFloat();
        this.runState = in.readInt();
    }
}
