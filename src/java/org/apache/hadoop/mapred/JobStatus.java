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

import org.apache.hadoop.io.*;

import java.io.*;

/**************************************************
 * Describes the current status of a job.  This is
 * not intended to be a comprehensive piece of data.
 * For that, look at JobProfile.
 *
 * @author Mike Cafarella
 **************************************************/
public class JobStatus implements Writable {

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

    private String jobid;
    private float mapProgress;
    private float reduceProgress;
    private int runState;
    private long startTime;
    private String user;
    /**
     */
    public JobStatus() {
    }

    /**
     * Create a job status object for a given jobid.
     * @param jobid The jobid of the job
     * @param mapProgress The progress made on the maps
     * @param reduceProgress The progress made on the reduces
     * @param runState The current state of the job
     */
    public JobStatus(String jobid, float mapProgress, float reduceProgress, int runState) {
        this.jobid = jobid;
        this.mapProgress = mapProgress;
        this.reduceProgress = reduceProgress;
        this.runState = runState;
        this.user = "nobody";
    }

    /**
     * @return The jobid of the Job
     */
    public String getJobId() { return jobid; }
    
    /**
     * @return Percentage of progress in maps 
     */
    public float mapProgress() { return mapProgress; }
    
    /**
     * Sets the map progress of this job
     * @param p The value of map progress to set to
     */
    void setMapProgress(float p) { 
      this.mapProgress = (float) Math.min(1.0, Math.max(0.0, p)); 
    
    }
    
    /**
     * @return Percentage of progress in reduce 
     */
    public float reduceProgress() { return reduceProgress; }
    
    /**
     * Sets the reduce progress of this Job
     * @param p The value of reduce progress to set to
     */
    void setReduceProgress(float p) { 
      this.reduceProgress = (float) Math.min(1.0, Math.max(0.0, p)); 
    }
    
    /**
     * @return running state of the job
     */
    public int getRunState() { return runState; }
    
    /**
     * Change the current run state of the job.
     */
    public void setRunState(int state) {
      this.runState = state;
    }
    
    /** 
     * Set the start time of the job
     * @param startTime The startTime of the job
     */
    void setStartTime(long startTime) { this.startTime = startTime;};
    
    /**
     * @return start time of the job
     */
    public long getStartTime() { return startTime;};

    /**
      * @param user The username of the job
      */
    void setUsername(String userName) { this.user = userName;};

    /**
      * @return the username of the job
      */
    public String getUsername() { return this.user;};
    ///////////////////////////////////////
    // Writable
    ///////////////////////////////////////
    public void write(DataOutput out) throws IOException {
        UTF8.writeString(out, jobid);
        out.writeFloat(mapProgress);
        out.writeFloat(reduceProgress);
        out.writeInt(runState);
        out.writeLong(startTime);
        UTF8.writeString(out, user);
    }
    public void readFields(DataInput in) throws IOException {
        this.jobid = UTF8.readString(in);
        this.mapProgress = in.readFloat();
        this.reduceProgress = in.readFloat();
        this.runState = in.readInt();
        this.startTime = in.readLong();
        this.user = UTF8.readString(in);
    }
}
