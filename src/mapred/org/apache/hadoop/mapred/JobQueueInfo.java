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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Class that contains the information regarding the Job Queues which are 
 * maintained by the Hadoop Map/Reduce framework.
 * 
 */

public class JobQueueInfo implements Writable {

  private String queueName = "";
  //The scheduling Information object is read back as String.
  //Once the scheduling information is set there is no way to recover it.
  private String schedulingInfo; 
  
  
  /**
   * Default constructor for Job Queue Info.
   * 
   */
  public JobQueueInfo() {
    
  }
  /**
   * Construct a new JobQueueInfo object using the queue name and the
   * scheduling information passed.
   * 
   * @param queueName Name of the job queue
   * @param schedulingInfo Scheduling Information associated with the job
   * queue
   */
  public JobQueueInfo(String queueName, String schedulingInfo) {
    this.queueName = queueName;
    this.schedulingInfo = schedulingInfo;
  }
  
  
  /**
   * Set the queue name of the JobQueueInfo
   * 
   * @param queueName Name of the job queue.
   */
  public void setQueueName(String queueName) {
    this.queueName = queueName;
  }

  /**
   * Get the queue name from JobQueueInfo
   * 
   * @return queue name
   */
  public String getQueueName() {
    return queueName;
  }

  /**
   * Set the scheduling information associated to particular job queue
   * 
   * @param schedulingInfo
   */
  public void setSchedulingInfo(String schedulingInfo) {
    this.schedulingInfo = schedulingInfo;
  }

  /**
   * Gets the scheduling information associated to particular job queue.
   * If nothing is set would return <b>"N/A"</b>
   * 
   * @return Scheduling information associated to particular Job Queue
   */
  public String getSchedulingInfo() {
    if(schedulingInfo != null) {
      return schedulingInfo;
    }else {
      return "N/A";
    }
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    queueName = Text.readString(in);
    schedulingInfo = Text.readString(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, queueName);
    if(schedulingInfo!= null) {
      Text.writeString(out, schedulingInfo);
    }else {
      Text.writeString(out, "N/A");
    }
  }
}
