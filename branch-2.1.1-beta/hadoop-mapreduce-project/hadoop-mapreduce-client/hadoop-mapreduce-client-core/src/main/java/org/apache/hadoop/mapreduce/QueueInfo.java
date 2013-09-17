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
package org.apache.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.StringInterner;

/**
 * Class that contains the information regarding the Job Queues which are 
 * maintained by the Hadoop Map/Reduce framework.
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class QueueInfo implements Writable {

  private String queueName = "";
  
  //The scheduling Information object is read back as String.
  //Once the scheduling information is set there is no way to recover it.
  private String schedulingInfo; 
  
  private QueueState queueState;
  
  // Jobs submitted to the queue
  private JobStatus[] stats;
  
  private List<QueueInfo> children;

  private Properties props;

  /**
   * Default constructor for QueueInfo.
   * 
   */
  public QueueInfo() {
    // make it running by default.
    this.queueState = QueueState.RUNNING;
    children = new ArrayList<QueueInfo>();
    props = new Properties();
  }
  
  /**
   * Construct a new QueueInfo object using the queue name and the
   * scheduling information passed.
   * 
   * @param queueName Name of the job queue
   * @param schedulingInfo Scheduling Information associated with the job
   * queue
   */
  public QueueInfo(String queueName, String schedulingInfo) {
    this();
    this.queueName = queueName;
    this.schedulingInfo = schedulingInfo;
  }
  
  /**
   * 
   * @param queueName
   * @param schedulingInfo
   * @param state
   * @param stats
   */
  public QueueInfo(String queueName, String schedulingInfo, QueueState state,
                   JobStatus[] stats) {
    this(queueName, schedulingInfo);
    this.queueState = state;
    this.stats = stats;
  }

  /**
   * Set the queue name of the JobQueueInfo
   * 
   * @param queueName Name of the job queue.
   */
  protected void setQueueName(String queueName) {
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
  protected void setSchedulingInfo(String schedulingInfo) {
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
  
  /**
   * Set the state of the queue
   * @param state state of the queue.
   */
  protected void setState(QueueState state) {
    queueState = state;
  }
  
  /**
   * Return the queue state
   * @return the queue state.
   */
  public QueueState getState() {
    return queueState;
  }
  
  protected void setJobStatuses(JobStatus[] stats) {
    this.stats = stats;
  }

  /** 
   * Get immediate children.
   * 
   * @return list of QueueInfo
   */
  public List<QueueInfo> getQueueChildren() {
    return children;
  }

  protected void setQueueChildren(List<QueueInfo> children) {
    this.children =  children; 
  }

  /**
   * Get properties.
   * 
   * @return Properties
   */
  public Properties getProperties() {
    return props;
  }

  protected void setProperties(Properties props) {
    this.props = props;
  }

  /**
   * Get the jobs submitted to queue
   * @return list of JobStatus for the submitted jobs
   */
  public JobStatus[] getJobStatuses() {
    return stats;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    queueName = StringInterner.weakIntern(Text.readString(in));
    queueState = WritableUtils.readEnum(in, QueueState.class);
    schedulingInfo = StringInterner.weakIntern(Text.readString(in));
    int length = in.readInt();
    stats = new JobStatus[length];
    for (int i = 0; i < length; i++) {
      stats[i] = new JobStatus();
      stats[i].readFields(in);
    }
    int count = in.readInt();
    children.clear();
    for (int i = 0; i < count; i++) {
      QueueInfo childQueueInfo = new QueueInfo();
      childQueueInfo.readFields(in);
      children.add(childQueueInfo);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, queueName);
    WritableUtils.writeEnum(out, queueState);
    
    if(schedulingInfo!= null) {
      Text.writeString(out, schedulingInfo);
    }else {
      Text.writeString(out, "N/A");
    }
    out.writeInt(stats.length);
    for (JobStatus stat : stats) {
      stat.write(out);
    }
    out.writeInt(children.size());
    for(QueueInfo childQueueInfo : children) {
      childQueueInfo.write(out);
    }
  }
}
