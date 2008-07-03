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
import java.text.NumberFormat;

/**
 * TaskID represents the immutable and unique identifier for 
 * a Map or Reduce Task. Each TaskID encompasses multiple attempts made to
 * execute the Map or Reduce Task, each of which are uniquely indentified by
 * their TaskAttemptID.
 * 
 * TaskID consists of 3 parts. First part is the {@link JobID}, that this 
 * TaskInProgress belongs to. Second part of the TaskID is either 'm' or 'r' 
 * representing whether the task is a map task or a reduce task. 
 * And the third part is the task number. <br> 
 * An example TaskID is : 
 * <code>task_200707121733_0003_m_000005</code> , which represents the
 * fifth map task in the third job running at the jobtracker 
 * started at <code>200707121733</code>. 
 * <p>
 * Applications should never construct or parse TaskID strings
 * , but rather use appropriate constructors or {@link #forName(String)} 
 * method. 
 * 
 * @see JobID
 * @see TaskAttemptID
 */
public class TaskID extends ID {
  private static final String TASK = "task";
  private static char UNDERSCORE = '_';  
  private static NumberFormat idFormat = NumberFormat.getInstance();
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(6);
  }
  
  private JobID jobId;
  private boolean isMap;

  /**
   * Constructs a TaskID object from given {@link JobID}.  
   * @param jobId JobID that this tip belongs to 
   * @param isMap whether the tip is a map 
   * @param id the tip number
   */
  public TaskID(JobID jobId, boolean isMap, int id) {
    super(id);
    if(jobId == null) {
      throw new IllegalArgumentException("jobId cannot be null");
    }
    this.jobId = jobId;
    this.isMap = isMap;
  }
  
  /**
   * Constructs a TaskInProgressId object from given parts.
   * @param jtIdentifier jobTracker identifier
   * @param jobId job number 
   * @param isMap whether the tip is a map 
   * @param id the tip number
   */
  public TaskID(String jtIdentifier, int jobId, boolean isMap, int id) {
    this(new JobID(jtIdentifier, jobId), isMap, id);
  }
  
  private TaskID() { }
  
  /** Returns the {@link JobID} object that this tip belongs to */
  public JobID getJobID() {
    return jobId;
  }
  
  /**Returns whether this TaskID is a map ID */
  public boolean isMap() {
    return isMap;
  }
  
  @Override
  public boolean equals(Object o) {
    if(o == null)
      return false;
    if(o.getClass().equals(TaskID.class)) {
      TaskID that = (TaskID)o;
      return this.id==that.id
        && this.isMap == that.isMap
        && this.jobId.equals(that.jobId);
    }
    else return false;
  }

  /**Compare TaskInProgressIds by first jobIds, then by tip numbers. Reduces are 
   * defined as greater then maps.*/
  @Override
  public int compareTo(ID o) {
    TaskID that = (TaskID)o;
    int jobComp = this.jobId.compareTo(that.jobId);
    if(jobComp == 0) {
      if(this.isMap == that.isMap) {
        return this.id - that.id;
      }
      else return this.isMap ? -1 : 1;
    }
    else return jobComp;
  }
  
  @Override
  public String toString() { 
    StringBuilder builder = new StringBuilder();
    return builder.append(TASK).append(UNDERSCORE)
      .append(toStringWOPrefix()).toString();
  }

  StringBuilder toStringWOPrefix() {
    StringBuilder builder = new StringBuilder();
    builder.append(jobId.toStringWOPrefix())
      .append(isMap ? "_m_" : "_r_");
    return builder.append(idFormat.format(id));
  }
  
  @Override
  public int hashCode() {
    return toStringWOPrefix().toString().hashCode();
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.jobId = JobID.read(in);
    this.isMap = in.readBoolean();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    jobId.write(out);
    out.writeBoolean(isMap);
  }
  
  public static TaskID read(DataInput in) throws IOException {
    TaskID tipId = new TaskID();
    tipId.readFields(in);
    return tipId;
  }
  
  /** Construct a TaskID object from given string 
   * @return constructed TaskID object or null if the given String is null
   * @throws IllegalArgumentException if the given string is malformed
   */
  public static TaskID forName(String str) 
    throws IllegalArgumentException {
    if(str == null)
      return null;
    try {
      String[] parts = str.split("_");
      if(parts.length == 5) {
        if(parts[0].equals(TASK)) {
          boolean isMap = false;
          if(parts[3].equals("m")) isMap = true;
          else if(parts[3].equals("r")) isMap = false;
          else throw new Exception();
          return new TaskID(parts[1], Integer.parseInt(parts[2]),
              isMap, Integer.parseInt(parts[4]));
        }
      }
    }catch (Exception ex) {//fall below
    }
    throw new IllegalArgumentException("TaskId string : " + str 
        + " is not properly formed");
  }
  
  /** 
   * Returns a regex pattern which matches task IDs. Arguments can 
   * be given null, in which case that part of the regex will be generic.  
   * For example to obtain a regex matching <i>the first map task</i> 
   * of <i>any jobtracker</i>, of <i>any job</i>, we would use :
   * <pre> 
   * TaskID.getTaskIDsPattern(null, null, true, 1);
   * </pre>
   * which will return :
   * <pre> "task_[^_]*_[0-9]*_m_000001*" </pre> 
   * @param jtIdentifier jobTracker identifier, or null
   * @param jobId job number, or null
   * @param isMap whether the tip is a map, or null 
   * @param taskId taskId number, or null
   * @return a regex pattern matching TaskIDs
   */
  public static String getTaskIDsPattern(String jtIdentifier, Integer jobId
      , Boolean isMap, Integer taskId) {
    StringBuilder builder = new StringBuilder(TASK).append(UNDERSCORE)
      .append(getTaskIDsPatternWOPrefix(jtIdentifier, jobId, isMap, taskId));
    return builder.toString();
  }
  
  static StringBuilder getTaskIDsPatternWOPrefix(String jtIdentifier
      , Integer jobId, Boolean isMap, Integer taskId) {
    StringBuilder builder = new StringBuilder();
    builder.append(JobID.getJobIDsPatternWOPrefix(jtIdentifier, jobId))
      .append(UNDERSCORE)
      .append(isMap != null ? (isMap ? "m" : "r") : "(m|r)").append(UNDERSCORE)
      .append(taskId != null ? idFormat.format(taskId) : "[0-9]*");
    return builder;
  }
  
}
