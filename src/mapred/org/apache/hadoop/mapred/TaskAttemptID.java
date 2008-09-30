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

import org.apache.hadoop.io.WritableUtils;

/**
 * TaskAttemptID represents the immutable and unique identifier for 
 * a task attempt. Each task attempt is one particular instance of a Map or
 * Reduce Task identified by its TaskID. 
 * 
 * TaskAttemptID consists of 3 parts. First part is the 
 * {@link TaskID}, that this TaskAttemptID belongs to.
 * Second part is the task attempt number. Third part is the unique identifier
 * for distinguishing tasks-attempts across jobtracker restarts.<br> 
 * An example TaskAttemptID is : 
 * <code>attempt_200707121733_0003_m_000005_0_1234567890123</code> , which 
 * represents the zeroth task attempt for the fifth map task in the third job 
 * running at the jobtracker started at <code>200707121733</code> with 
 * timestamp <code>1234567890123</code>. There could be another attempt with id
 * <code>attempt_200707121733_0003_m_000005_0_1234567890124</code> which 
 * indicates that the task was scheduled by the jobtracker started at timestamp
 * <code>1234567890124</code>. <code>200707121733</code> here indicates that 
 * the job was started by the jobtracker that was started at 
 * <code>200707121733</code>, although this task-attempt was scheduled by the 
 * new jobtracker. 
 * <p>
 * Applications should never construct or parse TaskAttemptID strings
 * , but rather use appropriate constructors or {@link #forName(String)} 
 * method. 
 * 
 * @see JobID
 * @see TaskID
 */
public class TaskAttemptID extends ID {
  private static final String ATTEMPT = "attempt";
  private TaskID taskId;
  private long jtTimestamp = 0;
  private static final char UNDERSCORE = '_';
  
  /**
   * @deprecated Use {@link #TaskAttemptID(TaskID, int, long)} instead.
   */
  public TaskAttemptID(TaskID taskId, int id) {
    this(taskId, id, 0);
  }
  
  /**
   * Constructs a TaskAttemptID object from given {@link TaskID}.  
   * @param taskId TaskID that this task belongs to  
   * @param id the task attempt number
   * @param jtTimestamp timestamp that uniquely identifies the task 
   *        attempt across restarts
   */
  public TaskAttemptID(TaskID taskId, int id, long jtTimestamp) {
    super(id);
    if(taskId == null) {
      throw new IllegalArgumentException("taskId cannot be null");
    }
    this.taskId = taskId;
    this.jtTimestamp = jtTimestamp;
  }
  
  /**
   * @deprecated 
   *   Use {@link #TaskAttemptID(String, int, boolean, int, int, long)} instead
   */
  public TaskAttemptID(String jtIdentifier, int jobId, boolean isMap, 
                       int taskId, int id) {
    this(new TaskID(jtIdentifier, jobId, isMap, taskId), id, 0);
  }
  
  /**
   * Constructs a TaskId object from given parts.
   * @param jtIdentifier jobTracker identifier
   * @param jobId job number 
   * @param isMap whether the tip is a map 
   * @param taskId taskId number
   * @param id the task attempt number
   * @param jtTimestamp timestamp that uniquely identifies the task attempt 
   *        across restarts
   */
  public TaskAttemptID(String jtIdentifier, int jobId, boolean isMap, 
                       int taskId, int id, long jtTimestamp) {
    this(new TaskID(jtIdentifier, jobId, isMap, taskId), id, 
                    jtTimestamp);
  }
  
  private TaskAttemptID() { }
  
  /** Returns the {@link JobID} object that this task attempt belongs to */
  public JobID getJobID() {
    return taskId.getJobID();
  }
  
  /** Returns the {@link TaskID} object that this task attempt belongs to */
  public TaskID getTaskID() {
    return taskId;
  }
  
  /**Returns whether this TaskAttemptID is a map ID */
  public boolean isMap() {
    return taskId.isMap();
  }
  
  @Override
  public boolean equals(Object o) {
    if (!super.equals(o))
      return false;

    TaskAttemptID that = (TaskAttemptID)o;
    return this.taskId.equals(that.taskId) && 
           this.jtTimestamp == that.jtTimestamp;
  }
  
  /**Compare TaskIds by first tipIds, then by task numbers. */
  @Override
  public int compareTo(ID o) {
    TaskAttemptID that = (TaskAttemptID)o;
    int tipComp = this.taskId.compareTo(that.taskId);
    if(tipComp == 0) {
      tipComp = this.id - that.id;
    }
    if (tipComp == 0) {
      tipComp = Long.valueOf(this.jtTimestamp).compareTo(that.jtTimestamp);
    }
    return tipComp;
  }
  @Override
  public String toString() { 
    StringBuilder builder = new StringBuilder();
    return builder.append(ATTEMPT).append(UNDERSCORE)
      .append(toStringWOPrefix()).toString();
  }

  StringBuilder toStringWOPrefix() {
    // This is just for backward compability.
    String appendForTimestamp = (jtTimestamp == 0) 
                                ? "" 
                                : UNDERSCORE + String.valueOf(jtTimestamp);
    StringBuilder builder = new StringBuilder();
    return builder.append(taskId.toStringWOPrefix())
                  .append(UNDERSCORE).append(id).append(appendForTimestamp);
  }
  
  @Override
  public int hashCode() {
    return toStringWOPrefix().toString().hashCode();
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.taskId = TaskID.read(in);
    this.jtTimestamp = WritableUtils.readVLong(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    taskId.write(out);
    WritableUtils.writeVLong(out, jtTimestamp);
  }
  
  public static TaskAttemptID read(DataInput in) throws IOException {
    TaskAttemptID taskId = new TaskAttemptID();
    taskId.readFields(in);
    return taskId;
  }
  
  /** Construct a TaskAttemptID object from given string 
   * @return constructed TaskAttemptID object or null if the given String is null
   * @throws IllegalArgumentException if the given string is malformed
   */
  public static TaskAttemptID forName(String str) throws IllegalArgumentException {
    if(str == null)
      return null;
    try {
      String[] parts = str.split("_");
      long jtTimestamp = 0;
      // This is for backward compability
      if(parts.length == 6 || parts.length == 7) {
        if(parts[0].equals(ATTEMPT)) {
          boolean isMap = false;
          if(parts[3].equals("m")) isMap = true;
          else if(parts[3].equals("r")) isMap = false;
          else throw new Exception();
          if (parts.length == 7) {
            jtTimestamp = Long.parseLong(parts[6]);
          }
          return new TaskAttemptID(parts[1], Integer.parseInt(parts[2]),
                                   isMap, Integer.parseInt(parts[4]), 
                                   Integer.parseInt(parts[5]), jtTimestamp);
        }
      }
    }catch (Exception ex) {//fall below
    }
    throw new IllegalArgumentException("TaskAttemptId string : " + str 
        + " is not properly formed");
  }
  
  /** 
   * @return a regex pattern matching TaskAttemptIDs
   * @deprecated Use {@link #getTaskAttemptIDsPattern(String, Integer, Boolean,
   *                                                  Integer, Integer, Long)} 
   *             instead.
   */
  public static String getTaskAttemptIDsPattern(String jtIdentifier,
      Integer jobId, Boolean isMap, Integer taskId, Integer attemptId) {
    StringBuilder builder = new StringBuilder(ATTEMPT).append(UNDERSCORE);
    builder.append(getTaskAttemptIDsPatternWOPrefix(jtIdentifier, jobId,
                   isMap, taskId, attemptId, null));
    return builder.toString();
  }
  
  /**
   * Returns a regex pattern which matches task attempt IDs. Arguments can 
   * be given null, in which case that part of the regex will be generic.  
   * For example to obtain a regex matching <i>all task attempt IDs</i> 
   * of <i>any jobtracker</i>, in <i>any job</i>, of the <i>first 
   * map task</i>, we would use :
   * <pre> 
   * TaskAttemptID.getTaskAttemptIDsPattern(null, null, true, 1, null);
   * </pre>
   * which will return :
   * <pre> "attempt_[^_]*_[0-9]*_m_000001_[0-9]*" </pre> 
   * @param jtIdentifier jobTracker identifier, or null
   * @param jobId job number, or null
   * @param isMap whether the tip is a map, or null 
   * @param taskId taskId number, or null
   * @param attemptId the task attempt number, or null
   * @param jtTimestamp Timestamp that is used to identify task attempts across
   *        jobtracker restarts. Make sure that timestamp has some valid value.
   */
  public static String getTaskAttemptIDsPattern(String jtIdentifier, 
      Integer jobId, Boolean isMap, Integer taskId, Integer attemptId, Long jtTimestamp) {
    StringBuilder builder = new StringBuilder(ATTEMPT).append(UNDERSCORE);
    builder.append(getTaskAttemptIDsPatternWOPrefix(jtIdentifier, jobId,
                   isMap, taskId, attemptId, jtTimestamp));
    return builder.toString();
  }
  
  /**
   * @deprecated 
   * Use {@link #getTaskAttemptIDsPatternWOPrefix(String, Integer, Boolean, 
   *                                              Integer, Integer, Long)} 
   * instead.
   */
  static StringBuilder getTaskAttemptIDsPatternWOPrefix(String jtIdentifier
      , Integer jobId, Boolean isMap, Integer taskId, Integer attemptId) {
    StringBuilder builder = new StringBuilder();
    builder.append(TaskID.getTaskIDsPatternWOPrefix(jtIdentifier
        , jobId, isMap, taskId))
        .append(UNDERSCORE)
        .append(attemptId != null ? attemptId : "[0-9]*");
    return builder;
  }
  
  static StringBuilder getTaskAttemptIDsPatternWOPrefix(String jtIdentifier, 
      Integer jobId, Boolean isMap, Integer taskId, Integer attemptId, 
      Long jtTimestamp) {
    StringBuilder builder = new StringBuilder();
    builder.append(TaskID.getTaskIDsPatternWOPrefix(jtIdentifier, jobId, isMap, taskId))
           .append(UNDERSCORE)
           .append(attemptId != null ? attemptId : "[0-9]*")
           .append(UNDERSCORE)
           .append(jtTimestamp != null ? jtTimestamp : "[0-9]*");
    return builder;
  }
}
