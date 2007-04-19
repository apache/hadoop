package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * This is used to track task completion events on 
 * job tracker. 
 *
 */
public class TaskCompletionEvent implements Writable{
  static public enum Status {FAILED, SUCCEEDED, OBSOLETE};
    
  private int eventId; 
  private String taskTrackerHttp;
  private String taskId;
  Status status; 
  boolean isMap = false;
  private int idWithinJob;
  public static final TaskCompletionEvent[] EMPTY_ARRAY = 
    new TaskCompletionEvent[0];
  /**
   * Default constructor for Writable.
   *
   */
  public TaskCompletionEvent(){}
  /**
   * Constructor. eventId should be created externally and incremented
   * per event for each job. 
   * @param eventId event id, event id should be unique and assigned in
   *  incrementally, starting from 0. 
   * @param taskId task id
   * @param status task's status 
   * @param taskTrackerHttp task tracker's host:port for http. 
   */
  public TaskCompletionEvent(int eventId, 
                             String taskId,
                             int idWithinJob,
                             boolean isMap,
                             Status status, 
                             String taskTrackerHttp){
      
    this.taskId = taskId;
    this.idWithinJob = idWithinJob;
    this.isMap = isMap;
    this.eventId = eventId; 
    this.status =status; 
    this.taskTrackerHttp = taskTrackerHttp;
  }
  /**
   * Returns event Id. 
   * @return event id
   */
  public int getEventId() {
    return eventId;
  }
  /**
   * Returns task id. 
   * @return task id
   */
  public String getTaskId() {
    return taskId;
  }
  /**
   * Returns enum Status.SUCESS or Status.FAILURE.
   * @return task tracker status
   */
  public Status getTaskStatus() {
    return status;
  }
  /**
   * http location of the tasktracker where this task ran. 
   * @return http location of tasktracker user logs
   */
  public String getTaskTrackerHttp() {
    return taskTrackerHttp;
  }
  /**
   * set event Id. should be assigned incrementally starting from 0. 
   * @param eventId
   */
  public void setEventId(
                         int eventId) {
    this.eventId = eventId;
  }
  /**
   * Sets task id. 
   * @param taskId
   */
  public void setTaskId(
                        String taskId) {
    this.taskId = taskId;
  }
  /**
   * Set task status. 
   * @param status
   */
  public void setTaskStatus(
                            Status status) {
    this.status = status;
  }
  /**
   * Set task tracker http location. 
   * @param taskTrackerHttp
   */
  public void setTaskTrackerHttp(
                                 String taskTrackerHttp) {
    this.taskTrackerHttp = taskTrackerHttp;
  }
    
  public String toString(){
    StringBuffer buf = new StringBuffer(); 
    buf.append("Task Id : "); 
    buf.append(taskId); 
    buf.append(", Status : ");  
    buf.append(status.name());
    return buf.toString();
  }
    
  public boolean isMapTask() {
    return isMap;
  }
    
  public int idWithinJob() {
    return idWithinJob;
  }
  //////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeString(out, taskId); 
    WritableUtils.writeVInt(out, idWithinJob);
    out.writeBoolean(isMap);
    WritableUtils.writeEnum(out, status); 
    WritableUtils.writeString(out, taskTrackerHttp);
  }
  
  public void readFields(DataInput in) throws IOException {
    this.taskId = WritableUtils.readString(in); 
    this.idWithinJob = WritableUtils.readVInt(in);
    this.isMap = in.readBoolean();
    this.status = WritableUtils.readEnum(in, Status.class);
    this.taskTrackerHttp = WritableUtils.readString(in);
  }
}
