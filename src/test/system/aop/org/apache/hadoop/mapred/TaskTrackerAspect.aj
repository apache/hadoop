package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.test.system.TTProtocol;
import org.apache.hadoop.mapreduce.test.system.TTTaskInfo;
import org.apache.hadoop.mapred.TTTaskInfoImpl.MapTTTaskInfo;
import org.apache.hadoop.mapred.TTTaskInfoImpl.ReduceTTTaskInfo;
import org.apache.hadoop.test.system.ControlAction;
import org.apache.hadoop.test.system.DaemonProtocol;
import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;

public privileged aspect TaskTrackerAspect {

  declare parents : TaskTracker implements TTProtocol;

  // Add a last sent status field to the Tasktracker class.
  TaskTrackerStatus TaskTracker.lastSentStatus = null;

  public synchronized TaskTrackerStatus TaskTracker.getStatus()
      throws IOException {
    return lastSentStatus;
  }

  public Configuration TaskTracker.getDaemonConf() throws IOException {
    return fConf;
  }

  public TTTaskInfo[] TaskTracker.getTasks() throws IOException {
    List<TTTaskInfo> infoList = new ArrayList<TTTaskInfo>();
    for (TaskInProgress tip : tasks.values()) {
      TTTaskInfo info = null;
      if (tip.task.isMapTask()) {
        info = new MapTTTaskInfo(((MapTask) tip.task), tip.slotTaken,
            tip.wasKilled, tip.diagnosticInfo.toString());
      } else {
        info = new ReduceTTTaskInfo(((ReduceTask) tip.task), tip.slotTaken,
            tip.wasKilled, tip.diagnosticInfo.toString());
      }
      infoList.add(info);
    }
    return (TTTaskInfo[]) infoList.toArray(new TTTaskInfo[infoList.size()]);
  }

  before(TaskTrackerStatus newStatus, TaskTracker tracker) : 
    set(TaskTrackerStatus TaskTracker.status) 
    && args(newStatus) && this(tracker) {
    if (newStatus == null) {
      tracker.lastSentStatus = tracker.status;
    }
  }

  pointcut ttConstructorPointCut(JobConf conf) : 
    call(TaskTracker.new(JobConf)) 
    && args(conf);

  after(JobConf conf) returning (TaskTracker tracker): 
    ttConstructorPointCut(conf) {
    tracker.setReady(true);
  }
  
  pointcut getVersionAspect(String protocol, long clientVersion) : 
    execution(public long TaskTracker.getProtocolVersion(String , 
      long) throws IOException) && args(protocol, clientVersion);

  long around(String protocol, long clientVersion) :  
    getVersionAspect(protocol, clientVersion) {
    if(protocol.equals(DaemonProtocol.class.getName())) {
      return DaemonProtocol.versionID;
    } else if(protocol.equals(TTProtocol.class.getName())) {
      return TTProtocol.versionID;
    } else {
      return proceed(protocol, clientVersion);
    }
  }

}
