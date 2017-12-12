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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapred.SortedRanges.Range;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.checkpoint.TaskCheckpointID;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.TaskHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.RMHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.app.rm.preemption.AMPreemptionPolicy;
import org.apache.hadoop.mapreduce.v2.app.security.authorize.MRAMPolicyProvider;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class is responsible for talking to the task umblical.
 * It also converts all the old data structures
 * to yarn data structures.
 * 
 * This class HAS to be in this package to access package private 
 * methods/classes.
 */
public class TaskAttemptListenerImpl extends CompositeService 
    implements TaskUmbilicalProtocol, TaskAttemptListener {

  private static final JvmTask TASK_FOR_INVALID_JVM = new JvmTask(null, true);

  private static final Logger LOG =
      LoggerFactory.getLogger(TaskAttemptListenerImpl.class);

  private AppContext context;
  private Server server;
  protected TaskHeartbeatHandler taskHeartbeatHandler;
  private RMHeartbeatHandler rmHeartbeatHandler;
  private long commitWindowMs;
  private InetSocketAddress address;
  private ConcurrentMap<WrappedJvmID, org.apache.hadoop.mapred.Task>
    jvmIDToActiveAttemptMap
      = new ConcurrentHashMap<WrappedJvmID, org.apache.hadoop.mapred.Task>();

  private ConcurrentMap<TaskAttemptId,
      AtomicReference<TaskAttemptStatus>> attemptIdToStatus
        = new ConcurrentHashMap<>();

  private Set<WrappedJvmID> launchedJVMs = Collections
      .newSetFromMap(new ConcurrentHashMap<WrappedJvmID, Boolean>());

  private JobTokenSecretManager jobTokenSecretManager = null;
  private AMPreemptionPolicy preemptionPolicy;
  private byte[] encryptedSpillKey;

  public TaskAttemptListenerImpl(AppContext context,
      JobTokenSecretManager jobTokenSecretManager,
      RMHeartbeatHandler rmHeartbeatHandler,
      AMPreemptionPolicy preemptionPolicy) {
    this(context, jobTokenSecretManager, rmHeartbeatHandler,
            preemptionPolicy, null);
  }

  public TaskAttemptListenerImpl(AppContext context,
      JobTokenSecretManager jobTokenSecretManager,
      RMHeartbeatHandler rmHeartbeatHandler,
      AMPreemptionPolicy preemptionPolicy, byte[] secretShuffleKey) {
    super(TaskAttemptListenerImpl.class.getName());
    this.context = context;
    this.jobTokenSecretManager = jobTokenSecretManager;
    this.rmHeartbeatHandler = rmHeartbeatHandler;
    this.preemptionPolicy = preemptionPolicy;
    this.encryptedSpillKey = secretShuffleKey;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
   registerHeartbeatHandler(conf);
   commitWindowMs = conf.getLong(MRJobConfig.MR_AM_COMMIT_WINDOW_MS,
       MRJobConfig.DEFAULT_MR_AM_COMMIT_WINDOW_MS);
   super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    startRpcServer();
    super.serviceStart();
  }

  protected void registerHeartbeatHandler(Configuration conf) {
    taskHeartbeatHandler = new TaskHeartbeatHandler(context.getEventHandler(), 
        context.getClock(), conf.getInt(MRJobConfig.MR_AM_TASK_LISTENER_THREAD_COUNT, 
            MRJobConfig.DEFAULT_MR_AM_TASK_LISTENER_THREAD_COUNT));
    addService(taskHeartbeatHandler);
  }

  protected void startRpcServer() {
    Configuration conf = getConfig();
    try {
      server = new RPC.Builder(conf).setProtocol(TaskUmbilicalProtocol.class)
          .setInstance(this).setBindAddress("0.0.0.0")
          .setPortRangeConfig(MRJobConfig.MR_AM_JOB_CLIENT_PORT_RANGE)
          .setNumHandlers(
          conf.getInt(MRJobConfig.MR_AM_TASK_LISTENER_THREAD_COUNT, 
          MRJobConfig.DEFAULT_MR_AM_TASK_LISTENER_THREAD_COUNT))
          .setVerbose(false).setSecretManager(jobTokenSecretManager).build();

      // Enable service authorization?
      if (conf.getBoolean(
          CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
          false)) {
        refreshServiceAcls(conf, new MRAMPolicyProvider());
      }

      server.start();
      this.address = NetUtils.createSocketAddrForHost(
          context.getNMHostname(),
          server.getListenerAddress().getPort());
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
  }

  void refreshServiceAcls(Configuration configuration, 
      PolicyProvider policyProvider) {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }

  @Override
  protected void serviceStop() throws Exception {
    stopRpcServer();
    super.serviceStop();
  }

  protected void stopRpcServer() {
    if (server != null) {
      server.stop();
    }
  }

  @Override
  public InetSocketAddress getAddress() {
    return address;
  }

  /**
   * Child checking whether it can commit.
   * 
   * <br>
   * Commit is a two-phased protocol. First the attempt informs the
   * ApplicationMaster that it is
   * {@link #commitPending(TaskAttemptID, TaskStatus)}. Then it repeatedly polls
   * the ApplicationMaster whether it {@link #canCommit(TaskAttemptID)} This is
   * a legacy from the centralized commit protocol handling by the JobTracker.
   */
  @Override
  public boolean canCommit(TaskAttemptID taskAttemptID) throws IOException {
    LOG.info("Commit go/no-go request from " + taskAttemptID.toString());
    // An attempt is asking if it can commit its output. This can be decided
    // only by the task which is managing the multiple attempts. So redirect the
    // request there.
    org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID =
        TypeConverter.toYarn(taskAttemptID);

    taskHeartbeatHandler.progressing(attemptID);

    // tell task to retry later if AM has not heard from RM within the commit
    // window to help avoid double-committing in a split-brain situation
    long now = context.getClock().getTime();
    if (now - rmHeartbeatHandler.getLastHeartbeatTime() > commitWindowMs) {
      return false;
    }

    Job job = context.getJob(attemptID.getTaskId().getJobId());
    Task task = job.getTask(attemptID.getTaskId());
    return task.canCommit(attemptID);
  }

  /**
   * TaskAttempt is reporting that it is in commit_pending and it is waiting for
   * the commit Response
   * 
   * <br>
   * Commit it a two-phased protocol. First the attempt informs the
   * ApplicationMaster that it is
   * {@link #commitPending(TaskAttemptID, TaskStatus)}. Then it repeatedly polls
   * the ApplicationMaster whether it {@link #canCommit(TaskAttemptID)} This is
   * a legacy from the centralized commit protocol handling by the JobTracker.
   */
  @Override
  public void commitPending(TaskAttemptID taskAttemptID, TaskStatus taskStatsu)
          throws IOException, InterruptedException {
    LOG.info("Commit-pending state update from " + taskAttemptID.toString());
    // An attempt is asking if it can commit its output. This can be decided
    // only by the task which is managing the multiple attempts. So redirect the
    // request there.
    org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID =
        TypeConverter.toYarn(taskAttemptID);

    taskHeartbeatHandler.progressing(attemptID);
    //Ignorable TaskStatus? - since a task will send a LastStatusUpdate
    context.getEventHandler().handle(
        new TaskAttemptEvent(attemptID, 
            TaskAttemptEventType.TA_COMMIT_PENDING));
  }

  @Override
  public void preempted(TaskAttemptID taskAttemptID, TaskStatus taskStatus)
          throws IOException, InterruptedException {
    LOG.info("Preempted state update from " + taskAttemptID.toString());
    // An attempt is telling us that it got preempted.
    org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID =
        TypeConverter.toYarn(taskAttemptID);

    preemptionPolicy.reportSuccessfulPreemption(attemptID);
    taskHeartbeatHandler.progressing(attemptID);

    context.getEventHandler().handle(
        new TaskAttemptEvent(attemptID,
            TaskAttemptEventType.TA_PREEMPTED));
  }

  @Override
  public void done(TaskAttemptID taskAttemptID) throws IOException {
    LOG.info("Done acknowledgment from " + taskAttemptID.toString());

    org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID =
        TypeConverter.toYarn(taskAttemptID);

    taskHeartbeatHandler.progressing(attemptID);

    context.getEventHandler().handle(
        new TaskAttemptEvent(attemptID, TaskAttemptEventType.TA_DONE));
  }

  @Override
  public void fatalError(TaskAttemptID taskAttemptID, String msg)
      throws IOException {
    // This happens only in Child and in the Task.
    LOG.error("Task: " + taskAttemptID + " - exited : " + msg);
    reportDiagnosticInfo(taskAttemptID, "Error: " + msg);

    org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID =
        TypeConverter.toYarn(taskAttemptID);

    // handling checkpoints
    preemptionPolicy.handleFailedContainer(attemptID);

    context.getEventHandler().handle(
        new TaskAttemptEvent(attemptID, TaskAttemptEventType.TA_FAILMSG));
  }

  @Override
  public void fsError(TaskAttemptID taskAttemptID, String message)
      throws IOException {
    // This happens only in Child.
    LOG.error("Task: " + taskAttemptID + " - failed due to FSError: "
        + message);
    reportDiagnosticInfo(taskAttemptID, "FSError: " + message);

    org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID =
        TypeConverter.toYarn(taskAttemptID);

    // handling checkpoints
    preemptionPolicy.handleFailedContainer(attemptID);

    context.getEventHandler().handle(
        new TaskAttemptEvent(attemptID, TaskAttemptEventType.TA_FAILMSG));
  }

  @Override
  public void shuffleError(TaskAttemptID taskAttemptID, String message) throws IOException {
    // TODO: This isn't really used in any MR code. Ask for removal.    
  }

  @Override
  public MapTaskCompletionEventsUpdate getMapCompletionEvents(
      JobID jobIdentifier, int startIndex, int maxEvents,
      TaskAttemptID taskAttemptID) throws IOException {
    LOG.info("MapCompletionEvents request from " + taskAttemptID.toString()
        + ". startIndex " + startIndex + " maxEvents " + maxEvents);

    // TODO: shouldReset is never used. See TT. Ask for Removal.
    boolean shouldReset = false;
    org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID =
      TypeConverter.toYarn(taskAttemptID);
    TaskCompletionEvent[] events =
        context.getJob(attemptID.getTaskId().getJobId()).getMapAttemptCompletionEvents(
            startIndex, maxEvents);

    taskHeartbeatHandler.progressing(attemptID);
    
    return new MapTaskCompletionEventsUpdate(events, shouldReset);
  }

  @Override
  public void reportDiagnosticInfo(TaskAttemptID taskAttemptID, String diagnosticInfo)
 throws IOException {
    diagnosticInfo = StringInterner.weakIntern(diagnosticInfo);
    LOG.info("Diagnostics report from " + taskAttemptID.toString() + ": "
        + diagnosticInfo);

    org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID =
      TypeConverter.toYarn(taskAttemptID);
    taskHeartbeatHandler.progressing(attemptID);

    // This is mainly used for cases where we want to propagate exception traces
    // of tasks that fail.

    // This call exists as a hadoop mapreduce legacy wherein all changes in
    // counters/progress/phase/output-size are reported through statusUpdate()
    // call but not diagnosticInformation.
    context.getEventHandler().handle(
        new TaskAttemptDiagnosticsUpdateEvent(attemptID, diagnosticInfo));
  }

  @Override
  public AMFeedback statusUpdate(TaskAttemptID taskAttemptID,
      TaskStatus taskStatus) throws IOException, InterruptedException {

    org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId yarnAttemptID =
        TypeConverter.toYarn(taskAttemptID);

    AtomicReference<TaskAttemptStatus> lastStatusRef =
        attemptIdToStatus.get(yarnAttemptID);
    if (lastStatusRef == null) {
      throw new IllegalStateException("Status update was called"
          + " with illegal TaskAttemptId: " + yarnAttemptID);
    }

    AMFeedback feedback = new AMFeedback();
    feedback.setTaskFound(true);

    // Propagating preemption to the task if TASK_PREEMPTION is enabled
    if (getConfig().getBoolean(MRJobConfig.TASK_PREEMPTION, false)
        && preemptionPolicy.isPreempted(yarnAttemptID)) {
      feedback.setPreemption(true);
      LOG.info("Setting preemption bit for task: "+ yarnAttemptID
          + " of type " + yarnAttemptID.getTaskId().getTaskType());
    }

    if (taskStatus == null) {
      //We are using statusUpdate only as a simple ping
      if (LOG.isDebugEnabled()) {
        LOG.debug("Ping from " + taskAttemptID.toString());
      }
      return feedback;
    }

    // if we are here there is an actual status update to be processed

    taskHeartbeatHandler.progressing(yarnAttemptID);
    TaskAttemptStatus taskAttemptStatus =
        new TaskAttemptStatus();
    taskAttemptStatus.id = yarnAttemptID;
    // Task sends the updated progress to the TT.
    taskAttemptStatus.progress = taskStatus.getProgress();
    LOG.info("Progress of TaskAttempt " + taskAttemptID + " is : "
        + taskStatus.getProgress());
    // Task sends the updated state-string to the TT.
    taskAttemptStatus.stateString = taskStatus.getStateString();
    // Task sends the updated phase to the TT.
    taskAttemptStatus.phase = TypeConverter.toYarn(taskStatus.getPhase());
    // Counters are updated by the task. Convert counters into new format as
    // that is the primary storage format inside the AM to avoid multiple
    // conversions and unnecessary heap usage.
    taskAttemptStatus.counters = new org.apache.hadoop.mapreduce.Counters(
      taskStatus.getCounters());

    // Map Finish time set by the task (map only)
    if (taskStatus.getIsMap() && taskStatus.getMapFinishTime() != 0) {
      taskAttemptStatus.mapFinishTime = taskStatus.getMapFinishTime();
    }

    // Shuffle Finish time set by the task (reduce only).
    if (!taskStatus.getIsMap() && taskStatus.getShuffleFinishTime() != 0) {
      taskAttemptStatus.shuffleFinishTime = taskStatus.getShuffleFinishTime();
    }

    // Sort finish time set by the task (reduce only).
    if (!taskStatus.getIsMap() && taskStatus.getSortFinishTime() != 0) {
      taskAttemptStatus.sortFinishTime = taskStatus.getSortFinishTime();
    }

    // Not Setting the task state. Used by speculation - will be set in TaskAttemptImpl
    //taskAttemptStatus.taskState =  TypeConverter.toYarn(taskStatus.getRunState());
    
    //set the fetch failures
    if (taskStatus.getFetchFailedMaps() != null 
        && taskStatus.getFetchFailedMaps().size() > 0) {
      taskAttemptStatus.fetchFailedMaps = 
        new ArrayList<org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId>();
      for (TaskAttemptID failedMapId : taskStatus.getFetchFailedMaps()) {
        taskAttemptStatus.fetchFailedMaps.add(
            TypeConverter.toYarn(failedMapId));
      }
    }

 // Task sends the information about the nextRecordRange to the TT
    
//    TODO: The following are not needed here, but needed to be set somewhere inside AppMaster.
//    taskStatus.getRunState(); // Set by the TT/JT. Transform into a state TODO
//    taskStatus.getStartTime(); // Used to be set by the TaskTracker. This should be set by getTask().
//    taskStatus.getFinishTime(); // Used to be set by TT/JT. Should be set when task finishes
//    // This was used by TT to do counter updates only once every minute. So this
//    // isn't ever changed by the Task itself.
//    taskStatus.getIncludeCounters();

    coalesceStatusUpdate(yarnAttemptID, taskAttemptStatus, lastStatusRef);

    return feedback;
  }

  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return TaskUmbilicalProtocol.versionID;
  }

  @Override
  public void reportNextRecordRange(TaskAttemptID taskAttemptID, Range range)
      throws IOException {
    // This is used when the feature of skipping records is enabled.

    // This call exists as a hadoop mapreduce legacy wherein all changes in
    // counters/progress/phase/output-size are reported through statusUpdate()
    // call but not the next record range information.
    throw new IOException("Not yet implemented.");
  }

  @Override
  public JvmTask getTask(JvmContext context) throws IOException {

    // A rough imitation of code from TaskTracker.

    JVMId jvmId = context.jvmId;
    LOG.info("JVM with ID : " + jvmId + " asked for a task");

    JvmTask jvmTask = null;
    // TODO: Is it an authorized container to get a task? Otherwise return null.

    // TODO: Child.java's firstTaskID isn't really firstTaskID. Ask for update
    // to jobId and task-type.

    WrappedJvmID wJvmID = new WrappedJvmID(jvmId.getJobId(), jvmId.isMap,
        jvmId.getId());

    // Try to look up the task. We remove it directly as we don't give
    // multiple tasks to a JVM
    if (!jvmIDToActiveAttemptMap.containsKey(wJvmID)) {
      LOG.info("JVM with ID: " + jvmId + " is invalid and will be killed.");
      jvmTask = TASK_FOR_INVALID_JVM;
    } else {
      if (!launchedJVMs.contains(wJvmID)) {
        jvmTask = null;
        LOG.info("JVM with ID: " + jvmId
            + " asking for task before AM launch registered. Given null task");
      } else {
        // remove the task as it is no more needed and free up the memory.
        // Also we have already told the JVM to process a task, so it is no
        // longer pending, and further request should ask it to exit.
        org.apache.hadoop.mapred.Task task =
            jvmIDToActiveAttemptMap.remove(wJvmID);
        launchedJVMs.remove(wJvmID);
        LOG.info("JVM with ID: " + jvmId + " given task: " + task.getTaskID());
        task.setEncryptedSpillKey(encryptedSpillKey);
        jvmTask = new JvmTask(task, false);
      }
    }
    return jvmTask;
  }

  @Override
  public void registerPendingTask(
      org.apache.hadoop.mapred.Task task, WrappedJvmID jvmID) {
    // Create the mapping so that it is easy to look up
    // when the jvm comes back to ask for Task.

    // A JVM not present in this map is an illegal task/JVM.
    jvmIDToActiveAttemptMap.put(jvmID, task);
  }

  @Override
  public void registerLaunchedTask(
      org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID,
      WrappedJvmID jvmId) {
    // The AM considers the task to be launched (Has asked the NM to launch it)
    // The JVM will only be given a task after this registartion.
    launchedJVMs.add(jvmId);

    taskHeartbeatHandler.register(attemptID);

    attemptIdToStatus.put(attemptID, new AtomicReference<>());
  }

  @Override
  public void unregister(
      org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID,
      WrappedJvmID jvmID) {

    // Unregistration also comes from the same TaskAttempt which does the
    // registration. Events are ordered at TaskAttempt, so unregistration will
    // always come after registration.

    // Remove from launchedJVMs before jvmIDToActiveAttemptMap to avoid
    // synchronization issue with getTask(). getTask should be checking
    // jvmIDToActiveAttemptMap before it checks launchedJVMs.
 
    // remove the mappings if not already removed
    launchedJVMs.remove(jvmID);
    jvmIDToActiveAttemptMap.remove(jvmID);

    //unregister this attempt
    taskHeartbeatHandler.unregister(attemptID);

    attemptIdToStatus.remove(attemptID);
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(this, 
        protocol, clientVersion, clientMethodsHash);
  }

  // task checkpoint bookeeping
  @Override
  public TaskCheckpointID getCheckpointID(TaskID taskId) {
    TaskId tid = TypeConverter.toYarn(taskId);
    return preemptionPolicy.getCheckpointID(tid);
  }

  @Override
  public void setCheckpointID(TaskID taskId, TaskCheckpointID cid) {
    TaskId tid = TypeConverter.toYarn(taskId);
    preemptionPolicy.setCheckpointID(tid, cid);
  }

  private void coalesceStatusUpdate(TaskAttemptId yarnAttemptID,
      TaskAttemptStatus taskAttemptStatus,
      AtomicReference<TaskAttemptStatus> lastStatusRef) {
    boolean asyncUpdatedNeeded = false;
    TaskAttemptStatus lastStatus = lastStatusRef.get();

    if (lastStatus == null) {
      lastStatusRef.set(taskAttemptStatus);
      asyncUpdatedNeeded = true;
    } else {
      List<TaskAttemptId> oldFetchFailedMaps =
          taskAttemptStatus.fetchFailedMaps;

      // merge fetchFailedMaps from the previous update
      if (lastStatus.fetchFailedMaps != null) {
        if (taskAttemptStatus.fetchFailedMaps == null) {
          taskAttemptStatus.fetchFailedMaps = lastStatus.fetchFailedMaps;
        } else {
          taskAttemptStatus.fetchFailedMaps.addAll(lastStatus.fetchFailedMaps);
        }
      }

      if (!lastStatusRef.compareAndSet(lastStatus, taskAttemptStatus)) {
        // update failed - async dispatcher has processed it in the meantime
        taskAttemptStatus.fetchFailedMaps = oldFetchFailedMaps;
        lastStatusRef.set(taskAttemptStatus);
        asyncUpdatedNeeded = true;
      }
    }

    if (asyncUpdatedNeeded) {
      context.getEventHandler().handle(
          new TaskAttemptStatusUpdateEvent(taskAttemptStatus.id,
              lastStatusRef));
    }
  }

  @VisibleForTesting
  ConcurrentMap<TaskAttemptId,
      AtomicReference<TaskAttemptStatus>> getAttemptIdToStatus() {
    return attemptIdToStatus;
  }
}
