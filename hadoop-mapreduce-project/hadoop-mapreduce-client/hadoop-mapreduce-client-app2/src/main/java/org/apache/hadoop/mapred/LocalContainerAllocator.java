/**
s* Licensed to the Apache Software Foundation (ASF) under one
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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app2.job.Job;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventContainerTerminated;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptRemoteStartEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.AMSchedulerEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.AMSchedulerTALaunchRequestEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.AMSchedulerEventTAEnded;
import org.apache.hadoop.mapreduce.v2.app2.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app2.rm.RMCommunicator;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;

/**
 * Allocates containers locally. Doesn't allocate a real container;
 * instead sends an allocated event for all requests.
 */
// TODO (Post-3902): Maybe implement this via an InterceptingHandler - like Recovery.
public class LocalContainerAllocator extends AbstractService
    implements ContainerAllocator {

  private static final Log LOG =
      LogFactory.getLog(LocalContainerAllocator.class);
  private static final File curDir = new File(".");

  @SuppressWarnings("rawtypes")
  private final EventHandler eventHandler;
  private final NodeId nodeId;
  private final int nmHttpPort;
  private final ContainerId amContainerId;
  private final TaskUmbilicalProtocol umbilical;
  private FileContext curFC = null;
  private final HashSet<File> localizedFiles;
  private final JobId jobId;
  private final AppContext appContext;
  private final TaskAttemptListener taskAttemptListenern;
  private final RMCommunicator rmCommunicator;
  
  private BlockingQueue<AMSchedulerEvent> eventQueue =
      new LinkedBlockingQueue<AMSchedulerEvent>();
  private Thread eventHandlingThread;
  private boolean stopEventHandling = false;

  public LocalContainerAllocator(AppContext appContext, JobId jobId,
      String nmHost, int nmPort, int nmHttpPort, ContainerId cId,
      TaskUmbilicalProtocol taskUmbilical,
      TaskAttemptListener taskAttemptListener, RMCommunicator rmComm) {
    super(LocalContainerAllocator.class.getSimpleName());
    this.appContext = appContext;
    this.eventHandler = appContext.getEventHandler();
    this.nodeId = BuilderUtils.newNodeId(nmHost, nmPort);
    this.nmHttpPort = nmHttpPort;
    this.amContainerId = cId;
    this.umbilical = taskUmbilical;
    this.jobId = jobId;
    this.taskAttemptListenern = taskAttemptListener;
    this.rmCommunicator = rmComm;
    // umbilical:  MRAppMaster creates (taskAttemptListener), passes to us
    // (TODO/FIXME:  pointless to use RPC to talk to self; should create
    // LocalTaskAttemptListener or similar:  implement umbilical protocol
    // but skip RPC stuff)
    
    try {
      curFC = FileContext.getFileContext(curDir.toURI());
    } catch (UnsupportedFileSystemException ufse) {
      LOG.error("Local filesystem " + curDir.toURI().toString()
                + " is unsupported?? (should never happen)");
    }

    // Save list of files/dirs that are supposed to be present so can delete
    // any extras created by one task before starting subsequent task.  Note
    // that there's no protection against deleted or renamed localization;
    // users who do that get what they deserve (and will have to disable
    // uberization in order to run correctly).
    File[] curLocalFiles = curDir.listFiles();
    localizedFiles = new HashSet<File>(curLocalFiles.length);
    for (int j = 0; j < curLocalFiles.length; ++j) {
      localizedFiles.add(curLocalFiles[j]);
    }

    // Relocalization note/future FIXME (per chrisdo, 20110315):  At moment,
    // full localization info is in AppSubmissionContext passed from client to
    // RM and then to NM for AM-container launch:  no difference between AM-
    // localization and MapTask- or ReduceTask-localization, so can assume all
    // OK.  Longer-term, will need to override uber-AM container-localization
    // request ("needed resources") with union of regular-AM-resources + task-
    // resources (and, if maps and reduces ever differ, then union of all three
    // types), OR will need localizer service/API that uber-AM can request
    // after running (e.g., "localizeForTask()" or "localizeForMapTask()").
  }

  @Override
  public void start() {
    this.eventHandlingThread = new Thread() {
      @SuppressWarnings("unchecked")
      @Override
      public void run() {

        AMSchedulerEvent event;

        while (!stopEventHandling && !Thread.currentThread().isInterrupted()) {
          try {
            event = LocalContainerAllocator.this.eventQueue.take();
          } catch (InterruptedException e) {
            LOG.error("Returning, interrupted : " + e);
            return;
          }

          try {
            handleEvent(event);
          } catch (Throwable t) {
            LOG.error("Error in handling event type " + event.getType()
                + " to the LocalContainreAllocator", t);
            // Kill the AM.
            eventHandler.handle(new JobEvent(jobId, JobEventType.INTERNAL_ERROR));
            return;
          }
        }
      }
    };
    eventHandlingThread.start();
    super.start();
  }

  @Override
  public void stop() {
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt();
    }
    super.stop();
  }

  @Override
  public void handle(AMSchedulerEvent event) {
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnException(e);  // FIXME? YarnException is "for runtime exceptions only"
    }
  }
  
  // Alternate Options
  // - Fake containerIds for each and every container. (AMContaienr uses JvmID instead ?)
  // - Since single threaded, works very well with the current flow. Normal flow via containers.
  // - Change container to maintain a list of pending tasks.

  // Currently, AMContainer and AMNode are short-circuited. Scheduler sends
  // events directly to the task, whcih sends events back to the scheduler.

  private boolean doneWithMaps = false;
  private int finishedSubMaps = 0;
  
  @SuppressWarnings("unchecked")
  void handleTaLaunchRequest(AMSchedulerTALaunchRequestEvent event) {
    
    LOG.info("Processing the event: " + event);
    Container container = BuilderUtils.newContainer(amContainerId, nodeId,
        this.nodeId.getHost() + ":" + nmHttpPort,
        Records.newRecord(Resource.class), Records.newRecord(Priority.class),
        null);

    appContext.getAllContainers().addContainerIfNew(container);
    appContext.getAllNodes().nodeSeen(nodeId);
    
    // Register a JVMId, so that the TAL can handle pings etc. 
    WrappedJvmID jvmId = new WrappedJvmID(TypeConverter.fromYarn(jobId), event
        .getAttemptID().getTaskId().getTaskType() == TaskType.MAP,
        amContainerId.getId());
    taskAttemptListenern.registerRunningJvm(jvmId, amContainerId);
    

    if (event.getAttemptID().getTaskId().getTaskType() == TaskType.MAP) {
      JobCounterUpdateEvent jce = new JobCounterUpdateEvent(event
          .getAttemptID().getTaskId().getJobId());
      // TODO Setting OTHER_LOCAL_MAP for now.
      jce.addCounterUpdate(JobCounter.OTHER_LOCAL_MAPS, 1);
      eventHandler.handle(jce);
    }

    AMSchedulerTALaunchRequestEvent launchEv = (AMSchedulerTALaunchRequestEvent) event;
    TaskAttemptId attemptID = launchEv.getTaskAttempt().getID();

    Job job = appContext.getJob(jobId);
    int numMapTasks = job.getTotalMaps();
    int numReduceTasks = job.getTotalReduces();

    // YARN (tracking) Task:
    org.apache.hadoop.mapreduce.v2.app2.job.Task ytask = job.getTask(attemptID
        .getTaskId());
    // classic mapred Task:
    org.apache.hadoop.mapred.Task remoteTask = launchEv.getRemoteTask();

    // There is no port number because we are not really talking to a task
    // tracker. The shuffle is just done through local files. So the
    // port number is set to -1 in this case.

    appContext.getEventHandler().handle(
        new TaskAttemptRemoteStartEvent(attemptID, amContainerId,
            rmCommunicator.getApplicationAcls(), -1));

    if (numMapTasks == 0) {
      doneWithMaps = true;
    }

    try {
      if (remoteTask.isMapOrReduce())
        if (attemptID.getTaskId().getTaskType() == TaskType.MAP
            || attemptID.getTaskId().getTaskType() == TaskType.REDUCE) {
          JobCounterUpdateEvent jce = new JobCounterUpdateEvent(attemptID
              .getTaskId().getJobId());
          jce.addCounterUpdate(JobCounter.TOTAL_LAUNCHED_UBERTASKS, 1);
          if (remoteTask.isMapTask()) {
            jce.addCounterUpdate(JobCounter.NUM_UBER_SUBMAPS, 1);
          } else {
            jce.addCounterUpdate(JobCounter.NUM_UBER_SUBREDUCES, 1);
          }
          appContext.getEventHandler().handle(jce);
        }
      
      // Register the sub-task with TaskAttemptListener.
      taskAttemptListenern.registerTaskAttempt(event.getAttemptID(), jvmId);
      runSubtask(remoteTask, ytask.getType(), attemptID, numMapTasks,
          (numReduceTasks > 0));
    } catch (RuntimeException re) {
      JobCounterUpdateEvent jce = new JobCounterUpdateEvent(attemptID
          .getTaskId().getJobId());
      jce.addCounterUpdate(JobCounter.NUM_FAILED_UBERTASKS, 1);
      appContext.getEventHandler().handle(jce);
      // this is our signal that the subtask failed in some way, so
      // simulate a failed JVM/container and send a container-completed
      // event to task attempt (i.e., move state machine from RUNNING
      // to FAILED [and ultimately to FAILED])

      // CLEANUP event generated.f
      appContext.getEventHandler().handle(
          new TaskAttemptEventContainerTerminated(attemptID, null));

    } catch (IOException ioe) {
      // if umbilical itself barfs (in error-handler of runSubMap()),
      // we're pretty much hosed, so do what YarnChild main() does
      // (i.e., exit clumsily--but can never happen, so no worries!)
      LOG.fatal("oopsie...  this can never happen: "
          + StringUtils.stringifyException(ioe));
      System.exit(-1);
    }
  }
  
  @SuppressWarnings("unchecked")
  public void handleTaStopRequest(AMSchedulerEventTAEnded sEvent) {
    // Implies a failed or killed task.
    // This will trigger a CLEANUP event. UberAM is supposed to fail if there's
    // event a single failed attempt. Hence the CLEANUP is OK (otherwise delay
    // cleanup till end of job). TODO Enforce job failure on single task attempt
    // failure.
    appContext.getEventHandler().handle(
        new TaskAttemptEventContainerTerminated(sEvent.getAttemptID(), null));
    taskAttemptListenern.unregisterTaskAttempt(sEvent.getAttemptID());
  }

  @SuppressWarnings("unchecked")
  public void handleTaSucceededRequest(AMSchedulerEventTAEnded sEvent) {
    // Successful taskAttempt.
    // Same CLEANUP comment as handleTaStopRequest
    appContext.getEventHandler().handle(
        new TaskAttemptEventContainerTerminated(sEvent.getAttemptID(), null));
    taskAttemptListenern.unregisterTaskAttempt(sEvent.getAttemptID());
  }

  public void handleEvent(AMSchedulerEvent sEvent) {
    LOG.info("Processing the event " + sEvent.toString());
    switch (sEvent.getType()) {
    case S_TA_LAUNCH_REQUEST:
      handleTaLaunchRequest((AMSchedulerTALaunchRequestEvent) sEvent);
      break;
    case S_TA_ENDED: // Effectively means a failure.
      AMSchedulerEventTAEnded event = (AMSchedulerEventTAEnded) sEvent;
      switch(event.getState()) {
      case FAILED:
      case KILLED:
        handleTaStopRequest(event);
        break;
      case SUCCEEDED:
        handleTaSucceededRequest(event);
        break;
      default:
        throw new YarnException("Unexpected TaskAttemptState: " + event.getState());
      }
      break;
    default:
      LOG.warn("Invalid event type for LocalContainerAllocator: "
          + sEvent.getType() + ", Event: " + sEvent);
    }
  }
  
  private class SubtaskRunner implements Runnable {
    SubtaskRunner() {
    }

    @Override
    public void run() {
    }

    private void runSubtask(org.apache.hadoop.mapred.Task task,
                            final TaskType taskType,
                            TaskAttemptId attemptID,
                            final int numMapTasks,
                            boolean renameOutputs)
    throws RuntimeException, IOException {
      org.apache.hadoop.mapred.TaskAttemptID classicAttemptID =
          TypeConverter.fromYarn(attemptID);

      try {
        JobConf conf = new JobConf(getConfig());
        conf.set(JobContext.TASK_ID, task.getTaskID().toString());
        conf.set(JobContext.TASK_ATTEMPT_ID, classicAttemptID.toString());
        conf.setBoolean(JobContext.TASK_ISMAP, (taskType == TaskType.MAP));
        conf.setInt(JobContext.TASK_PARTITION, task.getPartition());
        conf.set(JobContext.ID, task.getJobID().toString());

        // Use the AM's local dir env to generate the intermediate step 
        // output files
        String[] localSysDirs = StringUtils.getTrimmedStrings(
            System.getenv(ApplicationConstants.LOCAL_DIR_ENV));
        conf.setStrings(MRConfig.LOCAL_DIR, localSysDirs);
        LOG.info(MRConfig.LOCAL_DIR + " for uber task: "
            + conf.get(MRConfig.LOCAL_DIR));

        // mark this as an uberized subtask so it can set task counter
        // (longer-term/FIXME:  could redefine as job counter and send
        // "JobCounterEvent" to JobImpl on [successful] completion of subtask;
        // will need new Job state-machine transition and JobImpl jobCounters
        // map to handle)
        conf.setBoolean("mapreduce.task.uberized", true);

        // META-FIXME: do we want the extra sanity-checking (doneWithMaps,
        // etc.), or just assume/hope the state machine(s) and uber-AM work
        // as expected?
        if (taskType == TaskType.MAP) {
          if (doneWithMaps) {
            LOG.error("CONTAINER_REMOTE_LAUNCH contains a map task ("
                      + attemptID + "), but should be finished with maps");
            throw new RuntimeException();
          }

          MapTask map = (MapTask)task;
          map.setConf(conf);

          map.run(conf, umbilical);

          if (renameOutputs) {
            renameMapOutputForReduce(conf, attemptID, map.getMapOutputFile());
          }
          relocalize();

          if (++finishedSubMaps == numMapTasks) {
            doneWithMaps = true;
          }

        } else /* TaskType.REDUCE */ {

          if (!doneWithMaps) {
            // check if event-queue empty?  whole idea of counting maps vs. 
            // checking event queue is a tad wacky...but could enforce ordering
            // (assuming no "lost events") at LocalMRAppMaster [CURRENT BUG(?): 
            // doesn't send reduce event until maps all done]
            LOG.error("CONTAINER_REMOTE_LAUNCH contains a reduce task ("
                      + attemptID + "), but not yet finished with maps");
            throw new RuntimeException();
          }

          // a.k.a. "mapreduce.jobtracker.address" in LocalJobRunner:
          // set framework name to local to make task local
          conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
          conf.set(MRConfig.MASTER_ADDRESS, "local");  // bypass shuffle

          ReduceTask reduce = (ReduceTask)task;
          reduce.setConf(conf);          

          reduce.run(conf, umbilical);
          //relocalize();  // needed only if more than one reducer supported (is MAPREDUCE-434 fixed yet?)
        }

      } catch (FSError e) {
        LOG.fatal("FSError from child", e);
        // umbilical:  MRAppMaster creates (taskAttemptListener), passes to us
        umbilical.fsError(classicAttemptID, e.getMessage());
        throw new RuntimeException();

      } catch (Exception exception) {
        LOG.warn("Exception running local (uberized) 'child' : "
            + StringUtils.stringifyException(exception));
        try {
          if (task != null) {
            // do cleanup for the task
            task.taskCleanup(umbilical);
          }
        } catch (Exception e) {
          LOG.info("Exception cleaning up: "
              + StringUtils.stringifyException(e));
        }
        // Report back any failures, for diagnostic purposes
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        exception.printStackTrace(new PrintStream(baos));
        umbilical.reportDiagnosticInfo(classicAttemptID, baos.toString());
        throw new RuntimeException();

      } catch (Throwable throwable) {
        LOG.fatal("Error running local (uberized) 'child' : "
            + StringUtils.stringifyException(throwable));
        Throwable tCause = throwable.getCause();
        String cause = (tCause == null)
            ? throwable.getMessage()
                : StringUtils.stringifyException(tCause);
            umbilical.fatalError(classicAttemptID, cause);
        throw new RuntimeException();
      }
    }

    /**
     * Within the _local_ filesystem (not HDFS), all activity takes place within
     * a single subdir (${local.dir}/usercache/$user/appcache/$appId/$contId/),
     * and all sub-MapTasks create the same filename ("file.out").  Rename that
     * to something unique (e.g., "map_0.out") to avoid collisions.
     *
     * Longer-term, we'll modify [something] to use TaskAttemptID-based
     * filenames instead of "file.out". (All of this is entirely internal,
     * so there are no particular compatibility issues.)
     */
    private void renameMapOutputForReduce(JobConf conf, TaskAttemptId mapId,
                                          MapOutputFile subMapOutputFile)
    throws IOException {
      FileSystem localFs = FileSystem.getLocal(conf);
      // move map output to reduce input
      Path mapOut = subMapOutputFile.getOutputFile();
      FileStatus mStatus = localFs.getFileStatus(mapOut);      
      Path reduceIn = subMapOutputFile.getInputFileForWrite(
          TypeConverter.fromYarn(mapId).getTaskID(), mStatus.getLen());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Renaming map output file for task attempt "
            + mapId.toString() + " from original location " + mapOut.toString()
            + " to destination " + reduceIn.toString());
      }
      if (!localFs.mkdirs(reduceIn.getParent())) {
        throw new IOException("Mkdirs failed to create "
            + reduceIn.getParent().toString());
      }
      if (!localFs.rename(mapOut, reduceIn))
        throw new IOException("Couldn't rename " + mapOut);
    }

    /**
     * Also within the local filesystem, we need to restore the initial state
     * of the directory as much as possible.  Compare current contents against
     * the saved original state and nuke everything that doesn't belong, with
     * the exception of the renamed map outputs.
     *
     * Any jobs that go out of their way to rename or delete things from the
     * local directory are considered broken and deserve what they get...
     */
    private void relocalize() {
      File[] curLocalFiles = curDir.listFiles();
      for (int j = 0; j < curLocalFiles.length; ++j) {
        if (!localizedFiles.contains(curLocalFiles[j])) {
          // found one that wasn't there before:  delete it
          boolean deleted = false;
          try {
            if (curFC != null) {
              // this is recursive, unlike File delete():
              deleted = curFC.delete(new Path(curLocalFiles[j].getName()),true);
            }
          } catch (IOException e) {
            deleted = false;
          }
          if (!deleted) {
            LOG.warn("Unable to delete unexpected local file/dir "
                + curLocalFiles[j].getName() + ": insufficient permissions?");
          }
        }
      }
    }

  } // end SubtaskRunner

  
  // _must_ either run subtasks sequentially or accept expense of new JVMs
  // (i.e., fork()), else will get weird failures when maps try to create/
  // write same dirname or filename:  no chdir() in Java
  
  // TODO: Since this is only a single thread, AMContainer can be used
  // efficiently for the Uber lifecycle. Even if it is made multi-threaded, it
  // may make sense to let AMContainer maintain a queue of pending tasks,
  // instead of only a single pending task.

  /**
   * Blocking call. Only one attempt runs at any given point.
   */
  private void runSubtask(org.apache.hadoop.mapred.Task remoteTask,
                          final TaskType taskType,
                          TaskAttemptId attemptID,
                          final int numMapTasks, 
                          boolean renameOutputs)
  throws RuntimeException, IOException {
    SubtaskRunner subTaskRunner = new SubtaskRunner();
    subTaskRunner.runSubtask(remoteTask, taskType, attemptID, numMapTasks,
        renameOutputs);
  }
}
