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

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerLaunchedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncherEvent;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerRemoteLaunchEvent;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the container task locally in a thread.
 * Since all (sub)tasks share the same local directory, they must be executed
 * sequentially in order to avoid creating/deleting the same files/dirs.
 */
public class LocalContainerLauncher extends AbstractService implements
    ContainerLauncher {

  private static final File curDir = new File(".");
  private static final Logger LOG =
      LoggerFactory.getLogger(LocalContainerLauncher.class);

  private FileContext curFC = null;
  private Set<File> localizedFiles = new HashSet<File>();
  private final AppContext context;
  private final TaskUmbilicalProtocol umbilical;
  private final ClassLoader jobClassLoader;
  private ExecutorService taskRunner;
  private Thread eventHandler;
  private byte[] encryptedSpillKey = new byte[] {0};
  private BlockingQueue<ContainerLauncherEvent> eventQueue =
      new LinkedBlockingQueue<ContainerLauncherEvent>();

  public LocalContainerLauncher(AppContext context,
                                TaskUmbilicalProtocol umbilical) {
    this(context, umbilical, null);
  }

  public LocalContainerLauncher(AppContext context,
                                TaskUmbilicalProtocol umbilical,
                                ClassLoader jobClassLoader) {
    super(LocalContainerLauncher.class.getName());
    this.context = context;
    this.umbilical = umbilical;
        // umbilical:  MRAppMaster creates (taskAttemptListener), passes to us
        // (TODO/FIXME:  pointless to use RPC to talk to self; should create
        // LocalTaskAttemptListener or similar:  implement umbilical protocol
        // but skip RPC stuff)
    this.jobClassLoader = jobClassLoader;

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
    if (curLocalFiles != null) {
      HashSet<File> lf = new HashSet<File>(curLocalFiles.length);
      for (int j = 0; j < curLocalFiles.length; ++j) {
        lf.add(curLocalFiles[j]);
      }
      localizedFiles = Collections.unmodifiableSet(lf);
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

  public void serviceStart() throws Exception {
    // create a single thread for serial execution of tasks
    // make it a daemon thread so that the process can exit even if the task is
    // not interruptible
    taskRunner =
        HadoopExecutors.newSingleThreadExecutor(new ThreadFactoryBuilder().
            setDaemon(true).setNameFormat("uber-SubtaskRunner").build());
    // create and start an event handling thread
    eventHandler = new Thread(new EventHandler(), "uber-EventHandler");
    // if the job classloader is specified, set it onto the event handler as the
    // thread context classloader so that it can be used by the event handler
    // as well as the subtask runner threads
    if (jobClassLoader != null) {
      LOG.info("Setting " + jobClassLoader +
          " as the context classloader of thread " + eventHandler.getName());
      eventHandler.setContextClassLoader(jobClassLoader);
    } else {
      // note the current TCCL
      LOG.info("Context classloader of thread " + eventHandler.getName() +
          ": " + eventHandler.getContextClassLoader());
    }
    eventHandler.start();
    super.serviceStart();
  }

  public void serviceStop() throws Exception {
    if (eventHandler != null) {
      eventHandler.interrupt();
    }
    if (taskRunner != null) {
      taskRunner.shutdownNow();
    }
    super.serviceStop();
  }

  @Override
  public void handle(ContainerLauncherEvent event) {
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnRuntimeException(e);  // FIXME? YarnRuntimeException is "for runtime exceptions only"
    }
  }

  public void setEncryptedSpillKey(byte[] encryptedSpillKey) {
    if (encryptedSpillKey != null) {
      this.encryptedSpillKey = encryptedSpillKey;
    }
  }

  /*
   * Uber-AM lifecycle/ordering ("normal" case):
   *
   * - [somebody] sends TA_ASSIGNED
   *   - handled by ContainerAssignedTransition (TaskAttemptImpl.java)
   *     - creates "remoteTask" for us == real Task
   *     - sends CONTAINER_REMOTE_LAUNCH
   *     - TA: UNASSIGNED -> ASSIGNED
   * - CONTAINER_REMOTE_LAUNCH handled by LocalContainerLauncher (us)
   *   - sucks "remoteTask" out of TaskAttemptImpl via getRemoteTask()
   *   - sends TA_CONTAINER_LAUNCHED
   *     [[ elsewhere...
   *       - TA_CONTAINER_LAUNCHED handled by LaunchedContainerTransition
   *         - registers "remoteTask" with TaskAttemptListener (== umbilical)
   *         - NUKES "remoteTask"
   *         - sends T_ATTEMPT_LAUNCHED (Task: SCHEDULED -> RUNNING)
   *         - TA: ASSIGNED -> RUNNING
   *     ]]
   *   - runs Task (runSubMap() or runSubReduce())
   *     - TA can safely send TA_UPDATE since in RUNNING state
   */
  private class EventHandler implements Runnable {

    // doneWithMaps and finishedSubMaps are accessed from only
    // one thread. Therefore, no need to make them volatile.
    private boolean doneWithMaps = false;
    private int finishedSubMaps = 0;

    private final Map<TaskAttemptId,Future<?>> futures =
        new ConcurrentHashMap<TaskAttemptId,Future<?>>();

    EventHandler() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
      ContainerLauncherEvent event = null;

      // Collect locations of map outputs to give to reduces
      final Map<TaskAttemptID, MapOutputFile> localMapFiles =
          new HashMap<TaskAttemptID, MapOutputFile>();
      
      // _must_ either run subtasks sequentially or accept expense of new JVMs
      // (i.e., fork()), else will get weird failures when maps try to create/
      // write same dirname or filename:  no chdir() in Java
      while (!Thread.currentThread().isInterrupted()) {
        try {
          event = eventQueue.take();
        } catch (InterruptedException e) {  // mostly via T_KILL? JOB_KILL?
          LOG.warn("Returning, interrupted : " + e);
          break;
        }

        LOG.info("Processing the event " + event.toString());

        if (event.getType() == EventType.CONTAINER_REMOTE_LAUNCH) {

          final ContainerRemoteLaunchEvent launchEv =
              (ContainerRemoteLaunchEvent)event;
          
          // execute the task on a separate thread
          Future<?> future = taskRunner.submit(new Runnable() {
            public void run() {
              runTask(launchEv, localMapFiles);
            }
          });
          // remember the current attempt
          futures.put(event.getTaskAttemptID(), future);

        } else if (event.getType() == EventType.CONTAINER_REMOTE_CLEANUP) {

          if (event.getDumpContainerThreads()) {
            try {
              // Construct full thread dump header
              System.out.println(new java.util.Date());
              RuntimeMXBean rtBean = ManagementFactory.getRuntimeMXBean();
              System.out.println("Full thread dump " + rtBean.getVmName()
                  + " (" + rtBean.getVmVersion()
                  + " " + rtBean.getSystemProperties().get("java.vm.info")
                  + "):\n");
              // Dump threads' states and stacks
              ThreadMXBean tmxBean = ManagementFactory.getThreadMXBean();
              ThreadInfo[] tInfos = tmxBean.dumpAllThreads(
                  tmxBean.isObjectMonitorUsageSupported(),
                  tmxBean.isSynchronizerUsageSupported());
              for (ThreadInfo ti : tInfos) {
                System.out.println(ti.toString());
              }
            } catch (Throwable t) {
              // Failure to dump stack shouldn't cause method failure.
              System.out.println("Could not create full thread dump: "
                  + t.getMessage());
            }
          }

          // cancel (and interrupt) the current running task associated with the
          // event
          TaskAttemptId taId = event.getTaskAttemptID();
          Future<?> future = futures.remove(taId);
          if (future != null) {
            LOG.info("canceling the task attempt " + taId);
            future.cancel(true);
          }

          // send "cleaned" event to task attempt to move us from
          // SUCCESS_CONTAINER_CLEANUP to SUCCEEDED state (or 
          // {FAIL|KILL}_CONTAINER_CLEANUP to {FAIL|KILL}_TASK_CLEANUP)
          context.getEventHandler().handle(
              new TaskAttemptEvent(taId,
                  TaskAttemptEventType.TA_CONTAINER_CLEANED));
        } else if (event.getType() == EventType.CONTAINER_COMPLETED) {
          LOG.debug("Container completed " + event.toString());
        } else {
          LOG.warn("Ignoring unexpected event " + event.toString());
        }

      }
    }

    @SuppressWarnings("unchecked")
    private void runTask(ContainerRemoteLaunchEvent launchEv,
        Map<TaskAttemptID, MapOutputFile> localMapFiles) {
      TaskAttemptId attemptID = launchEv.getTaskAttemptID(); 

      Job job = context.getAllJobs().get(attemptID.getTaskId().getJobId());
      int numMapTasks = job.getTotalMaps();
      int numReduceTasks = job.getTotalReduces();

      // YARN (tracking) Task:
      org.apache.hadoop.mapreduce.v2.app.job.Task ytask =
          job.getTask(attemptID.getTaskId());
      // classic mapred Task:
      org.apache.hadoop.mapred.Task remoteTask = launchEv.getRemoteTask();

      // after "launching," send launched event to task attempt to move
      // state from ASSIGNED to RUNNING (also nukes "remoteTask", so must
      // do getRemoteTask() call first)
      
      //There is no port number because we are not really talking to a task
      // tracker.  The shuffle is just done through local files.  So the
      // port number is set to -1 in this case.
      context.getEventHandler().handle(
          new TaskAttemptContainerLaunchedEvent(attemptID, -1));

      if (numMapTasks == 0) {
        doneWithMaps = true;
      }

      try {
        if (remoteTask.isMapOrReduce()) {
          JobCounterUpdateEvent jce = new JobCounterUpdateEvent(attemptID.getTaskId().getJobId());
          jce.addCounterUpdate(JobCounter.TOTAL_LAUNCHED_UBERTASKS, 1);
          if (remoteTask.isMapTask()) {
            jce.addCounterUpdate(JobCounter.NUM_UBER_SUBMAPS, 1);
          } else {
            jce.addCounterUpdate(JobCounter.NUM_UBER_SUBREDUCES, 1);
          }
          context.getEventHandler().handle(jce);
        }
        runSubtask(remoteTask, ytask.getType(), attemptID, numMapTasks,
                   (numReduceTasks > 0), localMapFiles);

        // In non-uber mode, TA gets TA_CONTAINER_COMPLETED from MRAppMaster
        // as part of NM -> RM -> AM notification route.
        // In uber mode, given the task run inside the MRAppMaster container,
        // we have to simulate the notification.
        context.getEventHandler().handle(new TaskAttemptEvent(attemptID,
            TaskAttemptEventType.TA_CONTAINER_COMPLETED));

      } catch (RuntimeException re) {
        JobCounterUpdateEvent jce = new JobCounterUpdateEvent(attemptID.getTaskId().getJobId());
        jce.addCounterUpdate(JobCounter.NUM_FAILED_UBERTASKS, 1);
        context.getEventHandler().handle(jce);
        // this is our signal that the subtask failed in some way, so
        // simulate a failed JVM/container and send a container-completed
        // event to task attempt (i.e., move state machine from RUNNING
        // to FAIL_CONTAINER_CLEANUP [and ultimately to FAILED])
        context.getEventHandler().handle(new TaskAttemptEvent(attemptID,
            TaskAttemptEventType.TA_CONTAINER_COMPLETED));
      } catch (IOException ioe) {
        // if umbilical itself barfs (in error-handler of runSubMap()),
        // we're pretty much hosed, so do what YarnChild main() does
        // (i.e., exit clumsily--but can never happen, so no worries!)
        LOG.error("oopsie...  this can never happen: "
            + StringUtils.stringifyException(ioe));
        ExitUtil.terminate(-1);
      } finally {
        // remove my future
        if (futures.remove(attemptID) != null) {
          LOG.info("removed attempt " + attemptID +
              " from the futures to keep track of");
        }
      }
    }

    private void runSubtask(org.apache.hadoop.mapred.Task task,
                            final TaskType taskType,
                            TaskAttemptId attemptID,
                            final int numMapTasks,
                            boolean renameOutputs,
                            Map<TaskAttemptID, MapOutputFile> localMapFiles)
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
            System.getenv(Environment.LOCAL_DIRS.name()));
        conf.setStrings(MRConfig.LOCAL_DIR, localSysDirs);
        LOG.info(MRConfig.LOCAL_DIR + " for uber task: "
            + conf.get(MRConfig.LOCAL_DIR));

        // mark this as an uberized subtask so it can set task counter
        // (longer-term/FIXME:  could redefine as job counter and send
        // "JobCounterEvent" to JobImpl on [successful] completion of subtask;
        // will need new Job state-machine transition and JobImpl jobCounters
        // map to handle)
        conf.setBoolean("mapreduce.task.uberized", true);

        // Check and handle Encrypted spill key
        task.setEncryptedSpillKey(encryptedSpillKey);
        YarnChild.setEncryptedSpillKeyIfRequired(task);

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
            MapOutputFile renamed = renameMapOutputForReduce(conf, attemptID,
                map.getMapOutputFile());
            localMapFiles.put(classicAttemptID, renamed);
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
          reduce.setLocalMapFiles(localMapFiles);
          reduce.setConf(conf);          

          reduce.run(conf, umbilical);
          relocalize();
        }

      } catch (FSError e) {
        LOG.error("FSError from child", e);
        // umbilical:  MRAppMaster creates (taskAttemptListener), passes to us
        if (!ShutdownHookManager.get().isShutdownInProgress()) {
          umbilical.fsError(classicAttemptID, e.getMessage());
        }
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
        umbilical.reportDiagnosticInfo(classicAttemptID, 
            StringUtils.stringifyException(exception));
        throw new RuntimeException();

      } catch (Throwable throwable) {
        LOG.error("Error running local (uberized) 'child' : "
            + StringUtils.stringifyException(throwable));
        if (!ShutdownHookManager.get().isShutdownInProgress()) {
          Throwable tCause = throwable.getCause();
          String cause =
              (tCause == null) ? throwable.getMessage() : StringUtils
                  .stringifyException(tCause);
          umbilical.fatalError(classicAttemptID, cause);
        }
        throw new RuntimeException();
      }
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
      if (curLocalFiles != null) {
        for (int j = 0; j < curLocalFiles.length; ++j) {
          if (!localizedFiles.contains(curLocalFiles[j])) {
            // found one that wasn't there before:  delete it
            boolean deleted = false;
            try {
              if (curFC != null) {
                // this is recursive, unlike File delete():
                deleted =
                    curFC.delete(new Path(curLocalFiles[j].getName()), true);
              }
            } catch (IOException e) {
              deleted = false;
            }
            if (!deleted) {
              LOG.warn("Unable to delete unexpected local file/dir "
                  + curLocalFiles[j].getName()
                  + ": insufficient permissions?");
            }
          }
        }
      }
    }
  } // end EventHandler

  /**
   * Within the _local_ filesystem (not HDFS), all activity takes place within
   * a subdir inside one of the LOCAL_DIRS
   * (${local.dir}/usercache/$user/appcache/$appId/$contId/),
   * and all sub-MapTasks create the same filename ("file.out").  Rename that
   * to something unique (e.g., "map_0.out") to avoid possible collisions.
   *
   * Longer-term, we'll modify [something] to use TaskAttemptID-based
   * filenames instead of "file.out". (All of this is entirely internal,
   * so there are no particular compatibility issues.)
   */
  @VisibleForTesting
  protected static MapOutputFile renameMapOutputForReduce(JobConf conf,
      TaskAttemptId mapId, MapOutputFile subMapOutputFile) throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    // move map output to reduce input
    Path mapOut = subMapOutputFile.getOutputFile();
    FileStatus mStatus = localFs.getFileStatus(mapOut);
    Path reduceIn = subMapOutputFile.getInputFileForWrite(
        TypeConverter.fromYarn(mapId).getTaskID(), mStatus.getLen());
    Path mapOutIndex = subMapOutputFile.getOutputIndexFile();
    Path reduceInIndex = new Path(reduceIn.toString() + ".index");
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
    if (!localFs.rename(mapOutIndex, reduceInIndex))
      throw new IOException("Couldn't rename " + mapOutIndex);

    return new RenamedMapOutputFile(reduceIn);
  }

  private static class RenamedMapOutputFile extends MapOutputFile {
    private Path path;
    
    public RenamedMapOutputFile(Path path) {
      this.path = path;
    }
    
    @Override
    public Path getOutputFile() throws IOException {
      return path;
    }

    @Override
    public Path getOutputFileForWrite(long size) throws IOException {
      throw new UnsupportedOperationException();
    }
    @Override
    public Path getOutputFileForWriteInVolume(Path existing) {
      throw new UnsupportedOperationException();
    }
    @Override
    public Path getOutputIndexFile() throws IOException {
      throw new UnsupportedOperationException();
    }
    @Override
    public Path getOutputIndexFileForWrite(long size) throws IOException {
      throw new UnsupportedOperationException();
    }
    @Override
    public Path getOutputIndexFileForWriteInVolume(Path existing) {
      throw new UnsupportedOperationException();
    }
    @Override
    public Path getSpillFile(int spillNumber) throws IOException {
      throw new UnsupportedOperationException();
    }
    @Override
    public Path getSpillFileForWrite(int spillNumber, long size)
        throws IOException {
      throw new UnsupportedOperationException();
    }
    @Override
    public Path getSpillIndexFile(int spillNumber) throws IOException {
      throw new UnsupportedOperationException();
    }
    @Override
    public Path getSpillIndexFileForWrite(int spillNumber, long size)
        throws IOException {
      throw new UnsupportedOperationException();
    }
    @Override
    public Path getInputFile(int mapId) throws IOException {
      throw new UnsupportedOperationException();
    }
    @Override
    public Path getInputFileForWrite(TaskID mapId, long size)
        throws IOException {
      throw new UnsupportedOperationException();
    }
    @Override
    public void removeAll() throws IOException {
      throw new UnsupportedOperationException();
    }
  }

}
