/**
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.master;

import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.*;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.master.SplitLogManager.TaskFinisher.Status;
import org.apache.hadoop.hbase.regionserver.SplitLogWorker;
import org.apache.hadoop.hbase.regionserver.wal.HLogSplitter;
import org.apache.hadoop.hbase.regionserver.wal.OrphanHLogAfterSplitException;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog.TaskState;

/**
 * Distributes the task of log splitting to the available region servers.
 * Coordination happens via zookeeper. For every log file that has to be split a
 * znode is created under /hbase/splitlog. SplitLogWorkers race to grab a task.
 *
 * SplitLogManager monitors the task znodes that it creates using the
 * {@link #timeoutMonitor} thread. If a task's progress is slow then
 * {@link #resubmit(String, boolean)} will take away the task from the owner
 * {@link SplitLogWorker} and the task will be
 * upforgrabs again. When the task is done then the task's znode is deleted by
 * SplitLogManager.
 *
 * Clients call {@link #splitLogDistributed(Path)} to split a region server's
 * log files. The caller thread waits in this method until all the log files
 * have been split.
 *
 * All the zookeeper calls made by this class are asynchronous. This is mainly
 * to help reduce response time seen by the callers.
 *
 * There is race in this design between the SplitLogManager and the
 * SplitLogWorker. SplitLogManager might re-queue a task that has in reality
 * already been completed by a SplitLogWorker. We rely on the idempotency of
 * the log splitting task for correctness.
 *
 * It is also assumed that every log splitting task is unique and once
 * completed (either with success or with error) it will be not be submitted
 * again. If a task is resubmitted then there is a risk that old "delete task"
 * can delete the re-submission.
 */
public class SplitLogManager extends ZooKeeperListener {
  private static final Log LOG = LogFactory.getLog(SplitLogManager.class);

  private final Stoppable stopper;
  private final String serverName;
  private final TaskFinisher taskFinisher;
  private FileSystem fs;
  private Configuration conf;

  private long zkretries;
  private long resubmit_threshold;
  private long timeout;
  private long unassignedTimeout;
  private long lastNodeCreateTime = Long.MAX_VALUE;

  private ConcurrentMap<String, Task> tasks =
    new ConcurrentHashMap<String, Task>();
  private TimeoutMonitor timeoutMonitor;

  /**
   * Its OK to construct this object even when region-servers are not online. It
   * does lookup the orphan tasks in zk but it doesn't block for them to be
   * done.
   *
   * @param zkw
   * @param conf
   * @param stopper
   * @param serverName
   * @param services
   * @param service
   */
  public SplitLogManager(ZooKeeperWatcher zkw, final Configuration conf,
      Stoppable stopper, String serverName) {
    this(zkw, conf, stopper, serverName, new TaskFinisher() {
      @Override
      public Status finish(String workerName, String logfile) {
        String tmpname =
          ZKSplitLog.getSplitLogDirTmpComponent(workerName, logfile);
        try {
          HLogSplitter.moveRecoveredEditsFromTemp(tmpname, logfile, conf);
        } catch (IOException e) {
          LOG.warn("Could not finish splitting of log file " + logfile);
          return Status.ERR;
        }
        return Status.DONE;
      }
    });
  }
  public SplitLogManager(ZooKeeperWatcher zkw, Configuration conf,
      Stoppable stopper, String serverName, TaskFinisher tf) {
    super(zkw);
    this.taskFinisher = tf;
    this.conf = conf;
    this.stopper = stopper;
    this.zkretries = conf.getLong("hbase.splitlog.zk.retries",
        ZKSplitLog.DEFAULT_ZK_RETRIES);
    this.resubmit_threshold = conf.getLong("hbase.splitlog.max.resubmit",
        ZKSplitLog.DEFAULT_MAX_RESUBMIT);
    this.timeout = conf.getInt("hbase.splitlog.manager.timeout",
        ZKSplitLog.DEFAULT_TIMEOUT);
    this.unassignedTimeout =
      conf.getInt("hbase.splitlog.manager.unassigned.timeout",
        ZKSplitLog.DEFAULT_UNASSIGNED_TIMEOUT);
    LOG.debug("timeout = " + timeout);
    LOG.debug("unassigned timeout = " + unassignedTimeout);

    this.serverName = serverName;
    this.timeoutMonitor = new TimeoutMonitor(
        conf.getInt("hbase.splitlog.manager.timeoutmonitor.period",
            1000),
        stopper);
  }

  public void finishInitialization() {
    Threads.setDaemonThreadRunning(timeoutMonitor, serverName
        + ".splitLogManagerTimeoutMonitor");
    this.watcher.registerListener(this);
    lookForOrphans();
  }

  /**
   * The caller will block until all the log files of the given region server
   * have been processed - successfully split or an error is encountered - by an
   * available worker region server. This method must only be called after the
   * region servers have been brought online.
   *
   * @param serverName
   *          region server name
   * @throws IOException
   *          if there was an error while splitting any log file
   * @return cumulative size of the logfiles split
   */
  public long splitLogDistributed(final Path logDir) throws IOException {
    this.fs = logDir.getFileSystem(conf);
    if (!fs.exists(logDir)) {
      LOG.warn(logDir + " doesn't exist. Nothing to do!");
      return 0;
    }
    FileStatus[] logfiles = fs.listStatus(logDir); // TODO filter filenames?
    if (logfiles == null || logfiles.length == 0) {
      LOG.info(logDir + " is empty dir, no logs to split");
      return 0;
    }
    tot_mgr_log_split_batch_start.incrementAndGet();
    LOG.info("started splitting logs in " + logDir);
    long t = EnvironmentEdgeManager.currentTimeMillis();
    long totalSize = 0;
    TaskBatch batch = new TaskBatch();
    for (FileStatus lf : logfiles) {
      // TODO If the log file is still being written to - which is most likely
      // the case for the last log file - then its length will show up here
      // as zero. The size of such a file can only be retrieved after after
      // recover-lease is done. totalSize will be under in most cases and the
      // metrics that it drives will also be under-reported.
      totalSize += lf.getLen();
      if (installTask(lf.getPath().toString(), batch) == false) {
        throw new IOException("duplicate log split scheduled for "
            + lf.getPath());
      }
    }
    waitTasks(batch);
    if (batch.done != batch.installed) {
      stopTrackingTasks(batch);
      tot_mgr_log_split_batch_err.incrementAndGet();
      LOG.warn("error while splitting logs in " + logDir +
      " installed = " + batch.installed + " but only " + batch.done + " done");
      throw new IOException("error or interrupt while splitting logs in "
          + logDir + " Task = " + batch);
    }
    if (anyNewLogFiles(logDir, logfiles)) {
      tot_mgr_new_unexpected_hlogs.incrementAndGet();
      LOG.warn("new hlogs were produced while logs in " + logDir +
          " were being split");
      throw new OrphanHLogAfterSplitException();
    }
    tot_mgr_log_split_batch_success.incrementAndGet();
    if (!fs.delete(logDir, true)) {
      throw new IOException("Unable to delete src dir: " + logDir);
    }
    LOG.info("finished splitting (more than or equal to) " + totalSize +
        " bytes in " + batch.installed + " log files in " + logDir + " in " +
        (EnvironmentEdgeManager.currentTimeMillis() - t) + "ms");
    return totalSize;
  }

  boolean installTask(String taskname, TaskBatch batch) {
    tot_mgr_log_split_start.incrementAndGet();
    String path = ZKSplitLog.getEncodedNodeName(watcher, taskname);
    Task oldtask = createTaskIfAbsent(path, batch);
    if (oldtask == null) {
      // publish the task in zk
      createNode(path, zkretries);
      return true;
    }
    LOG.warn(path + "is already being split. " +
        "Two threads cannot wait for the same task");
    return false;
  }

  private void waitTasks(TaskBatch batch) {
    synchronized (batch) {
      while ((batch.done + batch.error) != batch.installed) {
        try {
          batch.wait(100);
          if (stopper.isStopped()) {
            LOG.warn("Stopped while waiting for log splits to be completed");
            return;
          }
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while waiting for log splits to be completed");
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  private void setDone(String path, boolean err) {
    if (!ZKSplitLog.isRescanNode(watcher, path)) {
      if (!err) {
        tot_mgr_log_split_success.incrementAndGet();
        LOG.info("Done splitting " + path);
      } else {
        tot_mgr_log_split_err.incrementAndGet();
        LOG.warn("Error splitting " + path);
      }
    }
    Task task = tasks.get(path);
    if (task == null) {
      if (!ZKSplitLog.isRescanNode(watcher, path)) {
        tot_mgr_unacquired_orphan_done.incrementAndGet();
        LOG.debug("unacquired orphan task is done " + path);
      }
    } else {
      // if in stopTrackingTasks() we were to make tasks orphan instead of
      // forgetting about them then we will have to handle the race when
      // accessing task.batch here.
      if (!task.isOrphan()) {
        synchronized (task.batch) {
          if (!err) {
            task.batch.done++;
          } else {
            task.batch.error++;
          }
          if ((task.batch.done + task.batch.error) == task.batch.installed) {
            task.batch.notify();
          }
        }
      }
      task.deleted = true;
    }
    // delete the task node in zk. Keep trying indefinitely - its an async
    // call and no one is blocked waiting for this node to be deleted. All
    // task names are unique (log.<timestamp>) there is no risk of deleting
    // a future task.
    deleteNode(path, Long.MAX_VALUE);
    return;
  }

  private void createNode(String path, Long retry_count) {
    ZKUtil.asyncCreate(this.watcher, path,
        TaskState.TASK_UNASSIGNED.get(serverName), new CreateAsyncCallback(),
        retry_count);
    tot_mgr_node_create_queued.incrementAndGet();
    return;
  }

  private void createNodeSuccess(String path) {
    lastNodeCreateTime = EnvironmentEdgeManager.currentTimeMillis();
    LOG.debug("put up splitlog task at znode " + path);
    getDataSetWatch(path, zkretries);
  }

  private void createNodeFailure(String path) {
    // TODO the Manger should split the log locally instead of giving up
    LOG.warn("failed to create task node" + path);
    setDone(path, true);
  }


  private void getDataSetWatch(String path, Long retry_count) {
    this.watcher.getZooKeeper().getData(path, this.watcher,
        new GetDataAsyncCallback(), retry_count);
    tot_mgr_get_data_queued.incrementAndGet();
  }

  private void getDataSetWatchSuccess(String path, byte[] data, int version) {
    if (data == null) {
      tot_mgr_null_data.incrementAndGet();
      LOG.fatal("logic error - got null data " + path);
      setDone(path, true);
      return;
    }
    // LOG.debug("set watch on " + path + " got data " + new String(data));
    if (TaskState.TASK_UNASSIGNED.equals(data)) {
      LOG.debug("task not yet acquired " + path + " ver = " + version);
      handleUnassignedTask(path);
    } else if (TaskState.TASK_OWNED.equals(data)) {
      registerHeartbeat(path, version,
          TaskState.TASK_OWNED.getWriterName(data));
    } else if (TaskState.TASK_RESIGNED.equals(data)) {
      LOG.info("task " + path + " entered state " + new String(data));
      resubmit(path, true);
    } else if (TaskState.TASK_DONE.equals(data)) {
      LOG.info("task " + path + " entered state " + new String(data));
      if (taskFinisher != null && !ZKSplitLog.isRescanNode(watcher, path)) {
        if (taskFinisher.finish(TaskState.TASK_DONE.getWriterName(data),
            ZKSplitLog.getFileName(path)) == Status.DONE) {
          setDone(path, false); // success
        } else {
          resubmit(path, false); // err
        }
      } else {
        setDone(path, false); // success
      }
    } else if (TaskState.TASK_ERR.equals(data)) {
      LOG.info("task " + path + " entered state " + new String(data));
      resubmit(path, false);
    } else {
      LOG.fatal("logic error - unexpected zk state for path = " + path
          + " data = " + new String(data));
      setDone(path, true);
    }
  }

  private void getDataSetWatchFailure(String path) {
    LOG.warn("failed to set data watch " + path);
    setDone(path, true);
  }

  /**
   * It is possible for a task to stay in UNASSIGNED state indefinitely - say
   * SplitLogManager wants to resubmit a task. It forces the task to UNASSIGNED
   * state but it dies before it could create the RESCAN task node to signal
   * the SplitLogWorkers to pick up the task. To prevent this scenario the
   * SplitLogManager resubmits all orphan and UNASSIGNED tasks at startup.
   *
   * @param path
   */
  private void handleUnassignedTask(String path) {
    if (ZKSplitLog.isRescanNode(watcher, path)) {
      return;
    }
    Task task = findOrCreateOrphanTask(path);
    if (task.isOrphan() && (task.incarnation == 0)) {
      LOG.info("resubmitting unassigned orphan task " + path);
      // ignore failure to resubmit. The timeout-monitor will handle it later
      // albeit in a more crude fashion
      resubmit(path, task, true);
    }
  }

  private void registerHeartbeat(String path, int new_version,
      String workerName) {
    Task task = findOrCreateOrphanTask(path);
    if (new_version != task.last_version) {
      if (task.isUnassigned()) {
        LOG.info("task " + path + " acquired by " + workerName);
      }
      // very noisy
      //LOG.debug("heartbeat for " + path + " last_version=" + task.last_version +
      //    " last_update=" + task.last_update + " new_version=" +
      //    new_version);
      task.last_update = EnvironmentEdgeManager.currentTimeMillis();
      task.last_version = new_version;
      tot_mgr_heartbeat.incrementAndGet();
    } else {
      assert false;
      LOG.warn("got dup heartbeat for " + path + " ver = " + new_version);
    }
    return;
  }

  private boolean resubmit(String path, Task task, boolean force) {
    // its ok if this thread misses the update to task.deleted. It will
    // fail later
    if (task.deleted) {
      return false;
    }
    int version;
    if (!force) {
      if ((EnvironmentEdgeManager.currentTimeMillis() - task.last_update) <
          timeout) {
        return false;
      }
      if (task.unforcedResubmits >= resubmit_threshold) {
        if (task.unforcedResubmits == resubmit_threshold) {
          tot_mgr_resubmit_threshold_reached.incrementAndGet();
          LOG.info("Skipping resubmissions of task " + path +
              " because threshold " + resubmit_threshold + " reached");
        }
        return false;
      }
      // race with registerHeartBeat that might be changing last_version
      version = task.last_version;
    } else {
      version = -1;
    }
    LOG.info("resubmitting task " + path);
    task.incarnation++;
    try {
      // blocking zk call but this is done from the timeout thread
      if (ZKUtil.setData(this.watcher, path,
          TaskState.TASK_UNASSIGNED.get(serverName),
          version) == false) {
        LOG.debug("failed to resubmit task " + path +
            " version changed");
        return false;
      }
    } catch (NoNodeException e) {
      LOG.debug("failed to resubmit " + path + " task done");
      return false;
    } catch (KeeperException e) {
      tot_mgr_resubmit_failed.incrementAndGet();
      LOG.warn("failed to resubmit " + path, e);
      return false;
    }
    // don't count forced resubmits
    if (!force) {
      task.unforcedResubmits++;
    }
    task.setUnassigned();
    createRescanNode(Long.MAX_VALUE);
    tot_mgr_resubmit.incrementAndGet();
    return true;
  }

  private void resubmit(String path, boolean force) {
    if (resubmit(path, findOrCreateOrphanTask(path), force) == false) {
      setDone(path, true); // error
    }
  }

  private void deleteNode(String path, Long retries) {
    tot_mgr_node_delete_queued.incrementAndGet();
    this.watcher.getZooKeeper().delete(path, -1, new DeleteAsyncCallback(),
        retries);
  }

  private void deleteNodeSuccess(String path) {
    Task task;
    task = tasks.remove(path);
    if (task == null) {
      if (ZKSplitLog.isRescanNode(watcher, path)) {
        tot_mgr_rescan_deleted.incrementAndGet();
      }
      tot_mgr_missing_state_in_delete.incrementAndGet();
      LOG.debug("deleted task without in memory state " + path);
      return;
    }
    tot_mgr_task_deleted.incrementAndGet();
  }

  private void deleteNodeFailure(String path) {
    LOG.fatal("logic failure, failing to delete a node should never happen " +
        "because delete has infinite retries");
    return;
  }

  /**
   * signal the workers that a task was resubmitted by creating the
   * RESCAN node.
   */
  private void createRescanNode(long retries) {
    watcher.getZooKeeper().create(ZKSplitLog.getRescanNode(watcher),
        TaskState.TASK_UNASSIGNED.get(serverName), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT_SEQUENTIAL,
        new CreateRescanAsyncCallback(), new Long(retries));
  }

  private void createRescanSuccess(String path) {
    tot_mgr_rescan.incrementAndGet();
    getDataSetWatch(path, zkretries);
  }

  private void createRescanFailure() {
    LOG.fatal("logic failure, rescan failure must not happen");
  }

  /**
   * @param path
   * @param batch
   * @return null on success, existing task on error
   */
  private Task createTaskIfAbsent(String path, TaskBatch batch) {
    Task oldtask;
    oldtask = tasks.putIfAbsent(path, new Task(batch));
    if (oldtask != null && oldtask.isOrphan()) {
        LOG.info("Previously orphan task " + path +
            " is now being waited upon");
        oldtask.setBatch(batch);
        return (null);
    }
    return oldtask;
  }

  /**
   * This function removes any knowledge of this batch's tasks from the
   * manager. It doesn't actually stop the active tasks. If the tasks are
   * resubmitted then the active tasks will be reacquired and monitored by the
   * manager. It is important to call this function when batch processing
   * terminates prematurely, otherwise if the tasks are re-submitted
   * then they might fail.
   * <p>
   * there is a slight race here. even after a task has been removed from
   * {@link #tasks} someone who had acquired a reference to it will continue to
   * process the task. That is OK since we don't actually change the task and
   * the batch objects.
   * <p>
   * TODO Its  probably better to convert these to orphan tasks but then we
   * have to deal with race conditions as we nullify Task's batch pointer etc.
   * <p>
   * @param batch
   */
  void stopTrackingTasks(TaskBatch batch) {
    for (Map.Entry<String, Task> e : tasks.entrySet()) {
      String path = e.getKey();
      Task t = e.getValue();
      if (t.batch == batch) { // == is correct. equals not necessary.
        tasks.remove(path);
      }
    }
  }

  Task findOrCreateOrphanTask(String path) {
    Task orphanTask = new Task(null);
    Task task;
    task = tasks.putIfAbsent(path, orphanTask);
    if (task == null) {
      LOG.info("creating orphan task " + path);
      tot_mgr_orphan_task_acquired.incrementAndGet();
      task = orphanTask;
    }
    return task;
  }

  @Override
  public void nodeDataChanged(String path) {
    if (tasks.get(path) != null || ZKSplitLog.isRescanNode(watcher, path)) {
      getDataSetWatch(path, zkretries);
    }
  }

  public void stop() {
    if (timeoutMonitor != null) {
      timeoutMonitor.interrupt();
    }
  }

  private void lookForOrphans() {
    List<String> orphans;
    try {
       orphans = ZKUtil.listChildrenNoWatch(this.watcher,
          this.watcher.splitLogZNode);
      if (orphans == null) {
        LOG.warn("could not get children of " + this.watcher.splitLogZNode);
        return;
      }
    } catch (KeeperException e) {
      LOG.warn("could not get children of " + this.watcher.splitLogZNode +
          " " + StringUtils.stringifyException(e));
      return;
    }
    int rescan_nodes = 0;
    for (String path : orphans) {
      String nodepath = ZKUtil.joinZNode(watcher.splitLogZNode, path);
      if (ZKSplitLog.isRescanNode(watcher, nodepath)) {
        rescan_nodes++;
        LOG.debug("found orphan rescan node " + path);
      } else {
        LOG.info("found orphan task " + path);
      }
      getDataSetWatch(nodepath, zkretries);
    }
    LOG.info("found " + (orphans.size() - rescan_nodes) + " orphan tasks and " +
        rescan_nodes + " rescan nodes");
  }

  /**
   * Keeps track of the batch of tasks submitted together by a caller in
   * splitLogDistributed(). Clients threads use this object to wait for all
   * their tasks to be done.
   * <p>
   * All access is synchronized.
   */
  static class TaskBatch {
    int installed;
    int done;
    int error;

    @Override
    public String toString() {
      return ("installed = " + installed + " done = " + done + " error = "
          + error);
    }
  }

  /**
   * in memory state of an active task.
   */
  static class Task {
    long last_update;
    int last_version;
    TaskBatch batch;
    boolean deleted;
    int incarnation;
    int unforcedResubmits;

    @Override
    public String toString() {
      return ("last_update = " + last_update +
          " last_version = " + last_version +
          " deleted = " + deleted +
          " incarnation = " + incarnation +
          " resubmits = " + unforcedResubmits +
          " batch = " + batch);
    }

    Task(TaskBatch tb) {
      incarnation = 0;
      last_version = -1;
      deleted = false;
      setBatch(tb);
      setUnassigned();
    }

    public void setBatch(TaskBatch batch) {
      if (batch != null && this.batch != null) {
        LOG.fatal("logic error - batch being overwritten");
      }
      this.batch = batch;
      if (batch != null) {
        batch.installed++;
      }
    }

    public boolean isOrphan() {
      return (batch == null);
    }

    public boolean isUnassigned() {
      return (last_update == -1);
    }

    public void setUnassigned() {
      last_update = -1;
    }
  }

  /**
   * Periodically checks all active tasks and resubmits the ones that have timed
   * out
   */
  private class TimeoutMonitor extends Chore {
    public TimeoutMonitor(final int period, Stoppable stopper) {
      super("SplitLogManager Timeout Monitor", period, stopper);
    }

    @Override
    protected void chore() {
      int resubmitted = 0;
      int unassigned = 0;
      int tot = 0;
      boolean found_assigned_task = false;

      for (Map.Entry<String, Task> e : tasks.entrySet()) {
        String path = e.getKey();
        Task task = e.getValue();
        tot++;
        // don't easily resubmit a task which hasn't been picked up yet. It
        // might be a long while before a SplitLogWorker is free to pick up a
        // task. This is because a SplitLogWorker picks up a task one at a
        // time. If we want progress when there are no region servers then we
        // will have to run a SplitLogWorker thread in the Master.
        if (task.isUnassigned()) {
          unassigned++;
          continue;
        }
        found_assigned_task = true;
        if (resubmit(path, task, false)) {
          resubmitted++;
        }
      }
      if (tot > 0) {
        LOG.debug("total tasks = " + tot + " unassigned = " + unassigned);
      }
      if (resubmitted > 0) {
        LOG.info("resubmitted " + resubmitted + " out of " + tot + " tasks");
      }
      // If there are pending tasks and all of them have been unassigned for
      // some time then put up a RESCAN node to ping the workers.
      // ZKSplitlog.DEFAULT_UNASSIGNED_TIMEOUT is of the order of minutes
      // because a. it is very unlikely that every worker had a
      // transient error when trying to grab the task b. if there are no
      // workers then all tasks wills stay unassigned indefinitely and the
      // manager will be indefinitely creating RESCAN nodes. TODO may be the
      // master should spawn both a manager and a worker thread to guarantee
      // that there is always one worker in the system
      if (tot > 0 && !found_assigned_task &&
          ((EnvironmentEdgeManager.currentTimeMillis() - lastNodeCreateTime) >
          unassignedTimeout)) {
        createRescanNode(Long.MAX_VALUE);
        tot_mgr_resubmit_unassigned.incrementAndGet();
        LOG.debug("resubmitting unassigned task(s) after timeout");
      }
    }
  }

  /**
   * Asynchronous handler for zk create node results.
   * Retries on failures.
   */
  class CreateAsyncCallback implements AsyncCallback.StringCallback {
    private final Log LOG = LogFactory.getLog(CreateAsyncCallback.class);

    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
      tot_mgr_node_create_result.incrementAndGet();
      if (rc != 0) {
        if (rc == KeeperException.Code.NODEEXISTS.intValue()) {
          LOG.debug("found pre-existing znode " + path);
          tot_mgr_node_already_exists.incrementAndGet();
        } else {
          Long retry_count = (Long)ctx;
          LOG.warn("create rc =" + KeeperException.Code.get(rc) + " for " +
              path + " retry=" + retry_count);
          if (retry_count == 0) {
            tot_mgr_node_create_err.incrementAndGet();
            createNodeFailure(path);
          } else {
            tot_mgr_node_create_retry.incrementAndGet();
            createNode(path, retry_count - 1);
          }
          return;
        }
      }
      createNodeSuccess(path);
    }
  }

  /**
   * Asynchronous handler for zk get-data-set-watch on node results.
   * Retries on failures.
   */
  class GetDataAsyncCallback implements AsyncCallback.DataCallback {
    private final Log LOG = LogFactory.getLog(GetDataAsyncCallback.class);

    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data,
        Stat stat) {
      tot_mgr_get_data_result.incrementAndGet();
      if (rc != 0) {
        Long retry_count = (Long) ctx;
        LOG.warn("getdata rc = " + KeeperException.Code.get(rc) + " " +
            path + " retry=" + retry_count);
        if (retry_count == 0) {
          tot_mgr_get_data_err.incrementAndGet();
          getDataSetWatchFailure(path);
        } else {
          tot_mgr_get_data_retry.incrementAndGet();
          getDataSetWatch(path, retry_count - 1);
        }
        return;
      }
      getDataSetWatchSuccess(path, data, stat.getVersion());
      return;
    }
  }

  /**
   * Asynchronous handler for zk delete node results.
   * Retries on failures.
   */
  class DeleteAsyncCallback implements AsyncCallback.VoidCallback {
    private final Log LOG = LogFactory.getLog(DeleteAsyncCallback.class);

    @Override
    public void processResult(int rc, String path, Object ctx) {
      tot_mgr_node_delete_result.incrementAndGet();
      if (rc != 0) {
        if (rc != KeeperException.Code.NONODE.intValue()) {
          tot_mgr_node_delete_err.incrementAndGet();
          Long retry_count = (Long) ctx;
          LOG.warn("delete rc=" + KeeperException.Code.get(rc) + " for " +
              path + " retry=" + retry_count);
          if (retry_count == 0) {
            LOG.warn("delete failed " + path);
            deleteNodeFailure(path);
          } else {
            deleteNode(path, retry_count - 1);
          }
          return;
        } else {
        LOG.debug(path
            + " does not exist, either was never created or was deleted"
            + " in earlier rounds, zkretries = " + (Long) ctx);
        }
      } else {
        LOG.debug("deleted " + path);
      }
      deleteNodeSuccess(path);
    }
  }

  /**
   * Asynchronous handler for zk create RESCAN-node results.
   * Retries on failures.
   * <p>
   * A RESCAN node is created using PERSISTENT_SEQUENTIAL flag. It is a signal
   * for all the {@link SplitLogWorker}s to rescan for new tasks.
   */
  class CreateRescanAsyncCallback implements AsyncCallback.StringCallback {
    private final Log LOG = LogFactory.getLog(CreateRescanAsyncCallback.class);

    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
      if (rc != 0) {
        Long retry_count = (Long)ctx;
        LOG.warn("rc=" + KeeperException.Code.get(rc) + " for "+ path +
            " retry=" + retry_count);
        if (retry_count == 0) {
          createRescanFailure();
        } else {
          createRescanNode(retry_count - 1);
        }
        return;
      }
      // path is the original arg, name is the actual name that was created
      createRescanSuccess(name);
    }
  }

  /**
   * checks whether any new files have appeared in logDir which were
   * not present in the original logfiles set
   * @param logdir
   * @param logfiles
   * @return True if a new log file is found
   * @throws IOException
   */
  public boolean anyNewLogFiles(Path logdir, FileStatus[] logfiles)
  throws IOException {
    if (logdir == null) {
      return false;
    }
    LOG.debug("re-listing " + logdir);
    tot_mgr_relist_logdir.incrementAndGet();
    FileStatus[] newfiles = fs.listStatus(logdir);
    if (newfiles == null) {
      return false;
    }
    boolean matched;
    for (FileStatus newfile : newfiles) {
      matched = false;
      for (FileStatus origfile : logfiles) {
        if (origfile.equals(newfile)) {
          matched = true;
          break;
        }
      }
      if (matched == false) {
        LOG.warn("Discovered orphan hlog " + newfile + " after split." +
        " Maybe HRegionServer was not dead when we started");
        return true;
      }
    }
    return false;
  }

  /**
   * {@link SplitLogManager} can use objects implementing this interface to
   * finish off a partially done task by {@link SplitLogWorker}. This provides
   * a serialization point at the end of the task processing.
   */
  static public interface TaskFinisher {
    /**
     * status that can be returned finish()
     */
    static public enum Status {
      /**
       * task completed successfully
       */
      DONE(),
      /**
       * task completed with error
       */
      ERR();
    }
    /**
     * finish the partially done task. workername provides clue to where the
     * partial results of the partially done tasks are present. taskname is the
     * name of the task that was put up in zookeeper.
     * <p>
     * @param workerName
     * @param taskname
     * @return DONE if task completed successfully, ERR otherwise
     */
    public Status finish(String workerName, String taskname);
  }
}