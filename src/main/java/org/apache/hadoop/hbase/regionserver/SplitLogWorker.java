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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.*;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.master.SplitLogManager;
import org.apache.hadoop.hbase.regionserver.wal.HLogSplitter;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog.TaskState;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * This worker is spawned in every regionserver (should we also spawn one in
 * the master?). The Worker waits for log splitting tasks to be put up by the
 * {@link SplitLogManager} running in the master and races with other workers
 * in other serves to acquire those tasks. The coordination is done via
 * zookeeper. All the action takes place at /hbase/splitlog znode.
 * <p>
 * If a worker has successfully moved the task from state UNASSIGNED to
 * OWNED then it owns the task. It keeps heart beating the manager by
 * periodically moving the task from OWNED to OWNED state. On success it
 * moves the task to SUCCESS. On unrecoverable error it moves task state to
 * ERR. If it cannot continue but wants the master to retry the task then it
 * moves the task state to RESIGNED.
 * <p>
 * The manager can take a task away from a worker by moving the task from
 * OWNED to UNASSIGNED. In the absence of a global lock there is a
 * unavoidable race here - a worker might have just finished its task when it
 * is stripped of its ownership. Here we rely on the idempotency of the log
 * splitting task for correctness
 */
public class SplitLogWorker extends ZooKeeperListener implements Runnable {
  private static final Log LOG = LogFactory.getLog(SplitLogWorker.class);

  Thread worker;
  private final String serverName;
  private final TaskExecutor executor;
  private long zkretries;

  private Object taskReadyLock = new Object();
  volatile int taskReadySeq = 0;
  private volatile String currentTask = null;
  private int currentVersion;
  private volatile boolean exitWorker;
  private Object grabTaskLock = new Object();
  private boolean workerInGrabTask = false;


  public SplitLogWorker(ZooKeeperWatcher watcher, Configuration conf,
      String serverName, TaskExecutor executor) {
    super(watcher);
    this.serverName = serverName;
    this.executor = executor;
    this.zkretries = conf.getLong("hbase.splitlog.zk.retries", 3);
  }

  public SplitLogWorker(ZooKeeperWatcher watcher, final Configuration conf,
      final String serverName) {
    this(watcher, conf, serverName, new TaskExecutor () {
      @Override
      public Status exec(String filename, CancelableProgressable p) {
        Path rootdir;
        FileSystem fs;
        try {
          rootdir = FSUtils.getRootDir(conf);
          fs = rootdir.getFileSystem(conf);
        } catch (IOException e) {
          LOG.warn("could not find root dir or fs", e);
          return Status.RESIGNED;
        }
        // TODO have to correctly figure out when log splitting has been
        // interrupted or has encountered a transient error and when it has
        // encountered a bad non-retry-able persistent error.
        try {
          String tmpname =
            ZKSplitLog.getSplitLogDirTmpComponent(serverName, filename);
          if (HLogSplitter.splitLogFileToTemp(rootdir, tmpname,
              fs.getFileStatus(new Path(filename)), fs, conf, p) == false) {
            return Status.PREEMPTED;
          }
        } catch (InterruptedIOException iioe) {
          LOG.warn("log splitting of " + filename + " interrupted, resigning",
              iioe);
          return Status.RESIGNED;
        } catch (IOException e) {
          Throwable cause = e.getCause();
          if (cause instanceof InterruptedException) {
            LOG.warn("log splitting of " + filename + " interrupted, resigning",
                e);
            return Status.RESIGNED;
          }
          LOG.warn("log splitting of " + filename + " failed, returning error",
              e);
          return Status.ERR;
        }
        return Status.DONE;
      }
    });
  }

  @Override
  public void run() {
    LOG.info("SplitLogWorker starting");
    this.watcher.registerListener(this);
    int res;
    // wait for master to create the splitLogZnode
    res = -1;
    while (res == -1) {
      try {
        res = ZKUtil.checkExists(watcher, watcher.splitLogZNode);
      } catch (KeeperException e) {
        // ignore
        LOG.warn("Exception when checking for " + watcher.splitLogZNode +
            " ... retrying", e);
      }
      if (res == -1) {
        try {
          LOG.info(watcher.splitLogZNode + " znode does not exist," +
              " waiting for master to create one");
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          LOG.debug("Interrupted while waiting for " + watcher.splitLogZNode);
          assert exitWorker == true;
        }
      }
    }

    taskLoop();

    LOG.info("SplitLogWorker exiting");
  }

  /**
   * Wait for tasks to become available at /hbase/splitlog zknode. Grab a task
   * one at a time. This policy puts an upper-limit on the number of
   * simultaneous log splitting that could be happening in a cluster.
   * <p>
   * Synchronization using {@link #task_ready_signal_seq} ensures that it will
   * try to grab every task that has been put up
   */
  private void taskLoop() {
    while (true) {
      int seq_start = taskReadySeq;
      List<String> paths = getTaskList();
      if (paths == null) {
        LOG.warn("Could not get tasks, did someone remove " +
            this.watcher.splitLogZNode + " ... worker thread exiting.");
        return;
      }
      int offset = (int)(Math.random() * paths.size());
      for (int i = 0; i < paths.size(); i ++) {
        int idx = (i + offset) % paths.size();
        // don't call ZKSplitLog.getNodeName() because that will lead to
        // double encoding of the path name
        grabTask(ZKUtil.joinZNode(watcher.splitLogZNode, paths.get(idx)));
        if (exitWorker == true) {
          return;
        }
      }
      synchronized (taskReadyLock) {
        while (seq_start == taskReadySeq) {
          try {
            taskReadyLock.wait();
          } catch (InterruptedException e) {
            LOG.warn("SplitLogWorker inteurrpted while waiting for task," +
                " exiting", e);
            assert exitWorker == true;
            return;
          }
        }
      }
    }
  }

  /**
   * try to grab a 'lock' on the task zk node to own and execute the task.
   * <p>
   * @param path zk node for the task
   */
  private void grabTask(String path) {
    Stat stat = new Stat();
    long t = -1;
    byte[] data;
    synchronized (grabTaskLock) {
      currentTask = path;
      workerInGrabTask = true;
      if (Thread.interrupted()) {
        return;
      }
    }
    try {
      try {
        if ((data = ZKUtil.getDataNoWatch(this.watcher, path, stat)) == null) {
          tot_wkr_failed_to_grab_task_no_data.incrementAndGet();
          return;
        }
      } catch (KeeperException e) {
        LOG.warn("Failed to get data for znode " + path, e);
        tot_wkr_failed_to_grab_task_exception.incrementAndGet();
        return;
      }
      if (TaskState.TASK_UNASSIGNED.equals(data) == false) {
        tot_wkr_failed_to_grab_task_owned.incrementAndGet();
        return;
      }

      currentVersion = stat.getVersion();
      if (ownTask() == false) {
        tot_wkr_failed_to_grab_task_lost_race.incrementAndGet();
        return;
      }

      if (ZKSplitLog.isRescanNode(watcher, currentTask)) {
        endTask(TaskState.TASK_DONE, tot_wkr_task_acquired_rescan);
        return;
      }
      LOG.info("worker " + serverName + " acquired task " + path);
      tot_wkr_task_acquired.incrementAndGet();
      getDataSetWatchAsync();

      t = System.currentTimeMillis();
      TaskExecutor.Status status;

      status = executor.exec(ZKSplitLog.getFileName(currentTask),
          new CancelableProgressable() {

        @Override
        public boolean progress() {
          if (ownTask() == false) {
            LOG.warn("Failed to heartbeat the task" + currentTask);
            return false;
          }
          return true;
        }
      });
      switch (status) {
        case DONE:
          endTask(TaskState.TASK_DONE, tot_wkr_task_done);
          break;
        case PREEMPTED:
          tot_wkr_preempt_task.incrementAndGet();
          LOG.warn("task execution prempted " + path);
          break;
        case ERR:
          if (!exitWorker) {
            endTask(TaskState.TASK_ERR, tot_wkr_task_err);
            break;
          }
          // if the RS is exiting then there is probably a tons of stuff
          // that can go wrong. Resign instead of signaling error.
          //$FALL-THROUGH$
        case RESIGNED:
          if (exitWorker) {
            LOG.info("task execution interrupted because worker is exiting " +
                path);
            endTask(TaskState.TASK_RESIGNED, tot_wkr_task_resigned);
          } else {
            tot_wkr_preempt_task.incrementAndGet();
            LOG.info("task execution interrupted via zk by manager " +
                path);
          }
          break;
      }
    } finally {
      if (t > 0) {
        LOG.info("worker " + serverName + " done with task " + path +
            " in " + (System.currentTimeMillis() - t) + "ms");
      }
      synchronized (grabTaskLock) {
        workerInGrabTask = false;
        // clear the interrupt from stopTask() otherwise the next task will
        // suffer
        Thread.interrupted();
      }
    }
    return;
  }

  /**
   * Try to own the task by transitioning the zk node data from UNASSIGNED to
   * OWNED.
   * <p>
   * This method is also used to periodically heartbeat the task progress by
   * transitioning the node from OWNED to OWNED.
   * <p>
   * @return true if task path is successfully locked
   */
  private boolean ownTask() {
    try {
      Stat stat = this.watcher.getZooKeeper().setData(currentTask,
          TaskState.TASK_OWNED.get(serverName), currentVersion);
      if (stat == null) {
        return (false);
      }
      currentVersion = stat.getVersion();
      if (LOG.isDebugEnabled()) {
        LOG.debug ("hearbeat for path " + currentTask +
            " successful, version = " + currentVersion);
      }
      tot_wkr_task_heartbeat.incrementAndGet();
      return (true);
    } catch (KeeperException e) {
      // either Bad Version or Node has been removed
      LOG.warn("failed to assert ownership for " + currentTask, e);
    } catch (InterruptedException e1) {
      LOG.warn("Interrupted while trying to assert ownership of " +
          currentTask + " " + StringUtils.stringifyException(e1));
      Thread.currentThread().interrupt();
    }
    tot_wkr_task_heartbeat_failed.incrementAndGet();
    return (false);
  }

  /**
   * endTask() can fail and the only way to recover out of it is for the
   * {@link SplitLogManager} to timeout the task node.
   * @param ts
   * @param ctr
   */
  private void endTask(ZKSplitLog.TaskState ts, AtomicLong ctr) {
    String path = currentTask;
    currentTask = null;
    try {
      if (ZKUtil.setData(this.watcher, path, ts.get(serverName),
          currentVersion)) {
        LOG.info("successfully transitioned task " + path +
            " to final state " + ts);
        ctr.incrementAndGet();
        return;
      }
      LOG.warn("failed to transistion task " + path + " to end state " + ts +
          " because of version mismatch ");
    } catch (KeeperException.BadVersionException bve) {
      LOG.warn("transisition task " + path + " to " + ts +
          " failed because of version mismatch", bve);
    } catch (KeeperException.NoNodeException e) {
      LOG.fatal("logic error - end task " + path + " " + ts +
          " failed because task doesn't exist", e);
    } catch (KeeperException e) {
      LOG.warn("failed to end task, " + path + " " + ts, e);
    }
    tot_wkr_final_transistion_failed.incrementAndGet();
    return;
  }

  void getDataSetWatchAsync() {
    this.watcher.getZooKeeper().getData(currentTask, this.watcher,
        new GetDataAsyncCallback(), null);
    tot_wkr_get_data_queued.incrementAndGet();
  }

  void getDataSetWatchSuccess(String path, byte[] data) {
    synchronized (grabTaskLock) {
      if (workerInGrabTask) {
        // currentTask can change but that's ok
        String taskpath = currentTask;
        if (taskpath != null && taskpath.equals(path)) {
          // have to compare data. cannot compare version because then there
          // will be race with ownTask()
          // cannot just check whether the node has been transitioned to
          // UNASSIGNED because by the time this worker sets the data watch
          // the node might have made two transitions - from owned by this
          // worker to unassigned to owned by another worker
          if (! TaskState.TASK_OWNED.equals(data, serverName) &&
              ! TaskState.TASK_DONE.equals(data, serverName) &&
              ! TaskState.TASK_ERR.equals(data, serverName) &&
              ! TaskState.TASK_RESIGNED.equals(data, serverName)) {
            LOG.info("task " + taskpath + " preempted from server " +
                serverName + " ... current task state and owner - " +
                new String(data));
            stopTask();
          }
        }
      }
    }
  }

  void getDataSetWatchFailure(String path) {
    synchronized (grabTaskLock) {
      if (workerInGrabTask) {
        // currentTask can change but that's ok
        String taskpath = currentTask;
        if (taskpath != null && taskpath.equals(path)) {
          LOG.info("retrying data watch on " + path);
          tot_wkr_get_data_retry.incrementAndGet();
          getDataSetWatchAsync();
        } else {
          // no point setting a watch on the task which this worker is not
          // working upon anymore
        }
      }
    }
  }




  @Override
  public void nodeDataChanged(String path) {
    // there will be a self generated dataChanged event every time ownTask()
    // heartbeats the task znode by upping its version
    synchronized (grabTaskLock) {
      if (workerInGrabTask) {
        // currentTask can change
        String taskpath = currentTask;
        if (taskpath!= null && taskpath.equals(path)) {
          getDataSetWatchAsync();
        }
      }
    }
  }


  private List<String> getTaskList() {
    for (int i = 0; i < zkretries; i++) {
      try {
        return (ZKUtil.listChildrenAndWatchForNewChildren(this.watcher,
            this.watcher.splitLogZNode));
      } catch (KeeperException e) {
        LOG.warn("Could not get children of znode " +
            this.watcher.splitLogZNode, e);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e1) {
          LOG.warn("Interrupted while trying to get task list ...", e1);
          Thread.currentThread().interrupt();
          return null;
        }
      }
    }
    LOG.warn("Tried " + zkretries + " times, still couldn't fetch " +
        "children of " + watcher.splitLogZNode + " giving up");
    return null;
  }


  @Override
  public void nodeChildrenChanged(String path) {
    if(path.equals(watcher.splitLogZNode)) {
      LOG.debug("tasks arrived or departed");
      synchronized (taskReadyLock) {
        taskReadySeq++;
        taskReadyLock.notify();
      }
    }
  }

  /**
   * If the worker is doing a task i.e. splitting a log file then stop the task.
   * It doesn't exit the worker thread.
   */
  void stopTask() {
    LOG.info("Sending interrupt to stop the worker thread");
    worker.interrupt(); // TODO interrupt often gets swallowed, do what else?
  }


  /**
   * start the SplitLogWorker thread
   */
  public void start() {
    worker = new Thread(null, this, "SplitLogWorker-" + serverName);
    exitWorker = false;
    worker.start();
    return;
  }

  /**
   * stop the SplitLogWorker thread
   */
  public void stop() {
    exitWorker = true;
    stopTask();
  }

  /**
   * Asynchronous handler for zk get-data-set-watch on node results.
   */
  class GetDataAsyncCallback implements AsyncCallback.DataCallback {
    private final Log LOG = LogFactory.getLog(GetDataAsyncCallback.class);

    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data,
        Stat stat) {
      tot_wkr_get_data_result.incrementAndGet();
      if (rc != 0) {
        LOG.warn("getdata rc = " + KeeperException.Code.get(rc) + " " + path);
        getDataSetWatchFailure(path);
        return;
      }
      getDataSetWatchSuccess(path, data);
      return;
    }
  }

  /**
   * Objects implementing this interface actually do the task that has been
   * acquired by a {@link SplitLogWorker}. Since there isn't a water-tight
   * guarantee that two workers will not be executing the same task therefore it
   * is better to have workers prepare the task and then have the
   * {@link SplitLogManager} commit the work in
   * {@link SplitLogManager.TaskFinisher}
   */
  static public interface TaskExecutor {
    static public enum Status {
      DONE(),
      ERR(),
      RESIGNED(),
      PREEMPTED();
    }
    public Status exec(String name, CancelableProgressable p);
  }
}
