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

package org.apache.hadoop.yarn.server.nodemanager;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.concurrent.HadoopScheduledThreadPoolExecutor;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredDeletionServiceState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class DeletionService extends AbstractService {
  static final Log LOG = LogFactory.getLog(DeletionService.class);
  private int debugDelay;
  private final ContainerExecutor exec;
  private ScheduledThreadPoolExecutor sched;
  private static final FileContext lfs = getLfs();
  private final NMStateStoreService stateStore;
  private AtomicInteger nextTaskId = new AtomicInteger(0);

  static final FileContext getLfs() {
    try {
      return FileContext.getLocalFSFileContext();
    } catch (UnsupportedFileSystemException e) {
      throw new RuntimeException(e);
    }
  }

  public DeletionService(ContainerExecutor exec) {
    this(exec, new NMNullStateStoreService());
  }

  public DeletionService(ContainerExecutor exec,
      NMStateStoreService stateStore) {
    super(DeletionService.class.getName());
    this.exec = exec;
    this.debugDelay = 0;
    this.stateStore = stateStore;
  }
  
  /**
   * Delete the path(s) as this user.
   * @param user The user to delete as, or the JVM user if null
   * @param subDir the sub directory name
   * @param baseDirs the base directories which contains the subDir's
   */
  public void delete(String user, Path subDir, Path... baseDirs) {
    // TODO if parent owned by NM, rename within parent inline
    if (debugDelay != -1) {
      List<Path> baseDirList = null;
      if (baseDirs != null && baseDirs.length != 0) {
        baseDirList = Arrays.asList(baseDirs);
      }
      FileDeletionTask task =
          new FileDeletionTask(this, user, subDir, baseDirList);
      recordDeletionTaskInStateStore(task);
      sched.schedule(task, debugDelay, TimeUnit.SECONDS);
    }
  }
  
  public void scheduleFileDeletionTask(FileDeletionTask fileDeletionTask) {
    if (debugDelay != -1) {
      recordDeletionTaskInStateStore(fileDeletionTask);
      sched.schedule(fileDeletionTask, debugDelay, TimeUnit.SECONDS);
    }
  }
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    ThreadFactory tf = new ThreadFactoryBuilder()
      .setNameFormat("DeletionService #%d")
      .build();
    if (conf != null) {
      sched = new HadoopScheduledThreadPoolExecutor(
          conf.getInt(YarnConfiguration.NM_DELETE_THREAD_COUNT,
          YarnConfiguration.DEFAULT_NM_DELETE_THREAD_COUNT), tf);
      debugDelay = conf.getInt(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 0);
    } else {
      sched = new HadoopScheduledThreadPoolExecutor(
          YarnConfiguration.DEFAULT_NM_DELETE_THREAD_COUNT, tf);
    }
    sched.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    sched.setKeepAliveTime(60L, SECONDS);
    if (stateStore.canRecover()) {
      recover(stateStore.loadDeletionServiceState());
    }
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStop() throws Exception {
    if (sched != null) {
      sched.shutdown();
      boolean terminated = false;
      try {
        terminated = sched.awaitTermination(10, SECONDS);
      } catch (InterruptedException e) {
      }
      if (terminated != true) {
        sched.shutdownNow();
      }
    }
    super.serviceStop();
  }

  /**
   * Determine if the service has completely stopped.
   * Used only by unit tests
   * @return true if service has completely stopped
   */
  @Private
  public boolean isTerminated() {
    return getServiceState() == STATE.STOPPED && sched.isTerminated();
  }

  public static class FileDeletionTask implements Runnable {
    public static final int INVALID_TASK_ID = -1;
    private int taskId;
    private final String user;
    private final Path subDir;
    private final List<Path> baseDirs;
    private final AtomicInteger numberOfPendingPredecessorTasks;
    private final Set<FileDeletionTask> successorTaskSet;
    private final DeletionService delService;
    // By default all tasks will start as success=true; however if any of
    // the dependent task fails then it will be marked as false in
    // fileDeletionTaskFinished().
    private boolean success;
    
    private FileDeletionTask(DeletionService delService, String user,
        Path subDir, List<Path> baseDirs) {
      this(INVALID_TASK_ID, delService, user, subDir, baseDirs);
    }

    private FileDeletionTask(int taskId, DeletionService delService,
        String user, Path subDir, List<Path> baseDirs) {
      this.taskId = taskId;
      this.delService = delService;
      this.user = user;
      this.subDir = subDir;
      this.baseDirs = baseDirs;
      this.successorTaskSet = new HashSet<FileDeletionTask>();
      this.numberOfPendingPredecessorTasks = new AtomicInteger(0);
      success = true;
    }
    
    /**
     * increments and returns pending predecessor task count
     */
    public int incrementAndGetPendingPredecessorTasks() {
      return numberOfPendingPredecessorTasks.incrementAndGet();
    }
    
    /**
     * decrements and returns pending predecessor task count
     */
    public int decrementAndGetPendingPredecessorTasks() {
      return numberOfPendingPredecessorTasks.decrementAndGet();
    }
    
    @VisibleForTesting
    public String getUser() {
      return this.user;
    }
    
    @VisibleForTesting
    public Path getSubDir() {
      return this.subDir;
    }
    
    @VisibleForTesting
    public List<Path> getBaseDirs() {
      return this.baseDirs;
    }
    
    public synchronized void setSuccess(boolean success) {
      this.success = success;
    }
    
    public synchronized boolean getSucess() {
      return this.success;
    }
    
    public synchronized FileDeletionTask[] getSuccessorTasks() {
      FileDeletionTask[] successors =
          new FileDeletionTask[successorTaskSet.size()];
      return successorTaskSet.toArray(successors);
    }

    @Override
    public void run() {
      if (LOG.isDebugEnabled()) {
        LOG.debug(this);
      }
      boolean error = false;
      if (null == user) {
        if (baseDirs == null || baseDirs.size() == 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("NM deleting absolute path : " + subDir);
          }
          try {
            lfs.delete(subDir, true);
          } catch (IOException e) {
            error = true;
            LOG.warn("Failed to delete " + subDir);
          }
        } else {
          for (Path baseDir : baseDirs) {
            Path del = subDir == null? baseDir : new Path(baseDir, subDir);
            if (LOG.isDebugEnabled()) {
              LOG.debug("NM deleting path : " + del);
            }
            try {
              lfs.delete(del, true);
            } catch (IOException e) {
              error = true;
              LOG.warn("Failed to delete " + subDir);
            }
          }
        }
      } else {
        try {
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "Deleting path: [" + subDir + "] as user: [" + user + "]");
          }
          if (baseDirs == null || baseDirs.size() == 0) {
            delService.exec.deleteAsUser(new DeletionAsUserContext.Builder()
                .setUser(user)
                .setSubDir(subDir)
                .build());
          } else {
            delService.exec.deleteAsUser(new DeletionAsUserContext.Builder()
                .setUser(user)
                .setSubDir(subDir)
                .setBasedirs(baseDirs.toArray(new Path[0]))
                .build());
          }
        } catch (IOException e) {
          error = true;
          LOG.warn("Failed to delete as user " + user, e);
        } catch (InterruptedException e) {
          error = true;
          LOG.warn("Failed to delete as user " + user, e);
        }
      }
      if (error) {
        setSuccess(!error);        
      }
      fileDeletionTaskFinished();
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer("\nFileDeletionTask : ");
      sb.append("  user : ").append(this.user);
      sb.append("  subDir : ").append(
        subDir == null ? "null" : subDir.toString());
      sb.append("  baseDir : ");
      if (baseDirs == null || baseDirs.size() == 0) {
        sb.append("null");
      } else {
        for (Path baseDir : baseDirs) {
          sb.append(baseDir.toString()).append(',');
        }
      }
      return sb.toString();
    }
    
    /**
     * If there is a task dependency between say tasks 1,2,3 such that
     * task2 and task3 can be started only after task1 then we should define
     * task2 and task3 as successor tasks for task1.
     * Note:- Task dependency should be defined prior to
     * @param successorTask
     */
    public synchronized void addFileDeletionTaskDependency(
        FileDeletionTask successorTask) {
      if (successorTaskSet.add(successorTask)) {
        successorTask.incrementAndGetPendingPredecessorTasks();
      }
    }
    
    /*
     * This is called when
     * 1) Current file deletion task ran and finished.
     * 2) This can be even directly called by predecessor task if one of the
     * dependent tasks of it has failed marking its success = false.  
     */
    private synchronized void fileDeletionTaskFinished() {
      try {
        delService.stateStore.removeDeletionTask(taskId);
      } catch (IOException e) {
        LOG.error("Unable to remove deletion task " + taskId
            + " from state store", e);
      }
      Iterator<FileDeletionTask> successorTaskI =
          this.successorTaskSet.iterator();
      while (successorTaskI.hasNext()) {
        FileDeletionTask successorTask = successorTaskI.next();
        if (!success) {
          successorTask.setSuccess(success);
        }
        int count = successorTask.decrementAndGetPendingPredecessorTasks();
        if (count == 0) {
          if (successorTask.getSucess()) {
            successorTask.delService.scheduleFileDeletionTask(successorTask);
          } else {
            successorTask.fileDeletionTaskFinished();
          }
        }
      }
    }
  }
  
  /**
   * Helper method to create file deletion task. To be used only if we need
   * a way to define dependencies between deletion tasks.
   * @param user user on whose behalf this task is suppose to run
   * @param subDir sub directory as required in 
   * {@link DeletionService#delete(String, Path, Path...)}
   * @param baseDirs base directories as required in
   * {@link DeletionService#delete(String, Path, Path...)}
   */
  public FileDeletionTask createFileDeletionTask(String user, Path subDir,
      Path[] baseDirs) {
    return new FileDeletionTask(this, user, subDir, Arrays.asList(baseDirs));
  }

  private void recover(RecoveredDeletionServiceState state)
      throws IOException {
    List<DeletionServiceDeleteTaskProto> taskProtos = state.getTasks();
    Map<Integer, DeletionTaskRecoveryInfo> idToInfoMap =
        new HashMap<Integer, DeletionTaskRecoveryInfo>(taskProtos.size());
    Set<Integer> successorTasks = new HashSet<Integer>();
    for (DeletionServiceDeleteTaskProto proto : taskProtos) {
      DeletionTaskRecoveryInfo info = parseTaskProto(proto);
      idToInfoMap.put(info.task.taskId, info);
      nextTaskId.set(Math.max(nextTaskId.get(), info.task.taskId));
      successorTasks.addAll(info.successorTaskIds);
    }

    // restore the task dependencies and schedule the deletion tasks that
    // have no predecessors
    final long now = System.currentTimeMillis();
    for (DeletionTaskRecoveryInfo info : idToInfoMap.values()) {
      for (Integer successorId : info.successorTaskIds){
        DeletionTaskRecoveryInfo successor = idToInfoMap.get(successorId);
        if (successor != null) {
          info.task.addFileDeletionTaskDependency(successor.task);
        } else {
          LOG.error("Unable to locate dependency task for deletion task "
              + info.task.taskId + " at " + info.task.getSubDir());
        }
      }
      if (!successorTasks.contains(info.task.taskId)) {
        long msecTilDeletion = info.deletionTimestamp - now;
        sched.schedule(info.task, msecTilDeletion, TimeUnit.MILLISECONDS);
      }
    }
  }

  private DeletionTaskRecoveryInfo parseTaskProto(
      DeletionServiceDeleteTaskProto proto) throws IOException {
    int taskId = proto.getId();
    String user = proto.hasUser() ? proto.getUser() : null;
    Path subdir = null;
    List<Path> basePaths = null;
    if (proto.hasSubdir()) {
      subdir = new Path(proto.getSubdir());
    }
    List<String> basedirs = proto.getBasedirsList();
    if (basedirs != null && basedirs.size() > 0) {
      basePaths = new ArrayList<Path>(basedirs.size());
      for (String basedir : basedirs) {
        basePaths.add(new Path(basedir));
      }
    }

    FileDeletionTask task = new FileDeletionTask(taskId, this, user,
        subdir, basePaths);
    return new DeletionTaskRecoveryInfo(task,
        proto.getSuccessorIdsList(),
        proto.getDeletionTime());
  }

  private int generateTaskId() {
    // get the next ID but avoid an invalid ID
    int taskId = nextTaskId.incrementAndGet();
    while (taskId == FileDeletionTask.INVALID_TASK_ID) {
      taskId = nextTaskId.incrementAndGet();
    }
    return taskId;
  }

  private void recordDeletionTaskInStateStore(FileDeletionTask task) {
    if (!stateStore.canRecover()) {
      // optimize the case where we aren't really recording
      return;
    }
    if (task.taskId != FileDeletionTask.INVALID_TASK_ID) {
      return;  // task already recorded
    }

    task.taskId = generateTaskId();

    FileDeletionTask[] successors = task.getSuccessorTasks();

    // store successors first to ensure task IDs have been generated for them
    for (FileDeletionTask successor : successors) {
      recordDeletionTaskInStateStore(successor);
    }

    DeletionServiceDeleteTaskProto.Builder builder =
        DeletionServiceDeleteTaskProto.newBuilder();
    builder.setId(task.taskId);
    if (task.getUser() != null) {
      builder.setUser(task.getUser());
    }
    if (task.getSubDir() != null) {
      builder.setSubdir(task.getSubDir().toString());
    }
    builder.setDeletionTime(System.currentTimeMillis() +
        TimeUnit.MILLISECONDS.convert(debugDelay, TimeUnit.SECONDS));
    if (task.getBaseDirs() != null) {
      for (Path dir : task.getBaseDirs()) {
        builder.addBasedirs(dir.toString());
      }
    }
    for (FileDeletionTask successor : successors) {
      builder.addSuccessorIds(successor.taskId);
    }

    try {
      stateStore.storeDeletionTask(task.taskId, builder.build());
    } catch (IOException e) {
      LOG.error("Unable to store deletion task " + task.taskId + " for "
          + task.getSubDir(), e);
    }
  }

  private static class DeletionTaskRecoveryInfo {
    FileDeletionTask task;
    List<Integer> successorTaskIds;
    long deletionTimestamp;

    public DeletionTaskRecoveryInfo(FileDeletionTask task,
        List<Integer> successorTaskIds, long deletionTimestamp) {
      this.task = task;
      this.successorTaskIds = successorTaskIds;
      this.deletionTimestamp = deletionTimestamp;
    }
  }
}