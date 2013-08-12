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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class DeletionService extends AbstractService {
  static final Log LOG = LogFactory.getLog(DeletionService.class);
  private int debugDelay;
  private final ContainerExecutor exec;
  private ScheduledThreadPoolExecutor sched;
  private static final FileContext lfs = getLfs();

  static final FileContext getLfs() {
    try {
      return FileContext.getLocalFSFileContext();
    } catch (UnsupportedFileSystemException e) {
      throw new RuntimeException(e);
    }
  }

  public DeletionService(ContainerExecutor exec) {
    super(DeletionService.class.getName());
    this.exec = exec;
    this.debugDelay = 0;
  }
  
  /**
   * 
  /**
   * Delete the path(s) as this user.
   * @param user The user to delete as, or the JVM user if null
   * @param subDir the sub directory name
   * @param baseDirs the base directories which contains the subDir's
   */
  public void delete(String user, Path subDir, Path... baseDirs) {
    // TODO if parent owned by NM, rename within parent inline
    if (debugDelay != -1) {
      if (baseDirs == null || baseDirs.length == 0) {
        sched.schedule(new FileDeletionTask(this, user, subDir, null),
          debugDelay, TimeUnit.SECONDS);
      } else {
        sched.schedule(
          new FileDeletionTask(this, user, subDir, Arrays.asList(baseDirs)),
          debugDelay, TimeUnit.SECONDS);
      }
    }
  }
  
  public void scheduleFileDeletionTask(FileDeletionTask fileDeletionTask) {
    if (debugDelay != -1) {
      sched.schedule(fileDeletionTask, debugDelay, TimeUnit.SECONDS);
    }
  }
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    ThreadFactory tf = new ThreadFactoryBuilder()
      .setNameFormat("DeletionService #%d")
      .build();
    if (conf != null) {
      sched = new ScheduledThreadPoolExecutor(
          conf.getInt(YarnConfiguration.NM_DELETE_THREAD_COUNT, YarnConfiguration.DEFAULT_NM_DELETE_THREAD_COUNT),
          tf);
      debugDelay = conf.getInt(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 0);
    } else {
      sched = new ScheduledThreadPoolExecutor(YarnConfiguration.DEFAULT_NM_DELETE_THREAD_COUNT,
          tf);
    }
    sched.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    sched.setKeepAliveTime(60L, SECONDS);
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
    
    @Override
    public void run() {
      if (LOG.isDebugEnabled()) {
        LOG.debug(this);
      }
      boolean error = false;
      if (null == user) {
        if (baseDirs == null || baseDirs.size() == 0) {
          LOG.debug("NM deleting absolute path : " + subDir);
          try {
            lfs.delete(subDir, true);
          } catch (IOException e) {
            error = true;
            LOG.warn("Failed to delete " + subDir);
          }
        } else {
          for (Path baseDir : baseDirs) {
            Path del = subDir == null? baseDir : new Path(baseDir, subDir);
            LOG.debug("NM deleting path : " + del);
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
          LOG.debug("Deleting path: [" + subDir + "] as user: [" + user + "]");
          if (baseDirs == null || baseDirs.size() == 0) {
            delService.exec.deleteAsUser(user, subDir, (Path[])null);
          } else {
            delService.exec.deleteAsUser(user, subDir,
              baseDirs.toArray(new Path[0]));
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
}