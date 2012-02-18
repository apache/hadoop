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
import java.util.List;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.mapred.CleanupQueue.PathDeletionContext;
import org.apache.hadoop.mapred.JvmManager.JvmEnv;
import org.apache.hadoop.mapreduce.util.ProcessTree;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

/**
 * The default implementation for controlling tasks.
 * 
 * This class provides an implementation for launching and killing 
 * tasks that need to be run as the tasktracker itself. Hence,
 * many of the initializing or cleanup methods are not required here.
 * 
 * <br/>
 * 
 */
@InterfaceAudience.Private
public class DefaultTaskController extends TaskController {

  private static final Log LOG = 
      LogFactory.getLog(DefaultTaskController.class);
  /**
   * Launch a new JVM for the task.
   * 
   * This method launches the new JVM for the task by executing the
   * the JVM command using the {@link Shell.ShellCommandExecutor}
   */
  void launchTaskJVM(TaskController.TaskControllerContext context) 
                                      throws IOException {
    initializeTask(context);

    JvmEnv env = context.env;
    List<String> wrappedCommand = 
      TaskLog.captureOutAndError(env.setup, env.vargs, env.stdout, env.stderr,
          env.logSize, true);
    ShellCommandExecutor shexec = 
        new ShellCommandExecutor(wrappedCommand.toArray(new String[0]), 
                                  env.workDir, env.env);
    // set the ShellCommandExecutor for later use.
    context.shExec = shexec;
    shexec.execute();
  }
    
  /**
   * Initialize the task environment.
   * 
   * Since tasks are launched as the tasktracker user itself, this
   * method has no action to perform.
   */
  void initializeTask(TaskController.TaskControllerContext context) {
    // The default task controller does not need to set up
    // any permissions for proper execution.
    // So this is a dummy method.
    return;
  }

  /*
   * No need to do anything as we don't need to do as we dont need anything
   * extra from what TaskTracker has done.
   */
  @Override
  void initializeJob(JobInitializationContext context) {
  }

  @Override
  void terminateTask(TaskControllerContext context) {
    ShellCommandExecutor shexec = context.shExec;
    if (shexec != null) {
      Process process = shexec.getProcess();
      if (Shell.WINDOWS) {
        // Currently we don't use setsid on WINDOWS. 
        //So kill the process alone.
        if (process != null) {
          process.destroy();
        }
      }
      else { // In addition to the task JVM, kill its subprocesses also.
        String pid = context.pid;
        if (pid != null) {
          if(ProcessTree.isSetsidAvailable) {
            ProcessTree.terminateProcessGroup(pid);
          }else {
            ProcessTree.terminateProcess(pid);
          }
        }
      }
    }
  }
  
  @Override
  void killTask(TaskControllerContext context) {
    ShellCommandExecutor shexec = context.shExec;
    if (shexec != null) {
      if (Shell.WINDOWS) {
        //We don't do send kill process signal in case of windows as 
        //already we have done a process.destroy() in terminateTaskJVM()
        return;
      }
      String pid = context.pid;
      if (pid != null) {
        if(ProcessTree.isSetsidAvailable) {
          ProcessTree.killProcessGroup(pid);
        } else {
          ProcessTree.killProcess(pid);
        }
      }
    }
  }

  @Override
  void dumpTaskStack(TaskControllerContext context) {
    ShellCommandExecutor shexec = context.shExec;
    if (shexec != null) {
      if (Shell.WINDOWS) {
        // We don't use signals in Windows.
        return;
      }
      String pid = context.pid;
      if (pid != null) {
        // Send SIGQUIT to get a stack dump
        if (ProcessTree.isSetsidAvailable) {
          ProcessTree.sigQuitProcessGroup(pid);
        } else {
          ProcessTree.sigQuitProcess(pid);
        }
      }
    }
  }

  @Override
  public void initializeDistributedCacheFile(DistributedCacheFileContext context)
      throws IOException {
    Path localizedUniqueDir = context.getLocalizedUniqueDir();
    try {
      // Setting recursive execute permission on localized dir
      LOG.info("Doing chmod on localdir :" + localizedUniqueDir);
      FileUtil.chmod(localizedUniqueDir.toString(), "+x", true);
    } catch (InterruptedException ie) {
      LOG.warn("Exception in doing chmod on" + localizedUniqueDir, ie);
      throw new IOException(ie);
    }
  }

  @Override
  public void initializeUser(InitializationContext context) {
    // Do nothing.
  }
  
  @Override
  void runDebugScript(DebugScriptContext context) throws IOException {
    List<String>  wrappedCommand = TaskLog.captureDebugOut(context.args, 
        context.stdout);
    // run the script.
    ShellCommandExecutor shexec = 
      new ShellCommandExecutor(wrappedCommand.toArray(new String[0]), context.workDir);
    shexec.execute();
    int exitCode = shexec.getExitCode();
    if (exitCode != 0) {
      throw new IOException("Task debug script exit with nonzero status of " 
          + exitCode + ".");
    }
  }

  /**
   * Enables the task for cleanup by changing permissions of the specified path
   * in the local filesystem
   */
  @Override
  void enableTaskForCleanup(PathDeletionContext context)
         throws IOException {
    enablePathForCleanup(context);
  }
  
  /**
   * Enables the job for cleanup by changing permissions of the specified path
   * in the local filesystem
   */
  @Override
  void enableJobForCleanup(PathDeletionContext context)
         throws IOException {
    enablePathForCleanup(context);
  }
  
  /**
   * Enables the path for cleanup by changing permissions of the specified path
   * in the local filesystem
   */
  private void enablePathForCleanup(PathDeletionContext context)
         throws IOException {
    try {
      FileUtil.chmod(context.fullPath, "u+rwx", true);
    } catch(InterruptedException e) {
      LOG.warn("Interrupted while setting permissions for " + context.fullPath +
          " for deletion.");
    } catch(IOException ioe) {
      LOG.warn("Unable to change permissions of " + context.fullPath);
    }
  }
}
