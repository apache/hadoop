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
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.security.token.DelegationTokenRenewal;
import org.apache.hadoop.security.UserGroupInformation;

public class CleanupQueue {

  public static final Log LOG =
    LogFactory.getLog(CleanupQueue.class);

  private static final PathCleanupThread cleanupThread =
    new PathCleanupThread();
  private static final CleanupQueue inst = new CleanupQueue();

  public static CleanupQueue getInstance() { return inst; }

  /**
   * Create a singleton path-clean-up queue. It can be used to delete
   * paths(directories/files) in a separate thread. This constructor creates a
   * clean-up thread and also starts it as a daemon. Callers can instantiate one
   * CleanupQueue per JVM and can use it for deleting paths. Use
   * {@link CleanupQueue#addToQueue(PathDeletionContext...)} to add paths for
   * deletion.
   */
  protected CleanupQueue() { }
  
  /**
   * Contains info related to the path of the file/dir to be deleted
   */
  static class PathDeletionContext {
    final Path fullPath;// full path of file or dir
    final Configuration conf;
    final UserGroupInformation ugi;
    final JobID jobIdTokenRenewalToCancel;

    public PathDeletionContext(Path fullPath, Configuration conf) {
      this(fullPath, conf, null, null);
    }

    public PathDeletionContext(Path fullPath, Configuration conf,
        UserGroupInformation ugi) {
      this(fullPath, conf, ugi, null);
    }
    
    /**
     * PathDeletionContext ctor which also allows for a job-delegation token
     * renewal to be cancelled.
     * 
     * This is usually used at the end of a job to delete it's final path and 
     * to cancel renewal of it's job-delegation token.
     * 
     * @param fullPath path to be deleted
     * @param conf job configuration
     * @param ugi ugi of the job to be used to delete the path
     * @param jobIdTokenRenewalToCancel jobId of the job whose job-delegation
     *                                  token renewal should be cancelled. No
     *                                  cancellation is attempted if this is
     *                                  <code>null</code>
     */
    public PathDeletionContext(Path fullPath, Configuration conf,
        UserGroupInformation ugi, JobID jobIdTokenRenewalToCancel) {
      this.fullPath = fullPath;
      this.conf = conf;
      this.ugi = ugi;
      this.jobIdTokenRenewalToCancel = jobIdTokenRenewalToCancel;
    }
    
    protected Path getPathForCleanup() {
      return fullPath;
    }

    /**
     * Deletes the path (and its subdirectories recursively)
     * @throws IOException, InterruptedException 
     */
    protected void deletePath() throws IOException, InterruptedException {
      final Path p = getPathForCleanup();
      (ugi == null ? UserGroupInformation.getLoginUser() : ugi).doAs(
          new PrivilegedExceptionAction<Object>() {
            public Object run() throws IOException {
              FileSystem fs = p.getFileSystem(conf);
              try {
                fs.delete(p, true);
                return null;
              } finally {
                // So that we don't leave an entry in the FileSystem cache for
                // every UGI that a job is submitted with.
                if (ugi != null) {
                  fs.close();
                }
              }
            }
          });
      
      // Cancel renewal of job-delegation token if necessary
      if (jobIdTokenRenewalToCancel != null && 
          conf.getBoolean(JobContext.JOB_CANCEL_DELEGATION_TOKEN, true)) {
        DelegationTokenRenewal.removeDelegationTokenRenewalForJob(
            jobIdTokenRenewalToCancel);
      }
    }

    @Override
    public String toString() {
      final Path p = getPathForCleanup();
      return (null == p) ? "undefined" : p.toString();
    }
  }

  /**
   * Adds the paths to the queue of paths to be deleted by cleanupThread.
   */
  public void addToQueue(PathDeletionContext... contexts) {
    cleanupThread.addToQueue(contexts);
  }

  // currently used by tests only
  protected boolean isQueueEmpty() {
    return (cleanupThread.queue.size() == 0);
  }

  private static class PathCleanupThread extends Thread {

    // cleanup queue which deletes files/directories of the paths queued up.
    private LinkedBlockingQueue<PathDeletionContext> queue =
      new LinkedBlockingQueue<PathDeletionContext>();

    public PathCleanupThread() {
      setName("Directory/File cleanup thread");
      setDaemon(true);
      start();
    }

    void addToQueue(PathDeletionContext[] contexts) {
      for (PathDeletionContext context : contexts) {
        try {
          queue.put(context);
        } catch(InterruptedException ie) {}
      }
    }

    public void run() {
      if (LOG.isDebugEnabled()) {
        LOG.debug(getName() + " started.");
      }
      PathDeletionContext context = null;
      while (true) {
        try {
          context = queue.take();
          context.deletePath();
          // delete the path.
          if (LOG.isDebugEnabled()) {
            LOG.debug("DELETED " + context);
          }
        } catch (InterruptedException t) {
          LOG.warn("Interrupted deletion of " + context);
          return;
        } catch (Throwable e) {
          LOG.warn("Error deleting path " + context, e);
        } 
      }
    }
  }
}
