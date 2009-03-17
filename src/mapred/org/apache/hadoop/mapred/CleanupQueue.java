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
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

class CleanupQueue {

  public static final Log LOG =
    LogFactory.getLog(CleanupQueue.class);

  private static PathCleanupThread cleanupThread;

  /**
   * Create a singleton path-clean-up queue. It can be used to delete
   * paths(directories/files) in a separate thread. This constructor creates a
   * clean-up thread and also starts it as a daemon. Callers can instantiate one
   * CleanupQueue per JVM and can use it for deleting paths. Use
   * {@link CleanupQueue#addToQueue(JobConf, Path...)} to add paths for
   * deletion.
   */
  public CleanupQueue() {
    synchronized (PathCleanupThread.class) {
      if (cleanupThread == null) {
        cleanupThread = new PathCleanupThread();
      }
    }
  }
  
  public void addToQueue(JobConf conf, Path...paths) {
    cleanupThread.addToQueue(conf,paths);
  }

  private static class PathCleanupThread extends Thread {

    static class PathAndConf {
      JobConf conf;
      Path path;
      PathAndConf(JobConf conf, Path path) {
        this.conf = conf;
        this.path = path;
      }
    }
    // cleanup queue which deletes files/directories of the paths queued up.
    private LinkedBlockingQueue<PathAndConf> queue = new LinkedBlockingQueue<PathAndConf>();

    public PathCleanupThread() {
      setName("Directory/File cleanup thread");
      setDaemon(true);
      start();
    }

    public void addToQueue(JobConf conf,Path... paths) {
      for (Path p : paths) {
        try {
          queue.put(new PathAndConf(conf,p));
        } catch (InterruptedException ie) {}
      }
    }

    public void run() {
      LOG.debug(getName() + " started.");
      PathAndConf pathAndConf = null;
      while (true) {
        try {
          pathAndConf = queue.take();
          // delete the path.
          FileSystem fs = pathAndConf.path.getFileSystem(pathAndConf.conf);
          fs.delete(pathAndConf.path, true);
          LOG.debug("DELETED " + pathAndConf.path);
        } catch (InterruptedException t) {
          return;
        } catch (Exception e) {
          LOG.warn("Error deleting path" + pathAndConf.path);
        } 
      }
    }
  }
}
