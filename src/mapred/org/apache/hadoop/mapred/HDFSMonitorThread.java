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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapred.JobTracker.SafeModeAction;

public class HDFSMonitorThread extends Thread {
  
  public static final Log LOG = LogFactory.getLog(HDFSMonitorThread.class);
  
  private final JobTracker jt;
  private final FileSystem fs;
  
  private final int hdfsMonitorInterval;
  
  public HDFSMonitorThread(Configuration conf, JobTracker jt, FileSystem fs) {
    super("JT-HDFS-Monitor-Thread");
    this.jt = jt;
    this.fs = fs;
    this.hdfsMonitorInterval = 
        conf.getInt(
            JobTracker.JT_HDFS_MONITOR_THREAD_INTERVAL, 
            JobTracker.DEFAULT_JT_HDFS_MONITOR_THREAD_INTERVAL_MS);
    setDaemon(true);
  }

  @Override
  public void run() {
    
    LOG.info("Starting HDFS Health Monitoring...");
    
    boolean previouslyHealthy = true;
    boolean done = false;
    
    while (!done && !isInterrupted()) {
      
      boolean currentlyHealthy = DistributedFileSystem.isHealthy(fs.getUri());
      if (currentlyHealthy != previouslyHealthy) {
        
        JobTracker.SafeModeAction action; 
        if (currentlyHealthy) {
          action = SafeModeAction.SAFEMODE_LEAVE;
          LOG.info("HDFS healthy again, instructing JobTracker to leave " +
              "'safemode' ...");
        } else {
          action = SafeModeAction.SAFEMODE_ENTER;
          LOG.info("HDFS is unhealthy, instructing JobTracker to enter " +
              "'safemode' ...");
        }
        
        try {
          if (jt.isInAdminSafeMode()) {
            // Don't override admin-set safemode
            LOG.info("JobTracker is in admin-set safemode, not overriding " +
            		"through " + action);
           previouslyHealthy = currentlyHealthy; 
          } else {
            previouslyHealthy = !(jt.setSafeModeInternal(action)); 
                                                         //safemode => !healthy
          }
        } catch (IOException ioe) {
          LOG.info("Failed to setSafeMode with action " + action, ioe);
        }
      }
      
      try {
        Thread.sleep(hdfsMonitorInterval);
      } catch (InterruptedException e) {
        done = true;
      }
    }
    
    LOG.info("Stoping HDFS Health Monitoring...");
  }
  
}
