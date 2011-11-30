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

package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.IOException;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;

import com.google.common.annotations.VisibleForTesting;

/**
 * EditLogTailer represents a thread which periodically reads from edits
 * journals and applies the transactions contained within to a given
 * FSNamesystem.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class EditLogTailer {
  public static final Log LOG = LogFactory.getLog(EditLogTailer.class);
  
  private final EditLogTailerThread tailerThread;
  
  public EditLogTailer(FSNamesystem namesystem) {
    this.tailerThread = new EditLogTailerThread(namesystem);
  }
  
  public void start() {
    tailerThread.start();
  }
  
  public void stop() throws IOException {
    tailerThread.setShouldRun(false);
    tailerThread.interrupt();
    try {
      tailerThread.join();
    } catch (InterruptedException e) {
      LOG.warn("Edit log tailer thread exited with an exception");
      throw new IOException(e);
    }
  }

  @VisibleForTesting
  public void setSleepTime(long sleepTime) {
    tailerThread.setSleepTime(sleepTime);
  }
  
  @VisibleForTesting
  public void interrupt() {
    tailerThread.interrupt();
  }

  /**
   * The thread which does the actual work of tailing edits journals and
   * applying the transactions to the FSNS.
   */
  private static class EditLogTailerThread extends Thread {

    private FSNamesystem namesystem;
    private FSImage image;
    private FSEditLog editLog;
    
    private volatile boolean shouldRun = true;
    private long sleepTime = 60 * 1000;
    
    private EditLogTailerThread(FSNamesystem namesystem) {
      super("Edit log tailer");
      this.namesystem = namesystem;
      image = namesystem.getFSImage();
      editLog = namesystem.getEditLog();
    }
    
    private void setShouldRun(boolean shouldRun) {
      this.shouldRun = shouldRun;
    }
    
    private void setSleepTime(long sleepTime) {
      this.sleepTime = sleepTime;
    }
    
    @Override
    public void run() {
      while (shouldRun) {
        try {
          long lastTxnId = image.getLastAppliedTxId();
          
          if (LOG.isDebugEnabled()) {
            LOG.debug("lastTxnId: " + lastTxnId);
          }
          try {
            // At least one record should be available.
            Collection<EditLogInputStream> streams = editLog
                .selectInputStreams(lastTxnId + 1, lastTxnId + 1, false);
            if (LOG.isDebugEnabled()) {
              LOG.debug("edit streams to load from: " + streams.size());
            }
            
            long editsLoaded = image.loadEdits(streams, namesystem);
            if (LOG.isDebugEnabled()) {
              LOG.debug("editsLoaded: " + editsLoaded);
            }
          } catch (IOException e) {
            // Will try again
            LOG.info("Got error, will try again.", e);
          }
        } catch (Throwable t) {
          // TODO(HA): What should we do in this case? Shutdown the standby NN?
          LOG.error("Edit log tailer received throwable", t);
        }

        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          LOG.warn("Edit log tailer interrupted", e);
        }
      }
    }
  }
}
