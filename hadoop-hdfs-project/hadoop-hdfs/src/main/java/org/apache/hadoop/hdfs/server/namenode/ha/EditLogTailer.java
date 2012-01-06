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
import org.apache.hadoop.hdfs.server.namenode.EditLogInputException;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

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
  
  private final FSNamesystem namesystem;
  private FSEditLog editLog;
  
  private volatile Runtime runtime = Runtime.getRuntime();
  
  public EditLogTailer(FSNamesystem namesystem) {
    this.tailerThread = new EditLogTailerThread();
    this.namesystem = namesystem;
    this.editLog = namesystem.getEditLog();
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
  
  @VisibleForTesting
  FSEditLog getEditLog() {
    return editLog;
  }
  
  @VisibleForTesting
  void setEditLog(FSEditLog editLog) {
    this.editLog = editLog;
  }
  
  @VisibleForTesting
  synchronized void setRuntime(Runtime runtime) {
    this.runtime = runtime;
  }
  
  public void catchupDuringFailover() throws IOException {
    Preconditions.checkState(tailerThread == null ||
        !tailerThread.isAlive(),
        "Tailer thread should not be running once failover starts");
    try {
      doTailEdits();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
  
  private void doTailEdits() throws IOException, InterruptedException {
    // Write lock needs to be interruptible here because the 
    // transitionToActive RPC takes the write lock before calling
    // tailer.stop() -- so if we're not interruptible, it will
    // deadlock.
    namesystem.writeLockInterruptibly();
    try {
      FSImage image = namesystem.getFSImage();

      long lastTxnId = image.getLastAppliedTxId();
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("lastTxnId: " + lastTxnId);
      }
      Collection<EditLogInputStream> streams;
      try {
        streams = editLog.selectInputStreams(lastTxnId + 1, 0, false);
      } catch (IOException ioe) {
        // This is acceptable. If we try to tail edits in the middle of an edits
        // log roll, i.e. the last one has been finalized but the new inprogress
        // edits file hasn't been started yet.
        LOG.warn("Edits tailer failed to find any streams. Will try again " +
            "later.", ioe);
        return;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("edit streams to load from: " + streams.size());
      }
      
      // Once we have streams to load, errors encountered are legitimate cause
      // for concern, so we don't catch them here. Simple errors reading from
      // disk are ignored.
      long editsLoaded = 0;
      try {
        editsLoaded = image.loadEdits(streams, namesystem);
      } catch (EditLogInputException elie) {
        LOG.warn("Error while reading edits from disk. Will try again.", elie);
        editsLoaded = elie.getNumEditsLoaded();
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("editsLoaded: " + editsLoaded);
      }
    } finally {
      namesystem.writeUnlock();
    }
  }

  /**
   * The thread which does the actual work of tailing edits journals and
   * applying the transactions to the FSNS.
   */
  private class EditLogTailerThread extends Thread {
    private volatile boolean shouldRun = true;
    private long sleepTime = 60 * 1000;
    
    private EditLogTailerThread() {
      super("Edit log tailer");
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
          doTailEdits();
        } catch (InterruptedException ie) {
          // interrupter should have already set shouldRun to false
          continue;
        } catch (Throwable t) {
          LOG.error("Error encountered while tailing edits. Shutting down " +
              "standby NN.", t);
          runtime.exit(1);
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
