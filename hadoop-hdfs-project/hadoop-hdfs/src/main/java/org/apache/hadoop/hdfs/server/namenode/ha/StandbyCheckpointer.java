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

import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.server.namenode.CheckpointConf;
import org.apache.hadoop.hdfs.server.namenode.CheckpointFaultInjector;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.SaveNamespaceCancelledException;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Thread which runs inside the NN when it's in Standby state,
 * periodically waking up to take a checkpoint of the namespace.
 * When it takes a checkpoint, it saves it to its local
 * storage and then uploads it to the remote NameNode.
 */
@InterfaceAudience.Private
public class StandbyCheckpointer {
  private static final Log LOG = LogFactory.getLog(StandbyCheckpointer.class);
  private static final long PREVENT_AFTER_CANCEL_MS = 2*60*1000L;
  private final CheckpointConf checkpointConf;
  private final Configuration conf;
  private final FSNamesystem namesystem;
  private long lastCheckpointTime;
  private long lastUploadTime;
  private final CheckpointerThread thread;
  private final ThreadFactory uploadThreadFactory;
  private List<URL> activeNNAddresses;
  private URL myNNAddress;

  private final Object cancelLock = new Object();
  private Canceler canceler;
  private boolean isPrimaryCheckPointer = true;

  // Keep track of how many checkpoints were canceled.
  // This is for use in tests.
  private static int canceledCount = 0;
  
  public StandbyCheckpointer(Configuration conf, FSNamesystem ns)
      throws IOException {
    this.namesystem = ns;
    this.conf = conf;
    this.checkpointConf = new CheckpointConf(conf); 
    this.thread = new CheckpointerThread();
    this.uploadThreadFactory = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat("TransferFsImageUpload-%d").build();

    setNameNodeAddresses(conf);
  }

  /**
   * Determine the address of the NN we are checkpointing
   * as well as our own HTTP address from the configuration.
   * @throws IOException 
   */
  private void setNameNodeAddresses(Configuration conf) throws IOException {
    // Look up our own address.
    myNNAddress = getHttpAddress(conf);

    // Look up the active node's address
    List<Configuration> confForActive = HAUtil.getConfForOtherNodes(conf);
    activeNNAddresses = new ArrayList<URL>(confForActive.size());
    for (Configuration activeConf : confForActive) {
      URL activeNNAddress = getHttpAddress(activeConf);

      // sanity check each possible active NN
      Preconditions.checkArgument(checkAddress(activeNNAddress),
          "Bad address for active NN: %s", activeNNAddress);

      activeNNAddresses.add(activeNNAddress);
    }

    // Sanity-check.
    Preconditions.checkArgument(checkAddress(myNNAddress), "Bad address for standby NN: %s",
        myNNAddress);
  }
  
  private URL getHttpAddress(Configuration conf) throws IOException {
    final String scheme = DFSUtil.getHttpClientScheme(conf);
    String defaultHost = NameNode.getServiceAddress(conf, true).getHostName();
    URI addr = DFSUtil.getInfoServerWithDefaultHost(defaultHost, conf, scheme);
    return addr.toURL();
  }
  
  /**
   * Ensure that the given address is valid and has a port
   * specified.
   */
  private static boolean checkAddress(URL addr) {
    return addr.getPort() != 0;
  }

  public void start() {
    LOG.info("Starting standby checkpoint thread...\n" +
        "Checkpointing active NN to possible NNs: " + activeNNAddresses + "\n" +
        "Serving checkpoints at " + myNNAddress);
    thread.start();
  }
  
  public void stop() throws IOException {
    cancelAndPreventCheckpoints("Stopping checkpointer");
    thread.setShouldRun(false);
    thread.interrupt();
    try {
      thread.join();
    } catch (InterruptedException e) {
      LOG.warn("Edit log tailer thread exited with an exception");
      throw new IOException(e);
    }
  }

  public void triggerRollbackCheckpoint() {
    thread.interrupt();
  }

  private void doCheckpoint(boolean sendCheckpoint) throws InterruptedException, IOException {
    assert canceler != null;
    final long txid;
    final NameNodeFile imageType;
    // Acquire cpLock to make sure no one is modifying the name system.
    // It does not need the full namesystem write lock, since the only thing
    // that modifies namesystem on standby node is edit log replaying.
    namesystem.cpLockInterruptibly();
    try {
      assert namesystem.getEditLog().isOpenForRead() :
        "Standby Checkpointer should only attempt a checkpoint when " +
        "NN is in standby mode, but the edit logs are in an unexpected state";

      FSImage img = namesystem.getFSImage();

      long prevCheckpointTxId = img.getStorage().getMostRecentCheckpointTxId();
      long thisCheckpointTxId = img.getCorrectLastAppliedOrWrittenTxId();
      assert thisCheckpointTxId >= prevCheckpointTxId;
      if (thisCheckpointTxId == prevCheckpointTxId) {
        LOG.info("A checkpoint was triggered but the Standby Node has not " +
            "received any transactions since the last checkpoint at txid " +
            thisCheckpointTxId + ". Skipping...");
        return;
      }

      if (namesystem.isRollingUpgrade()
          && !namesystem.getFSImage().hasRollbackFSImage()) {
        // if we will do rolling upgrade but have not created the rollback image
        // yet, name this checkpoint as fsimage_rollback
        imageType = NameNodeFile.IMAGE_ROLLBACK;
      } else {
        imageType = NameNodeFile.IMAGE;
      }
      img.saveNamespace(namesystem, imageType, canceler);
      txid = img.getStorage().getMostRecentCheckpointTxId();
      assert txid == thisCheckpointTxId : "expected to save checkpoint at txid=" +
          thisCheckpointTxId + " but instead saved at txid=" + txid;

      // Save the legacy OIV image, if the output dir is defined.
      String outputDir = checkpointConf.getLegacyOivImageDir();
      if (outputDir != null && !outputDir.isEmpty()) {
        try {
          img.saveLegacyOIVImage(namesystem, outputDir, canceler);
        } catch (IOException ioe) {
          LOG.warn("Exception encountered while saving legacy OIV image; "
                  + "continuing with other checkpointing steps", ioe);
        }
      }
    } finally {
      namesystem.cpUnlock();
    }

    //early exit if we shouldn't actually send the checkpoint to the ANN
    if(!sendCheckpoint){
      return;
    }

    // Upload the saved checkpoint back to the active
    // Do this in a separate thread to avoid blocking transition to active, but don't allow more
    // than the expected number of tasks to run or queue up
    // See HDFS-4816
    ExecutorService executor = new ThreadPoolExecutor(0, activeNNAddresses.size(), 100,
        TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(activeNNAddresses.size()),
        uploadThreadFactory);
    // for right now, just match the upload to the nn address by convention. There is no need to
    // directly tie them together by adding a pair class.
    List<Future<TransferFsImage.TransferResult>> uploads =
        new ArrayList<Future<TransferFsImage.TransferResult>>();
    for (final URL activeNNAddress : activeNNAddresses) {
      Future<TransferFsImage.TransferResult> upload =
          executor.submit(new Callable<TransferFsImage.TransferResult>() {
            @Override
            public TransferFsImage.TransferResult call()
                throws IOException, InterruptedException {
              CheckpointFaultInjector.getInstance().duringUploadInProgess();
              return TransferFsImage.uploadImageFromStorage(activeNNAddress, conf, namesystem
                  .getFSImage().getStorage(), imageType, txid, canceler);
            }
          });
      uploads.add(upload);
    }
    InterruptedException ie = null;
    IOException ioe= null;
    int i = 0;
    boolean success = false;
    for (; i < uploads.size(); i++) {
      Future<TransferFsImage.TransferResult> upload = uploads.get(i);
      try {
        // TODO should there be some smarts here about retries nodes that are not the active NN?
        if (upload.get() == TransferFsImage.TransferResult.SUCCESS) {
          success = true;
          //avoid getting the rest of the results - we don't care since we had a successful upload
          break;
        }

      } catch (ExecutionException e) {
        ioe = new IOException("Exception during image upload: " + e.getMessage(),
            e.getCause());
        break;
      } catch (InterruptedException e) {
        ie = e;
        break;
      }
    }
    if (ie == null && ioe == null) {
      //Update only when response from remote about success or
      lastUploadTime = monotonicNow();
      // we are primary if we successfully updated the ANN
      this.isPrimaryCheckPointer = success;
    }
    // cleaner than copying code for multiple catch statements and better than catching all
    // exceptions, so we just handle the ones we expect.
    if (ie != null || ioe != null) {

      // cancel the rest of the tasks, and close the pool
      for (; i < uploads.size(); i++) {
        Future<TransferFsImage.TransferResult> upload = uploads.get(i);
        // The background thread may be blocked waiting in the throttler, so
        // interrupt it.
        upload.cancel(true);
      }

      // shutdown so we interrupt anything running and don't start anything new
      executor.shutdownNow();
      // this is a good bit longer than the thread timeout, just to make sure all the threads
      // that are not doing any work also stop
      executor.awaitTermination(500, TimeUnit.MILLISECONDS);

      // re-throw the exception we got, since one of these two must be non-null
      if (ie != null) {
        throw ie;
      } else if (ioe != null) {
        throw ioe;
      }
    }
  }
  
  /**
   * Cancel any checkpoint that's currently being made,
   * and prevent any new checkpoints from starting for the next
   * minute or so.
   */
  public void cancelAndPreventCheckpoints(String msg) throws ServiceFailedException {
    synchronized (cancelLock) {
      // The checkpointer thread takes this lock and checks if checkpointing is
      // postponed. 
      thread.preventCheckpointsFor(PREVENT_AFTER_CANCEL_MS);

      // Before beginning a checkpoint, the checkpointer thread
      // takes this lock, and creates a canceler object.
      // If the canceler is non-null, then a checkpoint is in
      // progress and we need to cancel it. If it's null, then
      // the operation has not started, meaning that the above
      // time-based prevention will take effect.
      if (canceler != null) {
        canceler.cancel(msg);
      }
    }
  }
  
  @VisibleForTesting
  static int getCanceledCount() {
    return canceledCount;
  }

  private long countUncheckpointedTxns() {
    FSImage img = namesystem.getFSImage();
    return img.getCorrectLastAppliedOrWrittenTxId() -
      img.getStorage().getMostRecentCheckpointTxId();
  }

  private class CheckpointerThread extends Thread {
    private volatile boolean shouldRun = true;
    private volatile long preventCheckpointsUntil = 0;

    private CheckpointerThread() {
      super("Standby State Checkpointer");
    }
    
    private void setShouldRun(boolean shouldRun) {
      this.shouldRun = shouldRun;
    }

    @Override
    public void run() {
      // We have to make sure we're logged in as far as JAAS
      // is concerned, in order to use kerberized SSL properly.
      SecurityUtil.doAsLoginUserOrFatal(
          new PrivilegedAction<Object>() {
          @Override
          public Object run() {
            doWork();
            return null;
          }
        });
    }

    /**
     * Prevent checkpoints from occurring for some time period
     * in the future. This is used when preparing to enter active
     * mode. We need to not only cancel any concurrent checkpoint,
     * but also prevent any checkpoints from racing to start just
     * after the cancel call.
     * 
     * @param delayMs the number of MS for which checkpoints will be
     * prevented
     */
    private void preventCheckpointsFor(long delayMs) {
      preventCheckpointsUntil = monotonicNow() + delayMs;
    }

    private void doWork() {
      final long checkPeriod = 1000 * checkpointConf.getCheckPeriod();
      // Reset checkpoint time so that we don't always checkpoint
      // on startup.
      lastCheckpointTime = monotonicNow();
      lastUploadTime = monotonicNow();
      while (shouldRun) {
        boolean needRollbackCheckpoint = namesystem.isNeedRollbackFsImage();
        if (!needRollbackCheckpoint) {
          try {
            Thread.sleep(checkPeriod);
          } catch (InterruptedException ie) {
          }
          if (!shouldRun) {
            break;
          }
        }
        try {
          // We may have lost our ticket since last checkpoint, log in again, just in case
          if (UserGroupInformation.isSecurityEnabled()) {
            UserGroupInformation.getCurrentUser().checkTGTAndReloginFromKeytab();
          }
          
          final long now = monotonicNow();
          final long uncheckpointed = countUncheckpointedTxns();
          final long secsSinceLast = (now - lastCheckpointTime) / 1000;

          // if we need a rollback checkpoint, always attempt to checkpoint
          boolean needCheckpoint = needRollbackCheckpoint;

          if (needCheckpoint) {
            LOG.info("Triggering a rollback fsimage for rolling upgrade.");
          } else if (uncheckpointed >= checkpointConf.getTxnCount()) {
            LOG.info("Triggering checkpoint because there have been " + 
                uncheckpointed + " txns since the last checkpoint, which " +
                "exceeds the configured threshold " +
                checkpointConf.getTxnCount());
            needCheckpoint = true;
          } else if (secsSinceLast >= checkpointConf.getPeriod()) {
            LOG.info("Triggering checkpoint because it has been " +
                secsSinceLast + " seconds since the last checkpoint, which " +
                "exceeds the configured interval " + checkpointConf.getPeriod());
            needCheckpoint = true;
          }

          if (needCheckpoint) {
            synchronized (cancelLock) {
              if (now < preventCheckpointsUntil) {
                LOG.info("But skipping this checkpoint since we are about to failover!");
                canceledCount++;
                continue;
              }
              assert canceler == null;
              canceler = new Canceler();
            }

            // on all nodes, we build the checkpoint. However, we only ship the checkpoint if have a
            // rollback request, are the checkpointer, are outside the quiet period.
            final long secsSinceLastUpload = (now - lastUploadTime) / 1000;
            boolean sendRequest = isPrimaryCheckPointer
                || secsSinceLastUpload >= checkpointConf.getQuietPeriod();
            doCheckpoint(sendRequest);

            // reset needRollbackCheckpoint to false only when we finish a ckpt
            // for rollback image
            if (needRollbackCheckpoint
                && namesystem.getFSImage().hasRollbackFSImage()) {
              namesystem.setCreatedRollbackImages(true);
              namesystem.setNeedRollbackFsImage(false);
            }
            lastCheckpointTime = now;
          }
        } catch (SaveNamespaceCancelledException ce) {
          LOG.info("Checkpoint was cancelled: " + ce.getMessage());
          canceledCount++;
        } catch (InterruptedException ie) {
          LOG.info("Interrupted during checkpointing", ie);
          // Probably requested shutdown.
          continue;
        } catch (Throwable t) {
          LOG.error("Exception in doCheckpoint", t);
        } finally {
          synchronized (cancelLock) {
            canceler = null;
          }
        }
      }
    }
  }

  @VisibleForTesting
  List<URL> getActiveNNAddresses() {
    return activeNNAddresses;
  }
}
