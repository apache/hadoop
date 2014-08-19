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

package org.apache.hadoop.fs.azure.metrics;

import java.util.ArrayList;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Internal implementation class to help calculate the current bytes
 * uploaded/downloaded and the maximum bandwidth gauges.
 */
@InterfaceAudience.Private
public final class BandwidthGaugeUpdater {
  public static final Log LOG = LogFactory
      .getLog(BandwidthGaugeUpdater.class);
  
  public static final String THREAD_NAME = "AzureNativeFilesystemStore-UploadBandwidthUpdater";
  
  private static final int DEFAULT_WINDOW_SIZE_MS = 1000;
  private static final int PROCESS_QUEUE_INITIAL_CAPACITY = 1000;
  private int windowSizeMs;
  private ArrayList<BlockTransferWindow> allBlocksWritten =
      createNewToProcessQueue();
  private ArrayList<BlockTransferWindow> allBlocksRead =
      createNewToProcessQueue();
  private final Object blocksWrittenLock = new Object();
  private final Object blocksReadLock = new Object();
  private final AzureFileSystemInstrumentation instrumentation;
  private Thread uploadBandwidthUpdater;
  private volatile boolean suppressAutoUpdate = false;

  /**
   * Create a new updater object with default values.
   * @param instrumentation The metrics source to update.
   */
  public BandwidthGaugeUpdater(AzureFileSystemInstrumentation instrumentation) {
    this(instrumentation, DEFAULT_WINDOW_SIZE_MS, false);
  }

  /**
   * Create a new updater object with some overrides (used in unit tests).
   * @param instrumentation The metrics source to update.
   * @param windowSizeMs The window size to use for calculating bandwidth
   *                    (in milliseconds).
   * @param manualUpdateTrigger If true, then this object won't create the
   *                            auto-update threads, and will wait for manual
   *                            calls to triggerUpdate to occur.
   */
  public BandwidthGaugeUpdater(AzureFileSystemInstrumentation instrumentation,
      int windowSizeMs, boolean manualUpdateTrigger) {
    this.windowSizeMs = windowSizeMs;
    this.instrumentation = instrumentation;
    if (!manualUpdateTrigger) {
      uploadBandwidthUpdater = new Thread(new UploadBandwidthUpdater(), THREAD_NAME);
      uploadBandwidthUpdater.setDaemon(true);
      uploadBandwidthUpdater.start();
    }
  }

  /**
   * Indicate that a block has been uploaded.
   * @param startDate The exact time the upload started.
   * @param endDate The exact time the upload ended.
   * @param length The number of bytes uploaded in the block.
   */
  public void blockUploaded(Date startDate, Date endDate, long length) {
    synchronized (blocksWrittenLock) {
      allBlocksWritten.add(new BlockTransferWindow(startDate, endDate, length));
    }
  }

  /**
   * Indicate that a block has been downloaded.
   * @param startDate The exact time the download started.
   * @param endDate The exact time the download ended.
   * @param length The number of bytes downloaded in the block.
   */
  public void blockDownloaded(Date startDate, Date endDate, long length) {
    synchronized (blocksReadLock) {
      allBlocksRead.add(new BlockTransferWindow(startDate, endDate, length));
    }
  }

  /**
   * Creates a new ArrayList to hold incoming block transfer notifications
   * before they're processed.
   * @return The newly created ArrayList.
   */
  private static ArrayList<BlockTransferWindow> createNewToProcessQueue() {
    return new ArrayList<BlockTransferWindow>(PROCESS_QUEUE_INITIAL_CAPACITY);
  }

  /**
   * Update the metrics source gauge for how many bytes were transferred
   * during the last time window.
   * @param updateWrite If true, update the write (upload) counter.
   *                    Otherwise update the read (download) counter.
   * @param bytes The number of bytes transferred.
   */
  private void updateBytesTransferred(boolean updateWrite, long bytes) {
    if (updateWrite) {
      instrumentation.updateBytesWrittenInLastSecond(bytes);
    }
    else {
      instrumentation.updateBytesReadInLastSecond(bytes);
    }
  }

  /**
   * Update the metrics source gauge for what the current transfer rate
   * is.
   * @param updateWrite If true, update the write (upload) counter.
   *                    Otherwise update the read (download) counter.
   * @param bytesPerSecond The number of bytes per second we're seeing.
   */
  private void updateBytesTransferRate(boolean updateWrite, long bytesPerSecond) {
    if (updateWrite) {
      instrumentation.currentUploadBytesPerSecond(bytesPerSecond);
    }
    else {
      instrumentation.currentDownloadBytesPerSecond(bytesPerSecond);
    }
  }

  /**
   * For unit test purposes, suppresses auto-update of the metrics
   * from the dedicated thread.
   */
  public void suppressAutoUpdate() {
    suppressAutoUpdate = true;
  }

  /**
   * Resumes auto-update (undo suppressAutoUpdate).
   */
  public void resumeAutoUpdate() {
    suppressAutoUpdate = false;
  }

  /**
   * Triggers the update of the metrics gauge based on all the blocks
   * uploaded/downloaded so far. This is typically done periodically in a
   * dedicated update thread, but exposing as public for unit test purposes.
   * 
   * @param updateWrite If true, we'll update the write (upload) metrics.
   *                    Otherwise we'll update the read (download) ones.
   */
  public void triggerUpdate(boolean updateWrite) {
    ArrayList<BlockTransferWindow> toProcess = null;
    synchronized (updateWrite ? blocksWrittenLock : blocksReadLock) {
      if (updateWrite && !allBlocksWritten.isEmpty()) {
        toProcess = allBlocksWritten;
        allBlocksWritten = createNewToProcessQueue();
      } else if (!updateWrite && !allBlocksRead.isEmpty()) {
        toProcess = allBlocksRead;
        allBlocksRead = createNewToProcessQueue();        
      }
    }

    // Check to see if we have any blocks to process.
    if (toProcess == null) {
      // Nothing to process, set the current bytes and rate to zero.
      updateBytesTransferred(updateWrite, 0);
      updateBytesTransferRate(updateWrite, 0);
      return;
    }

    // The cut-off time for when we want to calculate rates is one
    // window size ago from now.
    long cutoffTime = new Date().getTime() - windowSizeMs;

    // Go through all the blocks we're processing, and calculate the
    // total number of bytes processed as well as the maximum transfer
    // rate we experienced for any single block during our time window.
    long maxSingleBlockTransferRate = 0;
    long bytesInLastSecond = 0;
    for (BlockTransferWindow currentWindow : toProcess) {
      long windowDuration = currentWindow.getEndDate().getTime() 
          - currentWindow.getStartDate().getTime();
      if (windowDuration == 0) {
        // Edge case, assume it took 1 ms but we were too fast
        windowDuration = 1;
      }
      if (currentWindow.getStartDate().getTime() > cutoffTime) {
        // This block was transferred fully within our time window,
        // just add its bytes to the total.
        bytesInLastSecond += currentWindow.bytesTransferred;
      } else if (currentWindow.getEndDate().getTime() > cutoffTime) {
        // This block started its transfer before our time window,
        // interpolate to estimate how many bytes from that block
        // were actually transferred during our time window.
        long adjustedBytes = (currentWindow.getBytesTransferred() 
            * (currentWindow.getEndDate().getTime() - cutoffTime)) 
            / windowDuration;
        bytesInLastSecond += adjustedBytes;
      }
      // Calculate the transfer rate for this block.
      long currentBlockTransferRate =
          (currentWindow.getBytesTransferred() * 1000) / windowDuration;
      maxSingleBlockTransferRate =
          Math.max(maxSingleBlockTransferRate, currentBlockTransferRate);
    }
    updateBytesTransferred(updateWrite, bytesInLastSecond);
    // The transfer rate we saw in the last second is a tricky concept to
    // define: If we saw two blocks, one 2 MB block transferred in 0.2 seconds,
    // and one 4 MB block transferred in 0.2 seconds, then the maximum rate
    // is 20 MB/s (the 4 MB block), the average of the two blocks is 15 MB/s,
    // and the aggregate rate is 6 MB/s (total of 6 MB transferred in one
    // second). As a first cut, I'm taking the definition to be the maximum
    // of aggregate or of any single block's rate (so in the example case it's
    // 6 MB/s).
    long aggregateTransferRate = bytesInLastSecond;
    long maxObservedTransferRate =
        Math.max(aggregateTransferRate, maxSingleBlockTransferRate);
    updateBytesTransferRate(updateWrite, maxObservedTransferRate);
  }

  /**
   * A single block transfer.
   */
  private static final class BlockTransferWindow {
    private final Date startDate;
    private final Date endDate;
    private final long bytesTransferred;

    public BlockTransferWindow(Date startDate, Date endDate,
        long bytesTransferred) {
      this.startDate = startDate;
      this.endDate = endDate;
      this.bytesTransferred = bytesTransferred;
    }

    public Date getStartDate() { return startDate; }
    public Date getEndDate() { return endDate; }
    public long getBytesTransferred() { return bytesTransferred; }
  }

  /**
   * The auto-update thread.
   */
  private final class UploadBandwidthUpdater implements Runnable {
    @Override
    public void run() {
      try {
        while (true) {
          Thread.sleep(windowSizeMs);
          if (!suppressAutoUpdate) {
            triggerUpdate(true);
            triggerUpdate(false);
          }
        }
      } catch (InterruptedException e) {
      }
    }
  }

  public void close() {
    if (uploadBandwidthUpdater != null) {
      // Interrupt and join the updater thread in death.
      uploadBandwidthUpdater.interrupt();
      try {
        uploadBandwidthUpdater.join();
      } catch (InterruptedException e) {
      }
      uploadBandwidthUpdater = null;
    }
  }

}