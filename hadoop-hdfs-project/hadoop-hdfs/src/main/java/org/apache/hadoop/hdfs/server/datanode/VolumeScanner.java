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

package org.apache.hadoop.hdfs.server.datanode;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.BlockScanner.Conf;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi.BlockIterator;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * VolumeScanner scans a single volume.  Each VolumeScanner has its own thread.<p/>
 * They are all managed by the DataNode's BlockScanner.
 */
public class VolumeScanner extends Thread {
  public static final Logger LOG =
      LoggerFactory.getLogger(VolumeScanner.class);

  /**
   * Number of seconds in a minute.
   */
  private final static int SECONDS_PER_MINUTE = 60;

  /**
   * Number of minutes in an hour.
   */
  private final static int MINUTES_PER_HOUR = 60;

  /**
   * Name of the block iterator used by this scanner.
   */
  private final static String BLOCK_ITERATOR_NAME = "scanner";

  /**
   * The configuration.
   */
  private final Conf conf;

  /**
   * The DataNode this VolumEscanner is associated with.
   */
  private final DataNode datanode;

  /**
   * A reference to the volume that we're scanning.
   */
  private final FsVolumeReference ref;

  /**
   * The volume that we're scanning.
   */
  final FsVolumeSpi volume;

  /**
   * The number of scanned bytes in each minute of the last hour.<p/>
   *
   * This array is managed as a circular buffer.  We take the monotonic time and
   * divide it up into one-minute periods.  Each entry in the array represents
   * how many bytes were scanned during that period.
   */
  private final long scannedBytes[] = new long[MINUTES_PER_HOUR];

  /**
   * The sum of all the values of scannedBytes.
   */
  private long scannedBytesSum = 0;

  /**
   * The throttler to use with BlockSender objects.
   */
  private final DataTransferThrottler throttler = new DataTransferThrottler(1);

  /**
   * The null output stream to use with BlockSender objects.
   */
  private final DataOutputStream nullStream =
      new DataOutputStream(new IOUtils.NullOutputStream());

  /**
   * The block iterators associated with this VolumeScanner.<p/>
   *
   * Each block pool has its own BlockIterator.
   */
  private final List<BlockIterator> blockIters =
      new LinkedList<BlockIterator>();

  /**
   * Blocks which are suspect.
   * The scanner prioritizes scanning these blocks.
   */
  private final LinkedHashSet<ExtendedBlock> suspectBlocks =
      new LinkedHashSet<ExtendedBlock>();

  /**
   * Blocks which were suspect which we have scanned.
   * This is used to avoid scanning the same suspect block over and over.
   */
  private final Cache<ExtendedBlock, Boolean> recentSuspectBlocks =
      CacheBuilder.newBuilder().maximumSize(1000)
        .expireAfterAccess(10, TimeUnit.MINUTES).build();

  /**
   * The current block iterator, or null if there is none.
   */
  private BlockIterator curBlockIter = null;

  /**
   * True if the thread is stopping.<p/>
   * Protected by this object's lock.
   */
  private boolean stopping = false;

  /**
   * The monotonic minute that the volume scanner was started on.
   */
  private long startMinute = 0;

  /**
   * The current minute, in monotonic terms.
   */
  private long curMinute = 0;

  /**
   * Handles scan results.
   */
  private final ScanResultHandler resultHandler;

  private final Statistics stats = new Statistics();

  static class Statistics {
    long bytesScannedInPastHour = 0;
    long blocksScannedInCurrentPeriod = 0;
    long blocksScannedSinceRestart = 0;
    long scansSinceRestart = 0;
    long scanErrorsSinceRestart = 0;
    long nextBlockPoolScanStartMs = -1;
    long blockPoolPeriodEndsMs = -1;
    ExtendedBlock lastBlockScanned = null;
    boolean eof = false;

    Statistics() {
    }

    Statistics(Statistics other) {
      this.bytesScannedInPastHour = other.bytesScannedInPastHour;
      this.blocksScannedInCurrentPeriod = other.blocksScannedInCurrentPeriod;
      this.blocksScannedSinceRestart = other.blocksScannedSinceRestart;
      this.scansSinceRestart = other.scansSinceRestart;
      this.scanErrorsSinceRestart = other.scanErrorsSinceRestart;
      this.nextBlockPoolScanStartMs = other.nextBlockPoolScanStartMs;
      this.blockPoolPeriodEndsMs = other.blockPoolPeriodEndsMs;
      this.lastBlockScanned = other.lastBlockScanned;
      this.eof = other.eof;
    }

    @Override
    public String toString() {
      return new StringBuilder().
          append("Statistics{").
          append("bytesScannedInPastHour=").append(bytesScannedInPastHour).
          append(", blocksScannedInCurrentPeriod=").
              append(blocksScannedInCurrentPeriod).
          append(", blocksScannedSinceRestart=").
              append(blocksScannedSinceRestart).
          append(", scansSinceRestart=").append(scansSinceRestart).
          append(", scanErrorsSinceRestart=").append(scanErrorsSinceRestart).
          append(", nextBlockPoolScanStartMs=").append(nextBlockPoolScanStartMs).
          append(", blockPoolPeriodEndsMs=").append(blockPoolPeriodEndsMs).
          append(", lastBlockScanned=").append(lastBlockScanned).
          append(", eof=").append(eof).
          append("}").toString();
    }
  }

  private static double positiveMsToHours(long ms) {
    if (ms <= 0) {
      return 0;
    } else {
      return TimeUnit.HOURS.convert(ms, TimeUnit.MILLISECONDS);
    }
  }

  public void printStats(StringBuilder p) {
    p.append(String.format("Block scanner information for volume %s with base" +
        " path %s%n", volume.getStorageID(), volume.getBasePath()));
    synchronized (stats) {
      p.append(String.format("Bytes verified in last hour       : %57d%n",
          stats.bytesScannedInPastHour));
      p.append(String.format("Blocks scanned in current period  : %57d%n",
          stats.blocksScannedInCurrentPeriod));
      p.append(String.format("Blocks scanned since restart      : %57d%n",
          stats.blocksScannedSinceRestart));
      p.append(String.format("Block pool scans since restart    : %57d%n",
          stats.scansSinceRestart));
      p.append(String.format("Block scan errors since restart   : %57d%n",
          stats.scanErrorsSinceRestart));
      if (stats.nextBlockPoolScanStartMs > 0) {
        p.append(String.format("Hours until next block pool scan  : %57.3f%n",
            positiveMsToHours(stats.nextBlockPoolScanStartMs -
                Time.monotonicNow())));
      }
      if (stats.blockPoolPeriodEndsMs > 0) {
        p.append(String.format("Hours until possible pool rescan  : %57.3f%n",
            positiveMsToHours(stats.blockPoolPeriodEndsMs -
                Time.now())));
      }
      p.append(String.format("Last block scanned                : %57s%n",
          ((stats.lastBlockScanned == null) ? "none" :
          stats.lastBlockScanned.toString())));
      p.append(String.format("More blocks to scan in period     : %57s%n",
          !stats.eof));
      p.append(System.lineSeparator());
    }
  }

  static class ScanResultHandler {
    private VolumeScanner scanner;

    public void setup(VolumeScanner scanner) {
      LOG.trace("Starting VolumeScanner {}",
          scanner.volume.getBasePath());
      this.scanner = scanner;
    }

    public void handle(ExtendedBlock block, IOException e) {
      FsVolumeSpi volume = scanner.volume;
      if (e == null) {
        LOG.trace("Successfully scanned {} on {}", block, volume.getBasePath());
        return;
      }
      // If the block does not exist anymore, then it's not an error.
      if (!volume.getDataset().contains(block)) {
        LOG.debug("Volume {}: block {} is no longer in the dataset.",
            volume.getBasePath(), block);
        return;
      }
      // If the block exists, the exception may due to a race with write:
      // The BlockSender got an old block path in rbw. BlockReceiver removed
      // the rbw block from rbw to finalized but BlockSender tried to open the
      // file before BlockReceiver updated the VolumeMap. The state of the
      // block can be changed again now, so ignore this error here. If there
      // is a block really deleted by mistake, DirectoryScan should catch it.
      if (e instanceof FileNotFoundException ) {
        LOG.info("Volume {}: verification failed for {} because of " +
                "FileNotFoundException.  This may be due to a race with write.",
            volume.getBasePath(), block);
        return;
      }
      LOG.warn("Reporting bad {} on {}", block, volume.getBasePath());
      try {
        scanner.datanode.reportBadBlocks(block);
      } catch (IOException ie) {
        // This is bad, but not bad enough to shut down the scanner.
        LOG.warn("Cannot report bad " + block.getBlockId(), e);
      }
    }
  }

  VolumeScanner(Conf conf, DataNode datanode, FsVolumeReference ref) {
    this.conf = conf;
    this.datanode = datanode;
    this.ref = ref;
    this.volume = ref.getVolume();
    ScanResultHandler handler;
    try {
      handler = conf.resultHandler.newInstance();
    } catch (Throwable e) {
      LOG.error("unable to instantiate {}", conf.resultHandler, e);
      handler = new ScanResultHandler();
    }
    this.resultHandler = handler;
    setName("VolumeScannerThread(" + volume.getBasePath() + ")");
    setDaemon(true);
  }

  private void saveBlockIterator(BlockIterator iter) {
    try {
      iter.save();
    } catch (IOException e) {
      LOG.warn("{}: error saving {}.", this, iter, e);
    }
  }

  private void expireOldScannedBytesRecords(long monotonicMs) {
    long newMinute =
        TimeUnit.MINUTES.convert(monotonicMs, TimeUnit.MILLISECONDS);
    if (curMinute == newMinute) {
      return;
    }
    // If a minute or more has gone past since we last updated the scannedBytes
    // array, zero out the slots corresponding to those minutes.
    for (long m = curMinute + 1; m <= newMinute; m++) {
      int slotIdx = (int)(m % MINUTES_PER_HOUR);
      LOG.trace("{}: updateScannedBytes is zeroing out slotIdx {}.  " +
              "curMinute = {}; newMinute = {}", this, slotIdx,
              curMinute, newMinute);
      scannedBytesSum -= scannedBytes[slotIdx];
      scannedBytes[slotIdx] = 0;
    }
    curMinute = newMinute;
  }

  /**
   * Find a usable block iterator.<p/>
   *
   * We will consider available block iterators in order.  This property is
   * important so that we don't keep rescanning the same block pool id over
   * and over, while other block pools stay unscanned.<p/>
   *
   * A block pool is always ready to scan if the iterator is not at EOF.  If
   * the iterator is at EOF, the block pool will be ready to scan when
   * conf.scanPeriodMs milliseconds have elapsed since the iterator was last
   * rewound.<p/>
   *
   * @return                     0 if we found a usable block iterator; the
   *                               length of time we should delay before
   *                               checking again otherwise.
   */
  private synchronized long findNextUsableBlockIter() {
    int numBlockIters = blockIters.size();
    if (numBlockIters == 0) {
      LOG.debug("{}: no block pools are registered.", this);
      return Long.MAX_VALUE;
    }
    int curIdx;
    if (curBlockIter == null) {
      curIdx = 0;
    } else {
      curIdx = blockIters.indexOf(curBlockIter);
      Preconditions.checkState(curIdx >= 0);
    }
    // Note that this has to be wall-clock time, not monotonic time.  This is
    // because the time saved in the cursor file is a wall-clock time.  We do
    // not want to save a monotonic time in the cursor file, because it resets
    // every time the machine reboots (on most platforms).
    long nowMs = Time.now();
    long minTimeoutMs = Long.MAX_VALUE;
    for (int i = 0; i < numBlockIters; i++) {
      int idx = (curIdx + i + 1) % numBlockIters;
      BlockIterator iter = blockIters.get(idx);
      if (!iter.atEnd()) {
        LOG.info("Now scanning bpid {} on volume {}",
            iter.getBlockPoolId(), volume.getBasePath());
        curBlockIter = iter;
        return 0L;
      }
      long iterStartMs = iter.getIterStartMs();
      long waitMs = (iterStartMs + conf.scanPeriodMs) - nowMs;
      if (waitMs <= 0) {
        iter.rewind();
        LOG.info("Now rescanning bpid {} on volume {}, after more than " +
            "{} hour(s)", iter.getBlockPoolId(), volume.getBasePath(),
            TimeUnit.HOURS.convert(conf.scanPeriodMs, TimeUnit.MILLISECONDS));
        curBlockIter = iter;
        return 0L;
      }
      minTimeoutMs = Math.min(minTimeoutMs, waitMs);
    }
    LOG.info("{}: no suitable block pools found to scan.  Waiting {} ms.",
        this, minTimeoutMs);
    return minTimeoutMs;
  }

  /**
   * Scan a block.
   *
   * @param cblock               The block to scan.
   * @param bytesPerSec          The bytes per second to scan at.
   *
   * @return                     The length of the block that was scanned, or
   *                               -1 if the block could not be scanned.
   */
  private long scanBlock(ExtendedBlock cblock, long bytesPerSec) {
    // 'cblock' has a valid blockId and block pool id, but we don't yet know the
    // genstamp the block is supposed to have.  Ask the FsDatasetImpl for this
    // information.
    ExtendedBlock block = null;
    try {
      Block b = volume.getDataset().getStoredBlock(
          cblock.getBlockPoolId(), cblock.getBlockId());
      if (b == null) {
        LOG.info("Replica {} was not found in the VolumeMap for volume {}",
            cblock, volume.getBasePath());
      } else {
        block = new ExtendedBlock(cblock.getBlockPoolId(), b);
      }
    } catch (FileNotFoundException e) {
      LOG.info("FileNotFoundException while finding block {} on volume {}",
          cblock, volume.getBasePath());
    } catch (IOException e) {
      LOG.warn("I/O error while finding block {} on volume {}",
            cblock, volume.getBasePath());
    }
    if (block == null) {
      return -1; // block not found.
    }
    BlockSender blockSender = null;
    try {
      blockSender = new BlockSender(block, 0, -1,
          false, true, true, datanode, null,
          CachingStrategy.newDropBehind());
      throttler.setBandwidth(bytesPerSec);
      long bytesRead = blockSender.sendBlock(nullStream, null, throttler);
      resultHandler.handle(block, null);
      return bytesRead;
    } catch (IOException e) {
      resultHandler.handle(block, e);
    } finally {
      IOUtils.cleanup(null, blockSender);
    }
    return -1;
  }

  @VisibleForTesting
  static boolean calculateShouldScan(String storageId, long targetBytesPerSec,
                   long scannedBytesSum, long startMinute, long curMinute) {
    long runMinutes = curMinute - startMinute;
    long effectiveBytesPerSec;
    if (runMinutes <= 0) {
      // avoid division by zero
      effectiveBytesPerSec = scannedBytesSum;
    } else {
      if (runMinutes > MINUTES_PER_HOUR) {
        // we only keep an hour's worth of rate information
        runMinutes = MINUTES_PER_HOUR;
      }
      effectiveBytesPerSec = scannedBytesSum /
          (SECONDS_PER_MINUTE * runMinutes);
    }

    boolean shouldScan = effectiveBytesPerSec <= targetBytesPerSec;
    LOG.trace("{}: calculateShouldScan: effectiveBytesPerSec = {}, and " +
        "targetBytesPerSec = {}.  startMinute = {}, curMinute = {}, " +
        "shouldScan = {}",
        storageId, effectiveBytesPerSec, targetBytesPerSec,
        startMinute, curMinute, shouldScan);
    return shouldScan;
  }

  /**
   * Run an iteration of the VolumeScanner loop.
   *
   * @param suspectBlock   A suspect block which we should scan, or null to
   *                       scan the next regularly scheduled block.
   *
   * @return     The number of milliseconds to delay before running the loop
   *               again, or 0 to re-run the loop immediately.
   */
  private long runLoop(ExtendedBlock suspectBlock) {
    long bytesScanned = -1;
    boolean scanError = false;
    ExtendedBlock block = null;
    try {
      long monotonicMs = Time.monotonicNow();
      expireOldScannedBytesRecords(monotonicMs);

      if (!calculateShouldScan(volume.getStorageID(), conf.targetBytesPerSec,
          scannedBytesSum, startMinute, curMinute)) {
        // If neededBytesPerSec is too low, then wait few seconds for some old
        // scannedBytes records to expire.
        return 30000L;
      }

      // Find a usable block pool to scan.
      if (suspectBlock != null) {
        block = suspectBlock;
      } else {
        if ((curBlockIter == null) || curBlockIter.atEnd()) {
          long timeout = findNextUsableBlockIter();
          if (timeout > 0) {
            LOG.trace("{}: no block pools are ready to scan yet.  Waiting " +
                "{} ms.", this, timeout);
            synchronized (stats) {
              stats.nextBlockPoolScanStartMs = Time.monotonicNow() + timeout;
            }
            return timeout;
          }
          synchronized (stats) {
            stats.scansSinceRestart++;
            stats.blocksScannedInCurrentPeriod = 0;
            stats.nextBlockPoolScanStartMs = -1;
          }
          return 0L;
        }
        try {
          block = curBlockIter.nextBlock();
        } catch (IOException e) {
          // There was an error listing the next block in the volume.  This is a
          // serious issue.
          LOG.warn("{}: nextBlock error on {}", this, curBlockIter);
          // On the next loop iteration, curBlockIter#eof will be set to true, and
          // we will pick a different block iterator.
          return 0L;
        }
        if (block == null) {
          // The BlockIterator is at EOF.
          LOG.info("{}: finished scanning block pool {}",
              this, curBlockIter.getBlockPoolId());
          saveBlockIterator(curBlockIter);
          return 0;
        }
      }
      if (curBlockIter != null) {
        long saveDelta = monotonicMs - curBlockIter.getLastSavedMs();
        if (saveDelta >= conf.cursorSaveMs) {
          LOG.debug("{}: saving block iterator {} after {} ms.",
              this, curBlockIter, saveDelta);
          saveBlockIterator(curBlockIter);
        }
      }
      bytesScanned = scanBlock(block, conf.targetBytesPerSec);
      if (bytesScanned >= 0) {
        scannedBytesSum += bytesScanned;
        scannedBytes[(int)(curMinute % MINUTES_PER_HOUR)] += bytesScanned;
      } else {
        scanError = true;
      }
      return 0L;
    } finally {
      synchronized (stats) {
        stats.bytesScannedInPastHour = scannedBytesSum;
        if (bytesScanned > 0) {
          stats.blocksScannedInCurrentPeriod++;
          stats.blocksScannedSinceRestart++;
        }
        if (scanError) {
          stats.scanErrorsSinceRestart++;
        }
        if (block != null) {
          stats.lastBlockScanned = block;
        }
        if (curBlockIter == null) {
          stats.eof = true;
          stats.blockPoolPeriodEndsMs = -1;
        } else {
          stats.eof = curBlockIter.atEnd();
          stats.blockPoolPeriodEndsMs =
              curBlockIter.getIterStartMs() + conf.scanPeriodMs;
        }
      }
    }
  }

  /**
   * If there are elements in the suspectBlocks list, removes
   * and returns the first one.  Otherwise, returns null.
   */
  private synchronized ExtendedBlock popNextSuspectBlock() {
    Iterator<ExtendedBlock> iter = suspectBlocks.iterator();
    if (!iter.hasNext()) {
      return null;
    }
    ExtendedBlock block = iter.next();
    iter.remove();
    return block;
  }

  @Override
  public void run() {
    // Record the minute on which the scanner started.
    this.startMinute =
        TimeUnit.MINUTES.convert(Time.monotonicNow(), TimeUnit.MILLISECONDS);
    this.curMinute = startMinute;
    try {
      LOG.trace("{}: thread starting.", this);
      resultHandler.setup(this);
      try {
        long timeout = 0;
        while (true) {
          ExtendedBlock suspectBlock = null;
          // Take the lock to check if we should stop, and access the
          // suspect block list.
          synchronized (this) {
            if (stopping) {
              break;
            }
            if (timeout > 0) {
              wait(timeout);
              if (stopping) {
                break;
              }
            }
            suspectBlock = popNextSuspectBlock();
          }
          timeout = runLoop(suspectBlock);
        }
      } catch (InterruptedException e) {
        // We are exiting because of an InterruptedException,
        // probably sent by VolumeScanner#shutdown.
        LOG.trace("{} exiting because of InterruptedException.", this);
      } catch (Throwable e) {
        LOG.error("{} exiting because of exception ", this, e);
      }
      LOG.info("{} exiting.", this);
      // Save the current position of all block iterators and close them.
      for (BlockIterator iter : blockIters) {
        saveBlockIterator(iter);
        IOUtils.cleanup(null, iter);
      }
    } finally {
      // When the VolumeScanner exits, release the reference we were holding
      // on the volume.  This will allow the volume to be removed later.
      IOUtils.cleanup(null, ref);
    }
  }

  @Override
  public String toString() {
    return "VolumeScanner(" + volume.getBasePath() +
        ", " + volume.getStorageID() + ")";
  }

  /**
   * Shut down this scanner.
   */
  public synchronized void shutdown() {
    stopping = true;
    notify();
    this.interrupt();
  }


  public synchronized void markSuspectBlock(ExtendedBlock block) {
    if (stopping) {
      LOG.debug("{}: Not scheduling suspect block {} for " +
          "rescanning, because this volume scanner is stopping.", this, block);
      return;
    }
    Boolean recent = recentSuspectBlocks.getIfPresent(block);
    if (recent != null) {
      LOG.debug("{}: Not scheduling suspect block {} for " +
          "rescanning, because we rescanned it recently.", this, block);
      return;
    }
    if (suspectBlocks.contains(block)) {
      LOG.debug("{}: suspect block {} is already queued for " +
          "rescanning.", this, block);
      return;
    }
    suspectBlocks.add(block);
    recentSuspectBlocks.put(block, true);
    LOG.debug("{}: Scheduling suspect block {} for rescanning.", this, block);
    notify(); // wake scanner thread.
  }

  /**
   * Allow the scanner to scan the given block pool.
   *
   * @param bpid       The block pool id.
   */
  public synchronized void enableBlockPoolId(String bpid) {
    for (BlockIterator iter : blockIters) {
      if (iter.getBlockPoolId().equals(bpid)) {
        LOG.warn("{}: already enabled scanning on block pool {}", this, bpid);
        return;
      }
    }
    BlockIterator iter = null;
    try {
      // Load a block iterator for the next block pool on the volume.
      iter = volume.loadBlockIterator(bpid, BLOCK_ITERATOR_NAME);
      LOG.trace("{}: loaded block iterator for {}.", this, bpid);
    } catch (FileNotFoundException e) {
      LOG.debug("{}: failed to load block iterator: " + e.getMessage(), this);
    } catch (IOException e) {
      LOG.warn("{}: failed to load block iterator.", this, e);
    }
    if (iter == null) {
      iter = volume.newBlockIterator(bpid, BLOCK_ITERATOR_NAME);
      LOG.trace("{}: created new block iterator for {}.", this, bpid);
    }
    iter.setMaxStalenessMs(conf.maxStalenessMs);
    blockIters.add(iter);
    notify();
  }

  /**
   * Disallow the scanner from scanning the given block pool.
   *
   * @param bpid       The block pool id.
   */
  public synchronized void disableBlockPoolId(String bpid) {
    Iterator<BlockIterator> i = blockIters.iterator();
    while (i.hasNext()) {
      BlockIterator iter = i.next();
      if (iter.getBlockPoolId().equals(bpid)) {
        LOG.trace("{}: disabling scanning on block pool {}", this, bpid);
        i.remove();
        IOUtils.cleanup(null, iter);
        if (curBlockIter == iter) {
          curBlockIter = null;
        }
        notify();
        return;
      }
    }
    LOG.warn("{}: can't remove block pool {}, because it was never " +
        "added.", this, bpid);
  }

  @VisibleForTesting
  Statistics getStatistics() {
    synchronized (stats) {
      return new Statistics(stats);
    }
  }
}
