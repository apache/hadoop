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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SCANNER_VOLUME_BYTES_PER_SECOND;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SCANNER_VOLUME_BYTES_PER_SECOND_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_DEFAULT;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.VolumeScanner.ScanResultHandler;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@InterfaceAudience.Private
public class BlockScanner {
  public static final Logger LOG =
      LoggerFactory.getLogger(BlockScanner.class);

  /**
   * The DataNode that this scanner is associated with.
   */
  private final DataNode datanode;

  /**
   * Maps Storage IDs to VolumeScanner objects.
   */
  private final TreeMap<String, VolumeScanner> scanners =
      new TreeMap<String, VolumeScanner>();

  /**
   * The scanner configuration.
   */
  private Conf conf;

  @VisibleForTesting
  void setConf(Conf conf) {
    this.conf = conf;
    for (Entry<String, VolumeScanner> entry : scanners.entrySet()) {
      entry.getValue().setConf(conf);
    }
  }

  /**
   * The cached scanner configuration.
   */
  static class Conf {
    // These are a few internal configuration keys used for unit tests.
    // They can't be set unless the static boolean allowUnitTestSettings has
    // been set to true.

    @VisibleForTesting
    static final String INTERNAL_DFS_DATANODE_SCAN_PERIOD_MS =
        "internal.dfs.datanode.scan.period.ms.key";

    @VisibleForTesting
    static final String INTERNAL_VOLUME_SCANNER_SCAN_RESULT_HANDLER =
        "internal.volume.scanner.scan.result.handler";

    @VisibleForTesting
    static final String INTERNAL_DFS_BLOCK_SCANNER_MAX_STALENESS_MS =
        "internal.dfs.block.scanner.max_staleness.ms";

    @VisibleForTesting
    static final long INTERNAL_DFS_BLOCK_SCANNER_MAX_STALENESS_MS_DEFAULT =
        TimeUnit.MILLISECONDS.convert(15, TimeUnit.MINUTES);

    @VisibleForTesting
    static final String INTERNAL_DFS_BLOCK_SCANNER_CURSOR_SAVE_INTERVAL_MS =
        "dfs.block.scanner.cursor.save.interval.ms";

    @VisibleForTesting
    static final long
        INTERNAL_DFS_BLOCK_SCANNER_CURSOR_SAVE_INTERVAL_MS_DEFAULT =
            TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

    static boolean allowUnitTestSettings = false;
    final long targetBytesPerSec;
    final long maxStalenessMs;
    final long scanPeriodMs;
    final long cursorSaveMs;
    final Class<? extends ScanResultHandler> resultHandler;

    private static long getUnitTestLong(Configuration conf, String key,
                                        long defVal) {
      if (allowUnitTestSettings) {
        return conf.getLong(key, defVal);
      } else {
        return defVal;
      }
    }

    /**
     * Determine the configured block scanner interval.
     *
     * For compatibility with prior releases of HDFS, if the
     * configured value is zero then the scan period is
     * set to 3 weeks.
     *
     * If the configured value is less than zero then the scanner
     * is disabled.
     *
     * @param conf Configuration object.
     * @return block scan period in milliseconds.
     */
    private static long getConfiguredScanPeriodMs(Configuration conf) {
      long tempScanPeriodMs = getUnitTestLong(
          conf, INTERNAL_DFS_DATANODE_SCAN_PERIOD_MS,
              TimeUnit.MILLISECONDS.convert(conf.getLong(
                  DFS_DATANODE_SCAN_PERIOD_HOURS_KEY,
                  DFS_DATANODE_SCAN_PERIOD_HOURS_DEFAULT), TimeUnit.HOURS));

      if (tempScanPeriodMs == 0) {
        tempScanPeriodMs = TimeUnit.MILLISECONDS.convert(
            DFS_DATANODE_SCAN_PERIOD_HOURS_DEFAULT, TimeUnit.HOURS);
      }

      return tempScanPeriodMs;
    }

    @SuppressWarnings("unchecked")
    Conf(Configuration conf) {
      this.targetBytesPerSec = Math.max(0L, conf.getLong(
          DFS_BLOCK_SCANNER_VOLUME_BYTES_PER_SECOND,
          DFS_BLOCK_SCANNER_VOLUME_BYTES_PER_SECOND_DEFAULT));
      this.maxStalenessMs = Math.max(0L, getUnitTestLong(conf,
          INTERNAL_DFS_BLOCK_SCANNER_MAX_STALENESS_MS,
          INTERNAL_DFS_BLOCK_SCANNER_MAX_STALENESS_MS_DEFAULT));
      this.scanPeriodMs = getConfiguredScanPeriodMs(conf);
      this.cursorSaveMs = Math.max(0L, getUnitTestLong(conf,
          INTERNAL_DFS_BLOCK_SCANNER_CURSOR_SAVE_INTERVAL_MS,
          INTERNAL_DFS_BLOCK_SCANNER_CURSOR_SAVE_INTERVAL_MS_DEFAULT));
      if (allowUnitTestSettings) {
        this.resultHandler = (Class<? extends ScanResultHandler>)
            conf.getClass(INTERNAL_VOLUME_SCANNER_SCAN_RESULT_HANDLER,
                          ScanResultHandler.class);
      } else {
        this.resultHandler = ScanResultHandler.class;
      }
    }
  }

  public BlockScanner(DataNode datanode, Configuration conf) {
    this.datanode = datanode;
    this.conf = new Conf(conf);
    if (isEnabled()) {
      LOG.info("Initialized block scanner with targetBytesPerSec {}",
          this.conf.targetBytesPerSec);
    } else {
      LOG.info("Disabled block scanner.");
    }
  }

  /**
   * Returns true if the block scanner is enabled.<p/>
   *
   * If the block scanner is disabled, no volume scanners will be created, and
   * no threads will start.
   */
  public boolean isEnabled() {
    return (conf.scanPeriodMs > 0) && (conf.targetBytesPerSec > 0);
  }

 /**
  * Set up a scanner for the given block pool and volume.
  *
  * @param ref              A reference to the volume.
  */
  public synchronized void addVolumeScanner(FsVolumeReference ref) {
    boolean success = false;
    try {
      FsVolumeSpi volume = ref.getVolume();
      if (!isEnabled()) {
        LOG.debug("Not adding volume scanner for {}, because the block " +
            "scanner is disabled.", volume.getBasePath());
        return;
      }
      VolumeScanner scanner = scanners.get(volume.getStorageID());
      if (scanner != null) {
        LOG.error("Already have a scanner for volume {}.",
            volume.getBasePath());
        return;
      }
      LOG.debug("Adding scanner for volume {} (StorageID {})",
          volume.getBasePath(), volume.getStorageID());
      scanner = new VolumeScanner(conf, datanode, ref);
      scanner.start();
      scanners.put(volume.getStorageID(), scanner);
      success = true;
    } finally {
      if (!success) {
        // If we didn't create a new VolumeScanner object, we don't
        // need this reference to the volume.
        IOUtils.cleanup(null, ref);
      }
    }
  }

  /**
   * Stops and removes a volume scanner.<p/>
   *
   * This function will block until the volume scanner has stopped.
   *
   * @param volume           The volume to remove.
   */
  public synchronized void removeVolumeScanner(FsVolumeSpi volume) {
    if (!isEnabled()) {
      LOG.debug("Not removing volume scanner for {}, because the block " +
          "scanner is disabled.", volume.getStorageID());
      return;
    }
    VolumeScanner scanner = scanners.get(volume.getStorageID());
    if (scanner == null) {
      LOG.warn("No scanner found to remove for volumeId {}",
          volume.getStorageID());
      return;
    }
    LOG.info("Removing scanner for volume {} (StorageID {})",
        volume.getBasePath(), volume.getStorageID());
    scanner.shutdown();
    scanners.remove(volume.getStorageID());
    Uninterruptibles.joinUninterruptibly(scanner, 5, TimeUnit.MINUTES);
  }

  /**
   * Stops and removes all volume scanners.<p/>
   *
   * This function will block until all the volume scanners have stopped.
   */
  public synchronized void removeAllVolumeScanners() {
    for (Entry<String, VolumeScanner> entry : scanners.entrySet()) {
      entry.getValue().shutdown();
    }
    for (Entry<String, VolumeScanner> entry : scanners.entrySet()) {
      Uninterruptibles.joinUninterruptibly(entry.getValue(),
          5, TimeUnit.MINUTES);
    }
    scanners.clear();
  }

  /**
   * Enable scanning a given block pool id.
   *
   * @param bpid        The block pool id to enable scanning for.
   */
  synchronized void enableBlockPoolId(String bpid) {
    Preconditions.checkNotNull(bpid);
    for (VolumeScanner scanner : scanners.values()) {
      scanner.enableBlockPoolId(bpid);
    }
  }

  /**
   * Disable scanning a given block pool id.
   *
   * @param bpid        The block pool id to disable scanning for.
   */
  synchronized void disableBlockPoolId(String bpid) {
    Preconditions.checkNotNull(bpid);
    for (VolumeScanner scanner : scanners.values()) {
      scanner.disableBlockPoolId(bpid);
    }
  }

  @VisibleForTesting
  synchronized VolumeScanner.Statistics getVolumeStats(String volumeId) {
    VolumeScanner scanner = scanners.get(volumeId);
    if (scanner == null) {
      return null;
    }
    return scanner.getStatistics();
  }

  synchronized void printStats(StringBuilder p) {
    // print out all bpids that we're scanning ?
    for (Entry<String, VolumeScanner> entry : scanners.entrySet()) {
      entry.getValue().printStats(p);
    }
  }

  /**
   * Mark a block as "suspect."
   *
   * This means that we should try to rescan it soon.  Note that the
   * VolumeScanner keeps a list of recently suspicious blocks, which
   * it uses to avoid rescanning the same block over and over in a short
   * time frame.
   *
   * @param storageId     The ID of the storage where the block replica
   *                      is being stored.
   * @param block         The block's ID and block pool id.
   */
  synchronized void markSuspectBlock(String storageId, ExtendedBlock block) {
    if (!isEnabled()) {
      LOG.debug("Not scanning suspicious block {} on {}, because the block " +
          "scanner is disabled.", block, storageId);
      return;
    }
    VolumeScanner scanner = scanners.get(storageId);
    if (scanner == null) {
      // This could happen if the volume is in the process of being removed.
      // The removal process shuts down the VolumeScanner, but the volume
      // object stays around as long as there are references to it (which
      // should not be that long.)
      LOG.info("Not scanning suspicious block {} on {}, because there is no " +
          "volume scanner for that storageId.", block, storageId);
      return;
    }
    scanner.markSuspectBlock(block);
  }

  @InterfaceAudience.Private
  public static class Servlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Override
    public void doGet(HttpServletRequest request,
                      HttpServletResponse response) throws IOException {
      response.setContentType("text/plain");

      DataNode datanode = (DataNode)
          getServletContext().getAttribute("datanode");
      BlockScanner blockScanner = datanode.getBlockScanner();

      StringBuilder buffer = new StringBuilder(8 * 1024);
      if (!blockScanner.isEnabled()) {
        LOG.warn("Periodic block scanner is not running");
        buffer.append("Periodic block scanner is not running. " +
            "Please check the datanode log if this is unexpected.");
      } else {
        buffer.append("Block Scanner Statistics\n\n");
        blockScanner.printStats(buffer);
      }
      String resp = buffer.toString();
      LOG.trace("Returned Servlet info {}", resp);
      response.getWriter().write(resp);
    }
  }
}
