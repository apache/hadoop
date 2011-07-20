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

package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader.EditLogValidation;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

class FSImageTransactionalStorageInspector extends FSImageStorageInspector {
  public static final Log LOG = LogFactory.getLog(
    FSImageTransactionalStorageInspector.class);

  private boolean needToSave = false;
  private boolean isUpgradeFinalized = true;
  
  List<FoundFSImage> foundImages = new ArrayList<FoundFSImage>();
  List<FoundEditLog> foundEditLogs = new ArrayList<FoundEditLog>();
  SortedMap<Long, LogGroup> logGroups = new TreeMap<Long, LogGroup>();
  long maxSeenTxId = 0;
  
  private static final Pattern IMAGE_REGEX = Pattern.compile(
    NameNodeFile.IMAGE.getName() + "_(\\d+)");
  private static final Pattern EDITS_REGEX = Pattern.compile(
    NameNodeFile.EDITS.getName() + "_(\\d+)-(\\d+)");
  private static final Pattern EDITS_INPROGRESS_REGEX = Pattern.compile(
    NameNodeFile.EDITS_INPROGRESS.getName() + "_(\\d+)");

  @Override
  public void inspectDirectory(StorageDirectory sd) throws IOException {
    // Was the directory just formatted?
    if (!sd.getVersionFile().exists()) {
      LOG.info("No version file in " + sd.getRoot());
      needToSave |= true;
      return;
    }
    
    File currentDir = sd.getCurrentDir();
    File filesInStorage[];
    try {
      filesInStorage = FileUtil.listFiles(currentDir);
    } catch (IOException ioe) {
      LOG.warn("Unable to inspect storage directory " + currentDir,
          ioe);
      return;
    }

    for (File f : filesInStorage) {
      LOG.debug("Checking file " + f);
      String name = f.getName();
      
      // Check for fsimage_*
      Matcher imageMatch = IMAGE_REGEX.matcher(name);
      if (imageMatch.matches()) {
        if (sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE)) {
          try {
            long txid = Long.valueOf(imageMatch.group(1));
            foundImages.add(new FoundFSImage(sd, f, txid));
          } catch (NumberFormatException nfe) {
            LOG.error("Image file " + f + " has improperly formatted " +
                      "transaction ID");
            // skip
          }
        } else {
          LOG.warn("Found image file at " + f + " but storage directory is " +
                   "not configured to contain images.");
        }
      }
    }
    

    // Check for a seen_txid file, which marks a minimum transaction ID that
    // must be included in our load plan.
    try {
      maxSeenTxId = Math.max(maxSeenTxId, NNStorage.readTransactionIdFile(sd));
    } catch (IOException ioe) {
      LOG.warn("Unable to determine the max transaction ID seen by " + sd, ioe);
    }
    
    List<FoundEditLog> editLogs = matchEditLogs(filesInStorage);
    if (sd.getStorageDirType().isOfType(NameNodeDirType.EDITS)) {
      for (FoundEditLog log : editLogs) {
        addEditLog(log);
      }
    } else if (!editLogs.isEmpty()){
      LOG.warn("Found the following edit log file(s) in " + sd +
          " even though it was not configured to store edits:\n" +
          "  " + Joiner.on("\n  ").join(editLogs));
          
    }
    
    // set finalized flag
    isUpgradeFinalized = isUpgradeFinalized && !sd.getPreviousDir().exists();
  }

  static List<FoundEditLog> matchEditLogs(File[] filesInStorage) {
    List<FoundEditLog> ret = Lists.newArrayList();
    for (File f : filesInStorage) {
      String name = f.getName();
      // Check for edits
      Matcher editsMatch = EDITS_REGEX.matcher(name);
      if (editsMatch.matches()) {
        try {
          long startTxId = Long.valueOf(editsMatch.group(1));
          long endTxId = Long.valueOf(editsMatch.group(2));
          ret.add(new FoundEditLog(f, startTxId, endTxId));
        } catch (NumberFormatException nfe) {
          LOG.error("Edits file " + f + " has improperly formatted " +
                    "transaction ID");
          // skip
        }          
      }
      
      // Check for in-progress edits
      Matcher inProgressEditsMatch = EDITS_INPROGRESS_REGEX.matcher(name);
      if (inProgressEditsMatch.matches()) {
        try {
          long startTxId = Long.valueOf(inProgressEditsMatch.group(1));
          ret.add(
            new FoundEditLog(f, startTxId, FoundEditLog.UNKNOWN_END));
        } catch (NumberFormatException nfe) {
          LOG.error("In-progress edits file " + f + " has improperly " +
                    "formatted transaction ID");
          // skip
        }          
      }
    }
    return ret;
  }

  private void addEditLog(FoundEditLog foundEditLog) {
    foundEditLogs.add(foundEditLog);
    LogGroup group = logGroups.get(foundEditLog.startTxId);
    if (group == null) {
      group = new LogGroup(foundEditLog.startTxId);
      logGroups.put(foundEditLog.startTxId, group);
    }
    group.add(foundEditLog);
  }


  @Override
  public boolean isUpgradeFinalized() {
    return isUpgradeFinalized;
  }
  
  /**
   * @return the image that has the most recent associated transaction ID.
   * If there are multiple storage directories which contain equal images 
   * the storage directory that was inspected first will be preferred.
   * 
   * Returns null if no images were found.
   */
  FoundFSImage getLatestImage() {
    FoundFSImage ret = null;
    for (FoundFSImage img : foundImages) {
      if (ret == null || img.txId > ret.txId) {
        ret = img;
      }
    }
    return ret;
  }
  
  public List<FoundFSImage> getFoundImages() {
    return ImmutableList.copyOf(foundImages);
  }
  
  public List<FoundEditLog> getFoundEditLogs() {
    return ImmutableList.copyOf(foundEditLogs);
  }

  @Override
  public LoadPlan createLoadPlan() throws IOException {
    if (foundImages.isEmpty()) {
      throw new FileNotFoundException("No valid image files found");
    }

    FoundFSImage recoveryImage = getLatestImage();
    LogLoadPlan logPlan = createLogLoadPlan(recoveryImage.txId, Long.MAX_VALUE);

    return new TransactionalLoadPlan(recoveryImage,
        logPlan);
  }
  
  /**
   * Plan which logs to load in order to bring the namespace up-to-date.
   * Transactions will be considered in the range (sinceTxId, maxTxId]
   * 
   * @param sinceTxId the highest txid that is already loaded 
   *                  (eg from the image checkpoint)
   * @param maxStartTxId ignore any log files that start after this txid
   */
  LogLoadPlan createLogLoadPlan(long sinceTxId, long maxStartTxId) throws IOException {
    long expectedTxId = sinceTxId + 1;
    
    List<FoundEditLog> recoveryLogs = new ArrayList<FoundEditLog>();
    
    SortedMap<Long, LogGroup> tailGroups = logGroups.tailMap(expectedTxId);
    if (logGroups.size() > tailGroups.size()) {
      LOG.debug("Excluded " + (logGroups.size() - tailGroups.size()) + 
          " groups of logs because they start with a txid less than image " +
          "txid " + sinceTxId);
    }
    
    SortedMap<Long, LogGroup> usefulGroups;
    if (maxStartTxId > sinceTxId) {
      usefulGroups = tailGroups.headMap(maxStartTxId);
    } else {
      usefulGroups = new TreeMap<Long, LogGroup>();
    }
    
    if (usefulGroups.size() > tailGroups.size()) {
      LOG.debug("Excluded " + (tailGroups.size() - usefulGroups.size()) + 
        " groups of logs because they start with a txid higher than max " +
        "txid " + sinceTxId);
    }


    for (Map.Entry<Long, LogGroup> entry : usefulGroups.entrySet()) {
      long logStartTxId = entry.getKey();
      LogGroup logGroup = entry.getValue();
      
      logGroup.planRecovery();
      
      if (expectedTxId != FSConstants.INVALID_TXID && logStartTxId != expectedTxId) {
        throw new IOException("Expected next log group would start at txid " +
            expectedTxId + " but starts at txid " + logStartTxId);
      }
      
      // We can pick any of the non-corrupt logs here
      recoveryLogs.add(logGroup.getBestNonCorruptLog());
      
      // If this log group was finalized, we know to expect the next
      // log group to start at the following txid (ie no gaps)
      if (logGroup.hasKnownLastTxId()) {
        expectedTxId = logGroup.getLastTxId() + 1;
      } else {
        // the log group was in-progress so we don't know what ID
        // the next group should start from.
        expectedTxId = FSConstants.INVALID_TXID;
      }
    }
    
    long lastLogGroupStartTxId = usefulGroups.isEmpty() ?
        0 : usefulGroups.lastKey();
    if (maxSeenTxId > sinceTxId &&
        maxSeenTxId > lastLogGroupStartTxId) {
      String msg = "At least one storage directory indicated it has seen a " +
        "log segment starting at txid " + maxSeenTxId;
      if (usefulGroups.isEmpty()) {
        msg += " but there are no logs to load.";
      } else {
        msg += " but the most recent log file found starts with txid " +
          lastLogGroupStartTxId;
      }
      throw new IOException(msg);
    }
    
    return new LogLoadPlan(recoveryLogs,
        Lists.newArrayList(usefulGroups.values()));

  }

  @Override
  public boolean needToSave() {
    return needToSave;
  }
  
  
  RemoteEditLogManifest getEditLogManifest(long sinceTxId) {
    List<RemoteEditLog> logs = Lists.newArrayList();
    for (LogGroup g : logGroups.values()) {
      if (!g.hasFinalized) continue;

      FoundEditLog fel = g.getBestNonCorruptLog();
      if (fel.getLastTxId() < sinceTxId) continue;
      
      logs.add(new RemoteEditLog(fel.getStartTxId(),
          fel.getLastTxId()));
    }
    
    return new RemoteEditLogManifest(logs);
  }

  /**
   * A group of logs that all start at the same txid.
   * 
   * Handles determining which logs are corrupt and which should be considered
   * candidates for loading.
   */
  static class LogGroup {
    long startTxId;
    List<FoundEditLog> logs = new ArrayList<FoundEditLog>();;
    private Set<Long> endTxIds = new TreeSet<Long>();
    private boolean hasInProgress = false;
    private boolean hasFinalized = false;
        
    LogGroup(long startTxId) {
      this.startTxId = startTxId;
    }
    
    FoundEditLog getBestNonCorruptLog() {
      // First look for non-corrupt finalized logs
      for (FoundEditLog log : logs) {
        if (!log.isCorrupt() && !log.isInProgress()) {
          return log;
        }
      }
      // Then look for non-corrupt in-progress logs
      for (FoundEditLog log : logs) {
        if (!log.isCorrupt()) {
          return log;
        }
      }

      // We should never get here, because we don't get to the planning stage
      // without calling planRecovery first, and if we've called planRecovery,
      // we would have already thrown if there were no non-corrupt logs!
      throw new IllegalStateException(
        "No non-corrupt logs for txid " + startTxId);
    }

    /**
     * @return true if we can determine the last txid in this log group.
     */
    boolean hasKnownLastTxId() {
      for (FoundEditLog log : logs) {
        if (!log.isInProgress()) {
          return true;
        }
      }
      return false;
    }

    /**
     * @return the last txid included in the logs in this group
     * @throws IllegalStateException if it is unknown -
     *                               {@see #hasKnownLastTxId()}
     */
    long getLastTxId() {
      for (FoundEditLog log : logs) {
        if (!log.isInProgress()) {
          return log.lastTxId;
        }
      }
      throw new IllegalStateException("LogGroup only has in-progress logs");
    }

    
    void add(FoundEditLog log) {
      assert log.getStartTxId() == startTxId;
      logs.add(log);
      
      if (log.isInProgress()) {
        hasInProgress = true;
      } else {
        hasFinalized = true;
        endTxIds.add(log.lastTxId);
      }
    }
    
    void planRecovery() throws IOException {
      assert hasInProgress || hasFinalized;
      
      checkConsistentEndTxIds();
        
      if (hasFinalized && hasInProgress) {
        planMixedLogRecovery();
      } else if (!hasFinalized && hasInProgress) {
        planAllInProgressRecovery();
      } else if (hasFinalized && !hasInProgress) {
        LOG.debug("No recovery necessary for logs starting at txid " +
                  startTxId);
      }
    }

    /**
     * Recovery case for when some logs in the group were in-progress, and
     * others were finalized. This happens when one of the storage
     * directories fails.
     *
     * The in-progress logs in this case should be considered corrupt.
     */
    private void planMixedLogRecovery() throws IOException {
      for (FoundEditLog log : logs) {
        if (log.isInProgress()) {
          LOG.warn("Log at " + log.getFile() + " is in progress, but " +
                   "other logs starting at the same txid " + startTxId +
                   " are finalized. Moving aside.");
          log.markCorrupt();
        }
      }
    }
    
    /**
     * Recovery case for when all of the logs in the group were in progress.
     * This happens if the NN completely crashes and restarts. In this case
     * we check the non-zero lengths of each log file, and any logs that are
     * less than the max of these lengths are considered corrupt.
     */
    private void planAllInProgressRecovery() throws IOException {
      // We only have in-progress logs. We need to figure out which logs have
      // the latest data to reccover them
      LOG.warn("Logs beginning at txid " + startTxId + " were are all " +
               "in-progress (probably truncated due to a previous NameNode " +
               "crash)");
      if (logs.size() == 1) {
        // Only one log, it's our only choice!
        FoundEditLog log = logs.get(0);
        if (log.validateLog().numTransactions == 0) {
          // If it has no transactions, we should consider it corrupt just
          // to be conservative.
          // See comment below for similar case
          LOG.warn("Marking log at " + log.getFile() + " as corrupt since " +
              "it has no transactions in it.");
          log.markCorrupt();          
        }
        return;
      }

      long maxValidTxnCount = Long.MIN_VALUE;
      for (FoundEditLog log : logs) {
        long validTxnCount = log.validateLog().numTransactions;
        LOG.warn("  Log " + log.getFile() +
            " valid txns=" + validTxnCount +
            " valid len=" + log.validateLog().validLength);
        maxValidTxnCount = Math.max(maxValidTxnCount, validTxnCount);
      }        

      for (FoundEditLog log : logs) {
        long txns = log.validateLog().numTransactions;
        if (txns < maxValidTxnCount) {
          LOG.warn("Marking log at " + log.getFile() + " as corrupt since " +
                   "it is has only " + txns + " valid txns whereas another " +
                   "log has " + maxValidTxnCount);
          log.markCorrupt();
        } else if (txns == 0) {
          // this can happen if the NN crashes right after rolling a log
          // but before the START_LOG_SEGMENT txn is written. Since the log
          // is empty, we can just move it aside to its corrupt name.
          LOG.warn("Marking log at " + log.getFile() + " as corrupt since " +
              "it has no transactions in it.");
          log.markCorrupt();
        }
      }
    }

    /**
     * Check for the case when we have multiple finalized logs and they have
     * different ending transaction IDs. This violates an invariant that all
     * log directories should roll together. We should abort in this case.
     */
    private void checkConsistentEndTxIds() throws IOException {
      if (hasFinalized && endTxIds.size() > 1) {
        throw new IOException("More than one ending txid was found " +
            "for logs starting at txid " + startTxId + ". " +
            "Found: " + StringUtils.join(endTxIds, ','));
      }
    }

    void recover() throws IOException {
      for (FoundEditLog log : logs) {
        if (log.isCorrupt()) {
          log.moveAsideCorruptFile();
        } else if (log.isInProgress()) {
          log.finalizeLog();
        }
      }
    }    
  }

  /**
   * Record of an image that has been located and had its filename parsed.
   */
  static class FoundFSImage {
    final StorageDirectory sd;    
    final long txId;
    private final File file;
    
    FoundFSImage(StorageDirectory sd, File file, long txId) {
      assert txId >= 0 : "Invalid txid on " + file +": " + txId;
      
      this.sd = sd;
      this.txId = txId;
      this.file = file;
    } 
    
    File getFile() {
      return file;
    }

    public long getTxId() {
      return txId;
    }
    
    @Override
    public String toString() {
      return file.toString();
    }
  }
  
  /**
   * Record of an edit log that has been located and had its filename parsed.
   */
  static class FoundEditLog {
    File file;
    final long startTxId;
    long lastTxId;
    
    private EditLogValidation cachedValidation = null;
    private boolean isCorrupt = false;
    
    static final long UNKNOWN_END = -1;
    
    FoundEditLog(File file,
        long startTxId, long endTxId) {
      assert endTxId == UNKNOWN_END || endTxId >= startTxId;
      assert startTxId > 0;
      assert file != null;
      
      this.startTxId = startTxId;
      this.lastTxId = endTxId;
      this.file = file;
    }
    
    public void finalizeLog() throws IOException {
      long numTransactions = validateLog().numTransactions;
      long lastTxId = startTxId + numTransactions - 1;
      File dst = new File(file.getParentFile(),
          NNStorage.getFinalizedEditsFileName(startTxId, lastTxId));
      LOG.info("Finalizing edits log " + file + " by renaming to "
          + dst.getName());
      if (!file.renameTo(dst)) {
        throw new IOException("Couldn't finalize log " +
            file + " to " + dst);
      }
      this.lastTxId = lastTxId;
      file = dst;
    }

    long getStartTxId() {
      return startTxId;
    }
    
    long getLastTxId() {
      return lastTxId;
    }

    EditLogValidation validateLog() throws IOException {
      if (cachedValidation == null) {
        cachedValidation = FSEditLogLoader.validateEditLog(file);
      }
      return cachedValidation;
    }

    boolean isInProgress() {
      return (lastTxId == UNKNOWN_END);
    }

    File getFile() {
      return file;
    }
    
    void markCorrupt() {
      isCorrupt = true;
    }
    
    boolean isCorrupt() {
      return isCorrupt;
    }

    void moveAsideCorruptFile() throws IOException {
      assert isCorrupt;
    
      File src = file;
      File dst = new File(src.getParent(), src.getName() + ".corrupt");
      boolean success = src.renameTo(dst);
      if (!success) {
        throw new IOException(
          "Couldn't rename corrupt log " + src + " to " + dst);
      }
      file = dst;
    }
    
    @Override
    public String toString() {
      return file.toString();
    }
  }

  static class TransactionalLoadPlan extends LoadPlan {
    final FoundFSImage image;
    final LogLoadPlan logPlan;
    
    public TransactionalLoadPlan(FoundFSImage image,
        LogLoadPlan logPlan) {
      super();
      this.image = image;
      this.logPlan = logPlan;
    }

    @Override
    boolean doRecovery() throws IOException {
      logPlan.doRecovery();
      return false;
    }

    @Override
    File getImageFile() {
      return image.getFile();
    }

    @Override
    List<File> getEditsFiles() {
      return logPlan.getEditsFiles();
    }

    @Override
    StorageDirectory getStorageDirectoryForProperties() {
      return image.sd;
    }
  }
  
  static class LogLoadPlan {
    final List<FoundEditLog> editLogs;
    final List<LogGroup> logGroupsToRecover;
    
    LogLoadPlan(List<FoundEditLog> editLogs,
        List<LogGroup> logGroupsToRecover) {
      this.editLogs = editLogs;
      this.logGroupsToRecover = logGroupsToRecover;
    }

    public void doRecovery() throws IOException {
      for (LogGroup g : logGroupsToRecover) {
        g.recover();
      }
    }

    public List<File> getEditsFiles() {
      List<File> ret = new ArrayList<File>();
      for (FoundEditLog log : editLogs) {
        ret.add(log.getFile());
      }
      return ret;
    }
  }
}
