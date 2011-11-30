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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Comparator;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorageRetentionManager.StoragePurger;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader.EditLogValidation;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.ComparisonChain;

/**
 * Journal manager for the common case of edits files being written
 * to a storage directory.
 * 
 * Note: this class is not thread-safe and should be externally
 * synchronized.
 */
class FileJournalManager implements JournalManager {
  private static final Log LOG = LogFactory.getLog(FileJournalManager.class);

  private final StorageDirectory sd;
  private int outputBufferCapacity = 512*1024;

  private static final Pattern EDITS_REGEX = Pattern.compile(
    NameNodeFile.EDITS.getName() + "_(\\d+)-(\\d+)");
  private static final Pattern EDITS_INPROGRESS_REGEX = Pattern.compile(
    NameNodeFile.EDITS_INPROGRESS.getName() + "_(\\d+)");

  private File currentInProgress = null;
  private long maxSeenTransaction = 0L;

  @VisibleForTesting
  StoragePurger purger
    = new NNStorageRetentionManager.DeletionStoragePurger();

  public FileJournalManager(StorageDirectory sd) {
    this.sd = sd;
  }

  @Override 
  public void close() throws IOException {}

  @Override
  synchronized public EditLogOutputStream startLogSegment(long txid) 
      throws IOException {
    currentInProgress = NNStorage.getInProgressEditsFile(sd, txid);
    EditLogOutputStream stm = new EditLogFileOutputStream(currentInProgress,
        outputBufferCapacity);
    stm.create();
    return stm;
  }

  @Override
  synchronized public void finalizeLogSegment(long firstTxId, long lastTxId)
      throws IOException {
    File inprogressFile = NNStorage.getInProgressEditsFile(sd, firstTxId);

    File dstFile = NNStorage.getFinalizedEditsFile(
        sd, firstTxId, lastTxId);
    LOG.info("Finalizing edits file " + inprogressFile + " -> " + dstFile);
    
    Preconditions.checkState(!dstFile.exists(),
        "Can't finalize edits file " + inprogressFile + " since finalized file " +
        "already exists");
    if (!inprogressFile.renameTo(dstFile)) {
      throw new IOException("Unable to finalize edits file " + inprogressFile);
    }
    if (inprogressFile.equals(currentInProgress)) {
      currentInProgress = null;
    }
  }

  @VisibleForTesting
  public StorageDirectory getStorageDirectory() {
    return sd;
  }

  @Override
  synchronized public void setOutputBufferCapacity(int size) {
    this.outputBufferCapacity = size;
  }

  @Override
  public void purgeLogsOlderThan(long minTxIdToKeep)
      throws IOException {
    LOG.info("Purging logs older than " + minTxIdToKeep);
    File[] files = FileUtil.listFiles(sd.getCurrentDir());
    List<EditLogFile> editLogs = 
      FileJournalManager.matchEditLogs(files);
    for (EditLogFile log : editLogs) {
      if (log.getFirstTxId() < minTxIdToKeep &&
          log.getLastTxId() < minTxIdToKeep) {
        purger.purgeLog(log);
      }
    }
  }

  /**
   * Find all editlog segments starting at or above the given txid.
   * @param fromTxId the txnid which to start looking
   * @return a list of remote edit logs
   * @throws IOException if edit logs cannot be listed.
   */
  List<RemoteEditLog> getRemoteEditLogs(long firstTxId) throws IOException {
    File currentDir = sd.getCurrentDir();
    List<EditLogFile> allLogFiles = matchEditLogs(
        FileUtil.listFiles(currentDir));
    List<RemoteEditLog> ret = Lists.newArrayListWithCapacity(
        allLogFiles.size());

    for (EditLogFile elf : allLogFiles) {
      if (elf.isCorrupt() || elf.isInProgress()) continue;
      if (elf.getFirstTxId() >= firstTxId) {
        ret.add(new RemoteEditLog(elf.firstTxId, elf.lastTxId));
      } else if ((firstTxId > elf.getFirstTxId()) &&
                 (firstTxId <= elf.getLastTxId())) {
        throw new IOException("Asked for firstTxId " + firstTxId
            + " which is in the middle of file " + elf.file);
      }
    }
    
    return ret;
  }

  static List<EditLogFile> matchEditLogs(File[] filesInStorage) {
    List<EditLogFile> ret = Lists.newArrayList();
    for (File f : filesInStorage) {
      String name = f.getName();
      // Check for edits
      Matcher editsMatch = EDITS_REGEX.matcher(name);
      if (editsMatch.matches()) {
        try {
          long startTxId = Long.valueOf(editsMatch.group(1));
          long endTxId = Long.valueOf(editsMatch.group(2));
          ret.add(new EditLogFile(f, startTxId, endTxId));
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
              new EditLogFile(f, startTxId, startTxId, true));
        } catch (NumberFormatException nfe) {
          LOG.error("In-progress edits file " + f + " has improperly " +
                    "formatted transaction ID");
          // skip
        }
      }
    }
    return ret;
  }

  @Override
  synchronized public EditLogInputStream getInputStream(long fromTxId)
      throws IOException {
    for (EditLogFile elf : getLogFiles(fromTxId)) {
      if (elf.getFirstTxId() == fromTxId) {
        if (elf.isInProgress()) {
          elf.validateLog();
        }
        if (LOG.isTraceEnabled()) {
          LOG.trace("Returning edit stream reading from " + elf);
        }
        return new EditLogFileInputStream(elf.getFile(), 
            elf.getFirstTxId(), elf.getLastTxId(), elf.isInProgress());
      }
    }

    throw new IOException("Cannot find editlog file with " + fromTxId
        + " as first first txid");
  }

  @Override
  public long getNumberOfTransactions(long fromTxId) 
      throws IOException, CorruptionException {
    long numTxns = 0L;
    
    for (EditLogFile elf : getLogFiles(fromTxId)) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Counting " + elf);
      }
      if (elf.getFirstTxId() > fromTxId) { // there must be a gap
        LOG.warn("Gap in transactions in " + sd.getRoot() + ". Gap is "
            + fromTxId + " - " + (elf.getFirstTxId() - 1));
        break;
      } else if (fromTxId == elf.getFirstTxId()) {
        if (elf.isInProgress()) {
          elf.validateLog();
        } 

        if (elf.isCorrupt()) {
          break;
        }
        fromTxId = elf.getLastTxId() + 1;
        numTxns += fromTxId - elf.getFirstTxId();
        
        if (elf.isInProgress()) {
          break;
        }
      } // else skip
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Journal " + this + " has " + numTxns 
                + " txns from " + fromTxId);
    }

    long max = findMaxTransaction();
    
    // fromTxId should be greater than max, as it points to the next 
    // transaction we should expect to find. If it is less than or equal
    // to max, it means that a transaction with txid == max has not been found
    if (numTxns == 0 && fromTxId <= max) { 
      String error = String.format("Gap in transactions, max txnid is %d"
                                   + ", 0 txns from %d", max, fromTxId);
      LOG.error(error);
      throw new CorruptionException(error);
    }

    return numTxns;
  }

  @Override
  synchronized public void recoverUnfinalizedSegments() throws IOException {
    File currentDir = sd.getCurrentDir();
    List<EditLogFile> allLogFiles = matchEditLogs(currentDir.listFiles());
    
    // make sure journal is aware of max seen transaction before moving corrupt 
    // files aside
    findMaxTransaction();

    for (EditLogFile elf : allLogFiles) {
      if (elf.getFile().equals(currentInProgress)) {
        continue;
      }
      if (elf.isInProgress()) {
        elf.validateLog();

        if (elf.isCorrupt()) {
          elf.moveAsideCorruptFile();
          continue;
        }
        finalizeLogSegment(elf.getFirstTxId(), elf.getLastTxId());
      }
    }
  }

  private List<EditLogFile> getLogFiles(long fromTxId) throws IOException {
    File currentDir = sd.getCurrentDir();
    List<EditLogFile> allLogFiles = matchEditLogs(currentDir.listFiles());
    List<EditLogFile> logFiles = Lists.newArrayList();
    
    for (EditLogFile elf : allLogFiles) {
      if (fromTxId > elf.getFirstTxId()
          && fromTxId <= elf.getLastTxId()) {
        throw new IOException("Asked for fromTxId " + fromTxId
            + " which is in middle of file " + elf.file);
      }
      if (fromTxId <= elf.getFirstTxId()) {
        logFiles.add(elf);
      }
    }
    
    Collections.sort(logFiles, EditLogFile.COMPARE_BY_START_TXID);

    return logFiles;
  }

  /** 
   * Find the maximum transaction in the journal.
   * This gets stored in a member variable, as corrupt edit logs
   * will be moved aside, but we still need to remember their first
   * tranaction id in the case that it was the maximum transaction in
   * the journal.
   */
  private long findMaxTransaction()
      throws IOException {
    for (EditLogFile elf : getLogFiles(0)) {
      if (elf.isInProgress()) {
        maxSeenTransaction = Math.max(elf.getFirstTxId(), maxSeenTransaction);
        elf.validateLog();
      }
      maxSeenTransaction = Math.max(elf.getLastTxId(), maxSeenTransaction);
    }
    return maxSeenTransaction;
  }

  @Override
  public String toString() {
    return String.format("FileJournalManager(root=%s)", sd.getRoot());
  }

  /**
   * Record of an edit log that has been located and had its filename parsed.
   */
  static class EditLogFile {
    private File file;
    private final long firstTxId;
    private long lastTxId;

    private boolean isCorrupt = false;
    private final boolean isInProgress;

    final static Comparator<EditLogFile> COMPARE_BY_START_TXID 
      = new Comparator<EditLogFile>() {
      public int compare(EditLogFile a, EditLogFile b) {
        return ComparisonChain.start()
        .compare(a.getFirstTxId(), b.getFirstTxId())
        .compare(a.getLastTxId(), b.getLastTxId())
        .result();
      }
    };

    EditLogFile(File file,
        long firstTxId, long lastTxId) {
      this(file, firstTxId, lastTxId, false);
      assert (lastTxId != HdfsConstants.INVALID_TXID)
        && (lastTxId >= firstTxId);
    }
    
    EditLogFile(File file, long firstTxId, 
                long lastTxId, boolean isInProgress) { 
      assert (lastTxId == HdfsConstants.INVALID_TXID && isInProgress)
        || (lastTxId != HdfsConstants.INVALID_TXID && lastTxId >= firstTxId);
      assert (firstTxId > 0) || (firstTxId == HdfsConstants.INVALID_TXID);
      assert file != null;
      
      this.firstTxId = firstTxId;
      this.lastTxId = lastTxId;
      this.file = file;
      this.isInProgress = isInProgress;
    }
    
    long getFirstTxId() {
      return firstTxId;
    }
    
    long getLastTxId() {
      return lastTxId;
    }

    /** 
     * Count the number of valid transactions in a log.
     * This will update the lastTxId of the EditLogFile or
     * mark it as corrupt if it is.
     */
    void validateLog() throws IOException {
      EditLogValidation val = EditLogFileInputStream.validateEditLog(file);
      if (val.getNumTransactions() == 0) {
        markCorrupt();
      } else {
        this.lastTxId = val.getEndTxId();
      }
    }

    boolean isInProgress() {
      return isInProgress;
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
      return String.format("EditLogFile(file=%s,first=%019d,last=%019d,"
                           +"inProgress=%b,corrupt=%b)", file.toString(),
                           firstTxId, lastTxId, isInProgress(), isCorrupt);
    }
  }
}
