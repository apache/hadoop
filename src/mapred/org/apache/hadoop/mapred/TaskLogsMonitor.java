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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.mapred.TaskLog.LogName;
import org.apache.hadoop.mapred.TaskLog.LogFileDetail;
import org.apache.hadoop.util.StringUtils;

class TaskLogsMonitor extends Thread {
  static final Log LOG = LogFactory.getLog(TaskLogsMonitor.class);

  long mapRetainSize, reduceRetainSize;

  public TaskLogsMonitor(long mapRetSize, long reduceRetSize) {
    mapRetainSize = mapRetSize;
    reduceRetainSize = reduceRetSize;
    LOG.info("Starting logs' monitor with mapRetainSize=" + mapRetainSize
        + " and reduceRetainSize=" + reduceRetSize);
  }

  /**
   * The list of tasks that have finished and so need their logs to be
   * truncated.
   */
  private Map<TaskAttemptID, PerJVMInfo> finishedJVMs =
      new HashMap<TaskAttemptID, PerJVMInfo>();

  private static final int DEFAULT_BUFFER_SIZE = 4 * 1024;

  static final int MINIMUM_RETAIN_SIZE_FOR_TRUNCATION = 0;

  private static class PerJVMInfo {

    List<Task> allAttempts;

    public PerJVMInfo(List<Task> allAtmpts) {
      this.allAttempts = allAtmpts;
    }
  }

  /**
   * Process(JVM/debug script) has finished. Asynchronously truncate the logs of
   * all the corresponding tasks to the configured limit. In case of JVM, both
   * the firstAttempt as well as the list of all attempts that ran in the same
   * JVM have to be passed. For debug script, the (only) attempt itself should
   * be passed as both the firstAttempt as well as the list of attempts.
   * 
   * @param firstAttempt
   * @param isTaskCleanup
   */
  void addProcessForLogTruncation(TaskAttemptID firstAttempt,
      List<Task> allAttempts) {
    LOG.info("Adding the jvm with first-attempt " + firstAttempt
        + " for logs' truncation");
    PerJVMInfo lInfo = new PerJVMInfo(allAttempts);
    synchronized (finishedJVMs) {
      finishedJVMs.put(firstAttempt, lInfo);
      finishedJVMs.notify();
    }
  }

  /**
   * Process the removed task's logs. This involves truncating them to
   * retainSize.
   */
  void truncateLogs(TaskAttemptID firstAttempt, PerJVMInfo lInfo) {

    // Read the log-file details for all the attempts that ran in this JVM
    Map<Task, Map<LogName, LogFileDetail>> taskLogFileDetails;
    try {
      taskLogFileDetails = getAllLogsFileDetails(lInfo.allAttempts);
    } catch (IOException e) {
      LOG.warn(
          "Exception in truncateLogs while getting allLogsFileDetails()."
              + " Ignoring the truncation of logs of this process.", e);
      return;
    }

    Map<Task, Map<LogName, LogFileDetail>> updatedTaskLogFileDetails =
        new HashMap<Task, Map<LogName, LogFileDetail>>();

    File attemptLogDir = TaskLog.getBaseDir(firstAttempt.toString());

    FileWriter tmpFileWriter;
    FileReader logFileReader;
    // Now truncate file by file
    logNameLoop: for (LogName logName : LogName.values()) {

      File logFile = TaskLog.getTaskLogFile(firstAttempt, logName);

      // //// Optimization: if no task is over limit, just skip truncation-code
      if (logFile.exists()
          && !isTruncationNeeded(lInfo, taskLogFileDetails, logName)) {
        LOG.debug("Truncation is not needed for "
            + logFile.getAbsolutePath());
        continue;
      }
      // //// End of optimization

      // Truncation is needed for this log-file. Go ahead now.
      File tmpFile = new File(attemptLogDir, "truncate.tmp");
      try {
        tmpFileWriter = new FileWriter(tmpFile);
      } catch (IOException ioe) {
        LOG.warn("Cannot open " + tmpFile.getAbsolutePath()
            + " for writing truncated log-file "
            + logFile.getAbsolutePath()
            + ". Continuing with other log files. ", ioe);
        continue;
      }

      try {
        logFileReader = new FileReader(logFile);
      } catch (FileNotFoundException fe) {
        LOG.warn("Cannot open " + logFile.getAbsolutePath()
            + " for reading. Continuing with other log files");
        if (!tmpFile.delete()) {
          LOG.warn("Cannot delete tmpFile " + tmpFile.getAbsolutePath());
        }
        continue;
      }

      long newCurrentOffset = 0;
      // Process each attempt from the ordered list passed.
      for (Task task : lInfo.allAttempts) {

        // Truncate the log files of this task-attempt so that only the last
        // retainSize many bytes of this log file is retained and the log
        // file is reduced in size saving disk space.
        long retainSize =
            (task.isMapTask() ? mapRetainSize : reduceRetainSize);
        LogFileDetail newLogFileDetail = new LogFileDetail();
        try {
          newLogFileDetail =
              truncateALogFileOfAnAttempt(task.getTaskID(),
                  taskLogFileDetails.get(task).get(logName), retainSize,
                  tmpFileWriter, logFileReader);
        } catch (IOException ioe) {
          LOG.warn("Cannot truncate the log file "
              + logFile.getAbsolutePath()
              + ". Caught exception while handling " + task.getTaskID(),
              ioe);
          // revert back updatedTaskLogFileDetails
          revertIndexFileInfo(lInfo, taskLogFileDetails,
              updatedTaskLogFileDetails, logName);
          if (!tmpFile.delete()) {
            LOG.warn("Cannot delete tmpFile " + tmpFile.getAbsolutePath());
          }
          continue logNameLoop;
        }

        // Track information for updating the index file properly.
        // Index files don't track DEBUGOUT and PROFILE logs, so skip'em.
        if (TaskLog.LOGS_TRACKED_BY_INDEX_FILES.contains(logName)) {
          if (!updatedTaskLogFileDetails.containsKey(task)) {
            updatedTaskLogFileDetails.put(task,
                new HashMap<LogName, LogFileDetail>());
          }
          // newLogFileDetail already has the location and length set, just
          // set the start offset now.
          newLogFileDetail.start = newCurrentOffset;
          updatedTaskLogFileDetails.get(task).put(logName, newLogFileDetail);
          newCurrentOffset += newLogFileDetail.length;
        }
      }

      try {
        tmpFileWriter.close();
      } catch (IOException ioe) {
        LOG.warn("Couldn't close the tmp file " + tmpFile.getAbsolutePath()
            + ". Deleting it.", ioe);
        revertIndexFileInfo(lInfo, taskLogFileDetails,
            updatedTaskLogFileDetails, logName);
        if (!tmpFile.delete()) {
          LOG.warn("Cannot delete tmpFile " + tmpFile.getAbsolutePath());
        }
        continue;
      }

      if (!tmpFile.renameTo(logFile)) {
        // If the tmpFile cannot be renamed revert back
        // updatedTaskLogFileDetails to maintain the consistency of the
        // original log file
        revertIndexFileInfo(lInfo, taskLogFileDetails,
            updatedTaskLogFileDetails, logName);
        if (!tmpFile.delete()) {
          LOG.warn("Cannot delete tmpFile " + tmpFile.getAbsolutePath());
        }
      }
    }

    // Update the index files
    updateIndicesAfterLogTruncation(firstAttempt, updatedTaskLogFileDetails);
  }

  /**
   * @param lInfo
   * @param taskLogFileDetails
   * @param updatedTaskLogFileDetails
   * @param logName
   */
  private void revertIndexFileInfo(PerJVMInfo lInfo,
      Map<Task, Map<LogName, LogFileDetail>> taskLogFileDetails,
      Map<Task, Map<LogName, LogFileDetail>> updatedTaskLogFileDetails,
      LogName logName) {
    if (TaskLog.LOGS_TRACKED_BY_INDEX_FILES.contains(logName)) {
      for (Task task : lInfo.allAttempts) {
        if (!updatedTaskLogFileDetails.containsKey(task)) {
          updatedTaskLogFileDetails.put(task,
              new HashMap<LogName, LogFileDetail>());
        }
        updatedTaskLogFileDetails.get(task).put(logName,
            taskLogFileDetails.get(task).get(logName));
      }
    }
  }

  /**
   * Get the logFileDetails of all the list of attempts passed.
   * 
   * @param lInfo
   * @return a map of task to the log-file detail
   * @throws IOException
   */
  private Map<Task, Map<LogName, LogFileDetail>> getAllLogsFileDetails(
      final List<Task> allAttempts) throws IOException {
    Map<Task, Map<LogName, LogFileDetail>> taskLogFileDetails =
        new HashMap<Task, Map<LogName, LogFileDetail>>();
    for (Task task : allAttempts) {
      Map<LogName, LogFileDetail> allLogsFileDetails;
      allLogsFileDetails =
          TaskLog.getAllLogsFileDetails(task.getTaskID(),
              task.isTaskCleanupTask());
      taskLogFileDetails.put(task, allLogsFileDetails);
    }
    return taskLogFileDetails;
  }

  /**
   * Check if truncation of logs is needed for the given jvmInfo. If all the
   * tasks that ran in a JVM are within the log-limits, then truncation is not
   * needed. Otherwise it is needed.
   * 
   * @param lInfo
   * @param taskLogFileDetails
   * @param logName
   * @return true if truncation is needed, false otherwise
   */
  private boolean isTruncationNeeded(PerJVMInfo lInfo,
      Map<Task, Map<LogName, LogFileDetail>> taskLogFileDetails,
      LogName logName) {
    boolean truncationNeeded = false;
    LogFileDetail logFileDetail = null;
    for (Task task : lInfo.allAttempts) {
      long taskRetainSize =
          (task.isMapTask() ? mapRetainSize : reduceRetainSize);
      Map<LogName, LogFileDetail> allLogsFileDetails =
          taskLogFileDetails.get(task);
      logFileDetail = allLogsFileDetails.get(logName);
      if (taskRetainSize > MINIMUM_RETAIN_SIZE_FOR_TRUNCATION
          && logFileDetail.length > taskRetainSize) {
        truncationNeeded = true;
        break;
      }
    }
    return truncationNeeded;
  }

  /**
   * Truncate the log file of this task-attempt so that only the last retainSize
   * many bytes of each log file is retained and the log file is reduced in size
   * saving disk space.
   * 
   * @param taskID Task whose logs need to be truncated
   * @param oldLogFileDetail contains the original log details for the attempt
   * @param taskRetainSize retain-size
   * @param tmpFileWriter New log file to write to. Already opened in append
   *          mode.
   * @param logFileReader Original log file to read from.
   * @return
   * @throws IOException
   */
  private LogFileDetail truncateALogFileOfAnAttempt(
      final TaskAttemptID taskID, final LogFileDetail oldLogFileDetail,
      final long taskRetainSize, final FileWriter tmpFileWriter,
      final FileReader logFileReader) throws IOException {
    LogFileDetail newLogFileDetail = new LogFileDetail();

    // ///////////// Truncate log file ///////////////////////

    // New location of log file is same as the old
    newLogFileDetail.location = oldLogFileDetail.location;
    if (taskRetainSize > MINIMUM_RETAIN_SIZE_FOR_TRUNCATION
        && oldLogFileDetail.length > taskRetainSize) {
      LOG.info("Truncating logs for " + taskID + " from "
          + oldLogFileDetail.length + "bytes to " + taskRetainSize
          + "bytes.");
      newLogFileDetail.length = taskRetainSize;
    } else {
      LOG.info("No truncation needed for " + taskID + " length is "
          + oldLogFileDetail.length + " retain size " + taskRetainSize
          + "bytes.");
      newLogFileDetail.length = oldLogFileDetail.length;
    }
    long charsSkipped =
        logFileReader.skip(oldLogFileDetail.length
            - newLogFileDetail.length);
    if (charsSkipped != oldLogFileDetail.length - newLogFileDetail.length) {
      throw new IOException("Erroneously skipped " + charsSkipped
          + " instead of the expected "
          + (oldLogFileDetail.length - newLogFileDetail.length));
    }
    long alreadyRead = 0;
    while (alreadyRead < newLogFileDetail.length) {
      char tmpBuf[]; // Temporary buffer to read logs
      if (newLogFileDetail.length - alreadyRead >= DEFAULT_BUFFER_SIZE) {
        tmpBuf = new char[DEFAULT_BUFFER_SIZE];
      } else {
        tmpBuf = new char[(int) (newLogFileDetail.length - alreadyRead)];
      }
      int bytesRead = logFileReader.read(tmpBuf);
      if (bytesRead < 0) {
        break;
      } else {
        alreadyRead += bytesRead;
      }
      tmpFileWriter.write(tmpBuf);
    }
    // ////// End of truncating log file ///////////////////////

    return newLogFileDetail;
  }

  /**
   * Truncation of logs is done. Now sync the index files to reflect the
   * truncated sizes.
   * 
   * @param firstAttempt
   * @param updatedTaskLogFileDetails
   */
  private void updateIndicesAfterLogTruncation(TaskAttemptID firstAttempt,
      Map<Task, Map<LogName, LogFileDetail>> updatedTaskLogFileDetails) {
    for (Entry<Task, Map<LogName, LogFileDetail>> entry : 
                                updatedTaskLogFileDetails.entrySet()) {
      Task task = entry.getKey();
      Map<LogName, LogFileDetail> logFileDetails = entry.getValue();
      Map<LogName, Long[]> logLengths = new HashMap<LogName, Long[]>();
      // set current and previous lengths
      for (LogName logName : TaskLog.LOGS_TRACKED_BY_INDEX_FILES) {
        logLengths.put(logName, new Long[] { Long.valueOf(0L),
            Long.valueOf(0L) });
        LogFileDetail lfd = logFileDetails.get(logName);
        if (lfd != null) {
          // Set previous lengths
          logLengths.get(logName)[0] = Long.valueOf(lfd.start);
          // Set current lengths
          logLengths.get(logName)[1] = Long.valueOf(lfd.start + lfd.length);
        }
      }
      try {
        TaskLog.writeToIndexFile(firstAttempt, task.getTaskID(),
            task.isTaskCleanupTask(), logLengths);
      } catch (IOException ioe) {
        LOG.warn("Exception in updateIndicesAfterLogTruncation : "
            + StringUtils.stringifyException(ioe));
        LOG.warn("Exception encountered while updating index file of task "
            + task.getTaskID()
            + ". Ignoring and continuing with other tasks.");
      }
    }
  }

  /**
   * 
   * @throws IOException
   */
  void monitorTaskLogs() throws IOException {

    Map<TaskAttemptID, PerJVMInfo> tasksBeingTruncated =
      new HashMap<TaskAttemptID, PerJVMInfo>();

    // Start monitoring newly added finishedJVMs
    synchronized (finishedJVMs) {
      tasksBeingTruncated.clear();
      tasksBeingTruncated.putAll(finishedJVMs);
      finishedJVMs.clear();
    }

    for (Entry<TaskAttemptID, PerJVMInfo> entry : 
                tasksBeingTruncated.entrySet()) {
      truncateLogs(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void run() {

    while (true) {
      try {
        monitorTaskLogs();
        try {
          synchronized (finishedJVMs) {
            while (finishedJVMs.isEmpty()) {
              finishedJVMs.wait();
            }
          }
        } catch (InterruptedException e) {
          LOG.warn(getName() + " is interrupted. Returning");
          return;
        }
      } catch (Throwable e) {
        LOG.warn(getName()
            + " encountered an exception while monitoring : "
            + StringUtils.stringifyException(e));
        LOG.info("Ingoring the exception and continuing monitoring.");
      }
    }
  }
}
