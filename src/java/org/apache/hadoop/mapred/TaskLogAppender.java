package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.ErrorCode;
import org.apache.log4j.spi.LoggingEvent;

/**
 * A simple log4j-appender for the task child's 
 * map-reduce system logs.
 * 
 * @author Arun C Murthy
 */
public class TaskLogAppender extends AppenderSkeleton {
  private TaskLog.Writer taskLogWriter = null;
  private String taskId;
  private int noKeepSplits;
  private long totalLogFileSize;
  private boolean purgeLogSplits;
  private int logsRetainHours;

  public void activateOptions() {
    taskLogWriter = 
      new TaskLog.Writer(taskId, TaskLog.LogFilter.SYSLOG, 
              noKeepSplits, totalLogFileSize, purgeLogSplits, logsRetainHours);
    try {
      taskLogWriter.init();
    } catch (IOException ioe) {
      taskLogWriter = null;
      errorHandler.error("Failed to initialize the task's logging " +
              "infrastructure: " + StringUtils.stringifyException(ioe));
    }
  }
  
  protected synchronized void append(LoggingEvent event) {
    if (taskLogWriter == null) {
      errorHandler.error("Calling 'append' on uninitialize/closed logger");
      return;
    }

    if (this.layout == null) {
      errorHandler.error("No layout for appender " + name , 
              null, ErrorCode.MISSING_LAYOUT );
    }
    
    // Log the message to the task's log
    String logMessage = this.layout.format(event);
    try {
      taskLogWriter.write(logMessage.getBytes(), 0, logMessage.length());
    } catch (IOException ioe) {
      errorHandler.error("Failed to log: '" + logMessage + 
              "' to the task's logging infrastructure with the exception: " + 
              StringUtils.stringifyException(ioe));
    }
  }

  public boolean requiresLayout() {
    return true;
  }

  public synchronized void close() {
    if (taskLogWriter != null) {
      try {
        taskLogWriter.close();
      } catch (IOException ioe) {
        errorHandler.error("Failed to close the task's log with the exception: " 
                + StringUtils.stringifyException(ioe));
      }
    } else {
      errorHandler.error("Calling 'close' on uninitialize/closed logger");
    }
  }

  /**
   * Getter/Setter methods for log4j.
   */
  
  public String getTaskId() {
    return taskId;
  }

  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  public int getNoKeepSplits() {
    return noKeepSplits;
  }

  public void setNoKeepSplits(int noKeepSplits) {
    this.noKeepSplits = noKeepSplits;
  }

  public int getLogsRetainHours() {
    return logsRetainHours;
  }

  public void setLogsRetainHours(int logsRetainHours) {
    this.logsRetainHours = logsRetainHours;
  }

  public boolean isPurgeLogSplits() {
    return purgeLogSplits;
  }

  public void setPurgeLogSplits(boolean purgeLogSplits) {
    this.purgeLogSplits = purgeLogSplits;
  }

  public long getTotalLogFileSize() {
    return totalLogFileSize;
  }

  public void setTotalLogFileSize(long splitFileSize) {
    this.totalLogFileSize = splitFileSize;
  }

}
