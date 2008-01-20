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

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.*;
import org.apache.hadoop.fs.FileUtil;

/**
 * A simple logger to handle the task-specific user logs.
 * This class uses the system property <code>hadoop.log.dir</code>.
 * 
 */
public class TaskLog {
  private static final Log LOG =
    LogFactory.getLog(TaskLog.class.getName());

  private static final File LOG_DIR = 
    new File(System.getProperty("hadoop.log.dir"), 
             "userlogs").getAbsoluteFile();
  
  static {
    if (!LOG_DIR.exists()) {
      LOG_DIR.mkdirs();
    }
  }

  public static File getTaskLogFile(String taskid, LogName filter) {
    return new File(new File(LOG_DIR, taskid), filter.toString());
  }
  
  /**
   * The filter for userlogs.
   */
  public static enum LogName {
    /** Log on the stdout of the task. */
    STDOUT ("stdout"),

    /** Log on the stderr of the task. */
    STDERR ("stderr"),
    
    /** Log on the map-reduce system logs of the task. */
    SYSLOG ("syslog"),
    
    /** The java profiler information. */
    PROFILE ("profile.out"),
    
    /** Log the debug script's stdout  */
    DEBUGOUT ("debugout");
        
    private String prefix;
    
    private LogName(String prefix) {
      this.prefix = prefix;
    }
    
    public String toString() {
      return prefix;
    }
  }

  private static class TaskLogsPurgeFilter implements FileFilter {
    long purgeTimeStamp;
  
    TaskLogsPurgeFilter(long purgeTimeStamp) {
      this.purgeTimeStamp = purgeTimeStamp;
    }

    public boolean accept(File file) {
      LOG.debug("PurgeFilter - file: " + file + ", mtime: " + file.lastModified() + ", purge: " + purgeTimeStamp);
      return file.lastModified() < purgeTimeStamp;
    }
  }
  /**
   * Purge old user logs.
   * 
   * @throws IOException
   */
  public static synchronized void cleanup(int logsRetainHours
                                          ) throws IOException {
    // Purge logs of tasks on this tasktracker if their  
    // mtime has exceeded "mapred.task.log.retain" hours
    long purgeTimeStamp = System.currentTimeMillis() - 
                            (logsRetainHours*60L*60*1000);
    File[] oldTaskLogs = LOG_DIR.listFiles
                           (new TaskLogsPurgeFilter(purgeTimeStamp));
    if (oldTaskLogs != null) {
      for (int i=0; i < oldTaskLogs.length; ++i) {
        FileUtil.fullyDelete(oldTaskLogs[i]);
      }
    }
  }

  public static class Reader extends InputStream {
    private long bytesRemaining;
    private FileInputStream file;
    /**
     * Read a log file from start to end positions. The offsets may be negative,
     * in which case they are relative to the end of the file. For example,
     * Reader(taskid, kind, 0, -1) is the entire file and 
     * Reader(taskid, kind, -4197, -1) is the last 4196 bytes. 
     * @param taskid the id of the task to read the log file for
     * @param kind the kind of log to read
     * @param start the offset to read from (negative is relative to tail)
     * @param end the offset to read upto (negative is relative to tail)
     * @throws IOException
     */
    public Reader(String taskid, LogName kind, 
                  long start, long end) throws IOException {
      // find the right log file
      File filename = getTaskLogFile(taskid, kind);
      // calculate the start and stop
      long size = filename.length();
      if (start < 0) {
        start += size + 1;
      }
      if (end < 0) {
        end += size + 1;
      }
      start = Math.max(0, Math.min(start, size));
      end = Math.max(0, Math.min(end, size));
      bytesRemaining = end - start;
      file = new FileInputStream(filename);
      // skip upto start
      long pos = 0;
      while (pos < start) {
        long result = file.skip(start - pos);
        if (result < 0) {
          bytesRemaining = 0;
          break;
        }
        pos += result;
      }
    }
    
    public int read() throws IOException {
      int result = -1;
      if (bytesRemaining > 0) {
        bytesRemaining -= 1;
        result = file.read();
      }
      return result;
    }
    
    public int read(byte[] buffer, int offset, int length) throws IOException {
      length = (int) Math.min(length, bytesRemaining);
      int bytes = file.read(buffer, offset, length);
      if (bytes > 0) {
        bytesRemaining -= bytes;
      }
      return bytes;
    }
    
    public int available() throws IOException {
      return (int) Math.min(bytesRemaining, file.available());
    }

    public void close() throws IOException {
      file.close();
    }
  }

  private static final String bashCommand = "bash";
  private static final String tailCommand = "tail";
  
  /**
   * Get the desired maximum length of task's logs.
   * @param conf the job to look in
   * @return the number of bytes to cap the log files at
   */
  public static long getTaskLogLength(JobConf conf) {
    return conf.getLong("mapred.userlog.limit.kb", 100) * 1024;
  }

  /**
   * Wrap a command in a shell to capture stdout and stderr to files.
   * If the tailLength is 0, the entire output will be saved.
   * @param cmd The command and the arguments that should be run
   * @param stdoutFilename The filename that stdout should be saved to
   * @param stderrFilename The filename that stderr should be saved to
   * @param tailLength The length of the tail to be saved.
   * @return the modified command that should be run
   */
  public static List<String> captureOutAndError(List<String> cmd, 
                                                File stdoutFilename,
                                                File stderrFilename,
                                                long tailLength
                                               ) throws IOException {
    String stdout = FileUtil.makeShellPath(stdoutFilename);
    String stderr = FileUtil.makeShellPath(stderrFilename);
    List<String> result = new ArrayList<String>(3);
    result.add(bashCommand);
    result.add("-c");
    StringBuffer mergedCmd = new StringBuffer();
    if (tailLength > 0) {
      mergedCmd.append("(");
    } else {
      mergedCmd.append("exec ");
    }
    boolean isExecutable = true;
    for(String s: cmd) {
      mergedCmd.append('\'');
      if (isExecutable) {
        // the executable name needs to be expressed as a shell path for the  
        // shell to find it.
        mergedCmd.append(FileUtil.makeShellPath(new File(s)));
        isExecutable = false; 
      } else {
        mergedCmd.append(s);
      }
      mergedCmd.append('\'');
      mergedCmd.append(" ");
    }
    mergedCmd.append(" < /dev/null ");
    if (tailLength > 0) {
      mergedCmd.append(" | ");
      mergedCmd.append(tailCommand);
      mergedCmd.append(" -c ");
      mergedCmd.append(tailLength);
      mergedCmd.append(" >> ");
      mergedCmd.append(stdout);
      mergedCmd.append(" ; exit $PIPESTATUS ) 2>&1 | ");
      mergedCmd.append(tailCommand);
      mergedCmd.append(" -c ");
      mergedCmd.append(tailLength);
      mergedCmd.append(" >> ");
      mergedCmd.append(stderr);
      mergedCmd.append(" ; exit $PIPESTATUS");
    } else {
      mergedCmd.append(" 1>> ");
      mergedCmd.append(stdout);
      mergedCmd.append(" 2>> ");
      mergedCmd.append(stderr);
    }
    result.add(mergedCmd.toString());
    return result;
  }
  
  /**
   * Wrap a command in a shell to capture debug script's 
   * stdout and stderr to debugout.
   * @param cmd The command and the arguments that should be run
   * @param debugoutFilename The filename that stdout and stderr
   *  should be saved to.
   * @return the modified command that should be run
   * @throws IOException
   */
  public static List<String> captureDebugOut(List<String> cmd, 
                                             File debugoutFilename
                                            ) throws IOException {
    String debugout = FileUtil.makeShellPath(debugoutFilename);
    List<String> result = new ArrayList<String>(3);
    result.add(bashCommand);
    result.add("-c");
    StringBuffer mergedCmd = new StringBuffer();
    mergedCmd.append("exec ");
    boolean isExecutable = true;
    for(String s: cmd) {
      if (isExecutable) {
        // the executable name needs to be expressed as a shell path for the  
        // shell to find it.
        mergedCmd.append(FileUtil.makeShellPath(new File(s)));
        isExecutable = false; 
      } else {
        mergedCmd.append(s);
      }
      mergedCmd.append(" ");
    }
    mergedCmd.append(" < /dev/null ");
    mergedCmd.append(" >");
    mergedCmd.append(debugout);
    mergedCmd.append(" 2>&1 ");
    result.add(mergedCmd.toString());
    return result;
  }
  
} // TaskLog
