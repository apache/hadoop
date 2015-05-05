/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.service.workflow;

import com.google.common.base.Preconditions;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Execute a long-lived process.
 *
 * <p>
 * Hadoop's {@link org.apache.hadoop.util.Shell} class assumes it is executing
 * a short lived application; this class allows for the process to run for the
 * life of the Java process that forked it.
 * It is designed to be embedded inside a YARN service, though this is not
 * the sole way that it can be used
 * <p>
 * Key Features:
 * <ol>
 *   <li>Output is streamed to the output logger provided</li>.
 *   <li>The most recent lines of output are saved to a linked list</li>.
 *   <li>A synchronous callback, {@link LongLivedProcessLifecycleEvent}, is raised on the start
 *   and finish of a process.</li>
 * </ol>
 * 
 */
public class LongLivedProcess implements Runnable {
  /**
   * Limit on number of lines to retain in the "recent" line list:{@value}
   */
  public static final int RECENT_LINE_LOG_LIMIT = 64;

  /**
   * Const defining the time in millis between polling for new text
   */
  private static final int STREAM_READER_SLEEP_TIME = 200;
  
  /**
   * limit on the length of a stream before it triggers an automatic newline
   */
  private static final int LINE_LENGTH = 256;
  private final ProcessBuilder processBuilder;
  private Process process;
  private Integer exitCode = null;
  private final String name;
  private final ExecutorService processExecutor;
  private final ExecutorService logExecutor;
  
  private ProcessStreamReader processStreamReader;
  //list of recent lines, recorded for extraction into reports
  private final List<String> recentLines = new LinkedList<>();
  private int recentLineLimit = RECENT_LINE_LOG_LIMIT;
  private LongLivedProcessLifecycleEvent lifecycleCallback;

  
  /**
   * Log supplied in the constructor for the spawned process -accessible
   * to inner classes
   */
  private final Logger processLog;
  
  /**
   * Class log -accessible to inner classes
   */
  private static final Logger LOG = LoggerFactory.getLogger(LongLivedProcess.class);

  /**
   * Volatile flag to indicate that the process is done
   */
  private volatile boolean finished;

  public LongLivedProcess(String name,
      Logger processLog,
      List<String> commands) {
    Preconditions.checkArgument(processLog != null, "processLog");
    Preconditions.checkArgument(commands != null, "commands");

    this.name = name;
    this.processLog = processLog;
    ServiceThreadFactory factory = new ServiceThreadFactory(name, true);
    processExecutor = Executors.newSingleThreadExecutor(factory);
    logExecutor=    Executors.newSingleThreadExecutor(factory);
    processBuilder = new ProcessBuilder(commands);
    processBuilder.redirectErrorStream(false);
  }

  /**
   * Set the limit on recent lines to retain
   * @param recentLineLimit size of rolling list of recent lines.
   */
  public void setRecentLineLimit(int recentLineLimit) {
    this.recentLineLimit = recentLineLimit;
  }

  /**
   * Set an optional application exit callback
   * @param lifecycleCallback callback to notify on application exit
   */
  public void setLifecycleCallback(LongLivedProcessLifecycleEvent lifecycleCallback) {
    this.lifecycleCallback = lifecycleCallback;
  }

  /**
   * Add an entry to the environment
   * @param envVar envVar -must not be null
   * @param val value 
   */
  public void setEnv(String envVar, String val) {
    Preconditions.checkArgument(envVar != null, "envVar");
    Preconditions.checkArgument(val != null, "val");
    processBuilder.environment().put(envVar, val);
  }

  /**
   * Bulk set the environment from a map. This does
   * not replace the existing environment, just extend it/overwrite single
   * entries.
   * @param map map to add
   */
  public void putEnvMap(Map<String, String> map) {
    for (Map.Entry<String, String> entry : map.entrySet()) {
      String val = entry.getValue();
      String key = entry.getKey();
      setEnv(key, val);
    }
  }

  /**
   * Get the process environment
   * @param variable environment variable
   * @return the value or null if there is no match
   */
  public String getEnv(String variable) {
    return processBuilder.environment().get(variable);
  }

  /**
   * Get the process reference
   * @return the process -null if the process is  not started
   */
  public Process getProcess() {
    return process;
  }

  /**
   * Get the process builder -this can be manipulated
   * up to the start() operation. As there is no synchronization
   * around it, it must only be used in the same thread setting up the commmand.
   * @return the process builder
   */
  public ProcessBuilder getProcessBuilder() {
    return processBuilder;
  }

  /**
   * Get the command list
   * @return the comands
   */
  public List<String> getCommands() {
    return processBuilder.command();
  }

  public String getCommand() {
    return getCommands().get(0);
  }

  /**
   * probe to see if the process is running
   * @return true iff the process has been started and is not yet finished
   */
  public boolean isRunning() {
    return process != null && !finished;
  }

  /**
   * Get the exit code: null until the process has finished
   * @return the exit code or null
   */
  public Integer getExitCode() {
    return exitCode;
  }

    /**
   * Get the exit code sign corrected: null until the process has finished
   * @return the exit code or null
   */
  public Integer getExitCodeSignCorrected() {
    Integer result;
    if (exitCode != null) {
      result = signCorrectExitCode(exitCode);
    } else {
      result = null;
    }
    return result;
  }

  /**
   * Stop the process if it is running.
   * This will trigger an application completion event with the given exit code
   */
  public void stop() {
    if (!isRunning()) {
      return;
    }
    process.destroy();
  }

  /**
   * Get a text description of the builder suitable for log output
   * @return a multiline string 
   */
  protected String describeBuilder() {
    StringBuilder buffer = new StringBuilder();
    for (String arg : processBuilder.command()) {
      buffer.append('"').append(arg).append("\" ");
    }
    return buffer.toString();
  }

  /**
   * Dump the environment to a string builder
   * @param buffer the buffer to append to
   */
  public void dumpEnv(StringBuilder buffer) {
    buffer.append("\nEnvironment\n-----------");
    Map<String, String> env = processBuilder.environment();
    Set<String> keys = env.keySet();
    List<String> sortedKeys = new ArrayList<>(keys);
    Collections.sort(sortedKeys);
    for (String key : sortedKeys) {
      buffer.append(key).append("=").append(env.get(key)).append('\n');
    }
  }

  /**
   * Execute the process.
   * @return the process
   * @throws IOException on any problem
   * @throws FileNotFoundException if the process could not be found
   */
  private Process spawnChildProcess() throws IOException {
    if (process != null) {
      throw new IOException("Process already started");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Spawning process:\n {}", describeBuilder());
    }
    process = processBuilder.start();
    return process;
  }

  /**
   * Entry point for waiting for the program to finish.
   */
  @Override // Runnable
  public void run() {
    LOG.debug("Lifecycle callback thread running");
    //notify the callback that the process has started
    if (lifecycleCallback != null) {
      lifecycleCallback.onProcessStarted(this);
    }
    try {
      //close stdin for the process
      IOUtils.closeStream(process.getOutputStream());
      exitCode = process.waitFor();
    } catch (InterruptedException e) {
      LOG.debug("Process wait interrupted -exiting thread", e);
    } finally {
      //here the process has finished
      LOG.debug("process {} has finished", name);
      //tell the logger it has to finish too
      finished = true;

      // shut down the threads
      logExecutor.shutdown();
      try {
        logExecutor.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException ignored) {
        //ignored
      }

      //now call the callback if it is set
      if (lifecycleCallback != null) {
        lifecycleCallback.onProcessExited(this, exitCode,
            getExitCodeSignCorrected());
      }
    }
  }

  /**
   * Spawn the application
   * @throws IOException IO problems
   */
  public void start() throws IOException {

    spawnChildProcess();
    processExecutor.submit(this);
    processStreamReader =
      new ProcessStreamReader(processLog, STREAM_READER_SLEEP_TIME);
    logExecutor.submit(processStreamReader);
  }

  /**
   * Get the lines of recent output
   * @return the last few lines of output; an empty list if there are none
   * or the process is not actually running
   */
  public synchronized List<String> getRecentOutput() {
    return new ArrayList<>(recentLines);
  }

  /**
   * add the recent line to the list of recent lines; deleting
   * an earlier on if the limit is reached.
   *
   * Implementation note: yes, a circular array would be more
   * efficient, especially with some power of two as the modulo,
   * but is it worth the complexity and risk of errors for
   * something that is only called once per line of IO?
   * @param line line to record
   * @param isErrorStream is the line from the error stream
   */
  private synchronized void recordRecentLine(String line,
                                             boolean isErrorStream) {
    if (line == null) {
      return;
    }
    String entry = (isErrorStream ? "[ERR] " : "[OUT] ") + line;
    recentLines.add(entry);
    if (recentLines.size() > recentLineLimit) {
      recentLines.remove(0);
    }
  }

  /**
   * Class to read data from the two process streams, and, when run in a thread
   * to keep running until the <code>done</code> flag is set. 
   * Lines are fetched from stdout and stderr and logged at info and error
   * respectively.
   */

  private class ProcessStreamReader implements Runnable {
    private final Logger streamLog;
    private final int sleepTime;

    private ProcessStreamReader(Logger streamLog, int sleepTime) {
      this.streamLog = streamLog;
      this.sleepTime = sleepTime;
    }

    /**
     * Return a character if there is one, -1 if nothing is ready yet
     * @param reader reader
     * @return the value from the reader, or -1 if it is not ready
     * @throws IOException IO problems
     */
    private int readCharNonBlocking(BufferedReader reader) throws IOException {
      if (reader.ready()) {
        return reader.read();
      } else {
        return -1;
      }
    }

    /**
     * Read in a line, or, if the limit has been reached, the buffer
     * so far
     * @param reader source of data
     * @param line line to build
     * @param limit limit of line length
     * @return true if the line can be printed
     * @throws IOException IO trouble
     */
    @SuppressWarnings("NestedAssignment")
    private boolean readAnyLine(BufferedReader reader,
                                StringBuilder line,
                                int limit)
      throws IOException {
      int next;
      while ((-1 != (next = readCharNonBlocking(reader)))) {
        if (next != '\n') {
          line.append((char) next);
          limit--;
          if (line.length() > limit) {
            //enough has been read in to print it any
            return true;
          }
        } else {
          //line end return flag to say so
          return true;
        }
      }
      //here the end of the stream is hit, or the limit
      return false;
    }


    @Override //Runnable
    @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
    public void run() {
      BufferedReader errReader = null;
      BufferedReader outReader = null;
      StringBuilder outLine = new StringBuilder(LINE_LENGTH);
      StringBuilder errorLine = new StringBuilder(LINE_LENGTH);
      try {
        errReader = new BufferedReader(
            new InputStreamReader(process.getErrorStream()));
        outReader = new BufferedReader(
            new InputStreamReader(process.getInputStream()));
        while (!finished) {
          boolean processed = false;
          if (readAnyLine(errReader, errorLine, LINE_LENGTH)) {
            String line = errorLine.toString();
            recordRecentLine(line, true);
            streamLog.warn(line);
            errorLine.setLength(0);
            processed = true;
          }
          if (readAnyLine(outReader, outLine, LINE_LENGTH)) {
            String line = outLine.toString();
            recordRecentLine(line, false);
            streamLog.info(line);
            outLine.setLength(0);
            processed |= true;
          }
          if (!processed && !finished) {
            //nothing processed: wait a bit for data.
            try {
              Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
              //ignore this, rely on the done flag
              LOG.debug("Ignoring ", e);
            }
          }
        }
        // finished: cleanup

        //print the current error line then stream through the rest
        streamLog.error(errorLine.toString());
        String line = errReader.readLine();
        while (line != null) {
          streamLog.error(line);
          if (Thread.interrupted()) {
            break;
          }
          line = errReader.readLine();
          recordRecentLine(line, true);
        }
        //now do the info line
        streamLog.info(outLine.toString());
        line = outReader.readLine();
        while (line != null) {
          streamLog.info(line);
          if (Thread.interrupted()) {
            break;
          }
          line = outReader.readLine();
          recordRecentLine(line, false);
        }

      } catch (Exception ignored) {
        LOG.warn("encountered {}", ignored, ignored);
        //process connection has been torn down
      } finally {
        IOUtils.closeStream(errReader);
        IOUtils.closeStream(outReader);
      }
    }
  }


  /**
   * Sign correct an exit code.
   * @param code an integer code value in the range 0-255.
   * @return a code in the range -127 to +128.
   */
  public static int signCorrectExitCode(int code) {
    return (code << 24) >> 24;
  }

}
