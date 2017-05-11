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
package org.apache.hadoop.fs;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/** Filesystem disk space usage statistics.  Uses the unix 'du' program*/
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class DU extends Shell {
  static final String JITTER_KEY = "fs.getspaceused.jitterMillis";
  static final long DEFAULT_JITTER = TimeUnit.MINUTES.toMillis(1);

  private String  dirPath;

  private AtomicLong used = new AtomicLong();
  private volatile boolean shouldRun = true;
  private Thread refreshUsed;
  private IOException duException = null;
  private long refreshInterval;
  private final long jitter;

  /**
   * Keeps track of disk usage.
   * @param path the path to check disk usage in
   * @param interval refresh the disk usage at this interval
   * @throws IOException if we fail to refresh the disk usage
   */
  public DU(File path, long interval) throws IOException {
    this(path, interval, 0L, -1L);
  }

  /**
   * Keeps track of disk usage.
   * @param path the path to check disk usage in
   * @param interval refresh the disk usage at this interval
   * @param jitter randomize the refresh interval timing by this amount; the
   *               actual interval will be randomly chosen between
   *               {@code interval-jitter} and {@code interval+jitter}
   * @param initialUsed use this value until next refresh
   * @throws IOException if we fail to refresh the disk usage
   */
  public DU(File path, long interval, long jitter, long initialUsed)
      throws IOException {
    super(0);
    this.jitter = jitter;

    //we set the Shell interval to 0 so it will always run our command
    //and use this one to set the thread sleep interval
    this.refreshInterval = interval;
    this.dirPath = path.getCanonicalPath();

    //populate the used variable if the initial value is not specified.
    if (initialUsed < 0) {
      run();
    } else {
      this.used.set(initialUsed);
    }
  }

  /**
   * Keeps track of disk usage.
   * @param path the path to check disk usage in
   * @param conf configuration object
   * @throws IOException if we fail to refresh the disk usage
   */
  public DU(File path, Configuration conf) throws IOException {
    this(path, conf, -1L);
  }

  /**
   * Keeps track of disk usage.
   * @param path the path to check disk usage in
   * @param conf configuration object
   * @param initialUsed use it until the next refresh.
   * @throws IOException if we fail to refresh the disk usage
   */
  public DU(File path, Configuration conf, long initialUsed)
      throws IOException {
    this(path, conf.getLong(CommonConfigurationKeys.FS_DU_INTERVAL_KEY,
        CommonConfigurationKeys.FS_DU_INTERVAL_DEFAULT),
        conf.getLong(JITTER_KEY, DEFAULT_JITTER), initialUsed);
  }
    
  

  /**
   * This thread refreshes the "used" variable.
   * 
   * Future improvements could be to not permanently
   * run this thread, instead run when getUsed is called.
   **/
  class DURefreshThread implements Runnable {
    
    @Override
    public void run() {
      
      while(shouldRun) {

        try {
          long thisRefreshInterval = refreshInterval;
          if (jitter > 0) {
            // add/subtract the jitter.
            thisRefreshInterval +=
                ThreadLocalRandom.current().nextLong(-jitter, jitter);
          }
          Thread.sleep(thisRefreshInterval);
          
          try {
            //update the used variable
            DU.this.run();
          } catch (IOException e) {
            synchronized (DU.this) {
              //save the latest exception so we can return it in getUsed()
              duException = e;
            }
            
            LOG.warn("Could not get disk usage information", e);
          }
        } catch (InterruptedException e) {
        }
      }
    }
  }
  
  /**
   * Decrease how much disk space we use.
   * @param value decrease by this value
   */
  public void decDfsUsed(long value) {
    used.addAndGet(-value);
  }

  /**
   * Increase how much disk space we use.
   * @param value increase by this value
   */
  public void incDfsUsed(long value) {
    used.addAndGet(value);
  }
  
  /**
   * @return disk space used 
   * @throws IOException if the shell command fails
   */
  public long getUsed() throws IOException {
    //if the updating thread isn't started, update on demand
    if(refreshUsed == null) {
      run();
    } else {
      synchronized (DU.this) {
        //if an exception was thrown in the last run, rethrow
        if(duException != null) {
          IOException tmp = duException;
          duException = null;
          throw tmp;
        }
      }
    }
    
    return Math.max(used.longValue(), 0L);
  }

  /**
   * @return the path of which we're keeping track of disk usage
   */
  public String getDirPath() {
    return dirPath;
  }


  /**
   * Override to hook in DUHelper class. Maybe this can be used more
   * generally as well on Unix/Linux based systems
   */
  @Override
  protected void run() throws IOException {
    if (WINDOWS) {
      used.set(DUHelper.getFolderUsage(dirPath));
      return;
    }
    super.run();
  }
  
  /**
   * Start the disk usage checking thread.
   */
  public void start() {
    //only start the thread if the interval is sane
    if(refreshInterval > 0) {
      refreshUsed = new Thread(new DURefreshThread(), 
          "refreshUsed-"+dirPath);
      refreshUsed.setDaemon(true);
      refreshUsed.start();
    }
  }
  
  /**
   * Shut down the refreshing thread.
   */
  public void shutdown() {
    this.shouldRun = false;
    
    if(this.refreshUsed != null) {
      this.refreshUsed.interrupt();
    }
  }
  
  @Override
  public String toString() {
    return
      "du -sk " + dirPath +"\n" +
      used + "\t" + dirPath;
  }

  @Override
  protected String[] getExecString() {
    return new String[] {"du", "-sk", dirPath};
  }
  
  @Override
  protected void parseExecResult(BufferedReader lines) throws IOException {
    String line = lines.readLine();
    if (line == null) {
      throw new IOException("Expecting a line not the end of stream");
    }
    String[] tokens = line.split("\t");
    if(tokens.length == 0) {
      throw new IOException("Illegal du output");
    }
    this.used.set(Long.parseLong(tokens[0])*1024);
  }

  public static void main(String[] args) throws Exception {
    String path = ".";
    if (args.length > 0) {
      path = args[0];
    }

    System.out.println(new DU(new File(path), new Configuration()).toString());
  }
}
