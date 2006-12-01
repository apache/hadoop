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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;
import java.text.DecimalFormat;
import org.apache.hadoop.util.Progressable;

/** Runs a reduce task. */
class ReduceTaskRunner extends TaskRunner {
  /** Number of ms before timing out a copy */
  private static final int STALLED_COPY_TIMEOUT = 3 * 60 * 1000;
  
  /** 
   * for cleaning up old map outputs
   */
  private MapOutputFile mapOutputFile;
  
  /**
   * our reduce task instance
   */
  private ReduceTask reduceTask;
    
  /**
   * the list of map outputs currently being copied
   */
  private List scheduledCopies;
  
  /**
   *  the results of dispatched copy attempts
   */
  private List copyResults;
  
  /**
   *  the number of outputs to copy in parallel
   */
  private int numCopiers;

  /**
   * the maximum amount of time (less 1 minute) to wait to 
   * contact a host after a copy from it fails. We wait for (1 min +
   * Random.nextInt(maxBackoff)) seconds.
   */
  private int maxBackoff;
  
  /**
   * busy hosts from which copies are being backed off
   * Map of host -> next contact time
   */
  private Map penaltyBox;

  /**
   * the set of unique hosts from which we are copying
   */
  private Set uniqueHosts;
  
  /**
   * the last time we polled the job tracker
   */
  private long lastPollTime;
  
  /**
   * A reference to the local file system for writing the map outputs to.
   */
  private FileSystem localFileSys;

  /**
   * The threads for fetching the files.
   */
  private MapOutputCopier[] copiers = null;
  
  /**
   * the minimum interval between jobtracker polls
   */
  private static final long MIN_POLL_INTERVAL = 5000;
  
  /**
   * the number of map output locations to poll for at one time
   */
  private static final int PROBE_SAMPLE_SIZE = 50;

  /** Represents the result of an attempt to copy a map output */
  private class CopyResult {
    
    // the map output location against which a copy attempt was made
    private final MapOutputLocation loc;
    
    // the size of the file copied, -1 if the transfer failed
    private final long size;
        
    CopyResult(MapOutputLocation loc, long size) {
      this.loc = loc;
      this.size = size;
    }
    
    public int getMapId() { return loc.getMapId(); }
    public boolean getSuccess() { return size >= 0; }
    public long getSize() { return size; }
    public String getHost() { return loc.getHost(); }
    public MapOutputLocation getLocation() { return loc; }
  }

  private class PingTimer implements Progressable {
    Task task = getTask();
    TaskTracker tracker = getTracker();
    
    public void progress() {
      task.reportProgress(tracker);
    }
  }

  private static int nextMapOutputCopierId = 0;

  /** Copies map outputs as they become available */
  private class MapOutputCopier extends Thread {

    private PingTimer pingTimer = new PingTimer();
    private MapOutputLocation currentLocation = null;
    private int id = nextMapOutputCopierId++;
    
    public MapOutputCopier() {
      setName("MapOutputCopier " + reduceTask.getTaskId() + "." + id);
      LOG.debug(getName() + " created");
    }
    
    /**
     * Fail the current file that we are fetching
     * @return were we currently fetching?
     */
    public synchronized boolean fail() {
      if (currentLocation != null) {
        finish(-1);
        return true;
      } else {
        return false;
      }
    }
    
    /**
     * Get the current map output location.
     */
    public synchronized MapOutputLocation getLocation() {
      return currentLocation;
    }
    
    private synchronized void start(MapOutputLocation loc) {
      currentLocation = loc;
    }
    
    private synchronized void finish(long size) {
      if (currentLocation != null) {
        LOG.debug(getName() + " finishing " + currentLocation + " = " + size);
        synchronized (copyResults) {
          copyResults.add(new CopyResult(currentLocation, size));
          copyResults.notify();
        }
        currentLocation = null;
      }
    }
    
    /** Loop forever and fetch map outputs as they become available.
     * The thread exits when it is interrupted by the {@link ReduceTaskRunner}
     */
    public void run() {
      while (true) {        
        try {
          MapOutputLocation loc = null;
          long size = -1;
          
          synchronized (scheduledCopies) {
            while (scheduledCopies.isEmpty()) {
              scheduledCopies.wait();
            }
            loc = (MapOutputLocation)scheduledCopies.remove(0);
          }

          try {
            start(loc);
            size = copyOutput(loc, pingTimer);
          } catch (IOException e) {
            LOG.warn(reduceTask.getTaskId() + " copy failed: " +
                        loc.getMapTaskId() + " from " + loc.getHost());
            LOG.warn(StringUtils.stringifyException(e));
          } finally {
            finish(size);
          }
        } catch (InterruptedException e) { 
          return; // ALL DONE
        } catch (Throwable th) {
          LOG.error("Map output copy failure: " + 
                    StringUtils.stringifyException(th));
        }
      }
    }

    /** Copies a a map output from a remote host, using raw RPC. 
     * @param currentLocation the map output location to be copied
     * @param pingee a status object to ping as we make progress
     * @return the size of the copied file
     * @throws IOException if there is an error copying the file
     * @throws InterruptedException if the copier should give up
     */
    private long copyOutput(MapOutputLocation loc, 
                            Progressable pingee
                            ) throws IOException, InterruptedException {

      String reduceId = reduceTask.getTaskId();
      LOG.info(reduceId + " Copying " + loc.getMapTaskId() +
               " output from " + loc.getHost() + ".");
      // the place where the file should end up
      Path finalFilename = conf.getLocalPath(reduceId + "/map_" +
                                             loc.getMapId() + ".out");
      // a working filename that will be unique to this attempt
      Path tmpFilename = new Path(finalFilename + "-" + id);
      // this copies the map output file
      long bytes = loc.getFile(localFileSys, tmpFilename,
                               reduceTask.getPartition(), pingee,
                               STALLED_COPY_TIMEOUT);
      // lock the ReduceTaskRunner while we do the rename
      synchronized (ReduceTaskRunner.this) {
        // if we can't rename the file, something is broken
        if (!(new File(tmpFilename.toString()).
                 renameTo(new File(finalFilename.toString())))) {
          localFileSys.delete(tmpFilename);
          throw new IOException("failure to rename map output " + tmpFilename);
        }
      }
      LOG.info(reduceTask.getTaskId() + " done copying " + loc.getMapTaskId() +
               " output from " + loc.getHost() + ".");
      
      return bytes;
    }

  }

  public ReduceTaskRunner(Task task, TaskTracker tracker, 
                          JobConf conf) throws IOException {
    super(task, tracker, conf);
    this.mapOutputFile = new MapOutputFile();
    this.mapOutputFile.setConf(conf);
    localFileSys = FileSystem.getNamed("local", conf);

    this.reduceTask = (ReduceTask)getTask();
    this.scheduledCopies = new ArrayList(100);
    this.copyResults = new ArrayList(100);    
    this.numCopiers = conf.getInt("mapred.reduce.parallel.copies", 5);
    this.maxBackoff = conf.getInt("mapred.reduce.copy.backoff", 300);

    // hosts -> next contact time
    this.penaltyBox = new Hashtable();
    
    // hostnames
    this.uniqueHosts = new HashSet();
    
    this.lastPollTime = 0;
  }

  /** Assemble all of the map output files */
  public boolean prepare() throws IOException {
    if (!super.prepare()) {
      return false;
    }
    
    // cleanup from failures
    this.mapOutputFile.removeAll(reduceTask.getTaskId());
    
    final int      numOutputs = reduceTask.getNumMaps();
    List           neededOutputs = new ArrayList(numOutputs);
    List           knownOutputs = new ArrayList(100);
    int            numInFlight = 0, numCopied = 0;
    int            lowThreshold = numCopiers*2;
    long           bytesTransferred = 0;
    DecimalFormat  mbpsFormat = new DecimalFormat("0.00");
    Random         backoff = new Random();
    final Progress copyPhase = getTask().getProgress().phase();
    
    for (int i = 0; i < numOutputs; i++) {
      neededOutputs.add(new Integer(i));
      copyPhase.addPhase();       // add sub-phase per file
    }

    InterTrackerProtocol jobClient = getTracker().getJobClient();
    copiers = new MapOutputCopier[numCopiers];

    // start all the copying threads
    for (int i=0; i < copiers.length; i++) {
      copiers[i] = new MapOutputCopier();
      copiers[i].start();
    }
    
    // start the clock for bandwidth measurement
    long startTime = System.currentTimeMillis();
    long currentTime = startTime;
    
    // loop until we get all required outputs or are killed
    while (!killed && numCopied < numOutputs) {

      LOG.info(reduceTask.getTaskId() + " Need " + (numOutputs-numCopied) +
               " map output(s)");

      if (!neededOutputs.isEmpty()) {
        LOG.info(reduceTask.getTaskId() + " Need " + neededOutputs.size() +
                 " map output location(s)");
        try {
          MapOutputLocation[] locs = queryJobTracker(neededOutputs, jobClient);
          
          // remove discovered outputs from needed list
          // and put them on the known list
          for (int i=0; i < locs.length; i++) {
            neededOutputs.remove(new Integer(locs[i].getMapId()));
            knownOutputs.add(locs[i]);
          }
          LOG.info(reduceTask.getTaskId() +
                   " Got " + (locs == null ? 0 : locs.length) + 
                   " map outputs from jobtracker");
        }
        catch (IOException ie) {
          LOG.warn(reduceTask.getTaskId() +
                      " Problem locating map outputs: " +
                      StringUtils.stringifyException(ie));
        }
      }
      
      // now walk through the cache and schedule what we can
      int numKnown = knownOutputs.size(), numScheduled = 0;
      int numSlow = 0, numDups = 0;

      LOG.info(reduceTask.getTaskId() + " Got " + numKnown + 
               " known map output location(s); scheduling...");

      synchronized (scheduledCopies) {
        ListIterator locIt = knownOutputs.listIterator();

        currentTime = System.currentTimeMillis();
        while (locIt.hasNext()) {

          MapOutputLocation loc = (MapOutputLocation)locIt.next();
          Long penaltyEnd = (Long)penaltyBox.get(loc.getHost());
          boolean penalized = false, duplicate = false;
 
          if (penaltyEnd != null && currentTime < penaltyEnd.longValue()) {
            penalized = true; numSlow++;
          }
          if (uniqueHosts.contains(loc.getHost())) {
            duplicate = true; numDups++;
          }
 
          if (!penalized && !duplicate) {
            uniqueHosts.add(loc.getHost());
            scheduledCopies.add(loc);
            locIt.remove();  // remove from knownOutputs
            numInFlight++; numScheduled++;
          }
        }
        scheduledCopies.notifyAll();
      }
      LOG.info(reduceTask.getTaskId() + " Scheduled " + numScheduled +
               " of " + numKnown + " known outputs (" + numSlow +
               " slow hosts and " + numDups + " dup hosts)");

      // if we have no copies in flight and we can't schedule anything
      // new, just wait for a bit
      try {
        if (numInFlight == 0 && numScheduled == 0) {
          getTask().reportProgress(getTracker());
          Thread.sleep(5000);
        }
      } catch (InterruptedException e) { } // IGNORE

      while (!killed && numInFlight > 0) {
        LOG.debug(reduceTask.getTaskId() + " numInFlight = " + numInFlight);
        CopyResult cr = getCopyResult();
        
        if (cr != null) {
          if (cr.getSuccess()) {  // a successful copy
            numCopied++;
            bytesTransferred += cr.getSize();
          
            long secsSinceStart = (System.currentTimeMillis()-startTime)/1000+1;
            float mbs = ((float)bytesTransferred)/(1024*1024);
            float transferRate = mbs/secsSinceStart;
          
            copyPhase.startNextPhase();
            copyPhase.setStatus("copy (" + numCopied + " of " + numOutputs + 
                                " at " +
                                mbpsFormat.format(transferRate) +  " MB/s)");          
            getTask().reportProgress(getTracker());
          } else {
            // this copy failed, put it back onto neededOutputs
            neededOutputs.add(new Integer(cr.getMapId()));
          
            // wait a random amount of time for next contact
            currentTime = System.currentTimeMillis();
            long nextContact = currentTime + 60 * 1000 +
                               backoff.nextInt(maxBackoff*1000);
            penaltyBox.put(cr.getHost(), new Long(nextContact));          
            LOG.warn(reduceTask.getTaskId() + " adding host " +
                     cr.getHost() + " to penalty box, next contact in " +
                     ((nextContact-currentTime)/1000) + " seconds");

            // other outputs from the failed host may be present in the
            // knownOutputs cache, purge them. This is important in case
            // the failure is due to a lost tasktracker (causes many
            // unnecessary backoffs). If not, we only take a small hit
            // polling the jobtracker a few more times
            ListIterator locIt = knownOutputs.listIterator();
            while (locIt.hasNext()) {
              MapOutputLocation loc = (MapOutputLocation)locIt.next();
              if (cr.getHost().equals(loc.getHost())) {
                locIt.remove();
                neededOutputs.add(new Integer(loc.getMapId()));
              }
            }
          }
          uniqueHosts.remove(cr.getHost());
          numInFlight--;
        }
        
        // ensure we have enough to keep us busy
        if (numInFlight < lowThreshold && (numOutputs-numCopied) > PROBE_SAMPLE_SIZE) {
          break;
        }
      }
      
    }

    // all done, inform the copiers to exit
    synchronized (copiers) {
      synchronized (scheduledCopies) {
        for (int i=0; i < copiers.length; i++) {
          copiers[i].interrupt();
          copiers[i] = null;
        }
      }
    }
    
    return numCopied == numOutputs && !killed;
  }
  
  
  private CopyResult getCopyResult() {  
   synchronized (copyResults) {
      while (!killed && copyResults.isEmpty()) {
        try {
          copyResults.wait();
        } catch (InterruptedException e) { }
      }
      if (copyResults.isEmpty()) {
        return null;
      } else {
        return (CopyResult) copyResults.remove(0);
      }
    }    
  }
  
  /** Queries the job tracker for a set of outputs ready to be copied
   * @param neededOutputs the list of currently unknown outputs
   * @param jobClient the job tracker
   * @return a set of locations to copy outputs from
   * @throws IOException
   */  
  private MapOutputLocation[] queryJobTracker(List neededOutputs,
                                              InterTrackerProtocol jobClient)
  throws IOException {
    
    // query for a just a random subset of needed segments so that we don't
    // overwhelm jobtracker.  ideally perhaps we could send a more compact
    // representation of all needed, i.e., a bit-vector
    int     checkSize = Math.min(PROBE_SAMPLE_SIZE, neededOutputs.size());
    int     neededIds[] = new int[checkSize];
      
    Collections.shuffle(neededOutputs);
      
    ListIterator itr = neededOutputs.listIterator();
    for (int i=0; i < checkSize; i++) {
      neededIds[i] = ((Integer)itr.next()).intValue();
    }

    long currentTime = System.currentTimeMillis();    
    long pollTime = lastPollTime + MIN_POLL_INTERVAL;
    while (currentTime < pollTime) {
      try {
        Thread.sleep(pollTime-currentTime);
      } catch (InterruptedException ie) { } // IGNORE
      currentTime = System.currentTimeMillis();
    }
    lastPollTime = currentTime;

    return jobClient.locateMapOutputs(reduceTask.getJobId().toString(), 
                                      neededIds,
                                      reduceTask.getPartition());
  }

  
  /** Delete all of the temporary map output files. */
  public void close() throws IOException {
    getTask().getProgress().setStatus("closed");
    this.mapOutputFile.removeAll(getTask().getTaskId());
  }

  /**
   * Kill the child process, but also kick getCopyResult so that it checks
   * the kill flag.
   */
  public void kill() {
    synchronized (copyResults) {
      super.kill();
      copyResults.notify();
    }
  }

}
