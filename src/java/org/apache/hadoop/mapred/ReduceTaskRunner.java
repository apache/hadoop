/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.hadoop.ipc.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.lang.reflect.Method;
import java.text.DecimalFormat;


/** Runs a reduce task. */
class ReduceTaskRunner extends TaskRunner {
  
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
   * timeout for copy operations
   */
  private int copyTimeout;
  
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
   * the minimum interval between jobtracker polls
   */
  private static final long MIN_POLL_INTERVAL = 5000;
  
  /**
   * the number of map output locations to poll for at one time
   */
  private static final int PROBE_SAMPLE_SIZE = 50;

  // initialization code to resolve "getFile" to a method object
  private static Method getFileMethod = null;
  static {
    Class[] paramTypes = { String.class, String.class,
                           int.class, int.class };    
    try {
      getFileMethod = 
        MapOutputProtocol.class.getDeclaredMethod("getFile", paramTypes);
    }
    catch (NoSuchMethodException e) {
      LOG.severe(StringUtils.stringifyException(e));
      throw new RuntimeException("Can't find \"getFile\" method "
                                 + "of MapOutputProtocol", e);
    }
  }
  
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
  
  /** Copies map outputs as they become available */
  private class MapOutputCopier extends Thread {

    public MapOutputCopier() {
    }
    
    /** Loop forever and fetch map outputs as they become available.
     * The thread exits when it is interrupted by the {@link ReduceTaskRunner}
     */
    public void run() {
      try {
        while (true) {        
          MapOutputLocation loc = null;
          long size = -1;
          
          synchronized (scheduledCopies) {
            while (scheduledCopies.isEmpty()) {
              scheduledCopies.wait();
            }
            loc = (MapOutputLocation)scheduledCopies.remove(0);
          }

          try {
            size = copyOutput(loc);
          } catch (IOException e) {
            LOG.warning(reduceTask.getTaskId() + " copy failed: " +
                        loc.getMapTaskId() + " from " + loc.getHost());
            LOG.warning(StringUtils.stringifyException(e));
          }
          
          synchronized (copyResults) {
            copyResults.add(new CopyResult(loc, size));
            copyResults.notifyAll();
          }
        }
      } catch (InterruptedException e) { }  // ALL DONE!
    }

    /** Copies a a map output from a remote host, using raw RPC. 
     * @param loc the map output location to be copied
     * @return the size of the copied file
     * @throws IOException if there is an error copying the file
     */
    private long copyOutput(MapOutputLocation loc)
    throws IOException {

      Object[] params = new Object[4];
      params[0] = loc.getMapTaskId();
      params[1] = reduceTask.getTaskId();
      params[2] = new Integer(loc.getMapId());
      params[3] = new Integer(reduceTask.getPartition());
      
      LOG.info(reduceTask.getTaskId() + " copy started: " +
               loc.getMapTaskId() + " from " + loc.getHost());

      Socket sock = new Socket(loc.getHost(), loc.getPort());
      try {
        sock.setSoTimeout(copyTimeout);

        // this copies the map output file
        MapOutputFile file =
          (MapOutputFile)RPC.callRaw(getFileMethod, params, sock, conf);

        LOG.info(reduceTask.getTaskId() + " copy finished: " +
                 loc.getMapTaskId() + " from " + loc.getHost());      

        return file.getSize();
      }
      finally {
        try {
          sock.close();
        } catch (IOException e) { } // IGNORE
      }
    }

  }
  
  public ReduceTaskRunner(Task task, TaskTracker tracker, JobConf conf) {
    super(task, tracker, conf);
    this.mapOutputFile = new MapOutputFile();
    this.mapOutputFile.setConf(conf);

    this.reduceTask = (ReduceTask)getTask();
    this.scheduledCopies = new ArrayList(100);
    this.copyResults = new ArrayList(100);    
    this.numCopiers = conf.getInt("mapred.reduce.parallel.copies", 5);
    this.copyTimeout = conf.getInt("ipc.client.timeout", 10000);
    this.maxBackoff = conf.getInt("mapred.reduce.copy.backoff", 300);

    // hosts -> next contact time
    this.penaltyBox = new Hashtable();
    
    // hostnames
    this.uniqueHosts = new HashSet();
    
    this.lastPollTime = 0;
  }

  /** Assemble all of the map output files */
  public boolean prepare() throws IOException {
    
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
    MapOutputCopier[] copiers = new MapOutputCopier[numCopiers];

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
          LOG.warning(reduceTask.getTaskId() +
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
          Thread.sleep(5000);
        }
      } catch (InterruptedException e) { } // IGNORE

      while (!killed && numInFlight > 0) {
        CopyResult cr = getCopyResult();
        
        if (cr.getSuccess()) {  // a successful copy
          numCopied++;
          bytesTransferred += cr.getSize();
          
          long secsSinceStart = (System.currentTimeMillis()-startTime)/1000+1;
          float mbs = ((float)bytesTransferred)/(1024*1024);
          float transferRate = mbs/secsSinceStart;
          
          copyPhase.startNextPhase();
          copyPhase.setStatus("copy (" + numCopied + " of " + numOutputs + " at " +
                              mbpsFormat.format(transferRate) +  " MB/s)");          
          getTask().reportProgress(getTracker());
        }
        else {
          // this copy failed, put it back onto neededOutputs
          neededOutputs.add(new Integer(cr.getMapId()));
          
          // wait a random amount of time for next contact
          currentTime = System.currentTimeMillis();
          long nextContact = currentTime + 60 * 1000 +
                             backoff.nextInt(maxBackoff*1000);
          penaltyBox.put(cr.getHost(), new Long(nextContact));          
          LOG.warning(reduceTask.getTaskId() + " adding host " +
                      cr.getHost() + " to penalty box, next contact in " +
                      ((nextContact-currentTime)/1000) + " seconds");
        }
        uniqueHosts.remove(cr.getHost());
        numInFlight--;
        
        // ensure we have enough to keep us busy
        if (numInFlight < lowThreshold && (numOutputs-numCopied) > PROBE_SAMPLE_SIZE) {
          break;
        }
      }
      
    }

    // all done, inform the copiers to exit
    synchronized (scheduledCopies) {
     for (int i=0; i < copiers.length; i++) {
       copiers[i].interrupt();
      }
    }
    
    return numCopied == numOutputs && !killed;
  }
  
  
  private CopyResult getCopyResult() {  
    CopyResult cr = null;
    
    synchronized (copyResults) {
      while (copyResults.isEmpty()) {
        try {
          copyResults.wait();
        } catch (InterruptedException e) { }
      }
      cr = (CopyResult)copyResults.remove(0);      
    }    
    return cr;
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
    lastPollTime = pollTime;

    return jobClient.locateMapOutputs(reduceTask.getJobId().toString(), 
                                      neededIds,
                                      reduceTask.getPartition());
  }

  
  /** Delete all of the temporary map output files. */
  public void close() throws IOException {
    getTask().getProgress().setStatus("closed");
    this.mapOutputFile.removeAll(getTask().getTaskId());
  }

}
