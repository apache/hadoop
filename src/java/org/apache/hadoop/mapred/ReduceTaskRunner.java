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
import org.apache.hadoop.fs.InMemoryFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Sorter.RawKeyValueIterator;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.IntWritable;

import java.io.*;
import java.util.*;
import java.net.*;
import java.text.DecimalFormat;
import org.apache.hadoop.util.Progressable;

/** Runs a reduce task. */
class ReduceTaskRunner extends TaskRunner implements MRConstants {
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
   * A reference to the in memory file system for writing the map outputs to.
   */
  private InMemoryFileSystem inMemFileSys;
  
  /**
   * A reference to the local file system for writing the map outputs to.
   */
  private FileSystem localFileSys;

  /**
   * An instance of the sorter used for doing merge
   */
  private SequenceFile.Sorter sorter;
  
  /**
   * A reference to the throwable object (if merge throws an exception)
   */
  private volatile Throwable mergeThrowable;
  
  /** 
   * A flag to indicate that merge is in progress
   */
  private volatile boolean mergeInProgress = false;

  /**
   * The threads for fetching the files.
   */
  private MapOutputCopier[] copiers = null;
  
  /**
   * The threads for fetching the files.
   */
  private MetricsRecord shuffleMetrics = null;
  
  /**
   * the minimum interval between jobtracker polls
   */
  private static final long MIN_POLL_INTERVAL = 5000;
  
  /**
   * the number of map output locations to poll for at one time
   */  
  private int probe_sample_size = 50;
  
  /**
   * a Random used during the map output fetching
   */
  private Random randForProbing;
  
  /**
   * a hashmap from mapId to MapOutputLocation for retrials
   */
  private Map<Integer, MapOutputLocation> retryFetches = new HashMap();
  
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

  private class PingTimer extends Thread implements Progressable {
    Task task = getTask();
    TaskTracker tracker = getTracker();

    public void run() {
      LOG.info(task.getTaskId() + " Started thread: " + getName());
      while (true) {
        try {
          progress();
          Thread.sleep(Task.PROGRESS_INTERVAL);
        }
        catch (InterruptedException i) {
          return;
        }
        catch (Throwable e) {
          LOG.info(task.getTaskId() + " Thread Exception in " +
                   "reporting sort progress\n" +
                   StringUtils.stringifyException(e));
          continue;
        }
      }
    }
    
    public void progress() {
      task.reportProgress(tracker);
    }
  }

  private static int nextMapOutputCopierId = 0;

  /** Copies map outputs as they become available */
  private class MapOutputCopier extends Thread {

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
            size = copyOutput(loc);
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
     * @return the path (fully qualified) of the copied file
     * @throws IOException if there is an error copying the file
     * @throws InterruptedException if the copier should give up
     */
    private long copyOutput(MapOutputLocation loc
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
      tmpFilename = loc.getFile(inMemFileSys, localFileSys, shuffleMetrics,
                               tmpFilename, reduceTask.getPartition(),
                               STALLED_COPY_TIMEOUT);
      if (tmpFilename == null)
        throw new IOException("File " + finalFilename + "-" + id + 
                              " not created");
      long bytes = -1;
      // lock the ReduceTaskRunner while we do the rename
      synchronized (ReduceTaskRunner.this) {
        // if we can't rename the file, something is broken (and IOException
        // will be thrown). This file could have been created in the inmemory
        // fs or the localfs. So need to get the filesystem owning the path. 
        FileSystem fs = tmpFilename.getFileSystem(conf);
        if (!fs.rename(tmpFilename, finalFilename)) {
          fs.delete(tmpFilename);
          throw new IOException("failure to rename map output " + tmpFilename);
        }
        bytes = fs.getLength(finalFilename);
        LOG.info(reduceId + " done copying " + loc.getMapTaskId() +
                 " output from " + loc.getHost() + ".");
        //Create a thread to do merges. Synchronize access/update to 
        //mergeInProgress
        if (!mergeInProgress && inMemFileSys.getPercentUsed() >= 
                                                       MAX_INMEM_FILESYS_USE) {
          LOG.info(reduceId + " InMemoryFileSystem " + 
                   inMemFileSys.getUri().toString() +
                   " is " + inMemFileSys.getPercentUsed() + 
                   " full. Triggering merge");
          InMemFSMergeThread m = new InMemFSMergeThread(inMemFileSys,
                                     (LocalFileSystem)localFileSys, sorter);
          m.setName("Thread for merging in memory files");
          m.setDaemon(true);
          mergeInProgress = true;
          m.start();
        }
      }
      return bytes;
    }

  }
  
  private void configureClasspath(JobConf conf)
    throws IOException {
    
    // get the task and the current classloader which will become the parent
    Task task = getTask();
    ClassLoader parent = conf.getClassLoader();   
    
    // get the work directory which holds the elements we are dynamically
    // adding to the classpath
    File workDir = new File(task.getJobFile()).getParentFile();
    File jobCacheDir = new File(workDir.getParent(), "work");
    ArrayList<URL> urllist = new ArrayList<URL>();
    
    // add the jars and directories to the classpath
    String jar = conf.getJar();
    if (jar != null) {      
      File[] libs = new File(jobCacheDir, "lib").listFiles();
      if (libs != null) {
        for (int i = 0; i < libs.length; i++) {
          urllist.add(libs[i].toURL());
        }
      }
      urllist.add(new File(jobCacheDir, "classes").toURL());
      urllist.add(jobCacheDir.toURL());
     
    }
    urllist.add(workDir.toURL());
    
    // create a new classloader with the old classloader as its parent
    // then set that classloader as the one used by the current jobconf
    URL[] urls = urllist.toArray(new URL[urllist.size()]);
    URLClassLoader loader = new URLClassLoader(urls, parent);
    conf.setClassLoader(loader);
  }

  public ReduceTaskRunner(Task task, TaskTracker tracker, 
                          JobConf conf) throws IOException {
    
    super(task, tracker, conf);
    configureClasspath(conf);
    this.mapOutputFile = new MapOutputFile();
    this.mapOutputFile.setConf(conf);

    this.reduceTask = (ReduceTask)getTask();
    this.scheduledCopies = new ArrayList(100);
    this.copyResults = new ArrayList(100);    
    this.numCopiers = conf.getInt("mapred.reduce.parallel.copies", 5);
    this.maxBackoff = conf.getInt("mapred.reduce.copy.backoff", 300);

    //we want to distinguish inmem fs instances for different reduces. Hence,
    //append a unique string in the uri for the inmem fs name
    URI uri = URI.create("ramfs://mapoutput" + reduceTask.hashCode());
    inMemFileSys = (InMemoryFileSystem)FileSystem.get(uri, conf);
    LOG.info(reduceTask.getTaskId() + " Created an InMemoryFileSystem, uri: " +
             uri);
    localFileSys = FileSystem.getLocal(conf);
    //create an instance of the sorter
    sorter =
      new SequenceFile.Sorter(inMemFileSys, conf.getOutputKeyComparator(), 
          conf.getMapOutputValueClass(), conf);
    
    // hosts -> next contact time
    this.penaltyBox = new Hashtable();
    
    // hostnames
    this.uniqueHosts = new HashSet();
    
    this.lastPollTime = 0;

    MetricsContext metricsContext = MetricsUtil.getContext("mapred");
    this.shuffleMetrics = 
        MetricsUtil.createRecord(metricsContext, "shuffleInput");
    this.shuffleMetrics.setTag("user", conf.getUser());
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
    
    //tweak the probe sample size (make it a function of numCopiers)
    probe_sample_size = Math.max(numCopiers*5, 50);
    randForProbing = new Random(reduceTask.getPartition() * 100);
    
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
    IntWritable fromEventId = new IntWritable(0);
    
    PingTimer pingTimer = new PingTimer();
    pingTimer.setName("Map output copy reporter for task " + 
                      reduceTask.getTaskId());
    pingTimer.setDaemon(true);
    pingTimer.start();
    try {
    // loop until we get all required outputs or are killed
    while (!killed && numCopied < numOutputs && mergeThrowable == null) {

      LOG.info(reduceTask.getTaskId() + " Need " + (numOutputs-numCopied) +
               " map output(s)");

      if (!neededOutputs.isEmpty()) {
        LOG.info(reduceTask.getTaskId() + " Need " + neededOutputs.size() +
                 " map output location(s)");
        try {
          // the call to queryJobTracker will modify fromEventId to a value
          // that it should be for the next call to queryJobTracker
          MapOutputLocation[] locs = queryJobTracker(fromEventId, jobClient);
          
          // remove discovered outputs from needed list
          // and put them on the known list
          int gotLocs = (locs == null ? 0 : locs.length);
          for (int i=0; i < locs.length; i++) {
            // check whether we actually need an output. It could happen
            // that a map task that successfully ran earlier got lost, but
            // if we already have copied the output of that unfortunate task
            // we need not copy it again from the new TT (we will ignore 
            // the event for the new rescheduled execution)
            if(neededOutputs.remove(new Integer(locs[i].getMapId()))) {
              // remove the mapId from the retryFetches hashmap since we now
              // prefer the new location instead of what we saved earlier
              retryFetches.remove(new Integer(locs[i].getMapId()));
              knownOutputs.add(locs[i]);
            }
            else gotLocs--; //we don't need this output
            
          }
          // now put the remaining hash entries for the failed fetches 
          // and clear the hashmap
          knownOutputs.addAll(retryFetches.values());
          LOG.info(reduceTask.getTaskId() +
                " Got " + gotLocs + 
                " new map outputs from jobtracker and " + retryFetches.size() +
                " map outputs from previous failures");
          retryFetches.clear();
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
          Thread.sleep(5000);
        }
      } catch (InterruptedException e) { } // IGNORE

      while (!killed && numInFlight > 0 && mergeThrowable == null) {
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
          } else {
            // this copy failed, put it back onto neededOutputs
            neededOutputs.add(new Integer(cr.getMapId()));
            retryFetches.put(new Integer(cr.getMapId()), cr.getLocation());
          
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
                retryFetches.put(new Integer(loc.getMapId()), loc);
                locIt.remove();
                neededOutputs.add(new Integer(loc.getMapId()));
              }
            }
          }
          uniqueHosts.remove(cr.getHost());
          numInFlight--;
        }
        
        // ensure we have enough to keep us busy
        if (numInFlight < lowThreshold && (numOutputs-numCopied) > probe_sample_size) {
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
    
    if (mergeThrowable != null) {
      //set the task state to FAILED
      TaskTracker tracker = ReduceTaskRunner.this.getTracker();
      TaskTracker.TaskInProgress tip = 
        tracker.runningTasks.get(reduceTask.getTaskId());
      tip.runstate = TaskStatus.State.FAILED;
      try {
        tip.cleanup();
      } catch (Throwable ie2) {
        // Ignore it, we are just trying to cleanup.
      }
      inMemFileSys.close();
    }
    
    //Do a merge of in-memory files (if there are any)
    if (!killed && mergeThrowable == null) {
      try {
        //wait for an ongoing merge (if it is in flight) to complete
        while (mergeInProgress) {
          Thread.sleep(200);
        }
        LOG.info(reduceTask.getTaskId() + 
                 " Copying of all map outputs complete. " + 
                 "Initiating the last merge on the remaining files in " + 
                 inMemFileSys.getUri());
        //initiate merge
        Path[] inMemClosedFiles = inMemFileSys.getFiles(MAP_OUTPUT_FILTER);
        if (inMemClosedFiles.length == 0) {
          LOG.info(reduceTask.getTaskId() + "Nothing to merge from " + 
              inMemFileSys.getUri());
          return numCopied == numOutputs;
        }
        RawKeyValueIterator rIter = 
          sorter.merge(inMemClosedFiles, true, inMemClosedFiles.length, 
                       new Path(reduceTask.getTaskId()));
        //name this output file same as the name of the first file that is 
        //there in the current list of inmem files (this is guaranteed to be
        //absent on the disk currently. So we don't overwrite a prev. 
        //created spill)
        SequenceFile.Writer writer = sorter.cloneFileAttributes(
            inMemFileSys.makeQualified(inMemClosedFiles[0]), 
            localFileSys.makeQualified(inMemClosedFiles[0]), null);
        sorter.writeFile(rIter, writer);
        writer.close();
        LOG.info(reduceTask.getTaskId() +
                 " Merge of the " +inMemClosedFiles.length +
                 " files in InMemoryFileSystem complete." +
                 " Local file is " + inMemClosedFiles[0]);
      } catch (Throwable t) {
        LOG.warn("Merge of the inmemory files threw an exception: " + 
            StringUtils.stringifyException(t));
        inMemFileSys.close();
        return false;
      }
    }
    return mergeThrowable == null && numCopied == numOutputs && !killed;
    } finally {
      pingTimer.interrupt();
    }
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
   * @param fromEventId the first event ID we want to start from, this will be
   * modified by the call to this method
   * @param jobClient the job tracker
   * @return a set of locations to copy outputs from
   * @throws IOException
   */  
  private MapOutputLocation[] queryJobTracker(IntWritable fromEventId, 
                                              InterTrackerProtocol jobClient)
  throws IOException {
    
    long currentTime = System.currentTimeMillis();    
    long pollTime = lastPollTime + MIN_POLL_INTERVAL;
    while (currentTime < pollTime) {
      try {
        Thread.sleep(pollTime-currentTime);
      } catch (InterruptedException ie) { } // IGNORE
      currentTime = System.currentTimeMillis();
    }
    lastPollTime = currentTime;

    TaskCompletionEvent t[] = jobClient.getTaskCompletionEvents(
                                      reduceTask.getJobId().toString(),
                                      fromEventId.get(),
                                      probe_sample_size);
    
    List <MapOutputLocation> mapOutputsList = new ArrayList();
    for (int i = 0; i < t.length; i++) {
      if (t[i].isMap && 
          t[i].getTaskStatus() == TaskCompletionEvent.Status.SUCCEEDED) {
        URI u = URI.create(t[i].getTaskTrackerHttp());
        String host = u.getHost();
        int port = u.getPort();
        String taskId = t[i].getTaskId();
        int mId = t[i].idWithinJob();
        mapOutputsList.add(new MapOutputLocation(taskId, mId, host, port));
      }
    }
    Collections.shuffle(mapOutputsList, randForProbing);
    MapOutputLocation[] locations =
                        new MapOutputLocation[mapOutputsList.size()];
    fromEventId.set(fromEventId.get() + t.length);
    return mapOutputsList.toArray(locations);
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

  private class InMemFSMergeThread extends Thread {
    private InMemoryFileSystem inMemFileSys;
    private LocalFileSystem localFileSys;
    private SequenceFile.Sorter sorter;
    
    public InMemFSMergeThread(InMemoryFileSystem inMemFileSys, 
        LocalFileSystem localFileSys, SequenceFile.Sorter sorter) {
      this.inMemFileSys = inMemFileSys;
      this.localFileSys = localFileSys;
      this.sorter = sorter;
    }
    public void run() {
      LOG.info(reduceTask.getTaskId() + " Thread started: " + getName());
      try {
        //initiate merge
        Path[] inMemClosedFiles = inMemFileSys.getFiles(MAP_OUTPUT_FILTER);
        //Note that the above Path[] could be of length 0 if all copies are 
        //in flight. So we make sure that we have some 'closed' map
        //output files to merge to get the benefit of in-memory merge
        if (inMemClosedFiles.length >= 
          (int)(MAX_INMEM_FILESYS_USE/MAX_INMEM_FILESIZE_FRACTION)) {
          RawKeyValueIterator rIter = sorter.merge(inMemClosedFiles, true, 
              inMemClosedFiles.length, new Path(reduceTask.getTaskId()));
          //name this output file same as the name of the first file that is 
          //there in the current list of inmem files (this is guaranteed to be
          //absent on the disk currently. So we don't overwrite a prev. 
          //created spill)
          SequenceFile.Writer writer = sorter.cloneFileAttributes(
              inMemFileSys.makeQualified(inMemClosedFiles[0]), 
              localFileSys.makeQualified(inMemClosedFiles[0]), null);
          sorter.writeFile(rIter, writer);
          writer.close();
          LOG.info(reduceTask.getTaskId() + 
                   " Merge of the " +inMemClosedFiles.length +
                   " files in InMemoryFileSystem complete." +
                   " Local file is " + inMemClosedFiles[0]);
        }
        else {
          LOG.info(reduceTask.getTaskId() + " Nothing to merge from " + 
              inMemFileSys.getUri());
        }
      } catch (Throwable t) {
        LOG.warn("Merge of the inmemory files threw an exception: " + 
            StringUtils.stringifyException(t));
        ReduceTaskRunner.this.mergeThrowable = t;
      }
      finally {
        mergeInProgress = false;
      }
    }
  }
  final private static PathFilter MAP_OUTPUT_FILTER = new PathFilter() {
    public boolean accept(Path file) {
      return file.toString().endsWith(".out");
    }     
  };
}
