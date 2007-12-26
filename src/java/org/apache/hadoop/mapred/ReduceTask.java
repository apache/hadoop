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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InMemoryFileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;

import static org.apache.hadoop.mapred.Task.Counter.*;

/** A Reduce task. */
class ReduceTask extends Task {

  static {                                        // register a ctor
    WritableFactories.setFactory
      (ReduceTask.class,
       new WritableFactory() {
         public Writable newInstance() { return new ReduceTask(); }
       });
  }
  
  private static final Log LOG = LogFactory.getLog(ReduceTask.class.getName());
  private int numMaps;
  private ReduceCopier reduceCopier;

  { 
    getProgress().setStatus("reduce"); 
    setPhase(TaskStatus.Phase.SHUFFLE);        // phase to start with 
  }

  private Progress copyPhase = getProgress().addPhase("copy");
  private Progress sortPhase  = getProgress().addPhase("sort");
  private Progress reducePhase = getProgress().addPhase("reduce");
  private Counters.Counter reduceInputKeyCounter = 
    getCounters().findCounter(REDUCE_INPUT_GROUPS);
  private Counters.Counter reduceInputValueCounter = 
    getCounters().findCounter(REDUCE_INPUT_RECORDS);
  private Counters.Counter reduceOutputCounter = 
    getCounters().findCounter(REDUCE_OUTPUT_RECORDS);

  public ReduceTask() {
    super();
  }

  public ReduceTask(String jobId, String jobFile, String tipId, String taskId,
                    int partition, int numMaps) {
    super(jobId, jobFile, tipId, taskId, partition);
    this.numMaps = numMaps;
  }

  public TaskRunner createRunner(TaskTracker tracker) throws IOException {
    return new ReduceTaskRunner(this, tracker, this.conf);
  }

  public boolean isMapTask() {
    return false;
  }

  public int getNumMaps() { return numMaps; }
  
  /**
   * Localize the given JobConf to be specific for this task.
   */
  public void localizeConfiguration(JobConf conf) throws IOException {
    super.localizeConfiguration(conf);
    conf.setNumMapTasks(numMaps);
  }

  public void write(DataOutput out) throws IOException {
    super.write(out);

    out.writeInt(numMaps);                        // write the number of maps
  }

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    numMaps = in.readInt();
  }

  /** Iterates values while keys match in sorted input. */
  static class ValuesIterator implements Iterator {
    private SequenceFile.Sorter.RawKeyValueIterator in; //input iterator
    private WritableComparable key;               // current key
    private Writable value;                       // current value
    private boolean hasNext;                      // more w/ this key
    private boolean more;                         // more in file
    private WritableComparator comparator;
    private Class keyClass;
    private Class valClass;
    private Configuration conf;
    private DataOutputBuffer valOut = new DataOutputBuffer();
    private DataInputBuffer valIn = new DataInputBuffer();
    private DataInputBuffer keyIn = new DataInputBuffer();
    protected Reporter reporter;

    public ValuesIterator (SequenceFile.Sorter.RawKeyValueIterator in, 
                           WritableComparator comparator, Class keyClass,
                           Class valClass, Configuration conf, 
                           Reporter reporter)
      throws IOException {
      this.in = in;
      this.conf = conf;
      this.comparator = comparator;
      this.keyClass = keyClass;
      this.valClass = valClass;
      this.reporter = reporter;
      getNext();
    }

    /// Iterator methods

    public boolean hasNext() { return hasNext; }

    public Object next() {
      Object result = value;                      // save value
      try {
        getNext();                                  // move to next
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      reporter.progress();
      return result;                              // return saved value
    }

    public void remove() { throw new RuntimeException("not implemented"); }

    /// Auxiliary methods

    /** Start processing next unique key. */
    public void nextKey() {
      while (hasNext) { next(); }                 // skip any unread
      hasNext = more;
    }

    /** True iff more keys remain. */
    public boolean more() { return more; }

    /** The current key. */
    public WritableComparable getKey() { return key; }

    private void getNext() throws IOException {
      Writable lastKey = key;                     // save previous key
      try {
        key = (WritableComparable)ReflectionUtils.newInstance(keyClass, this.conf);
        value = (Writable)ReflectionUtils.newInstance(valClass, this.conf);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      more = in.next();
      if (more) {
        //de-serialize the raw key/value
        keyIn.reset(in.getKey().getData(), in.getKey().getLength());
        key.readFields(keyIn);
        valOut.reset();
        (in.getValue()).writeUncompressedBytes(valOut);
        valIn.reset(valOut.getData(), valOut.getLength());
        value.readFields(valIn);

        if (lastKey == null) {
          hasNext = true;
        } else {
          hasNext = (comparator.compare(key, lastKey) == 0);
        }
      } else {
        hasNext = false;
      }
    }
  }
  private class ReduceValuesIterator extends ValuesIterator {
    public ReduceValuesIterator (SequenceFile.Sorter.RawKeyValueIterator in,
                                 WritableComparator comparator, Class keyClass,
                                 Class valClass,
                                 Configuration conf, Reporter reporter)
      throws IOException {
      super(in, comparator, keyClass, valClass, conf, reporter);
    }
    public void informReduceProgress() {
      reducePhase.set(super.in.getProgress().get()); // update progress
      reporter.progress();
    }
    public Object next() {
      reduceInputValueCounter.increment(1);
      return super.next();
    }
  }

  @SuppressWarnings("unchecked")
  public void run(JobConf job, final TaskUmbilicalProtocol umbilical)
    throws IOException {
    Reducer reducer = (Reducer)ReflectionUtils.newInstance(
                                                           job.getReducerClass(), job);

    // start thread that will handle communication with parent
    startCommunicationThread(umbilical);

    FileSystem lfs = FileSystem.getLocal(job);
    if (!job.get("mapred.job.tracker", "local").equals("local")) {
      reduceCopier = new ReduceCopier(umbilical, job);
      if (!reduceCopier.fetchOutputs()) {
        throw new IOException(getTaskId() + "The reduce copier failed");
      }
    }
    copyPhase.complete();                         // copy is already complete
    

    // open a file to collect map output
    // since we don't know how many map outputs got merged in memory, we have
    // to check whether a given map output exists, and if it does, add it in
    // the list of files to merge, otherwise not.
    List<Path> mapFilesList = new ArrayList<Path>();
    for(int i=0; i < numMaps; i++) {
      Path f;
      try {
        //catch and ignore DiskErrorException, since some map outputs will
        //really be absent (inmem merge).
        f = mapOutputFile.getInputFile(i, getTaskId());
      } catch (DiskErrorException d) { 
        continue;
      }
      if (lfs.exists(f))
        mapFilesList.add(f);
    }
    Path[] mapFiles = new Path[mapFilesList.size()];
    mapFiles = mapFilesList.toArray(mapFiles);
    
    Path tempDir = new Path(getTaskId()); 

    SequenceFile.Sorter.RawKeyValueIterator rIter;
 
    setPhase(TaskStatus.Phase.SORT); 

    final Reporter reporter = getReporter(umbilical);
    
    // sort the input file
    SequenceFile.Sorter sorter = new SequenceFile.Sorter(lfs, 
        job.getOutputKeyComparator(), job.getMapOutputValueClass(), job);
    sorter.setProgressable(reporter);
    rIter = sorter.merge(mapFiles, tempDir, 
        !conf.getKeepFailedTaskFiles()); // sort

    sortPhase.complete();                         // sort is complete
    setPhase(TaskStatus.Phase.REDUCE); 

    // make output collector
    String finalName = getOutputName(getPartition());
    FileSystem fs = FileSystem.get(job);

    final RecordWriter out = 
      job.getOutputFormat().getRecordWriter(fs, job, finalName, reporter);  
    
    OutputCollector collector = new OutputCollector() {
        @SuppressWarnings("unchecked")
        public void collect(WritableComparable key, Writable value)
          throws IOException {
          out.write(key, value);
          reduceOutputCounter.increment(1);
          // indicate that progress update needs to be sent
          reporter.progress();
        }
      };
    
    // apply reduce function
    try {
      Class keyClass = job.getMapOutputKeyClass();
      Class valClass = job.getMapOutputValueClass();
      
      ReduceValuesIterator values = new ReduceValuesIterator(rIter, 
          job.getOutputValueGroupingComparator(), keyClass, valClass, 
          job, reporter);
      values.informReduceProgress();
      while (values.more()) {
        reduceInputKeyCounter.increment(1);
        reducer.reduce(values.getKey(), values, collector, reporter);
        values.nextKey();
        values.informReduceProgress();
      }

      //Clean up: repeated in catch block below
      reducer.close();
      out.close(reporter);
      //End of clean up.
    } catch (IOException ioe) {
      try {
        reducer.close();
      } catch (IOException ignored) {}
        
      try {
        out.close(reporter);
      } catch (IOException ignored) {}
      
      throw ioe;
    }
    done(umbilical);
  }

  class ReduceCopier implements MRConstants {

    /** Reference to the umbilical object */
    private TaskUmbilicalProtocol umbilical;
    
    /** Reference to the task object */
    
    /** Number of ms before timing out a copy */
    private static final int STALLED_COPY_TIMEOUT = 3 * 60 * 1000;
    
    /**
     * our reduce task instance
     */
    private ReduceTask reduceTask;
    
    /**
     * the list of map outputs currently being copied
     */
    private List<MapOutputLocation> scheduledCopies;
    
    /**
     *  the results of dispatched copy attempts
     */
    private List<CopyResult> copyResults;
    
    /**
     *  the number of outputs to copy in parallel
     */
    private int numCopiers;
    
    /**
     * the amount of time spent on fetching one map output before considering 
     * it as failed and notifying the jobtracker about it.
     */
    private int maxBackoff;
    
    /**
     * busy hosts from which copies are being backed off
     * Map of host -> next contact time
     */
    private Map<String, Long> penaltyBox;
    
    /**
     * the set of unique hosts from which we are copying
     */
    private Set<String> uniqueHosts;
    
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
     * When we accumulate mergeThreshold number of files in ram, we merge/spill
     */
    private int mergeThreshold = 500;
    
    /**
     * The threads for fetching the files.
     */
    private MapOutputCopier[] copiers = null;
    
    /**
     * The object for metrics reporting.
     */
    private ShuffleClientMetrics shuffleClientMetrics = null;
    
    /**
     * the minimum interval between tasktracker polls
     */
    private static final long MIN_POLL_INTERVAL = 1000;
    
    /**
     * the number of map output locations to poll for at one time
     */  
    private int probe_sample_size = 100;
    
    /**
     * a list of map output locations for fetch retrials 
     */
    private List<MapOutputLocation> retryFetches =
      new ArrayList<MapOutputLocation>();
    
    /** 
     * The set of required map outputs
     */
    private Set <Integer> neededOutputs = 
      Collections.synchronizedSet(new TreeSet<Integer>());
    
    /** 
     * The set of obsolete map taskids.
     */
    private Set <String> obsoleteMapIds = 
      Collections.synchronizedSet(new TreeSet<String>());
    
    private Random random = null;
    
    /**
     * the max size of the merge output from ramfs
     */
    private long ramfsMergeOutputSize;
    
    /**
     * the max of all the map completion times
     */
    private int maxMapRuntime;
    
    /**
     * Maximum number of fetch-retries per-map.
     */
    private int maxFetchRetriesPerMap;
    
    /**
     * Maximum percent of failed fetch attempt before killing the reduce task.
     */
    private static final float MAX_ALLOWED_FAILED_FETCH_ATTEMPT_PERCENT = 0.5f;

    /**
     * Minimum percent of progress required to keep the reduce alive.
     */
    private static final float MIN_REQUIRED_PROGRESS_PERCENT = 0.5f;

    /**
     * Maximum percent of shuffle execution time required to keep the reducer alive.
     */
    private static final float MAX_ALLOWED_STALL_TIME_PERCENT = 0.5f;

    /**
     * Maximum no. of unique maps from which we failed to fetch map-outputs
     * even after {@link #maxFetchRetriesPerMap} retries; after this the
     * reduce task is failed.
     */
    private static final int MAX_FAILED_UNIQUE_FETCHES = 5;

    /**
     * The maps from which we fail to fetch map-outputs 
     * even after {@link #maxFetchRetriesPerMap} retries.
     */
    Set<Integer> fetchFailedMaps = new TreeSet<Integer>(); 
    
    /**
     * A map of taskId -> no. of failed fetches
     */
    Map<String, Integer> mapTaskToFailedFetchesMap = 
      new HashMap<String, Integer>();    

    /**
     * Initial backoff interval (milliseconds)
     */
    private static final int BACKOFF_INIT = 4000; 

    /**
     * This class contains the methods that should be used for metrics-reporting
     * the specific metrics for shuffle. This class actually reports the
     * metrics for the shuffle client (the ReduceTask), and hence the name
     * ShuffleClientMetrics.
     */
    class ShuffleClientMetrics implements Updater {
      private MetricsRecord shuffleMetrics = null;
      private int numFailedFetches = 0;
      private int numSuccessFetches = 0;
      private long numBytes = 0;
      private int numThreadsBusy = 0;
      ShuffleClientMetrics(JobConf conf) {
        MetricsContext metricsContext = MetricsUtil.getContext("mapred");
        this.shuffleMetrics = 
          MetricsUtil.createRecord(metricsContext, "shuffleInput");
        this.shuffleMetrics.setTag("user", conf.getUser());
        this.shuffleMetrics.setTag("jobName", conf.getJobName());
        this.shuffleMetrics.setTag("jobId", ReduceTask.this.getJobId());
        this.shuffleMetrics.setTag("taskId", getTaskId());
        this.shuffleMetrics.setTag("sessionId", conf.getSessionId());
        metricsContext.registerUpdater(this);
      }
      public synchronized void inputBytes(long numBytes) {
        this.numBytes += numBytes;
      }
      public synchronized void failedFetch() {
        ++numFailedFetches;
      }
      public synchronized void successFetch() {
        ++numSuccessFetches;
      }
      public synchronized void threadBusy() {
        ++numThreadsBusy;
      }
      public synchronized void threadFree() {
        --numThreadsBusy;
      }
      public void doUpdates(MetricsContext unused) {
        synchronized (this) {
          shuffleMetrics.incrMetric("shuffle_input_bytes", numBytes);
          shuffleMetrics.incrMetric("shuffle_failed_fetches", 
                                    numFailedFetches);
          shuffleMetrics.incrMetric("shuffle_success_fetches", 
                                    numSuccessFetches);
          if (numCopiers != 0) {
            shuffleMetrics.setMetric("shuffle_fetchers_busy_percent",
                100*((float)numThreadsBusy/numCopiers));
          } else {
            shuffleMetrics.setMetric("shuffle_fetchers_busy_percent", 0);
          }
          numBytes = 0;
          numSuccessFetches = 0;
          numFailedFetches = 0;
        }
        shuffleMetrics.update();
      }
    }

    /** Represents the result of an attempt to copy a map output */
    private class CopyResult {
      
      // the map output location against which a copy attempt was made
      private final MapOutputLocation loc;
      
      // the size of the file copied, -1 if the transfer failed
      private final long size;
      
      //a flag signifying whether a copy result is obsolete
      private static final int OBSOLETE = -2;
      
      CopyResult(MapOutputLocation loc, long size) {
        this.loc = loc;
        this.size = size;
      }
      
      public int getMapId() { return loc.getMapId(); }
      public boolean getSuccess() { return size >= 0; }
      public boolean isObsolete() { 
        return size == OBSOLETE;
      }
      public long getSize() { return size; }
      public String getHost() { return loc.getHost(); }
      public MapOutputLocation getLocation() { return loc; }
    }
    
    private int extractMapIdFromPathName(Path pathname) {
      //all paths end with map_<id>.out
      String firstPathName = pathname.getName();
      int beginIndex = firstPathName.lastIndexOf("map_");
      int endIndex = firstPathName.lastIndexOf(".out");
      return Integer.parseInt(firstPathName.substring(beginIndex +
                              "map_".length(), endIndex));
    }
    
    private int nextMapOutputCopierId = 0;
    
    /** Copies map outputs as they become available */
    private class MapOutputCopier extends Thread {
      
      private MapOutputLocation currentLocation = null;
      private int id = nextMapOutputCopierId++;
      private Reporter reporter;
      
      public MapOutputCopier(Reporter reporter) {
        setName("MapOutputCopier " + reduceTask.getTaskId() + "." + id);
        LOG.debug(getName() + " created");
        this.reporter = reporter;
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
          LOG.debug(getName() + " finishing " + currentLocation + " =" + size);
          synchronized (copyResults) {
            copyResults.add(new CopyResult(currentLocation, size));
            copyResults.notify();
          }
          currentLocation = null;
        }
      }
      
      /** Loop forever and fetch map outputs as they become available.
       * The thread exits when it is interrupted by {@link ReduceTaskRunner}
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
              loc = scheduledCopies.remove(0);
            }
            
            try {
              shuffleClientMetrics.threadBusy();
              start(loc);
              size = copyOutput(loc);
              shuffleClientMetrics.successFetch();
            } catch (IOException e) {
              LOG.warn(reduceTask.getTaskId() + " copy failed: " +
                       loc.getMapTaskId() + " from " + loc.getHost());
              LOG.warn(StringUtils.stringifyException(e));
              shuffleClientMetrics.failedFetch();
              
              // Reset 
              size = -1;
            } finally {
              shuffleClientMetrics.threadFree();
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
      
      /** Copies a a map output from a remote host, via HTTP. 
       * @param currentLocation the map output location to be copied
       * @return the path (fully qualified) of the copied file
       * @throws IOException if there is an error copying the file
       * @throws InterruptedException if the copier should give up
       */
      private long copyOutput(MapOutputLocation loc
                              ) throws IOException, InterruptedException {
        // check if we still need to copy the output from this location
        if (!neededOutputs.contains(loc.getMapId()) || 
            obsoleteMapIds.contains(loc.getMapTaskId())) {
          return CopyResult.OBSOLETE;
        } 
 
        String reduceId = reduceTask.getTaskId();
        LOG.info(reduceId + " Copying " + loc.getMapTaskId() +
                 " output from " + loc.getHost() + ".");
        // a temp filename. If this file gets created in ramfs, we're fine,
        // else, we will check the localFS to find a suitable final location
        // for this path
        Path filename = new Path("/" + reduceId + "/map_" +
                                 loc.getMapId() + ".out");
        // a working filename that will be unique to this attempt
        Path tmpFilename = new Path(filename + "-" + id);
        // this copies the map output file
        tmpFilename = loc.getFile(inMemFileSys, localFileSys, shuffleClientMetrics,
                                  tmpFilename, lDirAlloc, 
                                  conf, reduceTask.getPartition(), 
                                  STALLED_COPY_TIMEOUT, reporter);
        if (!neededOutputs.contains(loc.getMapId())) {
          if (tmpFilename != null) {
            FileSystem fs = tmpFilename.getFileSystem(conf);
            fs.delete(tmpFilename);
          }
          return CopyResult.OBSOLETE;
        }
        if (tmpFilename == null)
          throw new IOException("File " + filename + "-" + id + 
                                " not created");
        long bytes = -1;
        // lock the ReduceTask while we do the rename
        synchronized (ReduceTask.this) {
          // This file could have been created in the inmemory
          // fs or the localfs. So need to get the filesystem owning the path. 
          FileSystem fs = tmpFilename.getFileSystem(conf);
          if (!neededOutputs.contains(loc.getMapId())) {
            fs.delete(tmpFilename);
            return CopyResult.OBSOLETE;
          }
          
          bytes = fs.getLength(tmpFilename);
          //resolve the final filename against the directory where the tmpFile
          //got created
          filename = new Path(tmpFilename.getParent(), filename.getName());
          // if we can't rename the file, something is broken (and IOException
          // will be thrown). 
          if (!fs.rename(tmpFilename, filename)) {
            fs.delete(tmpFilename);
            bytes = -1;
            throw new IOException("failure to rename map output " + 
                                  tmpFilename);
          }
          
          LOG.info(reduceId + " done copying " + loc.getMapTaskId() +
                   " output from " + loc.getHost() + ".");
          //Create a thread to do merges. Synchronize access/update to 
          //mergeInProgress
          if (!mergeInProgress && 
              (inMemFileSys.getPercentUsed() >= MAX_INMEM_FILESYS_USE || 
               (mergeThreshold > 0 && 
                inMemFileSys.getNumFiles(MAP_OUTPUT_FILTER) >= 
                mergeThreshold))&&
              mergeThrowable == null) {
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
          neededOutputs.remove(loc.getMapId());
        }
        return bytes;
      }
      
    }
    
    private void configureClasspath(JobConf conf)
      throws IOException {
      
      // get the task and the current classloader which will become the parent
      Task task = ReduceTask.this;
      ClassLoader parent = conf.getClassLoader();   
      
      // get the work directory which holds the elements we are dynamically
      // adding to the classpath
      File workDir = new File(task.getJobFile()).getParentFile();
      ArrayList<URL> urllist = new ArrayList<URL>();
      
      // add the jars and directories to the classpath
      String jar = conf.getJar();
      if (jar != null) {      
        LocalDirAllocator lDirAlloc = 
                            new LocalDirAllocator("mapred.local.dir");
        File jobCacheDir = new File(lDirAlloc.getLocalPathToRead(
                                      TaskTracker.getJobCacheSubdir() 
                                      + Path.SEPARATOR + getJobId() 
                                      + Path.SEPARATOR  
                                      + "work", conf).toString());

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
    
    public ReduceCopier(TaskUmbilicalProtocol umbilical, JobConf conf)
      throws IOException {
      
      configureClasspath(conf);
      this.shuffleClientMetrics = new ShuffleClientMetrics(conf);
      this.umbilical = umbilical;      
      this.reduceTask = ReduceTask.this;
      this.scheduledCopies = new ArrayList<MapOutputLocation>(100);
      this.copyResults = new ArrayList<CopyResult>(100);    
      this.numCopiers = conf.getInt("mapred.reduce.parallel.copies", 5);
      this.maxBackoff = conf.getInt("mapred.reduce.copy.backoff", 300);
      
      // the exponential backoff formula
      //    backoff (t) = init * base^(t-1)
      // so for max retries we get
      //    backoff(1) + .... + backoff(max_fetch_retries) ~ max
      // solving which we get
      //    max_fetch_retries ~ log((max * (base - 1) / init) + 1) / log(base)
      // for the default value of max = 300 (5min) we get max_fetch_retries = 6
      // the order is 4,8,16,32,64,128. sum of which is 252 sec = 4.2 min
      
      // optimizing for the base 2
      this.maxFetchRetriesPerMap = getClosestPowerOf2((this.maxBackoff * 1000 
                                                       / BACKOFF_INIT) + 1); 
      this.mergeThreshold = conf.getInt("mapred.inmem.merge.threshold", 1000);
      
      //we want to distinguish inmem fs instances for different reduces. Hence,
      //append a unique string in the uri for the inmem fs name
      URI uri = URI.create("ramfs://mapoutput" + reduceTask.hashCode());
      inMemFileSys = (InMemoryFileSystem)FileSystem.get(uri, conf);
      LOG.info(reduceTask.getTaskId() + " Created an InMemoryFileSystem, uri: "
               + uri);
      ramfsMergeOutputSize = (long)(MAX_INMEM_FILESYS_USE * 
                                    inMemFileSys.getFSSize());
      localFileSys = FileSystem.getLocal(conf);
      //create an instance of the sorter
      sorter =
        new SequenceFile.Sorter(inMemFileSys, conf.getOutputKeyComparator(), 
                                conf.getMapOutputValueClass(), conf);
      sorter.setProgressable(getReporter(umbilical));
      
      // hosts -> next contact time
      this.penaltyBox = new Hashtable<String, Long>();
      
      // hostnames
      this.uniqueHosts = new HashSet<String>();
      
      this.lastPollTime = 0;
      

      // Seed the random number generator with a reasonably globally unique seed
      long randomSeed = System.nanoTime() + 
                        (long)Math.pow(this.reduceTask.getPartition(),
                                       (this.reduceTask.getPartition()%10)
                                      );
      this.random = new Random(randomSeed);
      this.maxMapRuntime = 0;
    }
    
    public boolean fetchOutputs() throws IOException {
      final int      numOutputs = reduceTask.getNumMaps();
      List<MapOutputLocation> knownOutputs = 
        new ArrayList<MapOutputLocation>(numCopiers);
      int totalFailures = 0;
      int            numInFlight = 0, numCopied = 0;
      int            lowThreshold = numCopiers*2;
      long           bytesTransferred = 0;
      DecimalFormat  mbpsFormat = new DecimalFormat("0.00");
      final Progress copyPhase = 
        reduceTask.getProgress().phase();
      
      //tweak the probe sample size (make it a function of numCopiers)
      probe_sample_size = Math.max(numCopiers*5, 50);
      
      for (int i = 0; i < numOutputs; i++) {
        neededOutputs.add(i);
        copyPhase.addPhase();       // add sub-phase per file
      }
      
      copiers = new MapOutputCopier[numCopiers];
      
      Reporter reporter = getReporter(umbilical);
      // start all the copying threads
      for (int i=0; i < copiers.length; i++) {
        copiers[i] = new MapOutputCopier(reporter);
        copiers[i].start();
      }
      
      // start the clock for bandwidth measurement
      long startTime = System.currentTimeMillis();
      long currentTime = startTime;
      long lastProgressTime = System.currentTimeMillis();
      IntWritable fromEventId = new IntWritable(0);
      
      try {
        // loop until we get all required outputs
        while (!neededOutputs.isEmpty() && mergeThrowable == null) {
          
          LOG.info(reduceTask.getTaskId() + " Need " + neededOutputs.size() +
          " map output(s)");
          
          try {
            // Put the hash entries for the failed fetches. Entries here
            // might be replaced by (mapId) hashkeys from new successful 
            // Map executions, if the fetch failures were due to lost tasks.
            // The replacements, if at all, will happen when we query the
            // tasktracker and put the mapId hashkeys with new 
            // MapOutputLocations as values
            knownOutputs.addAll(retryFetches);
             
            // The call getMapCompletionEvents will update fromEventId to
            // used for the next call to getMapCompletionEvents
            int currentNumKnownMaps = knownOutputs.size();
            int currentNumObsoleteMapIds = obsoleteMapIds.size();
            getMapCompletionEvents(fromEventId, knownOutputs);

            
            LOG.info(reduceTask.getTaskId() + ": " +  
                     "Got " + (knownOutputs.size()-currentNumKnownMaps) + 
                     " new map-outputs & " + 
                     (obsoleteMapIds.size()-currentNumObsoleteMapIds) + 
                     " obsolete map-outputs from tasktracker and " + 
                     retryFetches.size() + " map-outputs from previous failures"
                     );
            
            // clear the "failed" fetches hashmap
            retryFetches.clear();
          }
          catch (IOException ie) {
            LOG.warn(reduceTask.getTaskId() +
                    " Problem locating map outputs: " +
                    StringUtils.stringifyException(ie));
          }
          
          // now walk through the cache and schedule what we can
          int numKnown = knownOutputs.size(), numScheduled = 0;
          int numSlow = 0, numDups = 0;
          
          LOG.info(reduceTask.getTaskId() + " Got " + numKnown + 
                   " known map output location(s); scheduling...");
          
          synchronized (scheduledCopies) {
            // Randomize the map output locations to prevent 
            // all reduce-tasks swamping the same tasktracker
            Collections.shuffle(knownOutputs, this.random);
            
            Iterator locIt = knownOutputs.iterator();
            
            currentTime = System.currentTimeMillis();
            while (locIt.hasNext()) {
              
              MapOutputLocation loc = (MapOutputLocation)locIt.next();
              
              // Do not schedule fetches from OBSOLETE maps
              if (obsoleteMapIds.contains(loc.getMapTaskId())) {
                locIt.remove();
                continue;
              }
              
              Long penaltyEnd = penaltyBox.get(loc.getHost());
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
              // we should indicate progress as we don't want TT to think
              // we're stuck and kill us
              reporter.progress();
              Thread.sleep(5000);
            }
          } catch (InterruptedException e) { } // IGNORE
          
          while (numInFlight > 0 && mergeThrowable == null) {
            LOG.debug(reduceTask.getTaskId() + " numInFlight = " + 
                      numInFlight);
            CopyResult cr = getCopyResult();
            
            if (cr != null) {
              if (cr.getSuccess()) {  // a successful copy
                numCopied++;
                lastProgressTime = System.currentTimeMillis();
                bytesTransferred += cr.getSize();
                
                long secsSinceStart = 
                  (System.currentTimeMillis()-startTime)/1000+1;
                float mbs = ((float)bytesTransferred)/(1024*1024);
                float transferRate = mbs/secsSinceStart;
                
                copyPhase.startNextPhase();
                copyPhase.setStatus("copy (" + numCopied + " of " + numOutputs 
                                    + " at " +
                                    mbpsFormat.format(transferRate) +  " MB/s)");
                
                // Note successfull fetch for this mapId to invalidate
                // (possibly) old fetch-failures
                fetchFailedMaps.remove(cr.getLocation().getMapId());
              } else if (cr.isObsolete()) {
                //ignore
                LOG.info(reduceTask.getTaskId() + 
                         " Ignoring obsolete copy result for Map Task: " + 
                         cr.getLocation().getMapTaskId() + " from host: " + 
                         cr.getHost());
              } else {
                retryFetches.add(cr.getLocation());
                
                // note the failed-fetch
                String mapTaskId = cr.getLocation().getMapTaskId();
                Integer mapId = cr.getLocation().getMapId();
                
                totalFailures++;
                Integer noFailedFetches = 
                  mapTaskToFailedFetchesMap.get(mapTaskId);
                noFailedFetches = 
                  (noFailedFetches == null) ? 1 : (noFailedFetches + 1);
                mapTaskToFailedFetchesMap.put(mapTaskId, noFailedFetches);
                LOG.info("Task " + getTaskId() + ": Failed fetch #" + 
                         noFailedFetches + " from " + mapTaskId);
                
                // did the fetch fail too many times?
                // using a hybrid technique for notifying the jobtracker.
                //   a. the first notification is sent after max-retries 
                //   b. subsequent notifications are sent after 2 retries.   
                if ((noFailedFetches >= maxFetchRetriesPerMap) 
                    && ((noFailedFetches - maxFetchRetriesPerMap) % 2) == 0) {
                  synchronized (ReduceTask.this) {
                    taskStatus.addFetchFailedMap(mapTaskId);
                    LOG.info("Failed to fetch map-output from " + mapTaskId + 
                             " even after MAX_FETCH_RETRIES_PER_MAP retries... "
                             + " reporting to the JobTracker");
                  }
                }

                // note unique failed-fetch maps
                if (noFailedFetches == maxFetchRetriesPerMap) {
                  fetchFailedMaps.add(mapId);
                  
                  // did we have too many unique failed-fetch maps?
                  // and did we fail on too many fetch attempts?
                  // and did we progress enough
                  //     or did we wait for too long without any progress?
                  
                  // check if the reducer is healthy
                  boolean reducerHealthy = 
                      (((float)totalFailures / (totalFailures + numCopied)) 
                       < MAX_ALLOWED_FAILED_FETCH_ATTEMPT_PERCENT);
                  
                  // check if the reducer has progressed enough
                  boolean reducerProgressedEnough = 
                      (((float)numCopied / numMaps) 
                       >= MIN_REQUIRED_PROGRESS_PERCENT);
                  
                  // check if the reducer is stalled for a long time
                  
                  // duration for which the reducer is stalled
                  int stallDuration = 
                      (int)(System.currentTimeMillis() - lastProgressTime);
                  // duration for which the reducer ran with progress
                  int shuffleProgressDuration = 
                      (int)(lastProgressTime - startTime);
                  // min time the reducer should run without getting killed
                  int minShuffleRunDuration = 
                      (shuffleProgressDuration > maxMapRuntime) 
                      ? shuffleProgressDuration 
                      : maxMapRuntime;
                  boolean reducerStalled = 
                      (((float)stallDuration / minShuffleRunDuration) 
                       >= MAX_ALLOWED_STALL_TIME_PERCENT);
                  
                  // kill if not healthy and has insufficient progress
                  if ((fetchFailedMaps.size() >= MAX_FAILED_UNIQUE_FETCHES)
                      && !reducerHealthy 
                      && (!reducerProgressedEnough || reducerStalled)) { 
                    LOG.fatal("Shuffle failed with too many fetch failures " + 
                              "and insufficient progress!" +
                              "Killing task " + getTaskId() + ".");
                    umbilical.shuffleError(getTaskId(), 
                                           "Exceeded MAX_FAILED_UNIQUE_FETCHES;"
                                           + " bailing-out.");
                  }
                }
                
                // back off exponentially until num_retries <= max_retries
                // back off by max_backoff/2 on subsequent failed attempts
                currentTime = System.currentTimeMillis();
                int currentBackOff = noFailedFetches <= maxFetchRetriesPerMap 
                                     ? BACKOFF_INIT 
                                       * (1 << (noFailedFetches - 1)) 
                                     : (this.maxBackoff * 1000 / 2);
                penaltyBox.put(cr.getHost(), currentTime + currentBackOff);
                LOG.warn(reduceTask.getTaskId() + " adding host " +
                         cr.getHost() + " to penalty box, next contact in " +
                         (currentBackOff/1000) + " seconds");
                
                // other outputs from the failed host may be present in the
                // knownOutputs cache, purge them. This is important in case
                // the failure is due to a lost tasktracker (causes many
                // unnecessary backoffs). If not, we only take a small hit
                // polling the tasktracker a few more times
                Iterator locIt = knownOutputs.iterator();
                while (locIt.hasNext()) {
                  MapOutputLocation loc = (MapOutputLocation)locIt.next();
                  if (cr.getHost().equals(loc.getHost())) {
                    retryFetches.add(loc);
                    locIt.remove();
                  }
                }
              }
              uniqueHosts.remove(cr.getHost());
              numInFlight--;
            }
            
            boolean busy = true;
            // ensure we have enough to keep us busy
            if (numInFlight < lowThreshold && (numOutputs-numCopied) > 
                probe_sample_size) {
              busy = false;
            }
            //Check whether we have more CopyResult to check. If there is none,
            //and we are not busy enough, break
            synchronized (copyResults) {
              if (copyResults.size() == 0 && !busy) {
                break;
              }
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
        
        //Do a merge of in-memory files (if there are any)
        if (mergeThrowable == null) {
          try {
            //wait for an ongoing merge (if it is in flight) to complete
            while (mergeInProgress) {
              Thread.sleep(200);
            }
            LOG.info(reduceTask.getTaskId() + 
                     " Copying of all map outputs complete. " + 
                     "Initiating the last merge on the remaining files in " + 
                     inMemFileSys.getUri());
            if (mergeThrowable != null) {
              //this could happen if the merge that
              //was in progress threw an exception
              throw mergeThrowable;
            }
            //initiate merge
            Path[] inMemClosedFiles = inMemFileSys.getFiles(MAP_OUTPUT_FILTER);
            if (inMemClosedFiles.length == 0) {
              LOG.info(reduceTask.getTaskId() + "Nothing to merge from " + 
                       inMemFileSys.getUri());
              return neededOutputs.isEmpty();
            }
            //name this output file same as the name of the first file that is 
            //there in the current list of inmem files (this is guaranteed to be
            //absent on the disk currently. So we don't overwrite a prev. 
            //created spill). Also we need to create the output file now since
            //it is not guaranteed that this file will be present after merge
            //is called (we delete empty sequence files as soon as we see them
            //in the merge method)
            int mapId = extractMapIdFromPathName(inMemClosedFiles[0]);
            Path outputPath = mapOutputFile.getInputFileForWrite(mapId, 
                             reduceTask.getTaskId(), ramfsMergeOutputSize);
            SequenceFile.Writer writer = sorter.cloneFileAttributes(
                                                                    inMemFileSys.makeQualified(inMemClosedFiles[0]), 
                                                                    localFileSys.makeQualified(outputPath), null);
            
            SequenceFile.Sorter.RawKeyValueIterator rIter = null;
            try {
              rIter = sorter.merge(inMemClosedFiles, true, 
                                   inMemClosedFiles.length, 
                                   new Path(reduceTask.getTaskId()));
            } catch (Exception e) { 
              //make sure that we delete the ondisk file that we created earlier
              //when we invoked cloneFileAttributes
              writer.close();
              localFileSys.delete(inMemClosedFiles[0]);
              throw new IOException (StringUtils.stringifyException(e));
            }
            sorter.writeFile(rIter, writer);
            writer.close();
            LOG.info(reduceTask.getTaskId() +
                     " Merge of the " +inMemClosedFiles.length +
                     " files in InMemoryFileSystem complete." +
                     " Local file is " + outputPath);
          } catch (Throwable t) {
            LOG.warn(reduceTask.getTaskId() +
                     " Final merge of the inmemory files threw an exception: " + 
                     StringUtils.stringifyException(t));
            return false;
          }
        }
        return mergeThrowable == null && neededOutputs.isEmpty();
      } finally {
        inMemFileSys.close();
      }
    }
    
    
    private CopyResult getCopyResult() {  
      synchronized (copyResults) {
        while (copyResults.isEmpty()) {
          try {
            copyResults.wait();
          } catch (InterruptedException e) { }
        }
        if (copyResults.isEmpty()) {
          return null;
        } else {
          return copyResults.remove(0);
        }
      }    
    }
    
    /** 
     * Queries the {@link TaskTracker} for a set of map-completion events from
     * a given event ID.
     * 
     * @param fromEventId the first event ID we want to start from, this is
     *                     modified by the call to this method
     * @param jobClient the {@link JobTracker}
     * @return the set of map-completion events from the given event ID 
     * @throws IOException
     */  
    private void getMapCompletionEvents(IntWritable fromEventId, 
                                        List<MapOutputLocation> knownOutputs)
    throws IOException {
      
      long currentTime = System.currentTimeMillis();    
      long pollTime = lastPollTime + MIN_POLL_INTERVAL;
      while (currentTime < pollTime) {
        try {
          Thread.sleep(pollTime-currentTime);
        } catch (InterruptedException ie) { } // IGNORE
        currentTime = System.currentTimeMillis();
      }
      
      TaskCompletionEvent events[] = 
        umbilical.getMapCompletionEvents(reduceTask.getJobId(), 
                                         fromEventId.get(), probe_sample_size);
      
      // Note the last successful poll time-stamp
      lastPollTime = currentTime;

      // Update the last seen event ID
      fromEventId.set(fromEventId.get() + events.length);
      
      // Process the TaskCompletionEvents:
      // 1. Save the SUCCEEDED maps in knownOutputs to fetch the outputs.
      // 2. Save the OBSOLETE/FAILED/KILLED maps in obsoleteOutputs to stop fetching
      //    from those maps.
      // 3. Remove TIPFAILED maps from neededOutputs since we don't need their
      //    outputs at all.
      for (TaskCompletionEvent event : events) {
        switch (event.getTaskStatus()) {
          case SUCCEEDED:
          {
            URI u = URI.create(event.getTaskTrackerHttp());
            String host = u.getHost();
            int port = u.getPort();
            String taskId = event.getTaskId();
            int mId = event.idWithinJob();
            int duration = event.getTaskRunTime();
            if (duration > maxMapRuntime) {
              maxMapRuntime = duration; 
              // adjust max-fetch-retries based on max-map-run-time
              maxFetchRetriesPerMap = 
                  getClosestPowerOf2((maxMapRuntime / BACKOFF_INIT) + 1);
            }
            knownOutputs.add(new MapOutputLocation(taskId, mId, host, port));
          }
          break;
          case FAILED:
          case KILLED:
          case OBSOLETE:
          {
            obsoleteMapIds.add(event.getTaskId());
            LOG.info("Ignoring obsolete output of " + event.getTaskStatus() + 
                     " map-task: '" + event.getTaskId() + "'");
          }
          break;
          case TIPFAILED:
          {
            neededOutputs.remove(event.idWithinJob());
            LOG.info("Ignoring output of failed map TIP: '" +  
            		 event.getTaskId() + "'");
          }
          break;
        }
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
          Path[] inMemClosedFiles;
          //initiate merge
          synchronized (ReduceTask.this) {
            inMemClosedFiles = inMemFileSys.getFiles(MAP_OUTPUT_FILTER);
          }
          //Note that the above Path[] could be of length 0 if all copies are 
          //in flight. So we make sure that we have some 'closed' map
          //output files to merge to get the benefit of in-memory merge
          if (inMemClosedFiles.length >= 
              (int)(MAX_INMEM_FILESYS_USE/MAX_INMEM_FILESIZE_FRACTION)) {
            //name this output file same as the name of the first file that is 
            //there in the current list of inmem files (this is guaranteed to
            //be absent on the disk currently. So we don't overwrite a prev. 
            //created spill). Also we need to create the output file now since
            //it is not guaranteed that this file will be present after merge
            //is called (we delete empty sequence files as soon as we see them
            //in the merge method)

            //figure out the mapId 
            int mapId = extractMapIdFromPathName(inMemClosedFiles[0]);
            
            Path outputPath = mapOutputFile.getInputFileForWrite(mapId, 
                              reduceTask.getTaskId(), ramfsMergeOutputSize);

            SequenceFile.Writer writer = sorter.cloneFileAttributes(
                                                                    inMemFileSys.makeQualified(inMemClosedFiles[0]), 
                                                                    localFileSys.makeQualified(outputPath), null);
            SequenceFile.Sorter.RawKeyValueIterator rIter;
            try {
              rIter = sorter.merge(inMemClosedFiles, true, 
                                   inMemClosedFiles.length, new Path(reduceTask.getTaskId()));
            } catch (Exception e) { 
              //make sure that we delete the ondisk file that we created 
              //earlier when we invoked cloneFileAttributes
              writer.close();
              localFileSys.delete(outputPath);
              throw new IOException (StringUtils.stringifyException(e));
            }
            sorter.writeFile(rIter, writer);
            writer.close();
            LOG.info(reduceTask.getTaskId() + 
                     " Merge of the " +inMemClosedFiles.length +
                     " files in InMemoryFileSystem complete." +
                     " Local file is " + outputPath);
          }
          else {
            LOG.info(reduceTask.getTaskId() + " Nothing to merge from " + 
                     inMemFileSys.getUri());
          }
        } catch (Throwable t) {
          LOG.warn(reduceTask.getTaskId() +
                   " Intermediate Merge of the inmemory files threw an exception: "
                   + StringUtils.stringifyException(t));
          ReduceCopier.this.mergeThrowable = t;
        }
        finally {
          mergeInProgress = false;
        }
      }
    }
    final private PathFilter MAP_OUTPUT_FILTER = new PathFilter() {
        public boolean accept(Path file) {
          return file.toString().endsWith(".out");
        }     
      };
  }

  private static int getClosestPowerOf2(int value) {
    int power = 0;
    int approx = 1;
    while (approx < value) {
      ++power;
      approx = (approx << 1);
    }
    if ((value - (approx >> 1)) < (approx - value)) {
      --power;
    }
    return power;
  }
}
