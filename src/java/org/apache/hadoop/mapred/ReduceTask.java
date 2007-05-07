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
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
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
  private boolean sortComplete;
  private ReduceCopier reduceCopier;

  { 
    getProgress().setStatus("reduce"); 
    setPhase(TaskStatus.Phase.SHUFFLE);        // phase to start with 
  }

  private Progress copyPhase = getProgress().addPhase("copy");
  private Progress sortPhase  = getProgress().addPhase("sort");
  private Progress reducePhase = getProgress().addPhase("reduce");

  public ReduceTask() {}

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
      // ignore the error, since failures in progress shouldn't kill us
      try {
        reporter.progress();
      } catch (IOException ie) { 
        LOG.debug("caught exception from progress", ie);
      }
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
      try {
        reporter.progress();
      } catch (IOException ie) {
        LOG.debug("Exception caught from progress", ie);
      }
    }
    public Object next() {
      reporter.incrCounter(REDUCE_INPUT_RECORDS, 1);
      return super.next();
    }
  }

  public void run(JobConf job, final TaskUmbilicalProtocol umbilical)
    throws IOException {
    Class valueClass = job.getMapOutputValueClass();
    Reducer reducer = (Reducer)ReflectionUtils.newInstance(
                                                           job.getReducerClass(), job);
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
    
    // spawn a thread to give sort progress heartbeats
    Thread sortProgress = new Thread() {
        public void run() {
          while (!sortComplete) {
            try {
              reportProgress(umbilical);
              Thread.sleep(PROGRESS_INTERVAL);
            } catch (InterruptedException e) {
              return;
            } catch (Throwable e) {
              System.out.println("Thread Exception in " +
                                 "reporting sort progress\n" +
                                 StringUtils.stringifyException(e));
              continue;
            }
          }
        }
      };
    sortProgress.setName("Sort progress reporter for task "+getTaskId());

    Path tempDir = new Path(getTaskId()); 

    WritableComparator comparator = job.getOutputValueGroupingComparator();
    
    SequenceFile.Sorter.RawKeyValueIterator rIter;
 
    try {
      setPhase(TaskStatus.Phase.SORT); 
      sortProgress.start();

      // sort the input file
      SequenceFile.Sorter sorter =
        new SequenceFile.Sorter(lfs, comparator, valueClass, job);
      rIter = sorter.merge(mapFiles, tempDir, 
                           !conf.getKeepFailedTaskFiles()); // sort

    } finally {
      sortComplete = true;
    }

    sortPhase.complete();                         // sort is complete
    setPhase(TaskStatus.Phase.REDUCE); 

    final Reporter reporter = getReporter(umbilical);
    
    // make output collector
    String finalName = getOutputName(getPartition());
    FileSystem fs = FileSystem.get(job);

    final RecordWriter out = 
      job.getOutputFormat().getRecordWriter(fs, job, finalName, reporter);  
    
    OutputCollector collector = new OutputCollector() {
        public void collect(WritableComparable key, Writable value)
          throws IOException {
          out.write(key, value);
          reporter.incrCounter(REDUCE_OUTPUT_RECORDS, 1);
          reportProgress(umbilical);
        }
      };
    
    // apply reduce function
    try {
      Class keyClass = job.getMapOutputKeyClass();
      Class valClass = job.getMapOutputValueClass();
      ReduceValuesIterator values = new ReduceValuesIterator(rIter, comparator, 
                                                             keyClass, valClass, job, reporter);
      values.informReduceProgress();
      while (values.more()) {
        reporter.incrCounter(REDUCE_INPUT_GROUPS, 1);
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

  /** Construct output file names so that, when an output directory listing is
   * sorted lexicographically, positions correspond to output partitions.*/

  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();

  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }

  static synchronized String getOutputName(int partition) {
    return "part-" + NUMBER_FORMAT.format(partition);
  }

  private class ReduceCopier implements MRConstants {

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
     * the maximum amount of time (less 1 minute) to wait to 
     * contact a host after a copy from it fails. We wait for (1 min +
     * Random.nextInt(maxBackoff)) seconds.
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
     * The threads for fetching the files.
     */
    private MetricsRecord shuffleMetrics = null;
    
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
     * a TreeSet for needed map outputs
     */
    private Set <Integer> neededOutputs = 
      Collections.synchronizedSet(new TreeSet<Integer>());
    
    private Random random = null;
    
    /**
     * the max size of the merge output from ramfs
     */
    private long ramfsMergeOutputSize;
    
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
    
    private Thread createProgressThread(final TaskUmbilicalProtocol umbilical) {
      //spawn a thread to give copy progress heartbeats
      Thread copyProgress = new Thread() {
          public void run() {
            LOG.debug("Started thread: " + getName());
            while (true) {
              try {
                reportProgress(umbilical);
                Thread.sleep(PROGRESS_INTERVAL);
              } catch (InterruptedException e) {
                return;
              } catch (Throwable e) {
                LOG.info("Thread Exception in " +
                         "reporting copy progress\n" +
                         StringUtils.stringifyException(e));
                continue;
              }
            }
          }
        };
      copyProgress.setName("Copy progress reporter for task "+getTaskId());
      copyProgress.setDaemon(true);
      return copyProgress;
    }
    
    private int nextMapOutputCopierId = 0;
    
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
        if (!neededOutputs.contains(loc.getMapId())) {
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
        tmpFilename = loc.getFile(inMemFileSys, localFileSys, shuffleMetrics,
                                  tmpFilename, lDirAlloc, 
                                  conf, reduceTask.getPartition(), 
                                  STALLED_COPY_TIMEOUT);
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
    
    public ReduceCopier(TaskUmbilicalProtocol umbilical, JobConf conf)
      throws IOException {
      
      configureClasspath(conf);
      this.umbilical = umbilical;      
      this.reduceTask = ReduceTask.this;
      this.scheduledCopies = new ArrayList<MapOutputLocation>(100);
      this.copyResults = new ArrayList<CopyResult>(100);    
      this.numCopiers = conf.getInt("mapred.reduce.parallel.copies", 5);
      this.maxBackoff = conf.getInt("mapred.reduce.copy.backoff", 300);
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
      
      // hosts -> next contact time
      this.penaltyBox = new Hashtable<String, Long>();
      
      // hostnames
      this.uniqueHosts = new HashSet<String>();
      
      this.lastPollTime = 0;
      
      MetricsContext metricsContext = MetricsUtil.getContext("mapred");
      this.shuffleMetrics = 
        MetricsUtil.createRecord(metricsContext, "shuffleInput");
      this.shuffleMetrics.setTag("user", conf.getUser());

      // Seed the random number generator with a reasonably globally unique seed
      long randomSeed = System.nanoTime() + 
                        (long)Math.pow(this.reduceTask.getPartition(),
                                       (this.reduceTask.getPartition()%10)
                                      );
      this.random = new Random(randomSeed);
    }
    
    public boolean fetchOutputs() throws IOException {
      final int      numOutputs = reduceTask.getNumMaps();
      List<MapOutputLocation> knownOutputs = 
        new ArrayList<MapOutputLocation>(numCopiers);
      int            numInFlight = 0, numCopied = 0;
      int            lowThreshold = numCopiers*2;
      long           bytesTransferred = 0;
      DecimalFormat  mbpsFormat = new DecimalFormat("0.00");
      Random         backoff = new Random();
      final Progress copyPhase = 
        reduceTask.getProgress().phase();
      
      //tweak the probe sample size (make it a function of numCopiers)
      probe_sample_size = Math.max(numCopiers*5, 50);
      
      for (int i = 0; i < numOutputs; i++) {
        neededOutputs.add(new Integer(i));
        copyPhase.addPhase();       // add sub-phase per file
      }
      
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
      
      Thread copyProgress = createProgressThread(umbilical);
      copyProgress.start();
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
            // The call getsMapCompletionEvents will modify fromEventId to a val
            // that it should be for the next call to getSuccessMapEvents
            List <MapOutputLocation> locs = getMapCompletionEvents(fromEventId);

            // put discovered them on the known list
            for (int i=0; i < locs.size(); i++) {
              knownOutputs.add(locs.get(i));
            }
            LOG.info(reduceTask.getTaskId() +
                    " Got " + locs.size() + 
                    " new map outputs from tasktracker and " + retryFetches.size()
                    + " map outputs from previous failures");
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
                bytesTransferred += cr.getSize();
                
                long secsSinceStart = 
                  (System.currentTimeMillis()-startTime)/1000+1;
                float mbs = ((float)bytesTransferred)/(1024*1024);
                float transferRate = mbs/secsSinceStart;
                
                copyPhase.startNextPhase();
                copyPhase.setStatus("copy (" + numCopied + " of " + numOutputs 
                                    + " at " +
                                    mbpsFormat.format(transferRate) +  " MB/s)");          
              } else if (cr.isObsolete()) {
                //ignore
                LOG.info(reduceTask.getTaskId() + 
                         " Ignoring obsolete copy result for Map Task: " + 
                         cr.getLocation().getMapTaskId() + " from host: " + 
                         cr.getHost());
              } else {
                retryFetches.add(cr.getLocation());
                
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
        copyProgress.interrupt();
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
    
    /** Queries the task tracker for a set of outputs ready to be copied
     * @param fromEventId the first event ID we want to start from, this is
     * modified by the call to this method
     * @param jobClient the job tracker
     * @return a set of locations to copy outputs from
     * @throws IOException
     */  
    private List <MapOutputLocation> getMapCompletionEvents(IntWritable fromEventId)
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
      
      TaskCompletionEvent t[] = umbilical.getMapCompletionEvents(
                                                                 reduceTask.getJobId().toString(),
                                                                 fromEventId.get(),
                                                                 probe_sample_size);
      
      List <MapOutputLocation> mapOutputsList = 
        new ArrayList<MapOutputLocation>();
      for (TaskCompletionEvent event : t) {
        if (event.getTaskStatus() == TaskCompletionEvent.Status.SUCCEEDED) {
          URI u = URI.create(event.getTaskTrackerHttp());
          String host = u.getHost();
          int port = u.getPort();
          String taskId = event.getTaskId();
          int mId = event.idWithinJob();
          mapOutputsList.add(new MapOutputLocation(taskId, mId, host, port));
        } else if (event.getTaskStatus() == TaskCompletionEvent.Status.TIPFAILED) {
          neededOutputs.remove(event.idWithinJob());
          LOG.info("Ignoring output of failed map: '" + event.getTaskId() + "'");
        }
      }
    
      fromEventId.set(fromEventId.get() + t.length);
      return mapOutputsList;
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
}
