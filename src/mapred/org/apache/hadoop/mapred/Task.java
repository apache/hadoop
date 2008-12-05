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
import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.DistributedFileSystem;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.kfs.KosmosFileSystem;
import org.apache.hadoop.fs.s3.S3FileSystem;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/** Base class for tasks. */
abstract class Task implements Writable, Configurable {
  private static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.mapred.TaskRunner");

  // Counters used by Task subclasses
  protected static enum Counter { 
    MAP_INPUT_RECORDS, 
    MAP_OUTPUT_RECORDS,
    MAP_INPUT_BYTES, 
    MAP_OUTPUT_BYTES,
    COMBINE_INPUT_RECORDS,
    COMBINE_OUTPUT_RECORDS,
    REDUCE_INPUT_GROUPS,
    REDUCE_INPUT_RECORDS,
    REDUCE_OUTPUT_RECORDS
  }
  
  /**
   * Counters to measure the usage of the different file systems.
   */
  protected static enum FileSystemCounter {
    LOCAL_READ, LOCAL_WRITE, 
    HDFS_READ, HDFS_WRITE, 
    S3_READ, S3_WRITE,
    KFS_READ, KFSWRITE
  }

  ///////////////////////////////////////////////////////////
  // Helper methods to construct task-output paths
  ///////////////////////////////////////////////////////////
  
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

  ////////////////////////////////////////////
  // Fields
  ////////////////////////////////////////////

  private String jobFile;                         // job configuration file
  private TaskAttemptID taskId;                          // unique, includes job id
  private int partition;                          // id within job
  TaskStatus taskStatus; 										      // current status of the task
  private Path taskOutputPath;                    // task-specific output dir
  
  protected JobConf conf;
  protected MapOutputFile mapOutputFile = new MapOutputFile();
  protected LocalDirAllocator lDirAlloc;

  ////////////////////////////////////////////
  // Constructors
  ////////////////////////////////////////////

  public Task() {
    taskStatus = TaskStatus.createTaskStatus(isMapTask());
  }

  public Task(String jobFile, TaskAttemptID taskId, int partition) {
    this.jobFile = jobFile;
    this.taskId = taskId;
     
    this.partition = partition;
    this.taskStatus = TaskStatus.createTaskStatus(isMapTask(), this.taskId, 
                                                  0.0f, 
                                                  TaskStatus.State.UNASSIGNED, 
                                                  "", "", "", 
                                                  isMapTask() ? 
                                                    TaskStatus.Phase.MAP : 
                                                    TaskStatus.Phase.SHUFFLE, 
                                                  counters);
    this.mapOutputFile.setJobId(taskId.getJobID());
  }

  ////////////////////////////////////////////
  // Accessors
  ////////////////////////////////////////////
  public void setJobFile(String jobFile) { this.jobFile = jobFile; }
  public String getJobFile() { return jobFile; }
  public TaskAttemptID getTaskID() { return taskId; }
  public Counters getCounters() { return counters; }
  
  /**
   * Get the job name for this task.
   * @return the job name
   */
  public JobID getJobID() {
    return taskId.getJobID();
  }
  
  /**
   * Get the index of this task within the job.
   * @return the integer part of the task id
   */
  public int getPartition() {
    return partition;
  }
  /**
   * Return current phase of the task. 
   * needs to be synchronized as communication thread sends the phase every second
   * @return
   */
  public synchronized TaskStatus.Phase getPhase(){
    return this.taskStatus.getPhase(); 
  }
  /**
   * Set current phase of the task. 
   * @param p
   */
  protected synchronized void setPhase(TaskStatus.Phase phase){
    this.taskStatus.setPhase(phase); 
  }

  ////////////////////////////////////////////
  // Writable methods
  ////////////////////////////////////////////

  public void write(DataOutput out) throws IOException {
    Text.writeString(out, jobFile);
    taskId.write(out);
    out.writeInt(partition);
    if (taskOutputPath != null) {
      Text.writeString(out, taskOutputPath.toString());
    } else {
      Text.writeString(out, "");
    }
    taskStatus.write(out);
  }
  public void readFields(DataInput in) throws IOException {
    jobFile = Text.readString(in);
    taskId = TaskAttemptID.read(in);
    partition = in.readInt();
    String outPath = Text.readString(in);
    if (outPath.length() != 0) {
      taskOutputPath = new Path(outPath);
    } else {
      taskOutputPath = null;
    }
    taskStatus.readFields(in);
    this.mapOutputFile.setJobId(taskId.getJobID()); 
  }

  @Override
  public String toString() { return taskId.toString(); }

  private Path getTaskOutputPath(JobConf conf) {
    Path p = new Path(FileOutputFormat.getOutputPath(conf), 
      (MRConstants.TEMP_DIR_NAME + Path.SEPARATOR + "_" + taskId));
    try {
      FileSystem fs = p.getFileSystem(conf);
      return p.makeQualified(fs);
    } catch (IOException ie) {
      LOG.warn(StringUtils.stringifyException(ie));
      return p;
    }
  }
  
  /**
   * Localize the given JobConf to be specific for this task.
   */
  public void localizeConfiguration(JobConf conf) throws IOException {
    conf.set("mapred.tip.id", taskId.getTaskID().toString()); 
    conf.set("mapred.task.id", taskId.toString());
    conf.setBoolean("mapred.task.is.map", isMapTask());
    conf.setInt("mapred.task.partition", partition);
    conf.set("mapred.job.id", taskId.getJobID().toString());
    
    // The task-specific output path
    if (FileOutputFormat.getOutputPath(conf) != null) {
      taskOutputPath = getTaskOutputPath(conf);
      FileOutputFormat.setWorkOutputPath(conf, taskOutputPath);
    }
  }
  
  /** Run this task as a part of the named job.  This method is executed in the
   * child process and is what invokes user-supplied map, reduce, etc. methods.
   * @param umbilical for progress reports
   */
  public abstract void run(JobConf job, TaskUmbilicalProtocol umbilical)
    throws IOException;


  /** Return an approprate thread runner for this task. */
  public abstract TaskRunner createRunner(TaskTracker tracker
                                          ) throws IOException;

  /** The number of milliseconds between progress reports. */
  public static final int PROGRESS_INTERVAL = 3000;

  private transient Progress taskProgress = new Progress();

  // Current counters
  private transient Counters counters = new Counters();
  
  /**
   * flag that indicates whether progress update needs to be sent to parent.
   * If true, it has been set. If false, it has been reset. 
   * Using AtomicBoolean since we need an atomic read & reset method. 
   */  
  private AtomicBoolean progressFlag = new AtomicBoolean(false);
  /* flag to track whether task is done */
  private AtomicBoolean taskDone = new AtomicBoolean(false);
  // getters and setters for flag
  private void setProgressFlag() {
    progressFlag.set(true);
  }
  private boolean resetProgressFlag() {
    return progressFlag.getAndSet(false);
  }
  
  public abstract boolean isMapTask();

  public Progress getProgress() { return taskProgress; }

  InputSplit getInputSplit() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Input only available on map");
  }

  /** 
   * The communication thread handles communication with the parent (Task Tracker). 
   * It sends progress updates if progress has been made or if the task needs to 
   * let the parent know that it's alive. It also pings the parent to see if it's alive. 
   */
  protected void startCommunicationThread(final TaskUmbilicalProtocol umbilical) {
    Thread thread = new Thread(new Runnable() {
        public void run() {
          final int MAX_RETRIES = 3;
          int remainingRetries = MAX_RETRIES;
          // get current flag value and reset it as well
          boolean sendProgress = resetProgressFlag();
          while (!taskDone.get()) {
            try {
              boolean taskFound = true; // whether TT knows about this task
              // sleep for a bit
              try {
                Thread.sleep(PROGRESS_INTERVAL);
              } 
              catch (InterruptedException e) {
                LOG.debug(getTaskID() + " Progress/ping thread exiting " +
                                        "since it got interrupted");
                break;
              }
              
              if (sendProgress) {
                // we need to send progress update
                updateCounters();
                taskStatus.statusUpdate(taskProgress.get(), taskProgress.toString(), 
                        counters);
                taskFound = umbilical.statusUpdate(taskId, taskStatus);
                taskStatus.clearStatus();
              }
              else {
                // send ping 
                taskFound = umbilical.ping(taskId);
              }
              
              // if Task Tracker is not aware of our task ID (probably because it died and 
              // came back up), kill ourselves
              if (!taskFound) {
                LOG.warn("Parent died.  Exiting "+taskId);
                System.exit(66);
              }
              
              sendProgress = resetProgressFlag(); 
              remainingRetries = MAX_RETRIES;
            } 
            catch (Throwable t) {
              LOG.info("Communication exception: " + StringUtils.stringifyException(t));
              remainingRetries -=1;
              if (remainingRetries == 0) {
                ReflectionUtils.logThreadInfo(LOG, "Communication exception", 0);
                LOG.warn("Last retry, killing "+taskId);
                System.exit(65);
              }
            }
          }
        }
      }, "Comm thread for "+taskId);
    thread.setDaemon(true);
    thread.start();
    LOG.debug(getTaskID() + " Progress/ping thread started");
  }

  
  protected Reporter getReporter(final TaskUmbilicalProtocol umbilical) 
    throws IOException 
  {
    return new Reporter() {
        public void setStatus(String status) {
          taskProgress.setStatus(status);
          // indicate that progress update needs to be sent
          setProgressFlag();
        }
        public void progress() {
          // indicate that progress update needs to be sent
          setProgressFlag();
        }
        public void incrCounter(Enum key, long amount) {
          if (counters != null) {
            counters.incrCounter(key, amount);
          }
          setProgressFlag();
        }
        public void incrCounter(String group, String counter, long amount) {
          if (counters != null) {
            counters.incrCounter(group, counter, amount);
          }
          setProgressFlag();
        }
        public InputSplit getInputSplit() throws UnsupportedOperationException {
          return Task.this.getInputSplit();
        }
      };
  }

  public void setProgress(float progress) {
    taskProgress.set(progress);
    // indicate that progress update needs to be sent
    setProgressFlag();
  }

  /**
   * An updater that tracks the last number reported for a given file
   * system and only creates the counters when they are needed.
   */
  class FileSystemStatisticUpdater {
    private long prevReadBytes = 0;
    private long prevWriteBytes = 0;
    private FileSystem.Statistics stats;
    private Counters.Counter readCounter = null;
    private Counters.Counter writeCounter = null;
    private FileSystemCounter read;
    private FileSystemCounter write;

    FileSystemStatisticUpdater(FileSystemCounter read,
                               FileSystemCounter write,
                               Class<? extends FileSystem> cls) {
      stats = FileSystem.getStatistics(cls);
      this.read = read;
      this.write = write;
    }

    void updateCounters() {
      long newReadBytes = stats.getBytesRead();
      long newWriteBytes = stats.getBytesWritten();
      if (prevReadBytes != newReadBytes) {
        if (readCounter == null) {
          readCounter = counters.findCounter(read);
        }
        readCounter.increment(newReadBytes - prevReadBytes);
        prevReadBytes = newReadBytes;
      }
      if (prevWriteBytes != newWriteBytes) {
        if (writeCounter == null) {
          writeCounter = counters.findCounter(write);
        }
        writeCounter.increment(newWriteBytes - prevWriteBytes);
        prevWriteBytes = newWriteBytes;
      }
    }
  }
  
  /**
   * A list of all of the file systems that we want to report on.
   */
  private List<FileSystemStatisticUpdater> statisticUpdaters =
     new ArrayList<FileSystemStatisticUpdater>();
  {
    statisticUpdaters.add
      (new FileSystemStatisticUpdater(FileSystemCounter.LOCAL_READ,
                                      FileSystemCounter.LOCAL_WRITE,
                                      RawLocalFileSystem.class));
    statisticUpdaters.add
      (new FileSystemStatisticUpdater(FileSystemCounter.HDFS_READ,
                                      FileSystemCounter.HDFS_WRITE,
                                      DistributedFileSystem.class));
    statisticUpdaters.add
    (new FileSystemStatisticUpdater(FileSystemCounter.KFS_READ,
                                    FileSystemCounter.KFSWRITE,
                                    KosmosFileSystem.class));
    statisticUpdaters.add
    (new FileSystemStatisticUpdater(FileSystemCounter.S3_READ,
                                    FileSystemCounter.S3_WRITE,
                                    S3FileSystem.class));
  }

  private synchronized void updateCounters() {
    for(FileSystemStatisticUpdater updater: statisticUpdaters) {
      updater.updateCounters();
    }
  }

  public void done(TaskUmbilicalProtocol umbilical) throws IOException {
    int retries = 10;
    boolean needProgress = true;
    updateCounters();
    taskDone.set(true);
    while (true) {
      try {
        if (needProgress) {
          // send a final status report
          taskStatus.statusUpdate(taskProgress.get(), taskProgress.toString(), 
                                  counters);
          try {
            if (!umbilical.statusUpdate(getTaskID(), taskStatus)) {
              LOG.warn("Parent died.  Exiting "+taskId);
              System.exit(66);
            }
            taskStatus.clearStatus();
            needProgress = false;
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();       // interrupt ourself
          }
        }
        // Check whether there is any task output
        boolean shouldBePromoted = false;
        try {
          if (taskOutputPath != null) {
            // Get the file-system for the task output directory
            FileSystem fs = taskOutputPath.getFileSystem(conf);
            if (fs.exists(taskOutputPath)) {
              // Get the summary for the folder
              ContentSummary summary = fs.getContentSummary(taskOutputPath);
              // Check if the directory contains data to be promoted
              // i.e total-files + total-folders - 1(itself)
              if (summary != null 
                  && (summary.getFileCount() + summary.getDirectoryCount() - 1)
                      > 0) {
                shouldBePromoted = true;
              }
            } else {
              LOG.info(getTaskID() + ": No outputs to promote from " + 
                       taskOutputPath);
            }
          }
        } catch (IOException ioe) {
          // To be safe in case of an exception
          shouldBePromoted = true;
        }
        umbilical.done(taskId, shouldBePromoted);
        LOG.info("Task '" + getTaskID() + "' done.");
        return;
      } catch (IOException ie) {
        LOG.warn("Failure signalling completion: " + 
                 StringUtils.stringifyException(ie));
        if (--retries == 0) {
          throw ie;
        }
      }
    }
  }
  
  public void setConf(Configuration conf) {
    if (conf instanceof JobConf) {
      this.conf = (JobConf) conf;

      if (taskId != null && taskOutputPath == null && 
          FileOutputFormat.getOutputPath(this.conf) != null) {
        taskOutputPath = getTaskOutputPath(this.conf);
      }
    } else {
      this.conf = new JobConf(conf);
    }
    this.mapOutputFile.setConf(this.conf);
    this.lDirAlloc = new LocalDirAllocator("mapred.local.dir");
    // add the static resolutions (this is required for the junit to
    // work on testcases that simulate multiple nodes on a single physical
    // node.
    String hostToResolved[] = conf.getStrings("hadoop.net.static.resolutions");
    if (hostToResolved != null) {
      for (String str : hostToResolved) {
        String name = str.substring(0, str.indexOf('='));
        String resolvedName = str.substring(str.indexOf('=') + 1);
        NetUtils.addStaticResolution(name, resolvedName);
      }
    }
  }

  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Save the task's output on successful completion.
   * 
   * @throws IOException
   */
  void saveTaskOutput() throws IOException {

    if (taskOutputPath != null) {
      FileSystem fs = taskOutputPath.getFileSystem(conf);
      if (fs.exists(taskOutputPath)) {
        Path jobOutputPath = taskOutputPath.getParent().getParent();

        // Move the task outputs to their final place
        moveTaskOutputs(fs, jobOutputPath, taskOutputPath);

        // Delete the temporary task-specific output directory
        if (!fs.delete(taskOutputPath, true)) {
          LOG.info("Failed to delete the temporary output directory of task: " + 
                  getTaskID() + " - " + taskOutputPath);
        }
        
        LOG.info("Saved output of task '" + getTaskID() + "' to " + jobOutputPath);
      }
    }
  }
  
  void removeTaskOutput() throws IOException {
    if (taskOutputPath != null) {
      FileSystem fs = taskOutputPath.getFileSystem(conf);
      // Delete the temporary task-specific output directory
      fs.delete(taskOutputPath, true);
    }
  }
  
  private Path getFinalPath(Path jobOutputDir, Path taskOutput) throws IOException {
    URI taskOutputUri = taskOutput.toUri();
    URI relativePath = taskOutputPath.toUri().relativize(taskOutputUri);
    if (taskOutputUri == relativePath) {//taskOutputPath is not a parent of taskOutput
      throw new IOException("Can not get the relative path: base = " +
          taskOutputPath + " child = " + taskOutput);
    }
    if (relativePath.getPath().length() > 0) {
      return new Path(jobOutputDir, relativePath.getPath());
    } else {
      return jobOutputDir;
    }
  }
  
  private void moveTaskOutputs(FileSystem fs, Path jobOutputDir, Path taskOutput) 
  throws IOException {
    if (fs.isFile(taskOutput)) {
      Path finalOutputPath = getFinalPath(jobOutputDir, taskOutput);
      if (!fs.rename(taskOutput, finalOutputPath)) {
        if (!fs.delete(finalOutputPath, true)) {
          throw new IOException("Failed to delete earlier output of task: " + 
                  getTaskID());
        }
        if (!fs.rename(taskOutput, finalOutputPath)) {
          throw new IOException("Failed to save output of task: " + 
                  getTaskID());
        }
      }
      LOG.debug("Moved " + taskOutput + " to " + finalOutputPath);
    } else if(fs.isDirectory(taskOutput)) {
      FileStatus[] paths = fs.listStatus(taskOutput);
      Path finalOutputPath = getFinalPath(jobOutputDir, taskOutput);
      fs.mkdirs(finalOutputPath);
      if (paths != null) {
        for (FileStatus path : paths) {
          moveTaskOutputs(fs, jobOutputDir, path.getPath());
        }
      }
    }
  }

  /**
   * OutputCollector for the combiner.
   */
  protected static class CombineOutputCollector<K extends Object, V extends Object> 
  implements OutputCollector<K, V> {
    private Writer<K, V> writer;
    private Counters.Counter outCounter;
    public CombineOutputCollector(Counters.Counter outCounter) {
      this.outCounter = outCounter;
    }
    public synchronized void setWriter(Writer<K, V> writer) {
      this.writer = writer;
    }
    public synchronized void collect(K key, V value)
        throws IOException {
      outCounter.increment(1);
      writer.append(key, value);
    }
  }

  /** Iterates values while keys match in sorted input. */
  static class ValuesIterator<KEY,VALUE> implements Iterator<VALUE> {
    protected RawKeyValueIterator in; //input iterator
    private KEY key;               // current key
    private KEY nextKey;
    private VALUE value;             // current value
    private boolean hasNext;                      // more w/ this key
    private boolean more;                         // more in file
    private RawComparator<KEY> comparator;
    protected Progressable reporter;
    private Deserializer<KEY> keyDeserializer;
    private Deserializer<VALUE> valDeserializer;
    private DataInputBuffer keyIn = new DataInputBuffer();
    private DataInputBuffer valueIn = new DataInputBuffer();
    
    @SuppressWarnings("unchecked")
    public ValuesIterator (RawKeyValueIterator in, 
                           RawComparator<KEY> comparator, 
                           Class<KEY> keyClass,
                           Class<VALUE> valClass, Configuration conf, 
                           Progressable reporter)
      throws IOException {
      this.in = in;
      this.comparator = comparator;
      this.reporter = reporter;
      SerializationFactory serializationFactory = new SerializationFactory(conf);
      this.keyDeserializer = serializationFactory.getDeserializer(keyClass);
      this.keyDeserializer.open(keyIn);
      this.valDeserializer = serializationFactory.getDeserializer(valClass);
      this.valDeserializer.open(this.valueIn);
      readNextKey();
      key = nextKey;
      nextKey = null; // force new instance creation
      hasNext = more;
    }

    RawKeyValueIterator getRawIterator() { return in; }
    
    /// Iterator methods

    public boolean hasNext() { return hasNext; }

    private int ctr = 0;
    public VALUE next() {
      if (!hasNext) {
        throw new NoSuchElementException("iterate past last value");
      }
      try {
        readNextValue();
        readNextKey();
      } catch (IOException ie) {
        throw new RuntimeException("problem advancing post rec#"+ctr, ie);
      }
      reporter.progress();
      return value;
    }

    public void remove() { throw new RuntimeException("not implemented"); }

    /// Auxiliary methods

    /** Start processing next unique key. */
    void nextKey() throws IOException {
      // read until we find a new key
      while (hasNext) { 
        readNextKey();
      }
      ++ctr;
      
      // move the next key to the current one
      KEY tmpKey = key;
      key = nextKey;
      nextKey = tmpKey;
      hasNext = more;
    }

    /** True iff more keys remain. */
    boolean more() { 
      return more; 
    }

    /** The current key. */
    KEY getKey() { 
      return key; 
    }

    /** 
     * read the next key 
     */
    private void readNextKey() throws IOException {
      more = in.next();
      if (more) {
        DataInputBuffer nextKeyBytes = in.getKey();
        keyIn.reset(nextKeyBytes.getData(), nextKeyBytes.getPosition(), nextKeyBytes.getLength());
        nextKey = keyDeserializer.deserialize(nextKey);
        hasNext = key != null && (comparator.compare(key, nextKey) == 0);
      } else {
        hasNext = false;
      }
    }

    /**
     * Read the next value
     * @throws IOException
     */
    private void readNextValue() throws IOException {
      DataInputBuffer nextValueBytes = in.getValue();
      valueIn.reset(nextValueBytes.getData(), nextValueBytes.getPosition(), nextValueBytes.getLength());
      value = valDeserializer.deserialize(value);
    }
  }

  protected static class CombineValuesIterator<KEY,VALUE>
      extends ValuesIterator<KEY,VALUE> {

    private final Counters.Counter combineInputCounter;

    public CombineValuesIterator(RawKeyValueIterator in,
        RawComparator<KEY> comparator, Class<KEY> keyClass,
        Class<VALUE> valClass, Configuration conf, Reporter reporter,
        Counters.Counter combineInputCounter) throws IOException {
      super(in, comparator, keyClass, valClass, conf, reporter);
      this.combineInputCounter = combineInputCounter;
    }

    public VALUE next() {
      combineInputCounter.increment(1);
      return super.next();
    }
  }

}
