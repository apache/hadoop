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
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.crypto.SecretKey;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.apache.hadoop.mapreduce.task.ReduceContextImpl;
import org.apache.hadoop.mapreduce.util.MRJobConfUtil;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for tasks.
 */
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
abstract public class Task implements Writable, Configurable {
  private static final Logger LOG =
      LoggerFactory.getLogger(Task.class);

  public static String MERGED_OUTPUT_PREFIX = ".merged";
  public static final long DEFAULT_COMBINE_RECORDS_BEFORE_PROGRESS = 10000;
  
  /**
   * @deprecated Provided for compatibility. Use {@link TaskCounter} instead.
   */
  @Deprecated
  public enum Counter {
    MAP_INPUT_RECORDS, 
    MAP_OUTPUT_RECORDS,
    MAP_SKIPPED_RECORDS,
    MAP_INPUT_BYTES, 
    MAP_OUTPUT_BYTES,
    MAP_OUTPUT_MATERIALIZED_BYTES,
    COMBINE_INPUT_RECORDS,
    COMBINE_OUTPUT_RECORDS,
    REDUCE_INPUT_GROUPS,
    REDUCE_SHUFFLE_BYTES,
    REDUCE_INPUT_RECORDS,
    REDUCE_OUTPUT_RECORDS,
    REDUCE_SKIPPED_GROUPS,
    REDUCE_SKIPPED_RECORDS,
    SPILLED_RECORDS,
    SPLIT_RAW_BYTES,
    CPU_MILLISECONDS,
    PHYSICAL_MEMORY_BYTES,
    VIRTUAL_MEMORY_BYTES,
    COMMITTED_HEAP_BYTES,
    MAP_PHYSICAL_MEMORY_BYTES_MAX,
    MAP_VIRTUAL_MEMORY_BYTES_MAX,
    REDUCE_PHYSICAL_MEMORY_BYTES_MAX,
    REDUCE_VIRTUAL_MEMORY_BYTES_MAX
  }

  /**
   * Counters to measure the usage of the different file systems.
   * Always return the String array with two elements. First one is the name of  
   * BYTES_READ counter and second one is of the BYTES_WRITTEN counter.
   */
  protected static String[] getFileSystemCounterNames(String uriScheme) {
    String scheme = StringUtils.toUpperCase(uriScheme);
    return new String[]{scheme+"_BYTES_READ", scheme+"_BYTES_WRITTEN"};
  }
  
  /**
   * Name of the FileSystem counters' group
   */
  protected static final String FILESYSTEM_COUNTER_GROUP = "FileSystemCounters";

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
  private String user;                            // user running the job
  private TaskAttemptID taskId;                   // unique, includes job id
  private int partition;                          // id within job
  private byte[] encryptedSpillKey = new byte[] {0};  // Key Used to encrypt
  // intermediate spills
  TaskStatus taskStatus;                          // current status of the task
  protected JobStatus.State jobRunStateForCleanup;
  protected boolean jobCleanup = false;
  protected boolean jobSetup = false;
  protected boolean taskCleanup = false;
 
  // An opaque data field used to attach extra data to each task. This is used
  // by the Hadoop scheduler for Mesos to associate a Mesos task ID with each
  // task and recover these IDs on the TaskTracker.
  protected BytesWritable extraData = new BytesWritable(); 
  
  //skip ranges based on failed ranges from previous attempts
  private SortedRanges skipRanges = new SortedRanges();
  private boolean skipping = false;
  private boolean writeSkipRecs = true;
  
  //currently processing record start index
  private volatile long currentRecStartIndex; 
  private Iterator<Long> currentRecIndexIterator = 
    skipRanges.skipRangeIterator();

  private ResourceCalculatorProcessTree pTree;
  private long initCpuCumulativeTime = ResourceCalculatorProcessTree.UNAVAILABLE;

  protected JobConf conf;
  protected MapOutputFile mapOutputFile;
  protected LocalDirAllocator lDirAlloc;
  private final static int MAX_RETRIES = 10;
  protected JobContext jobContext;
  protected TaskAttemptContext taskContext;
  protected org.apache.hadoop.mapreduce.OutputFormat<?,?> outputFormat;
  protected org.apache.hadoop.mapreduce.OutputCommitter committer;
  protected final Counters.Counter spilledRecordsCounter;
  protected final Counters.Counter failedShuffleCounter;
  protected final Counters.Counter mergedMapOutputsCounter;
  private int numSlotsRequired;
  protected TaskUmbilicalProtocol umbilical;
  protected SecretKey tokenSecret;
  protected SecretKey shuffleSecret;
  protected GcTimeUpdater gcUpdater;
  final AtomicBoolean mustPreempt = new AtomicBoolean(false);

  ////////////////////////////////////////////
  // Constructors
  ////////////////////////////////////////////

  public Task() {
    taskStatus = TaskStatus.createTaskStatus(isMapTask());
    taskId = new TaskAttemptID();
    spilledRecordsCounter = 
      counters.findCounter(TaskCounter.SPILLED_RECORDS);
    failedShuffleCounter = 
      counters.findCounter(TaskCounter.FAILED_SHUFFLE);
    mergedMapOutputsCounter = 
      counters.findCounter(TaskCounter.MERGED_MAP_OUTPUTS);
    gcUpdater = new GcTimeUpdater();
  }

  public Task(String jobFile, TaskAttemptID taskId, int partition, 
              int numSlotsRequired) {
    this.jobFile = jobFile;
    this.taskId = taskId;
     
    this.partition = partition;
    this.numSlotsRequired = numSlotsRequired;
    this.taskStatus = TaskStatus.createTaskStatus(isMapTask(), this.taskId, 
                                                  0.0f, numSlotsRequired, 
                                                  TaskStatus.State.UNASSIGNED, 
                                                  "", "", "", 
                                                  isMapTask() ? 
                                                    TaskStatus.Phase.MAP : 
                                                    TaskStatus.Phase.SHUFFLE, 
                                                  counters);
    spilledRecordsCounter = counters.findCounter(TaskCounter.SPILLED_RECORDS);
    failedShuffleCounter = counters.findCounter(TaskCounter.FAILED_SHUFFLE);
    mergedMapOutputsCounter = 
      counters.findCounter(TaskCounter.MERGED_MAP_OUTPUTS);
    gcUpdater = new GcTimeUpdater();
  }

  @VisibleForTesting
  void setTaskDone() {
    taskDone.set(true);
  }

  ////////////////////////////////////////////
  // Accessors
  ////////////////////////////////////////////
  public void setJobFile(String jobFile) { this.jobFile = jobFile; }
  public String getJobFile() { return jobFile; }
  public TaskAttemptID getTaskID() { return taskId; }
  public int getNumSlotsRequired() {
    return numSlotsRequired;
  }

  Counters getCounters() { return counters; }
  
  /**
   * Get the job name for this task.
   * @return the job name
   */
  public JobID getJobID() {
    return taskId.getJobID();
  }

  /**
   * Set the job token secret 
   * @param tokenSecret the secret
   */
  public void setJobTokenSecret(SecretKey tokenSecret) {
    this.tokenSecret = tokenSecret;
  }

  /**
   * Get Encrypted spill key
   * @return encrypted spill key
   */
  public byte[] getEncryptedSpillKey() {
    return encryptedSpillKey;
  }

  /**
   * Set Encrypted spill key
   * @param encryptedSpillKey key
   */
  public void setEncryptedSpillKey(byte[] encryptedSpillKey) {
    if (encryptedSpillKey != null) {
      this.encryptedSpillKey = encryptedSpillKey;
    }
  }

  /**
   * Get the job token secret
   * @return the token secret
   */
  public SecretKey getJobTokenSecret() {
    return this.tokenSecret;
  }

  /**
   * Set the secret key used to authenticate the shuffle
   * @param shuffleSecret the secret
   */
  public void setShuffleSecret(SecretKey shuffleSecret) {
    this.shuffleSecret = shuffleSecret;
  }

  /**
   * Get the secret key used to authenticate the shuffle
   * @return the shuffle secret
   */
  public SecretKey getShuffleSecret() {
    return this.shuffleSecret;
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
   * @return the curent phase of the task
   */
  public synchronized TaskStatus.Phase getPhase(){
    return this.taskStatus.getPhase(); 
  }
  /**
   * Set current phase of the task. 
   * @param phase task phase 
   */
  protected synchronized void setPhase(TaskStatus.Phase phase){
    this.taskStatus.setPhase(phase); 
  }
  
  /**
   * Get whether to write skip records.
   */
  protected boolean toWriteSkipRecs() {
    return writeSkipRecs;
  }
      
  /**
   * Set whether to write skip records.
   */
  protected void setWriteSkipRecs(boolean writeSkipRecs) {
    this.writeSkipRecs = writeSkipRecs;
  }
  
  /**
   * Report a fatal error to the parent (task) tracker.
   */
  protected void reportFatalError(TaskAttemptID id, Throwable throwable, 
                                  String logMsg) {
    LOG.error(logMsg);
    
    if (ShutdownHookManager.get().isShutdownInProgress()) {
      return;
    }
    
    Throwable tCause = throwable.getCause();
    String cause = tCause == null 
                   ? StringUtils.stringifyException(throwable)
                   : StringUtils.stringifyException(tCause);
    try {
      umbilical.fatalError(id, cause);
    } catch (IOException ioe) {
      LOG.error("Failed to contact the tasktracker", ioe);
      System.exit(-1);
    }
  }

  /**
   * Gets a handle to the Statistics instance based on the scheme associated
   * with path.
   * 
   * @param path the path.
   * @param conf the configuration to extract the scheme from if not part of 
   *   the path.
   * @return a Statistics instance, or null if none is found for the scheme.
   */
  protected static List<Statistics> getFsStatistics(Path path, Configuration conf) throws IOException {
    List<Statistics> matchedStats = new ArrayList<FileSystem.Statistics>();
    path = path.getFileSystem(conf).makeQualified(path);
    String scheme = path.toUri().getScheme();
    for (Statistics stats : FileSystem.getAllStatistics()) {
      if (stats.getScheme().equals(scheme)) {
        matchedStats.add(stats);
      }
    }
    return matchedStats;
  }

  /**
   * Get skipRanges.
   */
  public SortedRanges getSkipRanges() {
    return skipRanges;
  }

  /**
   * Set skipRanges.
   */
  public void setSkipRanges(SortedRanges skipRanges) {
    this.skipRanges = skipRanges;
  }

  /**
   * Is Task in skipping mode.
   */
  public boolean isSkipping() {
    return skipping;
  }

  /**
   * Sets whether to run Task in skipping mode.
   * @param skipping
   */
  public void setSkipping(boolean skipping) {
    this.skipping = skipping;
  }

  /**
   * Return current state of the task. 
   * needs to be synchronized as communication thread 
   * sends the state every second
   * @return task state
   */
  synchronized TaskStatus.State getState(){
    return this.taskStatus.getRunState(); 
  }
  /**
   * Set current state of the task. 
   * @param state
   */
  synchronized void setState(TaskStatus.State state){
    this.taskStatus.setRunState(state); 
  }

  void setTaskCleanupTask() {
    taskCleanup = true;
  }
	   
  boolean isTaskCleanupTask() {
    return taskCleanup;
  }

  boolean isJobCleanupTask() {
    return jobCleanup;
  }

  boolean isJobAbortTask() {
    // the task is an abort task if its marked for cleanup and the final 
    // expected state is either failed or killed.
    return isJobCleanupTask() 
           && (jobRunStateForCleanup == JobStatus.State.KILLED 
               || jobRunStateForCleanup == JobStatus.State.FAILED);
  }
  
  boolean isJobSetupTask() {
    return jobSetup;
  }

  void setJobSetupTask() {
    jobSetup = true; 
  }

  void setJobCleanupTask() {
    jobCleanup = true; 
  }

  /**
   * Sets the task to do job abort in the cleanup.
   * @param status the final runstate of the job. 
   */
  void setJobCleanupTaskState(JobStatus.State status) {
    jobRunStateForCleanup = status;
  }
  
  boolean isMapOrReduce() {
    return !jobSetup && !jobCleanup && !taskCleanup;
  }

  /**
   * Get the name of the user running the job/task. TaskTracker needs task's
   * user name even before it's JobConf is localized. So we explicitly serialize
   * the user name.
   * 
   * @return user
   */
  String getUser() {
    return user;
  }
  
  void setUser(String user) {
    this.user = user;
  }

  ////////////////////////////////////////////
  // Writable methods
  ////////////////////////////////////////////

  public void write(DataOutput out) throws IOException {
    Text.writeString(out, jobFile);
    taskId.write(out);
    out.writeInt(partition);
    out.writeInt(numSlotsRequired);
    taskStatus.write(out);
    skipRanges.write(out);
    out.writeBoolean(skipping);
    out.writeBoolean(jobCleanup);
    if (jobCleanup) {
      WritableUtils.writeEnum(out, jobRunStateForCleanup);
    }
    out.writeBoolean(jobSetup);
    out.writeBoolean(writeSkipRecs);
    out.writeBoolean(taskCleanup);
    Text.writeString(out, user);
    out.writeInt(encryptedSpillKey.length);
    extraData.write(out);
    out.write(encryptedSpillKey);
  }
  
  public void readFields(DataInput in) throws IOException {
    jobFile = StringInterner.weakIntern(Text.readString(in));
    taskId = TaskAttemptID.read(in);
    partition = in.readInt();
    numSlotsRequired = in.readInt();
    taskStatus.readFields(in);
    skipRanges.readFields(in);
    currentRecIndexIterator = skipRanges.skipRangeIterator();
    currentRecStartIndex = currentRecIndexIterator.next();
    skipping = in.readBoolean();
    jobCleanup = in.readBoolean();
    if (jobCleanup) {
      jobRunStateForCleanup = 
        WritableUtils.readEnum(in, JobStatus.State.class);
    }
    jobSetup = in.readBoolean();
    writeSkipRecs = in.readBoolean();
    taskCleanup = in.readBoolean();
    if (taskCleanup) {
      setPhase(TaskStatus.Phase.CLEANUP);
    }
    user = StringInterner.weakIntern(Text.readString(in));
    int len = in.readInt();
    encryptedSpillKey = new byte[len];
    extraData.readFields(in);
    in.readFully(encryptedSpillKey);
  }

  @Override
  public String toString() { return taskId.toString(); }

  /**
   * Localize the given JobConf to be specific for this task.
   */
  public void localizeConfiguration(JobConf conf) throws IOException {
    conf.set(JobContext.TASK_ID, taskId.getTaskID().toString()); 
    conf.set(JobContext.TASK_ATTEMPT_ID, taskId.toString());
    conf.setBoolean(JobContext.TASK_ISMAP, isMapTask());
    conf.setInt(JobContext.TASK_PARTITION, partition);
    conf.set(JobContext.ID, taskId.getJobID().toString());
  }
  
  /** Run this task as a part of the named job.  This method is executed in the
   * child process and is what invokes user-supplied map, reduce, etc. methods.
   * @param umbilical for progress reports
   */
  public abstract void run(JobConf job, TaskUmbilicalProtocol umbilical)
    throws IOException, ClassNotFoundException, InterruptedException;

  private transient Progress taskProgress = new Progress();

  // Current counters
  private transient Counters counters = new Counters();

  /* flag to track whether task is done */
  private AtomicBoolean taskDone = new AtomicBoolean(false);
  
  public abstract boolean isMapTask();

  public Progress getProgress() { return taskProgress; }

  public void initialize(JobConf job, JobID id, 
                         Reporter reporter,
                         boolean useNewApi) throws IOException, 
                                                   ClassNotFoundException,
                                                   InterruptedException {
    jobContext = new JobContextImpl(job, id, reporter);
    taskContext = new TaskAttemptContextImpl(job, taskId, reporter);
    if (getState() == TaskStatus.State.UNASSIGNED) {
      setState(TaskStatus.State.RUNNING);
    }
    if (useNewApi) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("using new api for output committer");
      }
      outputFormat =
        ReflectionUtils.newInstance(taskContext.getOutputFormatClass(), job);
      committer = outputFormat.getOutputCommitter(taskContext);
    } else {
      committer = conf.getOutputCommitter();
    }
    Path outputPath = FileOutputFormat.getOutputPath(conf);
    if (outputPath != null) {
      if ((committer instanceof FileOutputCommitter)) {
        FileOutputFormat.setWorkOutputPath(conf, 
          ((FileOutputCommitter)committer).getTaskAttemptPath(taskContext));
      } else {
        FileOutputFormat.setWorkOutputPath(conf, outputPath);
      }
    }
    committer.setupTask(taskContext);
    Class<? extends ResourceCalculatorProcessTree> clazz =
        conf.getClass(MRConfig.RESOURCE_CALCULATOR_PROCESS_TREE,
            null, ResourceCalculatorProcessTree.class);
    pTree = ResourceCalculatorProcessTree
            .getResourceCalculatorProcessTree(System.getenv().get("JVM_PID"), clazz, conf);
    LOG.info(" Using ResourceCalculatorProcessTree : " + pTree);
    if (pTree != null) {
      pTree.updateProcessTree();
      initCpuCumulativeTime = pTree.getCumulativeCpuTime();
    }
  }

  public static String normalizeStatus(String status, Configuration conf) {
    // Check to see if the status string is too long
    // and truncate it if needed.
    int progressStatusLength = conf.getInt(
        MRConfig.PROGRESS_STATUS_LEN_LIMIT_KEY,
        MRConfig.PROGRESS_STATUS_LEN_LIMIT_DEFAULT);
    if (status.length() > progressStatusLength) {
      LOG.warn("Task status: \"" + status + "\" truncated to max limit ("
          + progressStatusLength + " characters)");
      status = status.substring(0, progressStatusLength);
    }
    return status;
  }

  @InterfaceAudience.LimitedPrivate({"MapReduce"})
  @InterfaceStability.Unstable
  public class TaskReporter 
      extends org.apache.hadoop.mapreduce.StatusReporter
      implements Runnable, Reporter {
    private TaskUmbilicalProtocol umbilical;
    private InputSplit split = null;
    private Progress taskProgress;
    private Thread pingThread = null;
    private boolean done = true;
    private Object lock = new Object();

    /**
     * flag that indicates whether progress update needs to be sent to parent.
     * If true, it has been set. If false, it has been reset. 
     * Using AtomicBoolean since we need an atomic read & reset method. 
     */  
    private AtomicBoolean progressFlag = new AtomicBoolean(false);

    @VisibleForTesting
    public TaskReporter(Progress taskProgress,
                 TaskUmbilicalProtocol umbilical) {
      this.umbilical = umbilical;
      this.taskProgress = taskProgress;
    }

    // getters and setters for flag
    void setProgressFlag() {
      progressFlag.set(true);
    }
    boolean resetProgressFlag() {
      return progressFlag.getAndSet(false);
    }
    public void setStatus(String status) {
      taskProgress.setStatus(normalizeStatus(status, conf));
      // indicate that progress update needs to be sent
      setProgressFlag();
    }
    public void setProgress(float progress) {
      // set current phase progress.
      // This method assumes that task has phases.
      taskProgress.phase().set(progress);
      // indicate that progress update needs to be sent
      setProgressFlag();
    }
    
    public float getProgress() {
      return taskProgress.getProgress();
    };
    
    public void progress() {
      // indicate that progress update needs to be sent
      setProgressFlag();
    }
    public Counters.Counter getCounter(String group, String name) {
      Counters.Counter counter = null;
      if (counters != null) {
        counter = counters.findCounter(group, name);
      }
      return counter;
    }
    public Counters.Counter getCounter(Enum<?> name) {
      return counters == null ? null : counters.findCounter(name);
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
      if(skipping && SkipBadRecords.COUNTER_GROUP.equals(group) && (
          SkipBadRecords.COUNTER_MAP_PROCESSED_RECORDS.equals(counter) ||
          SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS.equals(counter))) {
        //if application reports the processed records, move the 
        //currentRecStartIndex to the next.
        //currentRecStartIndex is the start index which has not yet been 
        //finished and is still in task's stomach.
        for(int i=0;i<amount;i++) {
          currentRecStartIndex = currentRecIndexIterator.next();
        }
      }
      setProgressFlag();
    }
    public void setInputSplit(InputSplit split) {
      this.split = split;
    }
    public InputSplit getInputSplit() throws UnsupportedOperationException {
      if (split == null) {
        throw new UnsupportedOperationException("Input only available on map");
      } else {
        return split;
      }
    }

    /**
     * exception thrown when the task exceeds some configured limits.
     */
    public class TaskLimitException extends IOException {
      public TaskLimitException(String str) {
        super(str);
      }
    }

    /**
     * check the counters to see whether the task has exceeded any configured
     * limits.
     * @throws TaskLimitException
     */
    protected void checkTaskLimits() throws TaskLimitException {
      // check the limit for writing to local file system
      long limit = conf.getLong(MRJobConfig.TASK_LOCAL_WRITE_LIMIT_BYTES,
              MRJobConfig.DEFAULT_TASK_LOCAL_WRITE_LIMIT_BYTES);
      if (limit >= 0) {
        Counters.Counter localWritesCounter = null;
        try {
          LocalFileSystem localFS = FileSystem.getLocal(conf);
          localWritesCounter = counters.findCounter(localFS.getScheme(),
                  FileSystemCounter.BYTES_WRITTEN);
        } catch (IOException e) {
          LOG.warn("Could not get LocalFileSystem BYTES_WRITTEN counter");
        }
        if (localWritesCounter != null
                && localWritesCounter.getCounter() > limit) {
          throw new TaskLimitException("too much write to local file system." +
                  " current value is " + localWritesCounter.getCounter() +
                  " the limit is " + limit);
        }
      }
    }

    /**
     * The communication thread handles communication with the parent (Task
     * Tracker). It sends progress updates if progress has been made or if
     * the task needs to let the parent know that it's alive. It also pings
     * the parent to see if it's alive.
     */
    public void run() {
      final int MAX_RETRIES = 3;
      int remainingRetries = MAX_RETRIES;
      // get current flag value and reset it as well
      boolean sendProgress = resetProgressFlag();

      long taskProgressInterval = MRJobConfUtil.
          getTaskProgressReportInterval(conf);

      boolean uberized = conf.getBoolean("mapreduce.task.uberized",
          false);

      while (!taskDone.get()) {
        synchronized (lock) {
          done = false;
        }
        try {
          boolean taskFound = true; // whether TT knows about this task
          AMFeedback amFeedback = null;
          // sleep for a bit
          synchronized(lock) {
            if (taskDone.get()) {
              break;
            }
            lock.wait(taskProgressInterval);
          }
          if (taskDone.get()) {
            break;
          }

          if (sendProgress) {
            // we need to send progress update
            updateCounters();
            checkTaskLimits();
            taskStatus.statusUpdate(taskProgress.get(),
                                    taskProgress.toString(),
                                    counters);
            amFeedback = umbilical.statusUpdate(taskId, taskStatus);
            taskFound = amFeedback.getTaskFound();
            taskStatus.clearStatus();
          }
          else {
            // send ping 
            amFeedback = umbilical.statusUpdate(taskId, null);
            taskFound = amFeedback.getTaskFound();
          }

          // if Task Tracker is not aware of our task ID (probably because it died and 
          // came back up), kill ourselves
          if (!taskFound) {
            if (uberized) {
              taskDone.set(true);
              break;
            } else {
              LOG.warn("Parent died.  Exiting "+taskId);
              resetDoneFlag();
              System.exit(66);
            }
          }

          // Set a flag that says we should preempt this is read by
          // ReduceTasks in places of the execution where it is
          // safe/easy to preempt
          boolean lastPreempt = mustPreempt.get();
          mustPreempt.set(mustPreempt.get() || amFeedback.getPreemption());

          if (lastPreempt ^ mustPreempt.get()) {
            LOG.info("PREEMPTION TASK: setting mustPreempt to " +
                mustPreempt.get() + " given " + amFeedback.getPreemption() +
                " for "+ taskId + " task status: " +taskStatus.getPhase());
          }
          sendProgress = resetProgressFlag();
          remainingRetries = MAX_RETRIES;
        } catch (TaskLimitException e) {
          String errMsg = "Task exceeded the limits: " +
                  StringUtils.stringifyException(e);
          LOG.error(errMsg);
          try {
            umbilical.fatalError(taskId, errMsg);
          } catch (IOException ioe) {
            LOG.error("Failed to update failure diagnosis", ioe);
          }
          LOG.error("Killing " + taskId);
          resetDoneFlag();
          ExitUtil.terminate(69);
        } catch (Throwable t) {
          LOG.info("Communication exception: " + StringUtils.stringifyException(t));
          remainingRetries -=1;
          if (remainingRetries == 0) {
            ReflectionUtils.logThreadInfo(LOG, "Communication exception", 0);
            LOG.warn("Last retry, killing "+taskId);
            resetDoneFlag();
            System.exit(65);
          }
        }
      }
      //Notify that we are done with the work
      resetDoneFlag();
    }
    void resetDoneFlag() {
      synchronized (lock) {
        done = true;
        lock.notify();
      }
    }
    public void startCommunicationThread() {
      if (pingThread == null) {
        pingThread = new Thread(this, "communication thread");
        pingThread.setDaemon(true);
        pingThread.start();
      }
    }
    public void stopCommunicationThread() throws InterruptedException {
      if (pingThread != null) {
        // Intent of the lock is to not send an interupt in the middle of an
        // umbilical.ping or umbilical.statusUpdate
        synchronized(lock) {
        //Interrupt if sleeping. Otherwise wait for the RPC call to return.
          lock.notify(); 
        }

        synchronized (lock) { 
          while (!done) {
            lock.wait();
          }
        }
        pingThread.interrupt();
        pingThread.join();
      }
    }
  }
  
  /**
   *  Reports the next executing record range to TaskTracker.
   *  
   * @param umbilical
   * @param nextRecIndex the record index which would be fed next.
   * @throws IOException
   */
  protected void reportNextRecordRange(final TaskUmbilicalProtocol umbilical, 
      long nextRecIndex) throws IOException{
    //currentRecStartIndex is the start index which has not yet been finished 
    //and is still in task's stomach.
    long len = nextRecIndex - currentRecStartIndex +1;
    SortedRanges.Range range = 
      new SortedRanges.Range(currentRecStartIndex, len);
    taskStatus.setNextRecordRange(range);
    if (LOG.isDebugEnabled()) {
      LOG.debug("sending reportNextRecordRange " + range);
    }
    umbilical.reportNextRecordRange(taskId, range);
  }

  /**
   * Create a TaskReporter and start communication thread
   */
  TaskReporter startReporter(final TaskUmbilicalProtocol umbilical) {  
    // start thread that will handle communication with parent
    TaskReporter reporter = new TaskReporter(getProgress(), umbilical);
    reporter.startCommunicationThread();
    return reporter;
  }

  /**
   * Update resource information counters
   */
  void updateResourceCounters() {
    // Update generic resource counters
    updateHeapUsageCounter();

    // Updating resources specified in ResourceCalculatorProcessTree
    if (pTree == null) {
      return;
    }
    pTree.updateProcessTree();
    long cpuTime = pTree.getCumulativeCpuTime();
    long pMem = pTree.getRssMemorySize();
    long vMem = pTree.getVirtualMemorySize();
    // Remove the CPU time consumed previously by JVM reuse
    if (cpuTime != ResourceCalculatorProcessTree.UNAVAILABLE &&
        initCpuCumulativeTime != ResourceCalculatorProcessTree.UNAVAILABLE) {
      cpuTime -= initCpuCumulativeTime;
    }
    
    if (cpuTime != ResourceCalculatorProcessTree.UNAVAILABLE) {
      counters.findCounter(TaskCounter.CPU_MILLISECONDS).setValue(cpuTime);
    }
    
    if (pMem != ResourceCalculatorProcessTree.UNAVAILABLE) {
      counters.findCounter(TaskCounter.PHYSICAL_MEMORY_BYTES).setValue(pMem);
    }

    if (vMem != ResourceCalculatorProcessTree.UNAVAILABLE) {
      counters.findCounter(TaskCounter.VIRTUAL_MEMORY_BYTES).setValue(vMem);
    }

    if (pMem != ResourceCalculatorProcessTree.UNAVAILABLE) {
      TaskCounter counter = isMapTask() ?
          TaskCounter.MAP_PHYSICAL_MEMORY_BYTES_MAX :
          TaskCounter.REDUCE_PHYSICAL_MEMORY_BYTES_MAX;
      Counters.Counter pMemCounter =
          counters.findCounter(counter);
      pMemCounter.setValue(Math.max(pMemCounter.getValue(), pMem));
    }

    if (vMem != ResourceCalculatorProcessTree.UNAVAILABLE) {
      TaskCounter counter = isMapTask() ?
          TaskCounter.MAP_VIRTUAL_MEMORY_BYTES_MAX :
          TaskCounter.REDUCE_VIRTUAL_MEMORY_BYTES_MAX;
      Counters.Counter vMemCounter =
          counters.findCounter(counter);
      vMemCounter.setValue(Math.max(vMemCounter.getValue(), vMem));
    }
  }

  /**
   * An updater that tracks the amount of time this task has spent in GC.
   */
  class GcTimeUpdater {
    private long lastGcMillis = 0;
    private List<GarbageCollectorMXBean> gcBeans = null;

    public GcTimeUpdater() {
      this.gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
      getElapsedGc(); // Initialize 'lastGcMillis' with the current time spent.
    }

    /**
     * @return the number of milliseconds that the gc has used for CPU
     * since the last time this method was called.
     */
    protected long getElapsedGc() {
      long thisGcMillis = 0;
      for (GarbageCollectorMXBean gcBean : gcBeans) {
        thisGcMillis += gcBean.getCollectionTime();
      }

      long delta = thisGcMillis - lastGcMillis;
      this.lastGcMillis = thisGcMillis;
      return delta;
    }

    /**
     * Increment the gc-elapsed-time counter.
     */
    public void incrementGcCounter() {
      if (null == counters) {
        return; // nothing to do.
      }

      org.apache.hadoop.mapred.Counters.Counter gcCounter =
        counters.findCounter(TaskCounter.GC_TIME_MILLIS);
      if (null != gcCounter) {
        gcCounter.increment(getElapsedGc());
      }
    }
  }

  /**
   * An updater that tracks the last number reported for a given file
   * system and only creates the counters when they are needed.
   */
  class FileSystemStatisticUpdater {
    private List<FileSystem.Statistics> stats;
    private Counters.Counter readBytesCounter, writeBytesCounter,
        readOpsCounter, largeReadOpsCounter, writeOpsCounter;
    private String scheme;
    FileSystemStatisticUpdater(List<FileSystem.Statistics> stats, String scheme) {
      this.stats = stats;
      this.scheme = scheme;
    }

    void updateCounters() {
      if (readBytesCounter == null) {
        readBytesCounter = counters.findCounter(scheme,
            FileSystemCounter.BYTES_READ);
      }
      if (writeBytesCounter == null) {
        writeBytesCounter = counters.findCounter(scheme,
            FileSystemCounter.BYTES_WRITTEN);
      }
      if (readOpsCounter == null) {
        readOpsCounter = counters.findCounter(scheme,
            FileSystemCounter.READ_OPS);
      }
      if (largeReadOpsCounter == null) {
        largeReadOpsCounter = counters.findCounter(scheme,
            FileSystemCounter.LARGE_READ_OPS);
      }
      if (writeOpsCounter == null) {
        writeOpsCounter = counters.findCounter(scheme,
            FileSystemCounter.WRITE_OPS);
      }
      long readBytes = 0;
      long writeBytes = 0;
      long readOps = 0;
      long largeReadOps = 0;
      long writeOps = 0;
      for (FileSystem.Statistics stat: stats) {
        readBytes = readBytes + stat.getBytesRead();
        writeBytes = writeBytes + stat.getBytesWritten();
        readOps = readOps + stat.getReadOps();
        largeReadOps = largeReadOps + stat.getLargeReadOps();
        writeOps = writeOps + stat.getWriteOps();
      }
      readBytesCounter.setValue(readBytes);
      writeBytesCounter.setValue(writeBytes);
      readOpsCounter.setValue(readOps);
      largeReadOpsCounter.setValue(largeReadOps);
      writeOpsCounter.setValue(writeOps);
    }
  }
  
  /**
   * A Map where Key-> URIScheme and value->FileSystemStatisticUpdater
   */
  private Map<String, FileSystemStatisticUpdater> statisticUpdaters =
     new HashMap<String, FileSystemStatisticUpdater>();
  
  private synchronized void updateCounters() {
    Map<String, List<FileSystem.Statistics>> map = new 
        HashMap<String, List<FileSystem.Statistics>>();
    for(Statistics stat: FileSystem.getAllStatistics()) {
      String uriScheme = stat.getScheme();
      if (map.containsKey(uriScheme)) {
        List<FileSystem.Statistics> list = map.get(uriScheme);
        list.add(stat);
      } else {
        List<FileSystem.Statistics> list = new ArrayList<FileSystem.Statistics>();
        list.add(stat);
        map.put(uriScheme, list);
      }
    }
    for (Map.Entry<String, List<FileSystem.Statistics>> entry: map.entrySet()) {
      FileSystemStatisticUpdater updater = statisticUpdaters.get(entry.getKey());
      if(updater==null) {//new FileSystem has been found in the cache
        updater = new FileSystemStatisticUpdater(entry.getValue(), entry.getKey());
        statisticUpdaters.put(entry.getKey(), updater);
      }
      updater.updateCounters();
    }
    
    gcUpdater.incrementGcCounter();
    updateResourceCounters();
  }

  /**
   * Updates the {@link TaskCounter#COMMITTED_HEAP_BYTES} counter to reflect the
   * current total committed heap space usage of this JVM.
   */
  @SuppressWarnings("deprecation")
  private void updateHeapUsageCounter() {
    long currentHeapUsage = Runtime.getRuntime().totalMemory();
    counters.findCounter(TaskCounter.COMMITTED_HEAP_BYTES)
            .setValue(currentHeapUsage);
  }

  public void done(TaskUmbilicalProtocol umbilical,
                   TaskReporter reporter
                   ) throws IOException, InterruptedException {
    updateCounters();
    if (taskStatus.getRunState() == TaskStatus.State.PREEMPTED ) {
      // If we are preempted, do no output promotion; signal done and exit
      committer.commitTask(taskContext);
      umbilical.preempted(taskId, taskStatus);
      taskDone.set(true);
      reporter.stopCommunicationThread();
      return;
    }
    LOG.info("Task:" + taskId + " is done."
        + " And is in the process of committing");
    boolean commitRequired = isCommitRequired();
    if (commitRequired) {
      int retries = MAX_RETRIES;
      setState(TaskStatus.State.COMMIT_PENDING);
      // say the task tracker that task is commit pending
      while (true) {
        try {
          umbilical.commitPending(taskId, taskStatus);
          break;
        } catch (InterruptedException ie) {
          // ignore
        } catch (IOException ie) {
          LOG.warn("Failure sending commit pending: " + 
                    StringUtils.stringifyException(ie));
          if (--retries == 0) {
            System.exit(67);
          }
        }
      }
      //wait for commit approval and commit
      commit(umbilical, reporter, committer);
    }
    taskDone.set(true);
    reporter.stopCommunicationThread();
    // Make sure we send at least one set of counter increments. It's
    // ok to call updateCounters() in this thread after comm thread stopped.
    updateCounters();
    sendLastUpdate(umbilical);
    //signal the tasktracker that we are done
    sendDone(umbilical);
    LOG.info("Final Counters for " + taskId + ": " +
              getCounters().toString());
    /**
     *   File System Counters
     *           FILE: Number of bytes read=0
     *           FILE: Number of bytes written=146972
     *           ...
     *   Map-Reduce Framework
     *           Map output records=6
     *           Map output records=6
     *           ...
     */
  }

  /**
   * Checks if this task has anything to commit, depending on the
   * type of task, as well as on whether the {@link OutputCommitter}
   * has anything to commit.
   * 
   * @return true if the task has to commit
   * @throws IOException
   */
  boolean isCommitRequired() throws IOException {
    boolean commitRequired = false;
    if (isMapOrReduce()) {
      commitRequired = committer.needsTaskCommit(taskContext);
    }
    return commitRequired;
  }

  /**
   * Send a status update to the task tracker
   * @param umbilical
   * @throws IOException
   */
  public void statusUpdate(TaskUmbilicalProtocol umbilical) 
  throws IOException {
    int retries = MAX_RETRIES;
    while (true) {
      try {
        if (!umbilical.statusUpdate(getTaskID(), taskStatus).getTaskFound()) {
          LOG.warn("Parent died.  Exiting "+taskId);
          System.exit(66);
        }
        taskStatus.clearStatus();
        return;
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt(); // interrupt ourself
      } catch (IOException ie) {
        LOG.warn("Failure sending status update: " + 
                  StringUtils.stringifyException(ie));
        if (--retries == 0) {
          throw ie;
        }
      }
    }
  }
  
  /**
   * Sends last status update before sending umbilical.done(); 
   */
  private void sendLastUpdate(TaskUmbilicalProtocol umbilical) 
  throws IOException {
    taskStatus.setOutputSize(calculateOutputSize());
    // send a final status report
    taskStatus.statusUpdate(taskProgress.get(),
                            taskProgress.toString(), 
                            counters);
    statusUpdate(umbilical);
  }

  /**
   * Calculates the size of output for this task.
   * 
   * @return -1 if it can't be found.
   */
   private long calculateOutputSize() throws IOException {
    if (!isMapOrReduce()) {
      return -1;
    }

    if (isMapTask() && conf.getNumReduceTasks() > 0) {
      try {
        Path mapOutput =  mapOutputFile.getOutputFile();
        FileSystem localFS = FileSystem.getLocal(conf);
        return localFS.getFileStatus(mapOutput).getLen();
      } catch (IOException e) {
        LOG.warn ("Could not find output size " , e);
      }
    }
    return -1;
  }

  private void sendDone(TaskUmbilicalProtocol umbilical) throws IOException {
    int retries = MAX_RETRIES;
    while (true) {
      try {
        umbilical.done(getTaskID());
        LOG.info("Task '" + taskId + "' done.");
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

  private void commit(TaskUmbilicalProtocol umbilical,
                      TaskReporter reporter,
                      org.apache.hadoop.mapreduce.OutputCommitter committer
                      ) throws IOException {
    int retries = MAX_RETRIES;
    while (true) {
      try {
        while (!umbilical.canCommit(taskId)) {
          try {
            Thread.sleep(1000);
          } catch(InterruptedException ie) {
            //ignore
          }
          reporter.setProgressFlag();
        }
        break;
      } catch (IOException ie) {
        LOG.warn("Failure asking whether task can commit: " + 
            StringUtils.stringifyException(ie));
        if (--retries == 0) {
          //if it couldn't query successfully then delete the output
          discardOutput(taskContext);
          System.exit(68);
        }
      }
    }
    
    // task can Commit now  
    try {
      LOG.info("Task " + taskId + " is allowed to commit now");
      committer.commitTask(taskContext);
      return;
    } catch (IOException iee) {
      LOG.warn("Failure committing: " + 
        StringUtils.stringifyException(iee));
      //if it couldn't commit a successfully then delete the output
      discardOutput(taskContext);
      throw iee;
    }
  }

  private 
  void discardOutput(TaskAttemptContext taskContext) {
    try {
      committer.abortTask(taskContext);
    } catch (IOException ioe)  {
      LOG.warn("Failure cleaning up: " + 
               StringUtils.stringifyException(ioe));
    }
  }

  protected void runTaskCleanupTask(TaskUmbilicalProtocol umbilical,
                                TaskReporter reporter) 
  throws IOException, InterruptedException {
    taskCleanup(umbilical);
    done(umbilical, reporter);
  }

  void taskCleanup(TaskUmbilicalProtocol umbilical) 
  throws IOException {
    // set phase for this task
    setPhase(TaskStatus.Phase.CLEANUP);
    getProgress().setStatus("cleanup");
    statusUpdate(umbilical);
    LOG.info("Running cleanup for the task");
    // do the cleanup
    committer.abortTask(taskContext);
  }

  protected void runJobCleanupTask(TaskUmbilicalProtocol umbilical,
                               TaskReporter reporter
                              ) throws IOException, InterruptedException {
    // set phase for this task
    setPhase(TaskStatus.Phase.CLEANUP);
    getProgress().setStatus("cleanup");
    statusUpdate(umbilical);
    // do the cleanup
    LOG.info("Cleaning up job");
    if (jobRunStateForCleanup == JobStatus.State.FAILED 
        || jobRunStateForCleanup == JobStatus.State.KILLED) {
      LOG.info("Aborting job with runstate : " + jobRunStateForCleanup.name());
      if (conf.getUseNewMapper()) {
        committer.abortJob(jobContext, jobRunStateForCleanup);
      } else {
        org.apache.hadoop.mapred.OutputCommitter oldCommitter = 
          (org.apache.hadoop.mapred.OutputCommitter)committer;
        oldCommitter.abortJob(jobContext, jobRunStateForCleanup);
      }
    } else if (jobRunStateForCleanup == JobStatus.State.SUCCEEDED){
      LOG.info("Committing job");
      committer.commitJob(jobContext);
    } else {
      throw new IOException("Invalid state of the job for cleanup. State found "
                            + jobRunStateForCleanup + " expecting "
                            + JobStatus.State.SUCCEEDED + ", " 
                            + JobStatus.State.FAILED + " or "
                            + JobStatus.State.KILLED);
    }
    
    // delete the staging area for the job
    JobConf conf = new JobConf(jobContext.getConfiguration());
    if (!keepTaskFiles(conf)) {
      String jobTempDir = conf.get(MRJobConfig.MAPREDUCE_JOB_DIR);
      Path jobTempDirPath = new Path(jobTempDir);
      FileSystem fs = jobTempDirPath.getFileSystem(conf);
      fs.delete(jobTempDirPath, true);
    }
    done(umbilical, reporter);
  }
  
  protected boolean keepTaskFiles(JobConf conf) {
    return (conf.getKeepTaskFilesPattern() != null || conf
        .getKeepFailedTaskFiles());
  }

  protected void runJobSetupTask(TaskUmbilicalProtocol umbilical,
                             TaskReporter reporter
                             ) throws IOException, InterruptedException {
    // do the setup
    getProgress().setStatus("setup");
    committer.setupJob(jobContext);
    done(umbilical, reporter);
  }
  
  public void setConf(Configuration conf) {
    if (conf instanceof JobConf) {
      this.conf = (JobConf) conf;
    } else {
      this.conf = new JobConf(conf);
    }
    this.mapOutputFile = ReflectionUtils.newInstance(
        conf.getClass(MRConfig.TASK_LOCAL_OUTPUT_CLASS,
          MROutputFiles.class, MapOutputFile.class), conf);
    this.lDirAlloc = new LocalDirAllocator(MRConfig.LOCAL_DIR);
    // add the static resolutions (this is required for the junit to
    // work on testcases that simulate multiple nodes on a single physical
    // node.
    String hostToResolved[] = conf.getStrings(MRConfig.STATIC_RESOLUTIONS);
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

  public MapOutputFile getMapOutputFile() {
    return mapOutputFile;
  }

  /**
   * OutputCollector for the combiner.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static class CombineOutputCollector<K extends Object, V extends Object> 
  implements OutputCollector<K, V> {
    private Writer<K, V> writer;
    private Counters.Counter outCounter;
    private Progressable progressable;
    private long progressBar;

    public CombineOutputCollector(Counters.Counter outCounter, Progressable progressable, Configuration conf) {
      this.outCounter = outCounter;
      this.progressable=progressable;
      progressBar = conf.getLong(MRJobConfig.COMBINE_RECORDS_BEFORE_PROGRESS, DEFAULT_COMBINE_RECORDS_BEFORE_PROGRESS);
    }
    
    public synchronized void setWriter(Writer<K, V> writer) {
      this.writer = writer;
    }

    public synchronized void collect(K key, V value)
        throws IOException {
      outCounter.increment(1);
      writer.append(key, value);
      if ((outCounter.getValue() % progressBar) == 0) {
        progressable.progress();
      }
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
    public void nextKey() throws IOException {
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
    public boolean more() { 
      return more; 
    }

    /** The current key. */
    public KEY getKey() { 
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

    /** Iterator to return Combined values */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static class CombineValuesIterator<KEY,VALUE>
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

  @SuppressWarnings("unchecked")
  protected static <INKEY,INVALUE,OUTKEY,OUTVALUE> 
  org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE>.Context
  createReduceContext(org.apache.hadoop.mapreduce.Reducer
                        <INKEY,INVALUE,OUTKEY,OUTVALUE> reducer,
                      Configuration job,
                      org.apache.hadoop.mapreduce.TaskAttemptID taskId, 
                      RawKeyValueIterator rIter,
                      org.apache.hadoop.mapreduce.Counter inputKeyCounter,
                      org.apache.hadoop.mapreduce.Counter inputValueCounter,
                      org.apache.hadoop.mapreduce.RecordWriter<OUTKEY,OUTVALUE> output, 
                      org.apache.hadoop.mapreduce.OutputCommitter committer,
                      org.apache.hadoop.mapreduce.StatusReporter reporter,
                      RawComparator<INKEY> comparator,
                      Class<INKEY> keyClass, Class<INVALUE> valueClass
  ) throws IOException, InterruptedException {
    org.apache.hadoop.mapreduce.ReduceContext<INKEY, INVALUE, OUTKEY, OUTVALUE> 
    reduceContext = 
      new ReduceContextImpl<INKEY, INVALUE, OUTKEY, OUTVALUE>(job, taskId, 
                                                              rIter, 
                                                              inputKeyCounter, 
                                                              inputValueCounter, 
                                                              output, 
                                                              committer, 
                                                              reporter, 
                                                              comparator, 
                                                              keyClass, 
                                                              valueClass);

    org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE>.Context 
        reducerContext = 
          new WrappedReducer<INKEY, INVALUE, OUTKEY, OUTVALUE>().getReducerContext(
              reduceContext);

    return reducerContext;
  }

  @InterfaceAudience.LimitedPrivate({"MapReduce"})
  @InterfaceStability.Unstable
  public static abstract class CombinerRunner<K,V> {
    protected final Counters.Counter inputCounter;
    protected final JobConf job;
    protected final TaskReporter reporter;

    CombinerRunner(Counters.Counter inputCounter,
                   JobConf job,
                   TaskReporter reporter) {
      this.inputCounter = inputCounter;
      this.job = job;
      this.reporter = reporter;
    }
    
    /**
     * Run the combiner over a set of inputs.
     * @param iterator the key/value pairs to use as input
     * @param collector the output collector
     */
    public abstract void combine(RawKeyValueIterator iterator, 
                          OutputCollector<K,V> collector
                         ) throws IOException, InterruptedException, 
                                  ClassNotFoundException;

    @SuppressWarnings("unchecked")
    public static <K,V> 
    CombinerRunner<K,V> create(JobConf job,
                               TaskAttemptID taskId,
                               Counters.Counter inputCounter,
                               TaskReporter reporter,
                               org.apache.hadoop.mapreduce.OutputCommitter committer
                              ) throws ClassNotFoundException {
      Class<? extends Reducer<K,V,K,V>> cls = 
        (Class<? extends Reducer<K,V,K,V>>) job.getCombinerClass();

      if (cls != null) {
        return new OldCombinerRunner(cls, job, inputCounter, reporter);
      }
      // make a task context so we can get the classes
      org.apache.hadoop.mapreduce.TaskAttemptContext taskContext =
        new org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl(job, taskId,
            reporter);
      Class<? extends org.apache.hadoop.mapreduce.Reducer<K,V,K,V>> newcls = 
        (Class<? extends org.apache.hadoop.mapreduce.Reducer<K,V,K,V>>)
           taskContext.getCombinerClass();
      if (newcls != null) {
        return new NewCombinerRunner<K,V>(newcls, job, taskId, taskContext, 
                                          inputCounter, reporter, committer);
      }
      
      return null;
    }
  }
  
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  protected static class OldCombinerRunner<K,V> extends CombinerRunner<K,V> {
    private final Class<? extends Reducer<K,V,K,V>> combinerClass;
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final RawComparator<K> comparator;

    @SuppressWarnings("unchecked")
    protected OldCombinerRunner(Class<? extends Reducer<K,V,K,V>> cls,
                                JobConf conf,
                                Counters.Counter inputCounter,
                                TaskReporter reporter) {
      super(inputCounter, conf, reporter);
      combinerClass = cls;
      keyClass = (Class<K>) job.getMapOutputKeyClass();
      valueClass = (Class<V>) job.getMapOutputValueClass();
      comparator = (RawComparator<K>)
          job.getCombinerKeyGroupingComparator();
    }

    @SuppressWarnings("unchecked")
    public void combine(RawKeyValueIterator kvIter,
                           OutputCollector<K,V> combineCollector
                           ) throws IOException {
      Reducer<K,V,K,V> combiner = 
        ReflectionUtils.newInstance(combinerClass, job);
      try {
        CombineValuesIterator<K,V> values = 
          new CombineValuesIterator<K,V>(kvIter, comparator, keyClass, 
                                         valueClass, job, reporter,
                                         inputCounter);
        while (values.more()) {
          combiner.reduce(values.getKey(), values, combineCollector,
              reporter);
          values.nextKey();
        }
      } finally {
        combiner.close();
      }
    }
  }
  
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  protected static class NewCombinerRunner<K, V> extends CombinerRunner<K,V> {
    private final Class<? extends org.apache.hadoop.mapreduce.Reducer<K,V,K,V>> 
        reducerClass;
    private final org.apache.hadoop.mapreduce.TaskAttemptID taskId;
    private final RawComparator<K> comparator;
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final org.apache.hadoop.mapreduce.OutputCommitter committer;

    @SuppressWarnings("unchecked")
    NewCombinerRunner(Class reducerClass,
                      JobConf job,
                      org.apache.hadoop.mapreduce.TaskAttemptID taskId,
                      org.apache.hadoop.mapreduce.TaskAttemptContext context,
                      Counters.Counter inputCounter,
                      TaskReporter reporter,
                      org.apache.hadoop.mapreduce.OutputCommitter committer) {
      super(inputCounter, job, reporter);
      this.reducerClass = reducerClass;
      this.taskId = taskId;
      keyClass = (Class<K>) context.getMapOutputKeyClass();
      valueClass = (Class<V>) context.getMapOutputValueClass();
      comparator = (RawComparator<K>) context.getCombinerKeyGroupingComparator();
      this.committer = committer;
    }

    private static class OutputConverter<K,V>
            extends org.apache.hadoop.mapreduce.RecordWriter<K,V> {
      OutputCollector<K,V> output;
      OutputConverter(OutputCollector<K,V> output) {
        this.output = output;
      }

      @Override
      public void close(org.apache.hadoop.mapreduce.TaskAttemptContext context){
      }

      @Override
      public void write(K key, V value
                        ) throws IOException, InterruptedException {
        output.collect(key,value);
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void combine(RawKeyValueIterator iterator, 
                 OutputCollector<K,V> collector
                 ) throws IOException, InterruptedException,
                          ClassNotFoundException {
      // make a reducer
      org.apache.hadoop.mapreduce.Reducer<K,V,K,V> reducer =
        (org.apache.hadoop.mapreduce.Reducer<K,V,K,V>)
          ReflectionUtils.newInstance(reducerClass, job);
      org.apache.hadoop.mapreduce.Reducer.Context 
           reducerContext = createReduceContext(reducer, job, taskId,
                                                iterator, null, inputCounter, 
                                                new OutputConverter(collector),
                                                committer,
                                                reporter, comparator, keyClass,
                                                valueClass);
      reducer.run(reducerContext);
    } 
  }

  BytesWritable getExtraData() {
    return extraData;
  }

  void setExtraData(BytesWritable extraData) {
    this.extraData = extraData;
  }
}
