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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.SortedRanges.SkipRangeIterator;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.task.reduce.Shuffle;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

/** A Reduce task. */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ReduceTask extends Task {

  static {                                        // register a ctor
    WritableFactories.setFactory
      (ReduceTask.class,
       new WritableFactory() {
         public Writable newInstance() { return new ReduceTask(); }
       });
  }
  
  private static final Log LOG = LogFactory.getLog(ReduceTask.class.getName());
  private int numMaps;

  private CompressionCodec codec;

  { 
    getProgress().setStatus("reduce"); 
  }

  private Progress copyPhase;
  private Progress sortPhase;
  private Progress reducePhase;
  private Counters.Counter shuffledMapsCounter = 
    getCounters().findCounter(TaskCounter.SHUFFLED_MAPS);
  private Counters.Counter reduceShuffleBytes = 
    getCounters().findCounter(TaskCounter.REDUCE_SHUFFLE_BYTES);
  private Counters.Counter reduceInputKeyCounter = 
    getCounters().findCounter(TaskCounter.REDUCE_INPUT_GROUPS);
  private Counters.Counter reduceInputValueCounter = 
    getCounters().findCounter(TaskCounter.REDUCE_INPUT_RECORDS);
  private Counters.Counter reduceOutputCounter = 
    getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS);
  private Counters.Counter reduceCombineInputCounter =
    getCounters().findCounter(TaskCounter.COMBINE_INPUT_RECORDS);
  private Counters.Counter reduceCombineOutputCounter =
    getCounters().findCounter(TaskCounter.COMBINE_OUTPUT_RECORDS);

  // A custom comparator for map output files. Here the ordering is determined
  // by the file's size and path. In case of files with same size and different
  // file paths, the first parameter is considered smaller than the second one.
  // In case of files with same size and path are considered equal.
  private Comparator<FileStatus> mapOutputFileComparator = 
    new Comparator<FileStatus>() {
      public int compare(FileStatus a, FileStatus b) {
        if (a.getLen() < b.getLen())
          return -1;
        else if (a.getLen() == b.getLen())
          if (a.getPath().toString().equals(b.getPath().toString()))
            return 0;
          else
            return -1; 
        else
          return 1;
      }
  };
  
  // A sorted set for keeping a set of map output files on disk
  private final SortedSet<FileStatus> mapOutputFilesOnDisk = 
    new TreeSet<FileStatus>(mapOutputFileComparator);

  public ReduceTask() {
    super();
    this.taskStatus = new ReduceTaskStatus();
  }

  public ReduceTask(String jobFile, TaskAttemptID taskId,
                    int partition, int numMaps, int numSlotsRequired) {
    super(jobFile, taskId, partition, numSlotsRequired);
    this.numMaps = numMaps;
/*
 */
    this.taskStatus = new ReduceTaskStatus(getTaskID(), 0.0f, numSlotsRequired,
                                           TaskStatus.State.UNASSIGNED,
                                           "", "", "", TaskStatus.Phase.SHUFFLE,
                                           getCounters());
  }

  static CompressionCodec initCodec(JobConf conf) {
    // check if map-outputs are to be compressed
    if (conf.getCompressMapOutput()) {
      Class<? extends CompressionCodec> codecClass =
        conf.getMapOutputCompressorClass(DefaultCodec.class);
      return ReflectionUtils.newInstance(codecClass, conf);
    } 

    return null;
  }

  @Override
  public boolean isMapTask() {
    return false;
  }

  /**
   * Is this really a combo-task masquerading as a plain MapTask?  Decidedly
   * not.
   */
  @Override
  public boolean isUberTask() {
    return false;
  }

  /**
   * Allow UberTask (or, potentially, JobInProgress or others) to set up a
   * deeper Progress hierarchy even if run() is skipped.  If setProgress()
   * is also needed, it should be called <I>before</I> createPhase() or else
   * the sub-phases created here will be wiped out.
   */
  void createPhase(TaskStatus.Phase phaseType, String status) {
    if (phaseType == TaskStatus.Phase.SHUFFLE) {
      copyPhase = getProgress().addPhase(status);
    } else if (phaseType == TaskStatus.Phase.SORT) {
      sortPhase = getProgress().addPhase(status);
    } else /* TaskStatus.Phase.REDUCE */ {
      reducePhase = getProgress().addPhase(status);
    }
  }

  /**
   * Allow UberTask to traverse the deeper Progress hierarchy in case run() is
   * skipped.
   */
  void completePhase(TaskStatus.Phase phaseType) {
    if (phaseType == TaskStatus.Phase.SHUFFLE) {
      copyPhase.complete();
    } else if (phaseType == TaskStatus.Phase.SORT) {
      sortPhase.complete();
    } else /* TaskStatus.Phase.REDUCE */ {
      reducePhase.complete();
    }
  }

  public int getNumMaps() { return numMaps; }
  
  /**
   * Localize the given JobConf to be specific for this task.
   */
  @Override
  public void localizeConfiguration(JobConf conf) throws IOException {
    super.localizeConfiguration(conf);
    conf.setNumMapTasks(numMaps);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);

    out.writeInt(numMaps);                        // write the number of maps
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    numMaps = in.readInt();
  }
  
  // Get the input files for the reducer.
  static Path[] getMapFiles(ReduceTask reduce, FileSystem fs, boolean isLocal) 
  throws IOException {
    List<Path> fileList = new ArrayList<Path>();
    if (isLocal) {
      // for local jobs
      for (int i = 0; i < reduce.numMaps; ++i) {
        fileList.add(reduce.mapOutputFile.getInputFile(i));
      }
    } else {
      // for non local jobs
      for (FileStatus filestatus : reduce.mapOutputFilesOnDisk) {
        fileList.add(filestatus.getPath());
      }
    }
    return fileList.toArray(new Path[0]);
  }

  private static class ReduceValuesIterator<KEY,VALUE> 
          extends ValuesIterator<KEY,VALUE> {
    ReduceTask reduce;
    public ReduceValuesIterator (ReduceTask reduce, RawKeyValueIterator in,
                                 RawComparator<KEY> comparator, 
                                 Class<KEY> keyClass,
                                 Class<VALUE> valClass,
                                 Configuration conf, Progressable reporter)
      throws IOException {
      super(in, comparator, keyClass, valClass, conf, reporter);
      this.reduce = reduce;
    }

    @Override
    public VALUE next() {
      reduce.reduceInputValueCounter.increment(1);
      return moveToNext();
    }
    
    protected VALUE moveToNext() {
      return super.next();
    }
    
    public void informReduceProgress() {
      // update progress:
      reduce.reducePhase.set(super.in.getProgress().getProgress());
      reporter.progress();
    }
  }

  private static class SkippingReduceValuesIterator<KEY,VALUE> 
     extends ReduceValuesIterator<KEY,VALUE> {
     private SkipRangeIterator skipIt;
     private TaskUmbilicalProtocol umbilical;
     private Counters.Counter skipGroupCounter;
     private Counters.Counter skipRecCounter;
     private long grpIndex = -1;
     private Class<KEY> keyClass;
     private Class<VALUE> valClass;
     private SequenceFile.Writer skipWriter;
     private boolean toWriteSkipRecs;
     private boolean hasNext;
     private TaskReporter reporter;

     public SkippingReduceValuesIterator(ReduceTask reduce,
         RawKeyValueIterator in,
         RawComparator<KEY> comparator, Class<KEY> keyClass,
         Class<VALUE> valClass, Configuration conf, TaskReporter reporter,
         TaskUmbilicalProtocol umbilical) throws IOException {
       super(reduce, in, comparator, keyClass, valClass, conf, reporter);
       this.umbilical = umbilical;
       this.skipGroupCounter =
         reporter.getCounter(TaskCounter.REDUCE_SKIPPED_GROUPS);
       this.skipRecCounter =
         reporter.getCounter(TaskCounter.REDUCE_SKIPPED_RECORDS);
       this.toWriteSkipRecs = reduce.toWriteSkipRecs() &&
         SkipBadRecords.getSkipOutputPath(conf)!=null;
       this.keyClass = keyClass;
       this.valClass = valClass;
       this.reporter = reporter;
       skipIt = reduce.getSkipRanges().skipRangeIterator();
       mayBeSkip();
     }

     public void nextKey() throws IOException {
       super.nextKey();
       mayBeSkip();
     }
     
     public boolean more() { 
       return super.more() && hasNext; 
     }
     
     private void mayBeSkip() throws IOException {
       hasNext = skipIt.hasNext();
       if(!hasNext) {
         LOG.warn("Further groups got skipped.");
         return;
       }
       grpIndex++;
       long nextGrpIndex = skipIt.next();
       long skip = 0;
       long skipRec = 0;
       while(grpIndex<nextGrpIndex && super.more()) {
         while (hasNext()) {
           VALUE value = moveToNext();
           if(toWriteSkipRecs) {
             writeSkippedRec(getKey(), value);
           }
           skipRec++;
         }
         super.nextKey();
         grpIndex++;
         skip++;
       }
       
       //close the skip writer once all the ranges are skipped
       if(skip>0 && skipIt.skippedAllRanges() && skipWriter!=null) {
         skipWriter.close();
       }
       skipGroupCounter.increment(skip);
       skipRecCounter.increment(skipRec);
       reduce.reportNextRecordRange(umbilical, grpIndex);
     }
     
     @SuppressWarnings("unchecked")
     private void writeSkippedRec(KEY key, VALUE value) throws IOException{
       if(skipWriter==null) {
         Path skipDir = SkipBadRecords.getSkipOutputPath(reduce.conf);
         Path skipFile = new Path(skipDir, reduce.getTaskID().toString());
         skipWriter = SequenceFile.createWriter(
               skipFile.getFileSystem(reduce.conf), reduce.conf, skipFile,
               keyClass, valClass, 
               CompressionType.BLOCK, reporter);
       }
       skipWriter.append(key, value);
     }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void run(JobConf job, final TaskUmbilicalProtocol umbilical)
    throws IOException, InterruptedException, ClassNotFoundException {
    job.setBoolean(JobContext.SKIP_RECORDS, isSkipping());

    if (isMapOrReduce()) {
      copyPhase = getProgress().addPhase("copy");
      sortPhase  = getProgress().addPhase("sort");
      reducePhase = getProgress().addPhase("reduce");
    }
    // start thread that will handle communication with parent
    TaskReporter reporter = startReporter(umbilical);
    
    boolean useNewApi = job.getUseNewReducer();
    initialize(job, getJobID(), reporter, useNewApi);

    if (job.getBoolean("mapreduce.task.uberized", false)) {
      reporter.getCounter(TaskCounter.NUM_UBER_AM_REDUCES).increment(1);
      // or getCounters().findCounter( ... ).increment(1);
    }

    // check if it is a cleanupJobTask
    if (jobCleanup) {
      runJobCleanupTask(umbilical, reporter);
      return;
    }
    if (jobSetup) {
      runJobSetupTask(umbilical, reporter);
      return;
    }
    if (taskCleanup) {
      runTaskCleanupTask(umbilical, reporter);
      return;
    }
    
    // Initialize the codec
    codec = initCodec(conf);
    RawKeyValueIterator rIter = null;
    boolean isLocal = "local".equals(job.get(MRConfig.MASTER_ADDRESS, "local"));
    if (!isLocal) {
      Class combinerClass = conf.getCombinerClass();
      CombineOutputCollector combineCollector = 
        (null != combinerClass) ? 
            new CombineOutputCollector(reduceCombineOutputCounter) : null;

      Shuffle shuffle = 
        new Shuffle(getTaskID(), job, FileSystem.getLocal(job), umbilical, 
                    super.lDirAlloc, reporter, codec, 
                    combinerClass, combineCollector, 
                    spilledRecordsCounter, reduceCombineInputCounter,
                    shuffledMapsCounter,
                    reduceShuffleBytes, failedShuffleCounter,
                    mergedMapOutputsCounter,
                    taskStatus, copyPhase, sortPhase, this,
                    mapOutputFile);
      rIter = shuffle.run();
    } else {
      final FileSystem rfs = FileSystem.getLocal(job).getRaw();
      rIter = Merger.merge(job, rfs, job.getMapOutputKeyClass(),
                           job.getMapOutputValueClass(), codec,
                           getMapFiles(this, rfs, true),
                           !conf.getKeepFailedTaskFiles(), 
                           job.getInt(JobContext.IO_SORT_FACTOR, 100),
                           new Path(getTaskID().toString()), 
                           job.getOutputKeyComparator(),
                           reporter, spilledRecordsCounter, null, null);
    }
    // free up the data structures
    mapOutputFilesOnDisk.clear();
    
    sortPhase.complete();                         // sort is complete
    setPhase(TaskStatus.Phase.REDUCE); 
    statusUpdate(umbilical);
    Class keyClass = job.getMapOutputKeyClass();
    Class valueClass = job.getMapOutputValueClass();
    RawComparator comparator = job.getOutputValueGroupingComparator();

    if (useNewApi) {
      runNewReducer(this, job, umbilical, reporter, rIter, comparator,
                    keyClass, valueClass);
    } else {
      runOldReducer(this, job, umbilical, reporter, rIter, comparator,
                    keyClass, valueClass);
    }
    done(umbilical, reporter);
  }

  private static class WrappedOutputCollector<OUTKEY, OUTVALUE>
  implements OutputCollector<OUTKEY, OUTVALUE> {
    RecordWriter<OUTKEY, OUTVALUE> out;
    TaskReporter reporter;
    Counters.Counter reduceOutputCounter;
    public WrappedOutputCollector(ReduceTask reduce,
                                  RecordWriter<OUTKEY, OUTVALUE> out,
                                  TaskReporter reporter) {
      this.out = out;
      this.reporter = reporter;
      this.reduceOutputCounter = reduce.reduceOutputCounter;
    }

    public void collect(OUTKEY key, OUTVALUE value)
    throws IOException {
      out.write(key, value);
      reduceOutputCounter.increment(1);
      // indicate that progress update needs to be sent
      reporter.progress();
    }
  }

  @SuppressWarnings("unchecked")
  static <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void runOldReducer(ReduceTask reduce, JobConf job,
                     TaskUmbilicalProtocol umbilical,
                     final TaskReporter reporter,
                     RawKeyValueIterator rIter,
                     RawComparator<INKEY> comparator,
                     Class<INKEY> keyClass,
                     Class<INVALUE> valueClass) throws IOException {
    Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE> reducer = 
      ReflectionUtils.newInstance(job.getReducerClass(), job);
    // make output collector
    String finalName = getOutputName(reduce.getPartition());

    FileSystem fs = FileSystem.get(job);

    final RecordWriter<OUTKEY,OUTVALUE> out = 
      job.getOutputFormat().getRecordWriter(fs, job, finalName, reporter);  
    
    OutputCollector<OUTKEY,OUTVALUE> collector = 
      new WrappedOutputCollector<OUTKEY, OUTVALUE>(reduce, out, reporter);
    
    // apply reduce function
    try {
      //increment processed counter only if skipping feature is enabled
      boolean incrProcCount = SkipBadRecords.getReducerMaxSkipGroups(job)>0 &&
        SkipBadRecords.getAutoIncrReducerProcCount(job);
      
      ReduceValuesIterator<INKEY,INVALUE> values = reduce.isSkipping() ?
          new SkippingReduceValuesIterator<INKEY,INVALUE>(reduce, rIter,
              comparator, keyClass, valueClass, 
              job, reporter, umbilical) :
          new ReduceValuesIterator<INKEY,INVALUE>(reduce, rIter,
          job.getOutputValueGroupingComparator(), keyClass, valueClass, 
          job, reporter);
      values.informReduceProgress();
      while (values.more()) {
        reduce.reduceInputKeyCounter.increment(1);
        reducer.reduce(values.getKey(), values, collector, reporter);
        if(incrProcCount) {
          reporter.incrCounter(SkipBadRecords.COUNTER_GROUP, 
              SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS, 1);
        }
        values.nextKey();
        values.informReduceProgress();
      }

      //Clean up: repeated in catch block below
      reducer.close();
      out.close(reporter);
      //End of cleanup.
    } catch (IOException ioe) {
      try {
        reducer.close();
      } catch (IOException ignored) {}
        
      try {
        out.close(reporter);
      } catch (IOException ignored) {}
      
      throw ioe;
    }
  }

  static class NewTrackingRecordWriter<K,V> 
      extends org.apache.hadoop.mapreduce.RecordWriter<K,V> {
    private final org.apache.hadoop.mapreduce.RecordWriter<K,V> real;
    private final org.apache.hadoop.mapreduce.Counter outputRecordCounter;
  
    NewTrackingRecordWriter(org.apache.hadoop.mapreduce.RecordWriter<K,V> real,
                            org.apache.hadoop.mapreduce.Counter recordCounter) {
      this.real = real;
      this.outputRecordCounter = recordCounter;
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
    InterruptedException {
      real.close(context);
    }

    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
      real.write(key,value);
      outputRecordCounter.increment(1);
    }
  }

  private static class WrappedRawKeyValueIterator implements RawKeyValueIterator {
    ReduceTask reduce;
    TaskReporter reporter;
    RawKeyValueIterator rawIter;
    public WrappedRawKeyValueIterator(ReduceTask reduce, TaskReporter reporter,
                          RawKeyValueIterator rawIter) {
      this.reduce = reduce;
      this.rawIter = rawIter;
      this.reporter = reporter;
    }
    public void close() throws IOException {
      rawIter.close();
    }
    public DataInputBuffer getKey() throws IOException {
      return rawIter.getKey();
    }
    public Progress getProgress() {
      return rawIter.getProgress();
    }
    public DataInputBuffer getValue() throws IOException {
      return rawIter.getValue();
    }
    public boolean next() throws IOException {
      boolean ret = rawIter.next();
      reporter.setProgress(rawIter.getProgress().getProgress());
      return ret;
    }
  }

  @SuppressWarnings("unchecked")
  static <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void runNewReducer(final ReduceTask reduce, JobConf job,
                     final TaskUmbilicalProtocol umbilical,
                     final TaskReporter reporter,
                     RawKeyValueIterator rIter,
                     RawComparator<INKEY> comparator,
                     Class<INKEY> keyClass,
                     Class<INVALUE> valueClass
                     ) throws IOException,InterruptedException, 
                              ClassNotFoundException {
    org.apache.hadoop.mapreduce.TaskAttemptID reduceId = reduce.getTaskID();
    // wrap value iterator to report progress.
    final RawKeyValueIterator rawIter = rIter;
    rIter = new WrappedRawKeyValueIterator(reduce, reporter, rawIter);
    // make a task context so we can get the classes
    org.apache.hadoop.mapreduce.TaskAttemptContext taskContext =
      new org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl(job, reduceId);
    // make a reducer
    org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE> reducer =
      (org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE>)
        ReflectionUtils.newInstance(taskContext.getReducerClass(), job);
    org.apache.hadoop.mapreduce.RecordWriter<OUTKEY,OUTVALUE> output =
      (org.apache.hadoop.mapreduce.RecordWriter<OUTKEY,OUTVALUE>)
        reduce.outputFormat.getRecordWriter(taskContext);
    org.apache.hadoop.mapreduce.RecordWriter<OUTKEY,OUTVALUE> trackedRW = 
      new NewTrackingRecordWriter<OUTKEY, OUTVALUE>(output, reduce.reduceOutputCounter);
    job.setBoolean(JobContext.SKIP_RECORDS, reduce.isSkipping());
    org.apache.hadoop.mapreduce.Reducer.Context reducerContext =
      createReduceContext(reducer, job, reduceId, rIter,
                          reduce.reduceInputKeyCounter, 
                          reduce.reduceInputValueCounter, 
                          trackedRW, reduce.committer, reporter,
                          comparator, keyClass, valueClass);
    reducer.run(reducerContext);
    output.close(reducerContext);
  }
}
