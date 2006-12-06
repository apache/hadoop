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

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.metrics.Metrics;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;
import java.text.*;

import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;

/** A Reduce task. */
class ReduceTask extends Task {

  static {                                        // register a ctor
    WritableFactories.setFactory
      (ReduceTask.class,
       new WritableFactory() {
         public Writable newInstance() { return new ReduceTask(); }
       });
  }

  private class ReduceTaskMetrics {
    private MetricsRecord metricsRecord = null;
    
    private long numInputRecords = 0L;
    private long numOutputRecords = 0L;
    
    ReduceTaskMetrics(String taskId) {
      metricsRecord = Metrics.createRecord("mapred", "reduce", "taskid", taskId);
    }
    
    synchronized void reduceInput() {
      Metrics.report(metricsRecord, "input-records", ++numInputRecords);
    }
    
    synchronized void reduceOutput() {
      Metrics.report(metricsRecord, "output-records", ++numOutputRecords);
    }
  }
  
  private ReduceTaskMetrics myMetrics = null;
  
  private int numMaps;
  private boolean sortComplete;

  { 
    getProgress().setStatus("reduce"); 
    setPhase(TaskStatus.Phase.SHUFFLE);        // phase to start with 
 }

  private Progress copyPhase = getProgress().addPhase("copy");
  private Progress sortPhase  = getProgress().addPhase("sort");
  private Progress reducePhase = getProgress().addPhase("reduce");
  private JobConf conf;
  private MapOutputFile mapOutputFile = new MapOutputFile();

  public ReduceTask() {}

  public ReduceTask(String jobId, String jobFile, String tipId, String taskId,
                    int partition, int numMaps) {
    super(jobId, jobFile, tipId, taskId, partition);
    this.numMaps = numMaps;
    myMetrics = new ReduceTaskMetrics(taskId);
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
  public void localizeConfiguration(JobConf conf) {
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
    if (myMetrics == null) {
        myMetrics = new ReduceTaskMetrics(getTaskId());
    }
  }

  /** Iterates values while keys match in sorted input. */
  private class ValuesIterator implements Iterator {
    private SequenceFile.Sorter.RawKeyValueIterator in; //input iterator
    private WritableComparable key;               // current key
    private Writable value;                       // current value
    private boolean hasNext;                      // more w/ this key
    private boolean more;                         // more in file
    private TaskUmbilicalProtocol umbilical;
    private WritableComparator comparator;
    private Class keyClass;
    private Class valClass;
    private Configuration conf;
    private DataOutputBuffer valOut = new DataOutputBuffer();
    private DataInputBuffer valIn = new DataInputBuffer();
    private DataInputBuffer keyIn = new DataInputBuffer();

    public ValuesIterator (SequenceFile.Sorter.RawKeyValueIterator in, 
                           WritableComparator comparator, Class keyClass,
                           Class valClass, TaskUmbilicalProtocol umbilical,
                           Configuration conf)
      throws IOException {
      this.in = in;
      this.umbilical = umbilical;
      this.conf = conf;
      this.comparator = comparator;
      this.keyClass = keyClass;
      this.valClass = valClass;
      getNext();
    }

    /// Iterator methods

    public boolean hasNext() { return hasNext; }

    public Object next() {
      try {
        Object result = value;                      // save value
        getNext();                                  // move to next
        return result;                              // return saved value
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
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
      reducePhase.set(in.getProgress().get()); // update progress
      reportProgress(umbilical);

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

  public void run(JobConf job, final TaskUmbilicalProtocol umbilical)
    throws IOException {
    Class valueClass = job.getMapOutputValueClass();
    Reducer reducer = (Reducer)ReflectionUtils.newInstance(
                                  job.getReducerClass(), job);
    reducer.configure(job);
    FileSystem lfs = FileSystem.getNamed("local", job);

    copyPhase.complete();                         // copy is already complete
    

    // open a file to collect map output
    Path[] mapFiles = new Path[numMaps];
    for(int i=0; i < numMaps; i++) {
      mapFiles[i] = mapOutputFile.getInputFile(i, getTaskId());
    }

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

    Path tempDir = job.getLocalPath(getTaskId()); 

    WritableComparator comparator = job.getOutputKeyComparator();
   
    SequenceFile.Sorter.RawKeyValueIterator rIter;
 
    try {
      setPhase(TaskStatus.Phase.SORT) ; 
      sortProgress.start();

      // sort the input file
      SequenceFile.Sorter sorter =
        new SequenceFile.Sorter(lfs, comparator, valueClass, job);
      rIter = sorter.sortAndIterate(mapFiles, tempDir, 
                                    !conf.getKeepFailedTaskFiles()); // sort

    } finally {
      sortComplete = true;
    }

    sortPhase.complete();                         // sort is complete
    setPhase(TaskStatus.Phase.REDUCE); 

    Reporter reporter = getReporter(umbilical, getProgress());
    
    // make output collector
    String finalName = getOutputName(getPartition());
    boolean runSpeculative = job.getSpeculativeExecution();
    FileSystem fs = FileSystem.get(job) ;

    if( runSpeculative ){
        fs = new PhasedFileSystem (fs , 
                      getJobId(), getTipId(), getTaskId());
    }
    
    final RecordWriter out = 
      job.getOutputFormat().getRecordWriter(fs, job, finalName, reporter) ;  
    
    OutputCollector collector = new OutputCollector() {
        public void collect(WritableComparable key, Writable value)
          throws IOException {
          out.write(key, value);
          myMetrics.reduceOutput();
          reportProgress(umbilical);
        }
      };
    
    // apply reduce function
    try {
      Class keyClass = job.getMapOutputKeyClass();
      Class valClass = job.getMapOutputValueClass();
      ValuesIterator values = new ValuesIterator(rIter, comparator, keyClass, 
                                                 valClass, umbilical, job);
      while (values.more()) {
        myMetrics.reduceInput();
        reducer.reduce(values.getKey(), values, collector, reporter);
        values.nextKey();
      }

    } finally {
      reducer.close();
      out.close(reporter);
      if( runSpeculative ){
        ((PhasedFileSystem)fs).commit(); 
       }
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

  public void setConf(Configuration conf) {
    if (conf instanceof JobConf) {
      this.conf = (JobConf) conf;
    } else {
      this.conf = new JobConf(conf);
    }
    this.mapOutputFile.setConf(this.conf);
  }

  public Configuration getConf() {
    return this.conf;
  }

}
