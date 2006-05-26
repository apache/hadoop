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

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;
import java.text.*;

/** A Reduce task. */
class ReduceTask extends Task {

  static {                                        // register a ctor
    WritableFactories.setFactory
      (ReduceTask.class,
       new WritableFactory() {
         public Writable newInstance() { return new ReduceTask(); }
       });
  }

  private UTF8 jobId = new UTF8();
  private int numMaps;
  private int partition;
  private boolean sortComplete;

  { getProgress().setStatus("reduce"); }

  private Progress copyPhase = getProgress().addPhase("copy");
  private Progress appendPhase = getProgress().addPhase("append");
  private Progress sortPhase  = getProgress().addPhase("sort");
  private Progress reducePhase = getProgress().addPhase("reduce");
  private JobConf conf;
  private MapOutputFile mapOutputFile = new MapOutputFile();

  public ReduceTask() {}

  public ReduceTask(String jobId, String jobFile, String taskId,
                    int numMaps, int partition) {
    super(jobFile, taskId);
    this.jobId.set(jobId);
    this.numMaps = numMaps;
    this.partition = partition;
  }

  public TaskRunner createRunner(TaskTracker tracker) throws IOException {
    return new ReduceTaskRunner(this, tracker, this.conf);
  }

  public boolean isMapTask() {
      return false;
  }

  /**
   * Get the job name for this task.
   * @return the job name
   */
  public UTF8 getJobId() {
    return jobId;
  }
  
  public int getNumMaps() { return numMaps; }
  public int getPartition() { return partition; }

  public void write(DataOutput out) throws IOException {
    super.write(out);

    jobId.write(out);
    out.writeInt(numMaps);                        // write the number of maps
    out.writeInt(partition);                      // write partition
  }

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    jobId.readFields(in);
    numMaps = in.readInt();
    this.partition = in.readInt();                // read partition
  }

  /** Iterates values while keys match in sorted input. */
  private class ValuesIterator implements Iterator {
    private SequenceFile.Reader in;               // input file
    private WritableComparable key;               // current key
    private Writable value;                       // current value
    private boolean hasNext;                      // more w/ this key
    private boolean more;                         // more in file
    private float progPerByte;
    private TaskUmbilicalProtocol umbilical;
    private WritableComparator comparator;

    public ValuesIterator (SequenceFile.Reader in, long length,
                           WritableComparator comparator,
                           TaskUmbilicalProtocol umbilical)
      throws IOException {
      this.in = in;
      this.progPerByte = 1.0f / (float)length;
      this.umbilical = umbilical;
      this.comparator = comparator;
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
      reducePhase.set(in.getPosition()*progPerByte); // update progress
      reportProgress(umbilical);

      Writable lastKey = key;                     // save previous key
      try {
        key = (WritableComparable)in.getKeyClass().newInstance();
        value = (Writable)in.getValueClass().newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      more = in.next(key, value);
      if (more) {
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
    Class keyClass = job.getMapOutputKeyClass();
    Class valueClass = job.getMapOutputValueClass();
    Reducer reducer = (Reducer)job.newInstance(job.getReducerClass());
    reducer.configure(job);
    FileSystem lfs = FileSystem.getNamed("local", job);

    copyPhase.complete();                         // copy is already complete

    // open a file to collect map output
    Path file = job.getLocalPath(getTaskId()+Path.SEPARATOR+"all.1");
    SequenceFile.Writer writer =
      new SequenceFile.Writer(lfs, file, keyClass, valueClass);
    try {
      // append all input files into a single input file
      for (int i = 0; i < numMaps; i++) {
        appendPhase.addPhase();                 // one per file
      }
      
      DataOutputBuffer buffer = new DataOutputBuffer();

      for (int i = 0; i < numMaps; i++) {
        Path partFile =
          this.mapOutputFile.getInputFile(i, getTaskId());
        float progPerByte = 1.0f / lfs.getLength(partFile);
        Progress phase = appendPhase.phase();
        phase.setStatus(partFile.toString());

        SequenceFile.Reader in = new SequenceFile.Reader(lfs, partFile, job);
        try {
          int keyLen;
          while((keyLen = in.next(buffer)) > 0) {
            writer.append(buffer.getData(), 0, buffer.getLength(), keyLen);
            phase.set(in.getPosition()*progPerByte);
            reportProgress(umbilical);
            buffer.reset();
          }
        } finally {
          in.close();
        }
        phase.complete();
      }
      
    } finally {
      writer.close();
    }
      
    appendPhase.complete();                     // append is complete

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

    Path sortedFile = job.getLocalPath(getTaskId()+Path.SEPARATOR+"all.2");

    WritableComparator comparator = job.getOutputKeyComparator();
    
    try {
      sortProgress.start();

      // sort the input file
      SequenceFile.Sorter sorter =
        new SequenceFile.Sorter(lfs, comparator, valueClass, job);
      sorter.sort(file, sortedFile);              // sort
      lfs.delete(file);                           // remove unsorted

    } finally {
      sortComplete = true;
    }

    sortPhase.complete();                         // sort is complete

    // make output collector
    String name = getOutputName(getPartition());
    final RecordWriter out =
      job.getOutputFormat().getRecordWriter(FileSystem.get(job), job, name);
    OutputCollector collector = new OutputCollector() {
        public void collect(WritableComparable key, Writable value)
          throws IOException {
          out.write(key, value);
          reportProgress(umbilical);
        }
      };
    
    // apply reduce function
    SequenceFile.Reader in = new SequenceFile.Reader(lfs, sortedFile, job);
    Reporter reporter = getReporter(umbilical, getProgress());
    long length = lfs.getLength(sortedFile);
    try {
      ValuesIterator values = new ValuesIterator(in, length, comparator,
                                                 umbilical);
      while (values.more()) {
        reducer.reduce(values.getKey(), values, collector, reporter);
        values.nextKey();
      }

    } finally {
      reducer.close();
      in.close();
      lfs.delete(sortedFile);                     // remove sorted
      out.close(reporter);
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

  private static synchronized String getOutputName(int partition) {
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
