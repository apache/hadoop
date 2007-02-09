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

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.SequenceFile.Sorter;
import org.apache.hadoop.io.SequenceFile.Sorter.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics.MetricsRecord;

import org.apache.commons.logging.*;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.ReduceTask.ValuesIterator;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.Updater;


/** A Map task. */
class MapTask extends Task {

    public static final Log LOG =
        LogFactory.getLog("org.apache.hadoop.mapred.MapTask");

  static {                                        // register a ctor
    WritableFactories.setFactory
      (MapTask.class,
       new WritableFactory() {
         public Writable newInstance() { return new MapTask(); }
       });
  }

  {   // set phase for this task
    setPhase(TaskStatus.Phase.MAP); 
  }
  
  private class MapTaskMetrics {
    private MetricsRecord mapInputMetrics = null;
    private MetricsRecord mapOutputMetrics = null;
    
    MapTaskMetrics(String user) {
        MetricsContext context = MetricsUtil.getContext("mapred");
        mapInputMetrics = MetricsUtil.createRecord(context, "mapInput", "user", user);
        mapOutputMetrics = MetricsUtil.createRecord(context, "mapOutput", "user", user);
    }
    
    synchronized void mapInput(int numBytes) {
        mapInputMetrics.incrMetric("input_records", 1);
        mapInputMetrics.incrMetric("input_bytes", numBytes);
        mapInputMetrics.update();
    }
    
    synchronized void mapOutput(int numBytes) {
        mapOutputMetrics.incrMetric("output_records", 1);
        mapOutputMetrics.incrMetric("output_bytes", numBytes);
        mapOutputMetrics.update();
    }
    
  }
  
  private MapTaskMetrics myMetrics = null;

  private InputSplit split;
  private MapOutputFile mapOutputFile = new MapOutputFile();
  private JobConf conf;

  public MapTask() {}

  public MapTask(String jobId, String jobFile, String tipId, String taskId, 
                 int partition, InputSplit split) {
    super(jobId, jobFile, tipId, taskId, partition);
    this.split = split;
  }

  public boolean isMapTask() {
      return true;
  }

  public void localizeConfiguration(JobConf conf) throws IOException {
    super.localizeConfiguration(conf);
    Path localSplit = new Path(new Path(getJobFile()).getParent(), 
                               "split.dta");
    DataOutputStream out = FileSystem.getLocal(conf).create(localSplit);
    split.write(out);
    out.close();
    if (split instanceof FileSplit) {
      conf.set("map.input.file", ((FileSplit) split).getPath().toString());
      conf.setLong("map.input.start", ((FileSplit) split).getStart());
      conf.setLong("map.input.length", ((FileSplit) split).getLength());
    }
  }
  
  public TaskRunner createRunner(TaskTracker tracker) {
    return new MapTaskRunner(this, tracker, this.conf);
  }

  public InputSplit getSplit() { return split; }

  public void write(DataOutput out) throws IOException {
    super.write(out);
    split.write(out);
    
  }
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    split = new FileSplit();
    split.readFields(in);
    if (myMetrics == null) {
        myMetrics = new MapTaskMetrics("unknown");
    }
  }

  public void run(final JobConf job, final TaskUmbilicalProtocol umbilical)
    throws IOException {

    myMetrics = new MapTaskMetrics(job.getUser());
    Reporter reporter = getReporter(umbilical, getProgress());

    MapOutputBuffer collector = new MapOutputBuffer(umbilical, job, reporter);
      
    final RecordReader rawIn =                  // open input
      job.getInputFormat().getRecordReader(split, job, reporter);

    RecordReader in = new RecordReader() {      // wrap in progress reporter

        public WritableComparable createKey() {
          return rawIn.createKey();
        }
          
        public Writable createValue() {
          return rawIn.createValue();
        }
         
        public synchronized boolean next(Writable key, Writable value)
          throws IOException {

          setProgress(getProgress());
          long beforePos = getPos();
          boolean ret = rawIn.next(key, value);
          myMetrics.mapInput((int)(getPos() - beforePos));
          return ret;
        }
        public long getPos() throws IOException { return rawIn.getPos(); }
        public void close() throws IOException { rawIn.close(); }
        public float getProgress() throws IOException {
          return rawIn.getProgress();
        }
      };

    Thread sortProgress = createProgressThread(umbilical);
    MapRunnable runner =
      (MapRunnable)ReflectionUtils.newInstance(job.getMapRunnerClass(), job);

    try {
      sortProgress.start();
      runner.run(in, collector, reporter);      // run the map
      //check whether the length of the key/value buffer is 0. If not, then
      //we need to spill that to disk. Note that we reset the key/val buffer
      //upon each spill (so a length > 0 means that we have not spilled yet)
      if (((MapOutputBuffer)collector).keyValBuffer.getLength() > 0) {
        ((MapOutputBuffer)collector).sortAndSpillToDisk();
      }
      //merge the partitions from the spilled files and create one output
      collector.mergeParts();
    } finally {
      //close
      in.close();                               // close input
      collector.close();
      sortProgress.interrupt();
    }
    done(umbilical);
  }

  private Thread createProgressThread(final TaskUmbilicalProtocol umbilical) {
    //spawn a thread to give merge progress heartbeats
    Thread sortProgress = new Thread() {
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
                                 "reporting sort progress\n" +
                                 StringUtils.stringifyException(e));
              continue;
          }
        }
      }
    };
    sortProgress.setName("Sort progress reporter for task "+getTaskId());
    sortProgress.setDaemon(true);
    return sortProgress;
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

  class MapOutputBuffer implements OutputCollector {

    private final int partitions;
    private Partitioner partitioner;
    private TaskUmbilicalProtocol umbilical;
    private JobConf job;
    private Reporter reporter;

    private DataOutputBuffer keyValBuffer; //the buffer where key/val will
                                           //be stored before they are 
                                           //spilled to disk
    private int maxBufferSize; //the max amount of in-memory space after which
                               //we will spill the keyValBuffer to disk
    private int numSpills; //maintains the no. of spills to disk done so far
    
    private FileSystem localFs;
    private CompressionCodec codec;
    private CompressionType compressionType;
    private Class keyClass;
    private Class valClass;
    private WritableComparator comparator;
    private BufferSorter []sortImpl;
    private SequenceFile.Writer writer;
    private FSDataOutputStream out;
    private FSDataOutputStream indexOut;
    private long segmentStart;
    public MapOutputBuffer(TaskUmbilicalProtocol umbilical, JobConf job, 
            Reporter reporter) throws IOException {
      this.partitions = job.getNumReduceTasks();
      this.partitioner = (Partitioner)ReflectionUtils.newInstance(
                                      job.getPartitionerClass(), job);
      maxBufferSize = job.getInt("io.sort.mb", 100) * 1024 * 1024;
      keyValBuffer = new DataOutputBuffer();

      this.umbilical = umbilical;
      this.job = job;
      this.reporter = reporter;
      this.comparator = job.getOutputKeyComparator();
      this.keyClass = job.getMapOutputKeyClass();
      this.valClass = job.getMapOutputValueClass();
      this.localFs = FileSystem.getLocal(job);
      this.codec = null;
      this.compressionType = CompressionType.NONE;
      if (job.getCompressMapOutput()) {
        // find the kind of compression to do, defaulting to record
        compressionType = job.getMapOutputCompressionType();

        // find the right codec
        Class codecClass = 
          job.getMapOutputCompressorClass(DefaultCodec.class);
        codec = (CompressionCodec) 
                   ReflectionUtils.newInstance(codecClass, job);
      }
      sortImpl = new BufferSorter[partitions];
      for (int i = 0; i < partitions; i++)
        sortImpl[i] = (BufferSorter)ReflectionUtils.newInstance(
                   job.getClass("map.sort.class", MergeSorter.class,
                   BufferSorter.class), job);
    }
    public void startPartition(int partNumber) throws IOException {
      //We create the sort output as multiple sequence files within a spilled
      //file. So we create a writer for each partition. 
      segmentStart = out.getPos();
      writer =
          SequenceFile.createWriter(job, out, job.getMapOutputKeyClass(),
                  job.getMapOutputValueClass(), compressionType, codec);
    }
    private void endPartition(int partNumber) throws IOException {
      //Need to write syncs especially if block compression is in use
      //We also update the index file to contain the part offsets per 
      //spilled file
      writer.sync();
      indexOut.writeLong(segmentStart);
      //we also store 0 length key/val segments to make the merge phase easier.
      indexOut.writeLong(out.getPos()-segmentStart);
    }
    
    public void collect(WritableComparable key,
              Writable value) throws IOException {
      synchronized (this) {
        //dump the key/value to buffer
        int keyOffset = keyValBuffer.getLength(); 
        key.write(keyValBuffer);
        int keyLength = keyValBuffer.getLength() - keyOffset;
        value.write(keyValBuffer);
        int valLength = keyValBuffer.getLength() - (keyOffset + keyLength);
      
        int partNumber = partitioner.getPartition(key, value, partitions);
        sortImpl[partNumber].addKeyValue(keyOffset, keyLength, valLength);

        myMetrics.mapOutput((int)(keyValBuffer.getLength() - keyOffset));

        //now check whether we need to spill to disk
        long totalMem = 0;
        for (int i = 0; i < partitions; i++)
          totalMem += sortImpl[i].getMemoryUtilized();
        if ((keyValBuffer.getLength() + totalMem) >= maxBufferSize) {
          sortAndSpillToDisk();
          keyValBuffer.reset();
          for (int i = 0; i < partitions; i++)
            sortImpl[i].close(); 
        }
      }
    }
    
    //sort, combine and spill to disk
    private void sortAndSpillToDisk() throws IOException {
      synchronized (this) {
        Path filename = mapOutputFile.getSpillFile(getTaskId(), numSpills);
        //we just create the FSDataOutputStream object here.
        out = localFs.create(filename);
        Path indexFilename = mapOutputFile.getSpillIndexFile(getTaskId(), 
                                                             numSpills);
        indexOut = localFs.create(indexFilename);
        LOG.debug("opened "+
        mapOutputFile.getSpillFile(getTaskId(), numSpills).getName());
          
        //invoke the sort
        for (int i = 0; i < partitions; i++) {
          sortImpl[i].setInputBuffer(keyValBuffer);
          RawKeyValueIterator rIter = sortImpl[i].sort();
          
          startPartition(i);
          if (rIter != null) {
            //invoke the combiner if one is defined
            if (job.getCombinerClass() != null) {
              //we instantiate and close the combiner for each partition. This
              //is required for streaming where the combiner runs as a separate
              //process and we want to make sure that the combiner process has
              //got all the input key/val, processed, and output the result 
              //key/vals before we write the partition header in the output file
              Reducer combiner = (Reducer)ReflectionUtils.newInstance(
                                         job.getCombinerClass(), job);
              // make collector
              OutputCollector combineCollector = new OutputCollector() {
                public void collect(WritableComparable key, Writable value)
                  throws IOException {
                  synchronized (this) {
                    writer.append(key, value);
                  }
                }
              };
              combineAndSpill(rIter, combiner, combineCollector);
              combiner.close();
            }
            else //just spill the sorted data
              spill(rIter);
          }
          endPartition(i);
        }
        numSpills++;
        out.close();
        indexOut.close();
      }
    }
    
    private void combineAndSpill(RawKeyValueIterator resultIter, 
    Reducer combiner, OutputCollector combineCollector) throws IOException {
      //combine the key/value obtained from the offset & indices arrays.
      CombineValuesIterator values = new CombineValuesIterator(resultIter,
              comparator, keyClass, valClass, umbilical, job);
      while (values.more()) {
        combiner.reduce(values.getKey(), values, combineCollector, reporter);
        values.nextKey();
      }
    }
    
    private void spill(RawKeyValueIterator resultIter) throws IOException {
      Writable key = null;
      Writable value = null;

      try {
        key = (WritableComparable)ReflectionUtils.newInstance(keyClass, job);
        value = (Writable)ReflectionUtils.newInstance(valClass, job);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      DataInputBuffer keyIn = new DataInputBuffer();
      DataInputBuffer valIn = new DataInputBuffer();
      DataOutputBuffer valOut = new DataOutputBuffer();
      while (resultIter.next()) {
        keyIn.reset(resultIter.getKey().getData(), 
                    resultIter.getKey().getLength());
        key.readFields(keyIn);
        valOut.reset();
        (resultIter.getValue()).writeUncompressedBytes(valOut);
        valIn.reset(valOut.getData(), valOut.getLength());
        value.readFields(valIn);

        writer.append(key, value);
      }
    }
    
    public void mergeParts() throws IOException {
      Path finalOutputFile = mapOutputFile.getOutputFile(getTaskId());
      Path finalIndexFile = mapOutputFile.getOutputIndexFile(getTaskId());
      
      if (numSpills == 1) { //the spill is the final output
        Path spillPath = mapOutputFile.getSpillFile(getTaskId(), 0);
        Path spillIndexPath = mapOutputFile.getSpillIndexFile(getTaskId(), 0);
        localFs.rename(spillPath, finalOutputFile);
        localFs.rename(spillIndexPath, finalIndexFile);
        return;
      }
      
      //The output stream for the final single output file
      FSDataOutputStream finalOut = localFs.create(finalOutputFile, true, 
                                                   4096);
      //The final index file output stream
      FSDataOutputStream finalIndexOut = localFs.create(finalIndexFile, true,
                                                           4096);
      long segmentStart;
      
      if (numSpills == 0) {
        //create dummy files
        for (int i = 0; i < partitions; i++) {
          segmentStart = finalOut.getPos();
          SequenceFile.createWriter(job, finalOut, 
                  job.getMapOutputKeyClass(), job.getMapOutputValueClass(), 
                  compressionType, codec);
          finalIndexOut.writeLong(segmentStart);
          finalIndexOut.writeLong(finalOut.getPos() - segmentStart);
        }
        finalOut.close();
        finalIndexOut.close();
        return;
      }
      {
        Path [] filename = new Path[numSpills];
        Path [] indexFileName = new Path[numSpills];
        
        for(int i = 0; i < numSpills; i++) {
          filename[i] = mapOutputFile.getSpillFile(getTaskId(), i);
          indexFileName[i] = mapOutputFile.getSpillIndexFile(getTaskId(), i);
        }
        
        //create a sorter object as we need access to the SegmentDescriptor
        //class and merge methods
        Sorter sorter = new Sorter(localFs, keyClass, valClass, job);
        
        for (int parts = 0; parts < partitions; parts++){
          List<SegmentDescriptor> segmentList = new ArrayList(numSpills);
          for(int i = 0; i < numSpills; i++) {
            FSDataInputStream indexIn = localFs.open(indexFileName[i]);
            indexIn.seek(parts * 16);
            long segmentOffset = indexIn.readLong();
            long segmentLength = indexIn.readLong();
            indexIn.close();
            SegmentDescriptor s = sorter.new SegmentDescriptor(segmentOffset,
                segmentLength, filename[i]);
            s.preserveInput(true);
            s.doSync();
            segmentList.add(i, s);
          }
          segmentStart = finalOut.getPos();
          SequenceFile.Writer writer = SequenceFile.createWriter(job, finalOut, 
              job.getMapOutputKeyClass(), job.getMapOutputValueClass(), 
              compressionType, codec);
          sorter.writeFile(sorter.merge(segmentList, new Path(getTaskId())), 
                           writer);
          //add a sync block - required esp. for block compression to ensure
          //partition data don't span partition boundaries
          writer.sync();
          //when we write the offset/length to the final index file, we write
          //longs for both. This helps us to reliably seek directly to the
          //offset/length for a partition when we start serving the byte-ranges
          //to the reduces. We probably waste some space in the file by doing
          //this as opposed to writing VLong but it helps us later on.
          finalIndexOut.writeLong(segmentStart);
          finalIndexOut.writeLong(finalOut.getPos()-segmentStart);
        }
        finalOut.close();
        finalIndexOut.close();
        //cleanup
        for(int i = 0; i < numSpills; i++) {
          localFs.delete(filename[i]);
          localFs.delete(indexFileName[i]);
        }
      }
    }
    
    public void close() throws IOException {
      //empty for now
    }
    
    private class CombineValuesIterator extends ValuesIterator {
        
      public CombineValuesIterator(SequenceFile.Sorter.RawKeyValueIterator in, 
              WritableComparator comparator, Class keyClass,
              Class valClass, TaskUmbilicalProtocol umbilical, 
              Configuration conf) 
      throws IOException {
        super(in, comparator, keyClass, valClass, umbilical, conf);
      }
    }
  }
}
