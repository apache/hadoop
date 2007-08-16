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
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Sorter;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Sorter.RawKeyValueIterator;
import org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.ReduceTask.ValuesIterator;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import static org.apache.hadoop.mapred.Task.Counter.*;

/** A Map task. */
class MapTask extends Task {

  private BytesWritable split = new BytesWritable();
  private String splitClass;
  private InputSplit instantiatedSplit = null;
  private final static int APPROX_HEADER_LENGTH = 150;

  private static final Log LOG = LogFactory.getLog(MapTask.class.getName());

  {   // set phase for this task
    setPhase(TaskStatus.Phase.MAP); 
  }

  public MapTask() {}

  public MapTask(String jobId, String jobFile, String tipId, String taskId, 
                 int partition, String splitClass, BytesWritable split
                 ) throws IOException {
    super(jobId, jobFile, tipId, taskId, partition);
    this.splitClass = splitClass;
    this.split.set(split);
  }

  public boolean isMapTask() {
    return true;
  }

  public void localizeConfiguration(JobConf conf) throws IOException {
    super.localizeConfiguration(conf);
    Path localSplit = new Path(new Path(getJobFile()).getParent(), 
                               "split.dta");
    LOG.debug("Writing local split to " + localSplit);
    DataOutputStream out = FileSystem.getLocal(conf).create(localSplit);
    Text.writeString(out, splitClass);
    split.write(out);
    out.close();
  }
  
  public TaskRunner createRunner(TaskTracker tracker) {
    return new MapTaskRunner(this, tracker, this.conf);
  }

  public void write(DataOutput out) throws IOException {
    super.write(out);
    Text.writeString(out, splitClass);
    split.write(out);
  }
  
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    splitClass = Text.readString(in);
    split.readFields(in);
  }

  InputSplit getInputSplit() throws UnsupportedOperationException {
    return instantiatedSplit;
  }

  @SuppressWarnings("unchecked")
  public void run(final JobConf job, final TaskUmbilicalProtocol umbilical)
    throws IOException {

    final Reporter reporter = getReporter(umbilical);

    // start thread that will handle communication with parent
    startCommunicationThread(umbilical);

    int numReduceTasks = conf.getNumReduceTasks();
    LOG.info("numReduceTasks: " + numReduceTasks);
    MapOutputCollector collector = null;
    if (numReduceTasks > 0) {
      collector = new MapOutputBuffer(umbilical, job, reporter);
    } else { 
      collector = new DirectMapOutputCollector(umbilical, job, reporter);
    }
    // reinstantiate the split
    try {
      instantiatedSplit = (InputSplit) 
        ReflectionUtils.newInstance(job.getClassByName(splitClass), job);
    } catch (ClassNotFoundException exp) {
      IOException wrap = new IOException("Split class " + splitClass + 
                                         " not found");
      wrap.initCause(exp);
      throw wrap;
    }
    DataInputBuffer splitBuffer = new DataInputBuffer();
    splitBuffer.reset(split.get(), 0, split.getSize());
    instantiatedSplit.readFields(splitBuffer);
    
    // if it is a file split, we can give more details
    if (instantiatedSplit instanceof FileSplit) {
      FileSplit fileSplit = (FileSplit) instantiatedSplit;
      job.set("map.input.file", fileSplit.getPath().toString());
      job.setLong("map.input.start", fileSplit.getStart());
      job.setLong("map.input.length", fileSplit.getLength());
    }
      
    final RecordReader rawIn =                  // open input
      job.getInputFormat().getRecordReader(instantiatedSplit, job, reporter);

    RecordReader in = new RecordReader() {      // wrap in progress reporter

        public WritableComparable createKey() {
          return rawIn.createKey();
        }
          
        public Writable createValue() {
          return rawIn.createValue();
        }
         
        public synchronized boolean next(WritableComparable key, Writable value)
          throws IOException {

          setProgress(getProgress());
          long beforePos = getPos();
          boolean ret = rawIn.next(key, value);
          if (ret) {
            reporter.incrCounter(MAP_INPUT_RECORDS, 1);
            reporter.incrCounter(MAP_INPUT_BYTES, (getPos() - beforePos));
          }
          return ret;
        }
        public long getPos() throws IOException { return rawIn.getPos(); }
        public void close() throws IOException { rawIn.close(); }
        public float getProgress() throws IOException {
          return rawIn.getProgress();
        }
      };

    MapRunnable runner =
      (MapRunnable)ReflectionUtils.newInstance(job.getMapRunnerClass(), job);

    try {
      runner.run(in, collector, reporter);      
      collector.flush();
    } finally {
      //close
      in.close();                               // close input
      collector.close();
    }
    done(umbilical);
  }

  interface MapOutputCollector<K extends WritableComparable,
                               V extends Writable>
    extends OutputCollector<K, V> {

    public void close() throws IOException;
    
    public void flush() throws IOException;
        
  }

  class DirectMapOutputCollector<K extends WritableComparable,
                                 V extends Writable>
    implements MapOutputCollector<K, V> {
 
    private RecordWriter<K, V> out = null;

    private Reporter reporter = null;

    @SuppressWarnings("unchecked")
    public DirectMapOutputCollector(TaskUmbilicalProtocol umbilical,
        JobConf job, Reporter reporter) throws IOException {
      this.reporter = reporter;
      String finalName = getOutputName(getPartition());
      FileSystem fs = FileSystem.get(job);

      out = job.getOutputFormat().getRecordWriter(fs, job, finalName, reporter);
    }

    public void close() throws IOException {
      if (this.out != null) {
        out.close(this.reporter);
      }

    }

    public void flush() throws IOException {
      // TODO Auto-generated method stub
      
    }

    public void collect(K key, V value) throws IOException {
      this.out.write(key, value);
    }
    
  }
  
  class MapOutputBuffer implements MapOutputCollector {

    private final int partitions;
    private Partitioner partitioner;
    private JobConf job;
    private Reporter reporter;
    final private TaskUmbilicalProtocol umbilical;

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

      this.job = job;
      this.reporter = reporter;
      this.umbilical = umbilical;
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
    
    private void startPartition(int partNumber) throws IOException {
      //We create the sort output as multiple sequence files within a spilled
      //file. So we create a writer for each partition. 
      segmentStart = out.getPos();
      writer =
        SequenceFile.createWriter(job, out, job.getMapOutputKeyClass(),
                                  job.getMapOutputValueClass(), compressionType, codec);
    }
    private void endPartition(int partNumber) throws IOException {
      //Need to close the file, especially if block compression is in use
      //We also update the index file to contain the part offsets per 
      //spilled file
      writer.close();
      indexOut.writeLong(segmentStart);
      //we also store 0 length key/val segments to make the merge phase easier.
      indexOut.writeLong(out.getPos()-segmentStart);
    }
    
    @SuppressWarnings("unchecked")
    public void collect(WritableComparable key,
                        Writable value) throws IOException {
      
      if (key.getClass() != keyClass) {
        throw new IOException("Type mismatch in key from map: expected "
                              + keyClass.getName() + ", recieved "
                              + key.getClass().getName());
      }
      if (value.getClass() != valClass) {
        throw new IOException("Type mismatch in value from map: expected "
                              + valClass.getName() + ", recieved "
                              + value.getClass().getName());
      }
      
      synchronized (this) {
        if (keyValBuffer == null) {
          keyValBuffer = new DataOutputBuffer();
        }
        //dump the key/value to buffer
        int keyOffset = keyValBuffer.getLength(); 
        key.write(keyValBuffer);
        int keyLength = keyValBuffer.getLength() - keyOffset;
        value.write(keyValBuffer);
        int valLength = keyValBuffer.getLength() - (keyOffset + keyLength);
      
        int partNumber = partitioner.getPartition(key, value, partitions);
        sortImpl[partNumber].addKeyValue(keyOffset, keyLength, valLength);

        reporter.incrCounter(MAP_OUTPUT_RECORDS, 1);
        reporter.incrCounter(MAP_OUTPUT_BYTES,
                             (keyValBuffer.getLength() - keyOffset));

        //now check whether we need to spill to disk
        long totalMem = 0;
        for (int i = 0; i < partitions; i++)
          totalMem += sortImpl[i].getMemoryUtilized();
        if ((keyValBuffer.getLength() + totalMem) >= maxBufferSize) {
          sortAndSpillToDisk();
          //we don't reuse the keyValBuffer. We want to maintain consistency
          //in the memory model (for negligible performance loss).
          keyValBuffer = null;
          for (int i = 0; i < partitions; i++) {
            sortImpl[i].close();
          }
        }
      }
    }
    
    //sort, combine and spill to disk
    private void sortAndSpillToDisk() throws IOException {
      synchronized (this) {
        //approximate the length of the output file to be the length of the
        //buffer + header lengths for the partitions
        long size = keyValBuffer.getLength() + 
                    partitions * APPROX_HEADER_LENGTH;
        Path filename = mapOutputFile.getSpillFileForWrite(getTaskId(), 
                                      numSpills, size);
        //we just create the FSDataOutputStream object here.
        out = localFs.create(filename);
        Path indexFilename = mapOutputFile.getSpillIndexFileForWrite(
                             getTaskId(), numSpills, partitions * 16);
        indexOut = localFs.create(indexFilename);
        LOG.debug("opened "+
                  mapOutputFile.getSpillFile(getTaskId(), numSpills).getName());
          
        //invoke the sort
        for (int i = 0; i < partitions; i++) {
          sortImpl[i].setInputBuffer(keyValBuffer);
          sortImpl[i].setProgressable(reporter);
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
    
    @SuppressWarnings("unchecked")
    private void combineAndSpill(RawKeyValueIterator resultIter, 
                                 Reducer combiner, OutputCollector combineCollector) throws IOException {
      //combine the key/value obtained from the offset & indices arrays.
      CombineValuesIterator values = new CombineValuesIterator(resultIter,
                                                               comparator, keyClass, valClass, job, reporter);
      while (values.more()) {
        combiner.reduce(values.getKey(), values, combineCollector, reporter);
        values.nextKey();
        reporter.incrCounter(COMBINE_OUTPUT_RECORDS, 1);
        // indicate we're making progress
        reporter.progress();
      }
    }
    
    private void spill(RawKeyValueIterator resultIter) throws IOException {
      Writable key = null;
      Writable value = null;

      try {
        // indicate progress, since constructor may take a while (because of 
        // user code) 
        reporter.progress();
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
        reporter.progress();
      }
    }
    
    private void mergeParts() throws IOException {
      // get the approximate size of the final output/index files
      long finalOutFileSize = 0;
      long finalIndexFileSize = 0;
      Path [] filename = new Path[numSpills];
      Path [] indexFileName = new Path[numSpills];
      
      for(int i = 0; i < numSpills; i++) {
        filename[i] = mapOutputFile.getSpillFile(getTaskId(), i);
        indexFileName[i] = mapOutputFile.getSpillIndexFile(getTaskId(), i);
        finalOutFileSize += localFs.getLength(filename[i]);
      }
      //make correction in the length to include the sequence file header
      //lengths for each partition
      finalOutFileSize += partitions * APPROX_HEADER_LENGTH;
      
      finalIndexFileSize = partitions * 16;
      
      Path finalOutputFile = mapOutputFile.getOutputFileForWrite(getTaskId(), 
                             finalOutFileSize);
      Path finalIndexFile = mapOutputFile.getOutputIndexFileForWrite(
                            getTaskId(), finalIndexFileSize);
      
      if (numSpills == 1) { //the spill is the final output
        localFs.rename(filename[0], finalOutputFile);
        localFs.rename(indexFileName[0], finalIndexFile);
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
          Writer writer = SequenceFile.createWriter(job, finalOut, 
                                                    job.getMapOutputKeyClass(), 
                                                    job.getMapOutputValueClass(), 
                                                    compressionType, codec);
          finalIndexOut.writeLong(segmentStart);
          finalIndexOut.writeLong(finalOut.getPos() - segmentStart);
          writer.close();
        }
        finalOut.close();
        finalIndexOut.close();
        return;
      }
      {
        //create a sorter object as we need access to the SegmentDescriptor
        //class and merge methods
        Sorter sorter = new Sorter(localFs, job.getOutputKeyComparator(), valClass, job);
        sorter.setProgressable(reporter);
        
        for (int parts = 0; parts < partitions; parts++){
          List<SegmentDescriptor> segmentList =
            new ArrayList<SegmentDescriptor>(numSpills);
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
          RawKeyValueIterator kvIter = sorter.merge(segmentList, new Path(getTaskId())); 
          SequenceFile.Writer writer = SequenceFile.createWriter(job, finalOut, 
                                                                 job.getMapOutputKeyClass(), job.getMapOutputValueClass(), 
                                                                 compressionType, codec);
          sorter.writeFile(kvIter, writer);
          //close the file - required esp. for block compression to ensure
          //partition data don't span partition boundaries
          writer.close();
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
                                   Class valClass, Configuration conf, Reporter reporter) 
        throws IOException {
        super(in, comparator, keyClass, valClass, conf, reporter);
      }
      
      public Object next() {
        reporter.incrCounter(COMBINE_INPUT_RECORDS, 1);
        return super.next();
      }
    }

    public void flush() throws IOException 
    {
      //check whether the length of the key/value buffer is 0. If not, then
      //we need to spill that to disk. Note that we reset the key/val buffer
      //upon each spill (so a length > 0 means that we have not spilled yet)
      if (keyValBuffer.getLength() > 0) {
        sortAndSpillToDisk();
      }
      mergeParts();
    }
  }
}
