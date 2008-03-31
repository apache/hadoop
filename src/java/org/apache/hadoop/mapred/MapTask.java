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
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
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
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Sorter.RawKeyValueIterator;
import org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor;
import org.apache.hadoop.io.SequenceFile.Sorter;
import org.apache.hadoop.io.SequenceFile.ValueBytes;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.ReduceTask.ValuesIterator;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.QuickSort;
import org.apache.hadoop.util.ReflectionUtils;

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

  public MapTask() {
    super();
  }

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

  /**
   * This class wraps the user's record reader to update the counters and progress
   * as records are read.
   * @param <K>
   * @param <V>
   */
  class TrackedRecordReader<K, V> 
      implements RecordReader<K,V> {
    private RecordReader<K,V> rawIn;
    private Counters.Counter inputByteCounter;
    private Counters.Counter inputRecordCounter;
    
    TrackedRecordReader(RecordReader<K,V> raw, Counters counters) {
      rawIn = raw;
      inputRecordCounter = counters.findCounter(MAP_INPUT_RECORDS);
      inputByteCounter = counters.findCounter(MAP_INPUT_BYTES);
    }

    public K createKey() {
      return rawIn.createKey();
    }
      
    public V createValue() {
      return rawIn.createValue();
    }
     
    public synchronized boolean next(K key, V value)
      throws IOException {

      setProgress(getProgress());
      long beforePos = getPos();
      boolean ret = rawIn.next(key, value);
      if (ret) {
        inputRecordCounter.increment(1);
        inputByteCounter.increment(getPos() - beforePos);
      }
      return ret;
    }
    public long getPos() throws IOException { return rawIn.getPos(); }
    public void close() throws IOException { rawIn.close(); }
    public float getProgress() throws IOException {
      return rawIn.getProgress();
    }
  };

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
      
    RecordReader rawIn =                  // open input
      job.getInputFormat().getRecordReader(instantiatedSplit, job, reporter);
    RecordReader in = new TrackedRecordReader(rawIn, getCounters());

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

  interface MapOutputCollector<K, V>
    extends OutputCollector<K, V> {

    public void close() throws IOException;
    
    public void flush() throws IOException;
        
  }

  class DirectMapOutputCollector<K, V>
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
    }

    public void collect(K key, V value) throws IOException {
      this.out.write(key, value);
    }
    
  }

  class MapOutputBuffer implements MapOutputCollector, IndexedSortable {
    private final int partitions;
    private final Partitioner partitioner;
    private final JobConf job;
    private final Reporter reporter;
    private final Class keyClass;
    private final Class valClass;
    private final RawComparator comparator;
    private final SerializationFactory serializationFactory;
    private final Serializer keySerializer;
    private final Serializer valSerializer;
    private final Class<? extends Reducer> combinerClass;
    private final CombineOutputCollector combineCollector;
    private final boolean compressMapOutput;
    private final CompressionCodec codec;
    private final CompressionType compressionType;

    // used if compressMapOutput && compressionType == RECORD
    // DataOutputBuffer req b/c compression codecs req continguous buffer
    private final DataOutputBuffer rawBuffer;
    private final CompressionOutputStream deflateFilter;
    private final DataOutputStream deflateStream;
    private final Compressor compressor;

    // k/v accounting
    private volatile int kvstart = 0;  // marks beginning of spill
    private volatile int kvend = 0;    // marks beginning of collectable
    private int kvindex = 0;           // marks end of collected
    private final int[] kvoffsets;     // indices into kvindices
    private final int[] kvindices;     // partition, k/v offsets into kvbuffer
    private volatile int bufstart = 0; // marks beginning of spill
    private volatile int bufend = 0;   // marks beginning of collectable
    private volatile int bufvoid = 0;  // marks the point where we should stop
                                       // reading at the end of the buffer
    private int bufindex = 0;          // marks end of collected
    private int bufmark = 0;           // marks end of record
    private byte[] kvbuffer;           // main output buffer
    private static final int PARTITION = 0; // partition offset in acct
    private static final int KEYSTART = 1;  // key offset in acct
    private static final int VALSTART = 2;  // val offset in acct
    private static final int ACCTSIZE = 3;  // total #fields in acct

    // spill accounting
    private volatile int numSpills = 0;
    private volatile IOException sortSpillException = null;
    private final int softRecordLimit;
    private final int softBufferLimit;
    private final Object spillLock = new Object();
    private final QuickSort sorter = new QuickSort();
    private final BlockingBuffer bb = new BlockingBuffer();

    private final FileSystem localFs;

    private final Counters.Counter mapOutputByteCounter;
    private final Counters.Counter mapOutputRecordCounter;
    private final Counters.Counter combineInputCounter;
    private final Counters.Counter combineOutputCounter;

    @SuppressWarnings("unchecked")
    public MapOutputBuffer(TaskUmbilicalProtocol umbilical, JobConf job,
                           Reporter reporter) throws IOException {
      this.job = job;
      this.reporter = reporter;
      localFs = FileSystem.getLocal(job);
      partitions = job.getNumReduceTasks();
      partitioner = (Partitioner)
        ReflectionUtils.newInstance(job.getPartitionerClass(), job);
      // sanity checks
      final float spillper = job.getFloat("io.sort.spill.percent",(float)0.8);
      final float recper = job.getFloat("io.sort.record.percent",(float)0.05);
      final int sortmb = job.getInt("io.sort.mb", 100);
      if (spillper > (float)1.0 || spillper < (float)0.0) {
        throw new IOException("Invalid \"io.sort.spill.percent\": " + spillper);
      }
      if (recper > (float)1.0 || recper < (float)0.01) {
        throw new IOException("Invalid \"io.sort.record.percent\": " + recper);
      }
      if ((sortmb & 0x7FF) != sortmb) {
        throw new IOException("Invalid \"io.sort.mb\": " + sortmb);
      }
      // buffers and accounting
      int maxMemUsage = sortmb << 20;
      int recordCapacity = (int)(maxMemUsage * recper);
      recordCapacity += (recordCapacity >>> 2) % 4;
      kvbuffer = new byte[maxMemUsage - recordCapacity];
      bufvoid = kvbuffer.length;
      int kvcapacity = recordCapacity >>> 2;
      kvoffsets = new int[kvcapacity];
      kvindices = new int[recordCapacity - kvcapacity];
      softBufferLimit = (int)(kvbuffer.length * spillper);
      softRecordLimit = (int)(kvoffsets.length * spillper);
      // k/v serialization
      comparator = job.getOutputKeyComparator();
      keyClass = job.getMapOutputKeyClass();
      valClass = job.getMapOutputValueClass();
      serializationFactory = new SerializationFactory(job);
      keySerializer = serializationFactory.getSerializer(keyClass);
      keySerializer.open(bb);
      valSerializer = serializationFactory.getSerializer(valClass);
      valSerializer.open(bb);
      // counters
      Counters counters = getCounters();
      mapOutputByteCounter = counters.findCounter(MAP_OUTPUT_BYTES);
      mapOutputRecordCounter = counters.findCounter(MAP_OUTPUT_RECORDS);
      combineInputCounter = getCounters().findCounter(COMBINE_INPUT_RECORDS);
      combineOutputCounter = counters.findCounter(COMBINE_OUTPUT_RECORDS);
      // combiner and compression
      compressMapOutput = job.getCompressMapOutput();
      combinerClass = job.getCombinerClass();
      if (compressMapOutput) {
        compressionType = job.getMapOutputCompressionType();
        Class<? extends CompressionCodec> codecClass =
          job.getMapOutputCompressorClass(DefaultCodec.class);
        codec = (CompressionCodec)
          ReflectionUtils.newInstance(codecClass, job);
        if (CompressionType.RECORD == compressionType
            && null == combinerClass) {
          compressor = codec.createCompressor();
          rawBuffer = new DataOutputBuffer();
          deflateFilter = codec.createOutputStream(rawBuffer, compressor);
          deflateStream = new DataOutputStream(deflateFilter);
          valSerializer.close();
          valSerializer.open(deflateStream);
        } else {
          rawBuffer = null;
          compressor = null;
          deflateStream = null;
          deflateFilter = null;
        }
      } else {
        compressionType = CompressionType.NONE;
        codec = null;
        rawBuffer = null;
        compressor = null;
        deflateStream = null;
        deflateFilter = null;
      }
      combineCollector = (null != combinerClass)
        ? new CombineOutputCollector()
        : null;
    }

    @SuppressWarnings("unchecked")
    public synchronized void collect(Object key, Object value)
        throws IOException {
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
      if (sortSpillException != null) {
        throw sortSpillException;
      }
      try {
        int keystart = bufindex;
        keySerializer.serialize(key);
        if (bufindex < keystart) {
          // wrapped the key; reset required
          bb.reset();
          keystart = 0;
        }
        int valstart = bufindex;
        if (compressMapOutput && CompressionType.RECORD == compressionType
            && null == combinerClass) {
          // compress serialized value bytes
          rawBuffer.reset();
          deflateFilter.resetState();
          valSerializer.serialize(value);
          deflateStream.flush();
          deflateFilter.finish();
          bb.write(rawBuffer.getData(), 0, rawBuffer.getLength());
          bb.markRecord();
          mapOutputByteCounter.increment((valstart - keystart) +
              compressor.getBytesRead());
        } else {
          // serialize value bytes into buffer
          valSerializer.serialize(value);
          int valend = bb.markRecord();
          mapOutputByteCounter.increment(valend > keystart
              ? valend - keystart
              : (bufvoid - keystart) + valend);
        }
        int partition = partitioner.getPartition(key, value, partitions);

        mapOutputRecordCounter.increment(1);

        // update accounting info
        int ind = kvindex * ACCTSIZE;
        kvoffsets[kvindex] = ind;
        kvindices[ind + PARTITION] = partition;
        kvindices[ind + KEYSTART] = keystart;
        kvindices[ind + VALSTART] = valstart;
        kvindex = (kvindex + 1) % kvoffsets.length;
      } catch (MapBufferTooSmallException e) {
        LOG.debug("Record too large for in-memory buffer: " + e.getMessage());
        spillSingleRecord(key, value);
        mapOutputRecordCounter.increment(1);
        return;
      }

    }

    /**
     * Compare logical range, st i, j MOD offset capacity.
     * Compare by partition, then by key.
     * @see IndexedSortable#compare
     */
    public int compare(int i, int j) {
      final int ii = kvoffsets[i % kvoffsets.length];
      final int ij = kvoffsets[j % kvoffsets.length];
      // sort by partition
      if (kvindices[ii + PARTITION] != kvindices[ij + PARTITION]) {
        return kvindices[ii + PARTITION] - kvindices[ij + PARTITION];
      }
      // sort by key
      return comparator.compare(kvbuffer,
          kvindices[ii + KEYSTART],
          kvindices[ii + VALSTART] - kvindices[ii + KEYSTART],
          kvbuffer,
          kvindices[ij + KEYSTART],
          kvindices[ij + VALSTART] - kvindices[ij + KEYSTART]);
    }

    /**
     * Swap logical indices st i, j MOD offset capacity.
     * @see IndexedSortable#swap
     */
    public void swap(int i, int j) {
      i %= kvoffsets.length;
      j %= kvoffsets.length;
      int tmp = kvoffsets[i];
      kvoffsets[i] = kvoffsets[j];
      kvoffsets[j] = tmp;
    }

    /**
     * Inner class managing the spill of serialized records to disk.
     */
    protected class BlockingBuffer extends DataOutputStream {

      public BlockingBuffer() {
        this(new Buffer());
      }

      private BlockingBuffer(OutputStream out) {
        super(out);
      }

      /**
       * Mark end of record. Note that this is required if the buffer is to
       * cut the spill in the proper place.
       */
      public int markRecord() {
        bufmark = bufindex;
        return bufindex;
      }

      /**
       * Set position from last mark to end of writable buffer, then rewrite
       * the data between last mark and kvindex.
       * This handles a special case where the key wraps around the buffer.
       * If the key is to be passed to a RawComparator, then it must be
       * contiguous in the buffer. This recopies the data in the buffer back
       * into itself, but starting at the beginning of the buffer. Note that
       * reset() should <b>only</b> be called immediately after detecting
       * this condition. To call it at any other time is undefined and would
       * likely result in data loss or corruption.
       * @see #markRecord()
       */
      protected synchronized void reset() throws IOException {
        // spillLock unnecessary; If spill wraps, then
        // bufindex < bufstart < bufend so contention is impossible
        // a stale value for bufstart does not affect correctness, since
        // we can only get false negatives that force the more
        // conservative path
        int headbytelen = bufvoid - bufmark;
        bufvoid = bufmark;
        if (bufindex + headbytelen < bufstart) {
          System.arraycopy(kvbuffer, 0, kvbuffer, headbytelen, bufindex);
          System.arraycopy(kvbuffer, bufvoid, kvbuffer, 0, headbytelen);
          bufindex += headbytelen;
        } else {
          byte[] keytmp = new byte[bufindex];
          System.arraycopy(kvbuffer, 0, keytmp, 0, bufindex);
          bufindex = 0;
          out.write(kvbuffer, bufmark, headbytelen);
          out.write(keytmp);
        }
      }
    }

    public class Buffer extends OutputStream {
      private final byte[] scratch = new byte[1];

      public synchronized void write(int v)
          throws IOException {
        scratch[0] = (byte)v;
        write(scratch, 0, 1);
      }

      /**
       * Attempt to write a sequence of bytes to the collection buffer.
       * This method will block if the spill thread is running and it
       * cannot write.
       * @throws MapBufferTooSmallException if record is too large to
       *    deserialize into the collection buffer.
       */
      public synchronized void write(byte b[], int off, int len)
          throws IOException {
        boolean kvfull = false;
        boolean buffull = false;
        boolean wrap = false;
        synchronized(spillLock) {
          do {
            if (sortSpillException != null) {
              throw (IOException)new IOException().initCause(
                  sortSpillException);
            }

            // sufficient accounting space?
            kvfull = (kvindex + 1) % kvoffsets.length == kvstart;
            // sufficient buffer space?
            if (bufstart <= bufend && bufend <= bufindex) {
              buffull = bufindex + len > bufvoid;
              wrap = (bufvoid - bufindex) + bufstart > len;
            } else {
              // bufindex <= bufstart <= bufend
              // bufend <= bufindex <= bufstart
              wrap = false;
              buffull = bufindex + len > bufstart;
            }

            if (kvstart == kvend) {
              // spill thread not running
              if (kvend != kvindex) {
                // we have records we can spill
                final boolean kvsoftlimit = (kvindex > kvend)
                  ? kvindex - kvend > softRecordLimit
                  : kvend - kvindex < kvoffsets.length - softRecordLimit;
                final boolean bufsoftlimit = (bufindex > bufend)
                  ? bufindex - bufend > softBufferLimit
                  : bufend - bufindex < bufvoid - softBufferLimit;
                if (kvsoftlimit || bufsoftlimit || (buffull && !wrap)) {
                  kvend = kvindex;
                  bufend = bufmark;
                  // TODO No need to recreate this thread every time
                  SpillThread t = new SpillThread();
                  t.setDaemon(true);
                  t.setName("SpillThread");
                  t.start();
                }
              } else if (buffull && !wrap) {
                // We have no buffered records, and this record is too large
                // to write into kvbuffer. We must spill it directly from
                // collect
                final int size = ((bufend <= bufindex)
                  ? bufindex - bufend
                  : (bufvoid - bufend) + bufindex) + len;
                bufstart = bufend = bufindex = bufmark = 0;
                kvstart = kvend = kvindex = 0;
                bufvoid = kvbuffer.length;
                throw new MapBufferTooSmallException(size + " bytes");
              }
            }

            if (kvfull || (buffull && !wrap)) {
              while (kvstart != kvend) {
                reporter.progress();
                try {
                  spillLock.wait();
                } catch (InterruptedException e) {
                  throw (IOException)new IOException(
                      "Buffer interrupted while waiting for the writer"
                      ).initCause(e);
                }
              }
            }
          } while (kvfull || (buffull && !wrap));
        }
        // here, we know that we have sufficient space to write
        if (buffull) {
          final int gaplen = bufvoid - bufindex;
          System.arraycopy(b, off, kvbuffer, bufindex, gaplen);
          len -= gaplen;
          off += gaplen;
          bufindex = 0;
        }
        System.arraycopy(b, off, kvbuffer, bufindex, len);
        bufindex += len;
      }
    }

    public synchronized void flush() throws IOException {
      synchronized (spillLock) {
        while (kvstart != kvend) {
          try {
            reporter.progress();
            spillLock.wait();
          } catch (InterruptedException e) {
            throw (IOException)new IOException(
                "Buffer interrupted while waiting for the writer"
                ).initCause(e);
          }
        }
      }
      if (sortSpillException != null) {
        throw (IOException)new IOException().initCause(sortSpillException);
      }
      if (kvend != kvindex) {
        kvend = kvindex;
        bufend = bufmark;
        sortAndSpill();
      }
      // release sort buffer before the merge
      kvbuffer = null;
      mergeParts();
    }

    public void close() { }

    protected class SpillThread extends Thread {

      public void run() {
        try {
          sortAndSpill();
        } catch (IOException e) {
          sortSpillException = e;
        } finally {
          synchronized(spillLock) {
            if (bufend < bufindex && bufindex < bufstart) {
              bufvoid = kvbuffer.length;
            }
            kvstart = kvend;
            bufstart = bufend;
            spillLock.notify();
          }
        }
      }
    }

    private void sortAndSpill() throws IOException {
      //approximate the length of the output file to be the length of the
      //buffer + header lengths for the partitions
      long size = (bufend > bufstart
          ? bufend - bufstart
          : (bufvoid - bufend) + bufstart) +
                  partitions * APPROX_HEADER_LENGTH;
      FSDataOutputStream out = null;
      FSDataOutputStream indexOut = null;
      try {
        // create spill file
        Path filename = mapOutputFile.getSpillFileForWrite(getTaskId(),
                                      numSpills, size);
        out = localFs.create(filename);
        // create spill index
        Path indexFilename = mapOutputFile.getSpillIndexFileForWrite(
                             getTaskId(), numSpills, partitions * 16);
        indexOut = localFs.create(indexFilename);
        final int endPosition = (kvend > kvstart)
          ? kvend
          : kvoffsets.length + kvend;
        sorter.sort(MapOutputBuffer.this, kvstart, endPosition, reporter);
        int spindex = kvstart;
        InMemValBytes vbytes = new InMemValBytes();
        for (int i = 0; i < partitions; ++i) {
          SequenceFile.Writer writer = null;
          try {
            long segmentStart = out.getPos();
            writer = SequenceFile.createWriter(job, out,
                keyClass, valClass, compressionType, codec);
            if (null == combinerClass) {
              // spill directly
              while (spindex < endPosition &&
                  kvindices[kvoffsets[spindex % kvoffsets.length]
                            + PARTITION] == i) {
                final int kvoff = kvoffsets[spindex % kvoffsets.length];
                getVBytesForOffset(kvoff, vbytes);
                writer.appendRaw(kvbuffer, kvindices[kvoff + KEYSTART],
                    kvindices[kvoff + VALSTART] - kvindices[kvoff + KEYSTART],
                    vbytes);
                ++spindex;
              }
            } else {
              int spstart = spindex;
              while (spindex < endPosition &&
                  kvindices[kvoffsets[spindex % kvoffsets.length]
                            + PARTITION] == i) {
                ++spindex;
              }
              // Note: we would like to avoid the combiner if we've fewer
              // than some threshold of records for a partition, but we left
              // our records uncompressed for the combiner. We accept the trip
              // through the combiner to effect the compression for now;
              // to remedy this would require us to observe the compression
              // strategy here as we do in collect
              if (spstart != spindex) {
                Reducer combiner =
                  (Reducer)ReflectionUtils.newInstance(combinerClass, job);
                combineCollector.setWriter(writer);
                combineAndSpill(spstart, spindex, combiner, combineCollector);
                // combineAndSpill closes combiner
              }
            }
            // we need to close the writer to flush buffered data, obtaining
            // the correct offset
            writer.close();
            writer = null;
            indexOut.writeLong(segmentStart);
            indexOut.writeLong(out.getPos() - segmentStart);
          } finally {
            if (null != writer) writer.close();
          }
        }
        ++numSpills;
      } finally {
        if (out != null) out.close();
        if (indexOut != null) indexOut.close();
      }
    }

    /**
     * Handles the degenerate case where serialization fails to fit in
     * the in-memory buffer, so we must spill the record from collect
     * directly to a spill file. Consider this "losing".
     */
    @SuppressWarnings("unchecked")
    private void spillSingleRecord(final Object key, final Object value) 
        throws IOException {
      long size = kvbuffer.length + partitions * APPROX_HEADER_LENGTH;
      FSDataOutputStream out = null;
      FSDataOutputStream indexOut = null;
      final int partition = partitioner.getPartition(key, value, partitions);
      try {
        // create spill file
        Path filename = mapOutputFile.getSpillFileForWrite(getTaskId(),
                                      numSpills, size);
        out = localFs.create(filename);
        // create spill index
        Path indexFilename = mapOutputFile.getSpillIndexFileForWrite(
                             getTaskId(), numSpills, partitions * 16);
        indexOut = localFs.create(indexFilename);
        // we don't run the combiner for a single record
        for (int i = 0; i < partitions; ++i) {
          SequenceFile.Writer writer = null;
          try {
            long segmentStart = out.getPos();
            writer = SequenceFile.createWriter(job, out,
                keyClass, valClass, compressionType, codec);
            if (i == partition) {
              final long recordStart = out.getPos();
              writer.append(key, value);
              // Note that our map byte count will not be accurate with
              // compression
              mapOutputByteCounter.increment(out.getPos() - recordStart);
            }
            writer.close();
            indexOut.writeLong(segmentStart);
            indexOut.writeLong(out.getPos() - segmentStart);
          } catch (IOException e) {
            if (null != writer) writer.close();
            throw e;
          }
        }
        ++numSpills;
      } finally {
        if (out != null) out.close();
        if (indexOut != null) indexOut.close();
      }
    }

    /**
     * Given an offset, populate vbytes with the associated set of
     * deserialized value bytes. Should only be called during a spill.
     */
    private void getVBytesForOffset(int kvoff, InMemValBytes vbytes) {
      final int nextindex = kvoff / ACCTSIZE == kvend - 1
        ? bufend
        : kvindices[kvoff + ACCTSIZE + KEYSTART];
      int vallen = (nextindex > kvindices[kvoff + VALSTART])
        ? nextindex - kvindices[kvoff + VALSTART]
        : (bufvoid - kvindices[kvoff + VALSTART]) + nextindex;
      vbytes.reset(kvindices[kvoff + VALSTART], vallen);
    }

    @SuppressWarnings("unchecked")
    private void combineAndSpill(int start, int end, Reducer combiner,
        OutputCollector combineCollector) throws IOException {
      try {
        CombineValuesIterator values = new CombineValuesIterator(
            new MRResultIterator(start, end), comparator, keyClass, valClass,
            job, reporter);
        while (values.more()) {
          combiner.reduce(values.getKey(), values, combineCollector, reporter);
          values.nextKey();
          combineOutputCounter.increment(1);
          // indicate we're making progress
          reporter.progress();
        }
      } finally {
        combiner.close();
      }
    }

    /**
     * Inner class wrapping valuebytes, used for appendRaw.
     */
    protected class InMemValBytes implements ValueBytes {
      private int start;
      private int len;
      public void reset(int start, int len) {
        this.start = start;
        this.len = len;
      }
      public int getSize() {
        return len;
      }
      public void writeUncompressedBytes(DataOutputStream outStream)
          throws IOException {
        if (start + len > bufvoid) {
          final int taillen = bufvoid - start;
          outStream.write(kvbuffer, start, taillen);
          outStream.write(kvbuffer, 0, len - taillen);
          return;
        }
        outStream.write(kvbuffer, start, len);
      }
      public void writeCompressedBytes(DataOutputStream outStream)
          throws IOException {
        // If writing record-compressed data, kvbuffer vals rec-compressed
        // and may be written directly. Note: not contiguous
        writeUncompressedBytes(outStream);
      }
    }

    protected class MRResultIterator implements RawKeyValueIterator {
      private final DataOutputBuffer keybuf = new DataOutputBuffer();
      private final InMemValBytes vbytes = new InMemValBytes();
      private final int end;
      private int current;
      public MRResultIterator(int start, int end) {
        this.end = end;
        current = start - 1;
      }
      public boolean next() throws IOException {
        return ++current < end;
      }
      public DataOutputBuffer getKey() throws IOException {
        final int kvoff = kvoffsets[current % kvoffsets.length];
        keybuf.reset();
        keybuf.write(kvbuffer, kvindices[kvoff + KEYSTART],
            kvindices[kvoff + VALSTART] - kvindices[kvoff + KEYSTART]);
        return keybuf;
      }
      public ValueBytes getValue() throws IOException {
        getVBytesForOffset(kvoffsets[current % kvoffsets.length], vbytes);
        return vbytes;
      }
      public Progress getProgress() {
        return null;
      }
      public void close() { }
    }

    private class CombineValuesIterator<KEY,VALUE>
        extends ValuesIterator<KEY,VALUE> {

      public CombineValuesIterator(SequenceFile.Sorter.RawKeyValueIterator in,
          RawComparator<KEY> comparator, Class<KEY> keyClass,
          Class<VALUE> valClass, Configuration conf, Reporter reporter)
          throws IOException {
        super(in, comparator, keyClass, valClass, conf, reporter);
      }

      public VALUE next() {
        combineInputCounter.increment(1);
        return super.next();
      }
    }

    private void mergeParts() throws IOException {
      // get the approximate size of the final output/index files
      long finalOutFileSize = 0;
      long finalIndexFileSize = 0;
      Path [] filename = new Path[numSpills];
      Path [] indexFileName = new Path[numSpills];
      FileSystem localFs = FileSystem.getLocal(job);
      
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
        Sorter sorter = new Sorter(localFs, job.getOutputKeyComparator(),
                                   keyClass, valClass, job);
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
          localFs.delete(filename[i], true);
          localFs.delete(indexFileName[i], true);
        }
      }
    }

  }

  /**
   * OutputCollector for the combiner.
   */
  private static class CombineOutputCollector implements OutputCollector {
    private SequenceFile.Writer writer;
    public synchronized void setWriter(SequenceFile.Writer writer) {
      this.writer = writer;
    }
    public synchronized void collect(Object key, Object value)
        throws IOException {
        writer.append(key, value);
    }
  }

  /**
   * Exception indicating that the allocated sort buffer is insufficient
   * to hold the current record.
   */
  @SuppressWarnings("serial")
  private static class MapBufferTooSmallException extends IOException {
    public MapBufferTooSmallException(String s) {
      super(s);
    }
  }

}
