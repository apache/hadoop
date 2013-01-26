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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.IFile.Reader;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.util.PriorityQueue;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;

/**
 * Merger is an utility class used by the Map and Reduce tasks for merging
 * both their memory and disk segments
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Merger {  
  private static final Log LOG = LogFactory.getLog(Merger.class);

  // Local directories
  private static LocalDirAllocator lDirAlloc = 
    new LocalDirAllocator(MRConfig.LOCAL_DIR);

  public static <K extends Object, V extends Object>
  RawKeyValueIterator merge(Configuration conf, FileSystem fs,
                            Class<K> keyClass, Class<V> valueClass, 
                            CompressionCodec codec,
                            Path[] inputs, boolean deleteInputs, 
                            int mergeFactor, Path tmpDir,
                            RawComparator<K> comparator, Progressable reporter,
                            Counters.Counter readsCounter,
                            Counters.Counter writesCounter,
                            Progress mergePhase)
  throws IOException {
    return 
      new MergeQueue<K, V>(conf, fs, inputs, deleteInputs, codec, comparator, 
                           reporter, null).merge(keyClass, valueClass,
                                           mergeFactor, tmpDir,
                                           readsCounter, writesCounter, 
                                           mergePhase);
  }

  public static <K extends Object, V extends Object>
  RawKeyValueIterator merge(Configuration conf, FileSystem fs,
                            Class<K> keyClass, Class<V> valueClass, 
                            CompressionCodec codec,
                            Path[] inputs, boolean deleteInputs, 
                            int mergeFactor, Path tmpDir,
                            RawComparator<K> comparator,
                            Progressable reporter,
                            Counters.Counter readsCounter,
                            Counters.Counter writesCounter,
                            Counters.Counter mergedMapOutputsCounter,
                            Progress mergePhase)
  throws IOException {
    return 
      new MergeQueue<K, V>(conf, fs, inputs, deleteInputs, codec, comparator, 
                           reporter, mergedMapOutputsCounter).merge(
                                           keyClass, valueClass,
                                           mergeFactor, tmpDir,
                                           readsCounter, writesCounter,
                                           mergePhase);
  }
  
  public static <K extends Object, V extends Object>
  RawKeyValueIterator merge(Configuration conf, FileSystem fs, 
                            Class<K> keyClass, Class<V> valueClass, 
                            List<Segment<K, V>> segments, 
                            int mergeFactor, Path tmpDir,
                            RawComparator<K> comparator, Progressable reporter,
                            Counters.Counter readsCounter,
                            Counters.Counter writesCounter,
                            Progress mergePhase)
      throws IOException {
    return merge(conf, fs, keyClass, valueClass, segments, mergeFactor, tmpDir,
                 comparator, reporter, false, readsCounter, writesCounter,
                 mergePhase);
  }

  public static <K extends Object, V extends Object>
  RawKeyValueIterator merge(Configuration conf, FileSystem fs,
                            Class<K> keyClass, Class<V> valueClass,
                            List<Segment<K, V>> segments,
                            int mergeFactor, Path tmpDir,
                            RawComparator<K> comparator, Progressable reporter,
                            boolean sortSegments,
                            Counters.Counter readsCounter,
                            Counters.Counter writesCounter,
                            Progress mergePhase)
      throws IOException {
    return new MergeQueue<K, V>(conf, fs, segments, comparator, reporter,
                           sortSegments).merge(keyClass, valueClass,
                                               mergeFactor, tmpDir,
                                               readsCounter, writesCounter,
                                               mergePhase);
  }

  public static <K extends Object, V extends Object>
  RawKeyValueIterator merge(Configuration conf, FileSystem fs,
                            Class<K> keyClass, Class<V> valueClass,
                            CompressionCodec codec,
                            List<Segment<K, V>> segments,
                            int mergeFactor, Path tmpDir,
                            RawComparator<K> comparator, Progressable reporter,
                            boolean sortSegments,
                            Counters.Counter readsCounter,
                            Counters.Counter writesCounter,
                            Progress mergePhase)
      throws IOException {
    return new MergeQueue<K, V>(conf, fs, segments, comparator, reporter,
                           sortSegments, codec).merge(keyClass, valueClass,
                                               mergeFactor, tmpDir,
                                               readsCounter, writesCounter,
                                               mergePhase);
  }

  public static <K extends Object, V extends Object>
    RawKeyValueIterator merge(Configuration conf, FileSystem fs,
                            Class<K> keyClass, Class<V> valueClass,
                            List<Segment<K, V>> segments,
                            int mergeFactor, int inMemSegments, Path tmpDir,
                            RawComparator<K> comparator, Progressable reporter,
                            boolean sortSegments,
                            Counters.Counter readsCounter,
                            Counters.Counter writesCounter,
                            Progress mergePhase)
      throws IOException {
    return new MergeQueue<K, V>(conf, fs, segments, comparator, reporter,
                           sortSegments).merge(keyClass, valueClass,
                                               mergeFactor, inMemSegments,
                                               tmpDir,
                                               readsCounter, writesCounter,
                                               mergePhase);
  }


  static <K extends Object, V extends Object>
  RawKeyValueIterator merge(Configuration conf, FileSystem fs,
                          Class<K> keyClass, Class<V> valueClass,
                          CompressionCodec codec,
                          List<Segment<K, V>> segments,
                          int mergeFactor, int inMemSegments, Path tmpDir,
                          RawComparator<K> comparator, Progressable reporter,
                          boolean sortSegments,
                          Counters.Counter readsCounter,
                          Counters.Counter writesCounter,
                          Progress mergePhase)
    throws IOException {
  return new MergeQueue<K, V>(conf, fs, segments, comparator, reporter,
                         sortSegments, codec).merge(keyClass, valueClass,
                                             mergeFactor, inMemSegments,
                                             tmpDir,
                                             readsCounter, writesCounter,
                                             mergePhase);
}

  public static <K extends Object, V extends Object>
  void writeFile(RawKeyValueIterator records, Writer<K, V> writer, 
                 Progressable progressable, Configuration conf) 
  throws IOException {
    long progressBar = conf.getLong(JobContext.RECORDS_BEFORE_PROGRESS,
        10000);
    long recordCtr = 0;
    while(records.next()) {
      writer.append(records.getKey(), records.getValue());
      
      if (((recordCtr++) % progressBar) == 0) {
        progressable.progress();
      }
    }
}

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static class Segment<K extends Object, V extends Object> {
    Reader<K, V> reader = null;
    final DataInputBuffer key = new DataInputBuffer();
    
    Configuration conf = null;
    FileSystem fs = null;
    Path file = null;
    boolean preserve = false;
    CompressionCodec codec = null;
    long segmentOffset = 0;
    long segmentLength = -1;
    long rawDataLength = -1;
    
    Counters.Counter mapOutputsCounter = null;

    public Segment(Configuration conf, FileSystem fs, Path file,
                   CompressionCodec codec, boolean preserve)
    throws IOException {
      this(conf, fs, file, codec, preserve, null);
    }

    public Segment(Configuration conf, FileSystem fs, Path file,
                   CompressionCodec codec, boolean preserve,
                   Counters.Counter mergedMapOutputsCounter)
  throws IOException {
      this(conf, fs, file, 0, fs.getFileStatus(file).getLen(), codec, preserve, 
           mergedMapOutputsCounter);
    }
    
    public Segment(Configuration conf, FileSystem fs, Path file,
        CompressionCodec codec, boolean preserve,
        Counters.Counter mergedMapOutputsCounter, long rawDataLength)
            throws IOException {
      this(conf, fs, file, 0, fs.getFileStatus(file).getLen(), codec, preserve, 
          mergedMapOutputsCounter);
      this.rawDataLength = rawDataLength;
    }

    public Segment(Configuration conf, FileSystem fs, Path file,
                   long segmentOffset, long segmentLength,
                   CompressionCodec codec,
                   boolean preserve) throws IOException {
      this(conf, fs, file, segmentOffset, segmentLength, codec, preserve, null);
    }

    public Segment(Configuration conf, FileSystem fs, Path file,
        long segmentOffset, long segmentLength, CompressionCodec codec,
        boolean preserve, Counters.Counter mergedMapOutputsCounter)
    throws IOException {
      this.conf = conf;
      this.fs = fs;
      this.file = file;
      this.codec = codec;
      this.preserve = preserve;

      this.segmentOffset = segmentOffset;
      this.segmentLength = segmentLength;
      
      this.mapOutputsCounter = mergedMapOutputsCounter;
    }
    
    public Segment(Reader<K, V> reader, boolean preserve) {
      this(reader, preserve, null);
    }

    public Segment(Reader<K, V> reader, boolean preserve, long rawDataLength) {
      this(reader, preserve, null);
      this.rawDataLength = rawDataLength;
    }
    
    public Segment(Reader<K, V> reader, boolean preserve, 
                   Counters.Counter mapOutputsCounter) {
      this.reader = reader;
      this.preserve = preserve;
      
      this.segmentLength = reader.getLength();
      
      this.mapOutputsCounter = mapOutputsCounter;
    }

    void init(Counters.Counter readsCounter) throws IOException {
      if (reader == null) {
        FSDataInputStream in = fs.open(file);
        in.seek(segmentOffset);
        reader = new Reader<K, V>(conf, in, segmentLength, codec, readsCounter);
      }
      
      if (mapOutputsCounter != null) {
        mapOutputsCounter.increment(1);
      }
    }
    
    boolean inMemory() {
      return fs == null;
    }
    
    DataInputBuffer getKey() { return key; }

    DataInputBuffer getValue(DataInputBuffer value) throws IOException {
      nextRawValue(value);
      return value;
    }

    public long getLength() { 
      return (reader == null) ?
        segmentLength : reader.getLength();
    }
    
    public long getRawDataLength() {
      return (rawDataLength > 0) ? rawDataLength : getLength();
    }

    boolean nextRawKey() throws IOException {
      return reader.nextRawKey(key);
    }

    void nextRawValue(DataInputBuffer value) throws IOException {
      reader.nextRawValue(value);
    }

    void closeReader() throws IOException {
      if (reader != null) {
        reader.close();
        reader = null;
      }
    }
    
    void close() throws IOException {
      closeReader();
      if (!preserve && fs != null) {
        fs.delete(file, false);
      }
    }

    public long getPosition() throws IOException {
      return reader.getPosition();
    }

    // This method is used by BackupStore to extract the 
    // absolute position after a reset
    long getActualPosition() throws IOException {
      return segmentOffset + reader.getPosition();
    }

    Reader<K,V> getReader() {
      return reader;
    }
    
    // This method is used by BackupStore to reinitialize the
    // reader to start reading from a different segment offset
    void reinitReader(int offset) throws IOException {
      if (!inMemory()) {
        closeReader();
        segmentOffset = offset;
        segmentLength = fs.getFileStatus(file).getLen() - segmentOffset;
        init(null);
      }
    }
  }
  
  // Boolean variable for including/considering final merge as part of sort
  // phase or not. This is true in map task, false in reduce task. It is
  // used in calculating mergeProgress.
  static boolean includeFinalMerge = false;
  
  /**
   * Sets the boolean variable includeFinalMerge to true. Called from
   * map task before calling merge() so that final merge of map task
   * is also considered as part of sort phase.
   */
  static void considerFinalMergeForProgress() {
    includeFinalMerge = true;
  }
  
  private static class MergeQueue<K extends Object, V extends Object> 
  extends PriorityQueue<Segment<K, V>> implements RawKeyValueIterator {
    Configuration conf;
    FileSystem fs;
    CompressionCodec codec;
    
    List<Segment<K, V>> segments = new ArrayList<Segment<K,V>>();
    
    RawComparator<K> comparator;
    
    private long totalBytesProcessed;
    private float progPerByte;
    private Progress mergeProgress = new Progress();
    
    Progressable reporter;
    
    DataInputBuffer key;
    final DataInputBuffer value = new DataInputBuffer();
    final DataInputBuffer diskIFileValue = new DataInputBuffer();
    
    Segment<K, V> minSegment;
    Comparator<Segment<K, V>> segmentComparator =   
      new Comparator<Segment<K, V>>() {
      public int compare(Segment<K, V> o1, Segment<K, V> o2) {
        if (o1.getLength() == o2.getLength()) {
          return 0;
        }

        return o1.getLength() < o2.getLength() ? -1 : 1;
      }
    };

    
    public MergeQueue(Configuration conf, FileSystem fs, 
                      Path[] inputs, boolean deleteInputs, 
                      CompressionCodec codec, RawComparator<K> comparator,
                      Progressable reporter) 
    throws IOException {
      this(conf, fs, inputs, deleteInputs, codec, comparator, reporter, null);
    }
    
    public MergeQueue(Configuration conf, FileSystem fs, 
                      Path[] inputs, boolean deleteInputs, 
                      CompressionCodec codec, RawComparator<K> comparator,
                      Progressable reporter, 
                      Counters.Counter mergedMapOutputsCounter) 
    throws IOException {
      this.conf = conf;
      this.fs = fs;
      this.codec = codec;
      this.comparator = comparator;
      this.reporter = reporter;
      
      for (Path file : inputs) {
        LOG.debug("MergeQ: adding: " + file);
        segments.add(new Segment<K, V>(conf, fs, file, codec, !deleteInputs, 
                                       (file.toString().endsWith(
                                           Task.MERGED_OUTPUT_PREFIX) ? 
                                        null : mergedMapOutputsCounter)));
      }
      
      // Sort segments on file-lengths
      Collections.sort(segments, segmentComparator); 
    }
    
    public MergeQueue(Configuration conf, FileSystem fs,
        List<Segment<K, V>> segments, RawComparator<K> comparator,
        Progressable reporter) {
      this(conf, fs, segments, comparator, reporter, false);
    }

    public MergeQueue(Configuration conf, FileSystem fs, 
        List<Segment<K, V>> segments, RawComparator<K> comparator,
        Progressable reporter, boolean sortSegments) {
      this.conf = conf;
      this.fs = fs;
      this.comparator = comparator;
      this.segments = segments;
      this.reporter = reporter;
      if (sortSegments) {
        Collections.sort(segments, segmentComparator);
      }
    }

    public MergeQueue(Configuration conf, FileSystem fs,
        List<Segment<K, V>> segments, RawComparator<K> comparator,
        Progressable reporter, boolean sortSegments, CompressionCodec codec) {
      this(conf, fs, segments, comparator, reporter, sortSegments);
      this.codec = codec;
    }

    public void close() throws IOException {
      Segment<K, V> segment;
      while((segment = pop()) != null) {
        segment.close();
      }
    }

    public DataInputBuffer getKey() throws IOException {
      return key;
    }

    public DataInputBuffer getValue() throws IOException {
      return value;
    }

    private void adjustPriorityQueue(Segment<K, V> reader) throws IOException{
      long startPos = reader.getPosition();
      boolean hasNext = reader.nextRawKey();
      long endPos = reader.getPosition();
      totalBytesProcessed += endPos - startPos;
      mergeProgress.set(totalBytesProcessed * progPerByte);
      if (hasNext) {
        adjustTop();
      } else {
        pop();
        reader.close();
      }
    }

    public boolean next() throws IOException {
      if (size() == 0)
        return false;

      if (minSegment != null) {
        //minSegment is non-null for all invocations of next except the first
        //one. For the first invocation, the priority queue is ready for use
        //but for the subsequent invocations, first adjust the queue 
        adjustPriorityQueue(minSegment);
        if (size() == 0) {
          minSegment = null;
          return false;
        }
      }
      minSegment = top();
      if (!minSegment.inMemory()) {
        //When we load the value from an inmemory segment, we reset
        //the "value" DIB in this class to the inmem segment's byte[].
        //When we load the value bytes from disk, we shouldn't use
        //the same byte[] since it would corrupt the data in the inmem
        //segment. So we maintain an explicit DIB for value bytes
        //obtained from disk, and if the current segment is a disk
        //segment, we reset the "value" DIB to the byte[] in that (so 
        //we reuse the disk segment DIB whenever we consider
        //a disk segment).
        value.reset(diskIFileValue.getData(), diskIFileValue.getLength());
      }
      long startPos = minSegment.getPosition();
      key = minSegment.getKey();
      minSegment.getValue(value);
      long endPos = minSegment.getPosition();
      totalBytesProcessed += endPos - startPos;
      mergeProgress.set(totalBytesProcessed * progPerByte);
      return true;
    }

    @SuppressWarnings("unchecked")
    protected boolean lessThan(Object a, Object b) {
      DataInputBuffer key1 = ((Segment<K, V>)a).getKey();
      DataInputBuffer key2 = ((Segment<K, V>)b).getKey();
      int s1 = key1.getPosition();
      int l1 = key1.getLength() - s1;
      int s2 = key2.getPosition();
      int l2 = key2.getLength() - s2;

      return comparator.compare(key1.getData(), s1, l1, key2.getData(), s2, l2) < 0;
    }
    
    public RawKeyValueIterator merge(Class<K> keyClass, Class<V> valueClass,
                                     int factor, Path tmpDir,
                                     Counters.Counter readsCounter,
                                     Counters.Counter writesCounter,
                                     Progress mergePhase)
        throws IOException {
      return merge(keyClass, valueClass, factor, 0, tmpDir,
                   readsCounter, writesCounter, mergePhase);
    }

    RawKeyValueIterator merge(Class<K> keyClass, Class<V> valueClass,
                                     int factor, int inMem, Path tmpDir,
                                     Counters.Counter readsCounter,
                                     Counters.Counter writesCounter,
                                     Progress mergePhase)
        throws IOException {
      LOG.info("Merging " + segments.size() + " sorted segments");

      /*
       * If there are inMemory segments, then they come first in the segments
       * list and then the sorted disk segments. Otherwise(if there are only
       * disk segments), then they are sorted segments if there are more than
       * factor segments in the segments list.
       */
      int numSegments = segments.size();
      int origFactor = factor;
      int passNo = 1;
      if (mergePhase != null) {
        mergeProgress = mergePhase;
      }

      long totalBytes = computeBytesInMerges(factor, inMem);
      if (totalBytes != 0) {
        progPerByte = 1.0f / (float)totalBytes;
      }
      
      //create the MergeStreams from the sorted map created in the constructor
      //and dump the final output to a file
      do {
        //get the factor for this pass of merge. We assume in-memory segments
        //are the first entries in the segment list and that the pass factor
        //doesn't apply to them
        factor = getPassFactor(factor, passNo, numSegments - inMem);
        if (1 == passNo) {
          factor += inMem;
        }
        List<Segment<K, V>> segmentsToMerge =
          new ArrayList<Segment<K, V>>();
        int segmentsConsidered = 0;
        int numSegmentsToConsider = factor;
        long startBytes = 0; // starting bytes of segments of this merge
        while (true) {
          //extract the smallest 'factor' number of segments  
          //Call cleanup on the empty segments (no key/value data)
          List<Segment<K, V>> mStream = 
            getSegmentDescriptors(numSegmentsToConsider);
          for (Segment<K, V> segment : mStream) {
            // Initialize the segment at the last possible moment;
            // this helps in ensuring we don't use buffers until we need them
            segment.init(readsCounter);
            long startPos = segment.getPosition();
            boolean hasNext = segment.nextRawKey();
            long endPos = segment.getPosition();
            
            if (hasNext) {
              startBytes += endPos - startPos;
              segmentsToMerge.add(segment);
              segmentsConsidered++;
            }
            else {
              segment.close();
              numSegments--; //we ignore this segment for the merge
            }
          }
          //if we have the desired number of segments
          //or looked at all available segments, we break
          if (segmentsConsidered == factor || 
              segments.size() == 0) {
            break;
          }
            
          numSegmentsToConsider = factor - segmentsConsidered;
        }
        
        //feed the streams to the priority queue
        initialize(segmentsToMerge.size());
        clear();
        for (Segment<K, V> segment : segmentsToMerge) {
          put(segment);
        }
        
        //if we have lesser number of segments remaining, then just return the
        //iterator, else do another single level merge
        if (numSegments <= factor) {
          if (!includeFinalMerge) { // for reduce task

            // Reset totalBytesProcessed and recalculate totalBytes from the
            // remaining segments to track the progress of the final merge.
            // Final merge is considered as the progress of the reducePhase,
            // the 3rd phase of reduce task.
            totalBytesProcessed = 0;
            totalBytes = 0;
            for (int i = 0; i < segmentsToMerge.size(); i++) {
              totalBytes += segmentsToMerge.get(i).getRawDataLength();
            }
          }
          if (totalBytes != 0) //being paranoid
            progPerByte = 1.0f / (float)totalBytes;
          
          totalBytesProcessed += startBytes;         
          if (totalBytes != 0)
            mergeProgress.set(totalBytesProcessed * progPerByte);
          else
            mergeProgress.set(1.0f); // Last pass and no segments left - we're done
          
          LOG.info("Down to the last merge-pass, with " + numSegments + 
                   " segments left of total size: " +
                   (totalBytes - totalBytesProcessed) + " bytes");
          return this;
        } else {
          LOG.info("Merging " + segmentsToMerge.size() + 
                   " intermediate segments out of a total of " + 
                   (segments.size()+segmentsToMerge.size()));
          
          long bytesProcessedInPrevMerges = totalBytesProcessed;
          totalBytesProcessed += startBytes;

          //we want to spread the creation of temp files on multiple disks if 
          //available under the space constraints
          long approxOutputSize = 0; 
          for (Segment<K, V> s : segmentsToMerge) {
            approxOutputSize += s.getLength() + 
                                ChecksumFileSystem.getApproxChkSumLength(
                                s.getLength());
          }
          Path tmpFilename = 
            new Path(tmpDir, "intermediate").suffix("." + passNo);

          Path outputFile =  lDirAlloc.getLocalPathForWrite(
                                              tmpFilename.toString(),
                                              approxOutputSize, conf);

          Writer<K, V> writer = 
            new Writer<K, V>(conf, fs, outputFile, keyClass, valueClass, codec,
                             writesCounter);
          writeFile(this, writer, reporter, conf);
          writer.close();
          
          //we finished one single level merge; now clean up the priority 
          //queue
          this.close();

          // Add the newly create segment to the list of segments to be merged
          Segment<K, V> tempSegment = 
            new Segment<K, V>(conf, fs, outputFile, codec, false);

          // Insert new merged segment into the sorted list
          int pos = Collections.binarySearch(segments, tempSegment,
                                             segmentComparator);
          if (pos < 0) {
            // binary search failed. So position to be inserted at is -pos-1
            pos = -pos-1;
          }
          segments.add(pos, tempSegment);
          numSegments = segments.size();
          
          // Subtract the difference between expected size of new segment and 
          // actual size of new segment(Expected size of new segment is
          // inputBytesOfThisMerge) from totalBytes. Expected size and actual
          // size will match(almost) if combiner is not called in merge.
          long inputBytesOfThisMerge = totalBytesProcessed -
                                       bytesProcessedInPrevMerges;
          totalBytes -= inputBytesOfThisMerge - tempSegment.getRawDataLength();
          if (totalBytes != 0) {
            progPerByte = 1.0f / (float)totalBytes;
          }
          
          passNo++;
        }
        //we are worried about only the first pass merge factor. So reset the 
        //factor to what it originally was
        factor = origFactor;
      } while(true);
    }
    
    /**
     * Determine the number of segments to merge in a given pass. Assuming more
     * than factor segments, the first pass should attempt to bring the total
     * number of segments - 1 to be divisible by the factor - 1 (each pass
     * takes X segments and produces 1) to minimize the number of merges.
     */
    private int getPassFactor(int factor, int passNo, int numSegments) {
      if (passNo > 1 || numSegments <= factor || factor == 1) 
        return factor;
      int mod = (numSegments - 1) % (factor - 1);
      if (mod == 0)
        return factor;
      return mod + 1;
    }
    
    /** Return (& remove) the requested number of segment descriptors from the
     * sorted map.
     */
    private List<Segment<K, V>> getSegmentDescriptors(int numDescriptors) {
      if (numDescriptors > segments.size()) {
        List<Segment<K, V>> subList = new ArrayList<Segment<K,V>>(segments);
        segments.clear();
        return subList;
      }
      
      List<Segment<K, V>> subList = 
        new ArrayList<Segment<K,V>>(segments.subList(0, numDescriptors));
      for (int i=0; i < numDescriptors; ++i) {
        segments.remove(0);
      }
      return subList;
    }
    
    /**
     * Compute expected size of input bytes to merges, will be used in
     * calculating mergeProgress. This simulates the above merge() method and
     * tries to obtain the number of bytes that are going to be merged in all
     * merges(assuming that there is no combiner called while merging).
     * @param factor mapreduce.task.io.sort.factor
     * @param inMem  number of segments in memory to be merged
     */
    long computeBytesInMerges(int factor, int inMem) {
      int numSegments = segments.size();
      List<Long> segmentSizes = new ArrayList<Long>(numSegments);
      long totalBytes = 0;
      int n = numSegments - inMem;
      // factor for 1st pass
      int f = getPassFactor(factor, 1, n) + inMem;
      n = numSegments;
 
      for (int i = 0; i < numSegments; i++) {
        // Not handling empty segments here assuming that it would not affect
        // much in calculation of mergeProgress.
        segmentSizes.add(segments.get(i).getRawDataLength());
      }
      
      // If includeFinalMerge is true, allow the following while loop iterate
      // for 1 more iteration. This is to include final merge as part of the
      // computation of expected input bytes of merges
      boolean considerFinalMerge = includeFinalMerge;
      
      while (n > f || considerFinalMerge) {
        if (n <=f ) {
          considerFinalMerge = false;
        }
        long mergedSize = 0;
        f = Math.min(f, segmentSizes.size());
        for (int j = 0; j < f; j++) {
          mergedSize += segmentSizes.remove(0);
        }
        totalBytes += mergedSize;
        
        // insert new size into the sorted list
        int pos = Collections.binarySearch(segmentSizes, mergedSize);
        if (pos < 0) {
          pos = -pos-1;
        }
        segmentSizes.add(pos, mergedSize);
        
        n -= (f-1);
        f = factor;
      }

      return totalBytes;
    }

    public Progress getProgress() {
      return mergeProgress;
    }

  }
}
