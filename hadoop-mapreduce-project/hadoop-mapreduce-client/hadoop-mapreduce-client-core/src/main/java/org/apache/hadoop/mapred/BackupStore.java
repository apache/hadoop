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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.IFile.Reader;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.security.IntermediateEncryptedStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <code>BackupStore</code> is an utility class that is used to support
 * the mark-reset functionality of values iterator
 *
 * <p>It has two caches - a memory cache and a file cache where values are
 * stored as they are iterated, after a mark. On reset, values are retrieved
 * from these caches. Framework moves from the memory cache to the 
 * file cache when the memory cache becomes full.
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class BackupStore<K,V> {

  private static final Logger LOG =
      LoggerFactory.getLogger(BackupStore.class.getName());
  private static final int MAX_VINT_SIZE = 9;
  private static final int EOF_MARKER_SIZE = 2 * MAX_VINT_SIZE;
  private final TaskAttemptID tid;
 
  private MemoryCache memCache;
  private FileCache fileCache;

  List<Segment<K,V>> segmentList = new LinkedList<Segment<K,V>>();
  private int readSegmentIndex = 0;
  private int firstSegmentOffset = 0;

  private int currentKVOffset = 0;
  private int nextKVOffset = -1;

  private DataInputBuffer currentKey = null;
  private DataInputBuffer currentValue = new DataInputBuffer();
  private DataInputBuffer currentDiskValue = new DataInputBuffer();
 
  private boolean hasMore = false;
  private boolean inReset = false;
  private boolean clearMarkFlag = false;
  private boolean lastSegmentEOF = false;
  
  private Configuration conf;

  public BackupStore(Configuration conf, TaskAttemptID taskid)
  throws IOException {
    
    final float bufferPercent =
      conf.getFloat(JobContext.REDUCE_MARKRESET_BUFFER_PERCENT, 0f);

    if (bufferPercent > 1.0 || bufferPercent < 0.0) {
      throw new IOException(JobContext.REDUCE_MARKRESET_BUFFER_PERCENT +
          bufferPercent);
    }

    int maxSize = (int)Math.min(
        Runtime.getRuntime().maxMemory() * bufferPercent, Integer.MAX_VALUE);

    // Support an absolute size also.
    int tmp = conf.getInt(JobContext.REDUCE_MARKRESET_BUFFER_SIZE, 0);
    if (tmp >  0) {
      maxSize = tmp;
    }

    memCache = new MemoryCache(maxSize);
    fileCache = new FileCache(conf);
    tid = taskid;
    
    this.conf = conf;
    
    LOG.info("Created a new BackupStore with a memory of " + maxSize);

  }

  /**
   * Write the given K,V to the cache. 
   * Write to memcache if space is available, else write to the filecache
   * @param key
   * @param value
   * @throws IOException
   */
  public void write(DataInputBuffer key, DataInputBuffer value)
  throws IOException {

    assert (key != null && value != null);

    if (fileCache.isActive()) {
      fileCache.write(key, value);
      return;
    }

    if (memCache.reserveSpace(key, value)) {
      memCache.write(key, value);
    } else {
      fileCache.activate();
      fileCache.write(key, value);
    }
  }

  public void mark() throws IOException {

    // We read one KV pair in advance in hasNext. 
    // If hasNext has read the next KV pair from a new segment, but the
    // user has not called next() for that KV, then reset the readSegmentIndex
    // to the previous segment

    if (nextKVOffset == 0) {
      assert (readSegmentIndex != 0);
      assert (currentKVOffset != 0);
      readSegmentIndex --;
    }

    // just drop segments before the current active segment

    int i = 0;
    Iterator<Segment<K,V>> itr = segmentList.iterator();
    while (itr.hasNext()) {
      Segment<K,V> s = itr.next();
      if (i == readSegmentIndex) {
        break;
      }
      s.close();
      itr.remove();
      i++;
      LOG.debug("Dropping a segment");
    }

    // FirstSegmentOffset is the offset in the current segment from where we
    // need to start reading on the next reset

    firstSegmentOffset = currentKVOffset;
    readSegmentIndex = 0;

    LOG.debug("Setting the FirsSegmentOffset to " + currentKVOffset);
  }

  public void reset() throws IOException {

    // Create a new segment for the previously written records only if we
    // are not already in the reset mode
    
    if (!inReset) {
      if (fileCache.isActive) {
        fileCache.createInDiskSegment();
      } else {
        memCache.createInMemorySegment();
      }
    } 

    inReset = true;
    
    // Reset the segments to the correct position from where the next read
    // should begin. 
    for (int i = 0; i < segmentList.size(); i++) {
      Segment<K,V> s = segmentList.get(i);
      if (s.inMemory()) {
        int offset = (i == 0) ? firstSegmentOffset : 0;
        s.getReader().reset(offset);
      } else {
        s.closeReader();
        if (i == 0) {
          s.reinitReader(firstSegmentOffset);
          s.getReader().disableChecksumValidation();
        }
      }
    }
    
    currentKVOffset = firstSegmentOffset;
    nextKVOffset = -1;
    readSegmentIndex = 0;
    hasMore = false;
    lastSegmentEOF = false;

    LOG.debug("Reset - First segment offset is " + firstSegmentOffset +
        " Segment List Size is " + segmentList.size());
  }

  public boolean hasNext() throws IOException {
    
    if (lastSegmentEOF) {
      return false;
    }
    
    // We read the next KV from the cache to decide if there is any left.
    // Since hasNext can be called several times before the actual call to 
    // next(), we use hasMore to avoid extra reads. hasMore is set to false
    // when the user actually consumes this record in next()

    if (hasMore) {
      return true;
    }

    Segment<K,V> seg = segmentList.get(readSegmentIndex);
    // Mark the current position. This would be set to currentKVOffset
    // when the user consumes this record in next(). 
    nextKVOffset = (int) seg.getActualPosition();
    if (seg.nextRawKey()) {
      currentKey = seg.getKey();
      seg.getValue(currentValue);
      hasMore = true;
      return true;
    } else {
      if (!seg.inMemory()) {
        seg.closeReader();
      }
    }

    // If this is the last segment, mark the lastSegmentEOF flag and return
    if (readSegmentIndex == segmentList.size() - 1) {
      nextKVOffset = -1;
      lastSegmentEOF = true;
      return false;
    }

    nextKVOffset = 0;
    readSegmentIndex ++;

    Segment<K,V> nextSegment = segmentList.get(readSegmentIndex);
    
    // We possibly are moving from a memory segment to a disk segment.
    // Reset so that we do not corrupt the in-memory segment buffer.
    // See HADOOP-5494
    
    if (!nextSegment.inMemory()) {
      currentValue.reset(currentDiskValue.getData(), 
          currentDiskValue.getLength());
      nextSegment.init(null);
    }
 
    if (nextSegment.nextRawKey()) {
      currentKey = nextSegment.getKey();
      nextSegment.getValue(currentValue);
      hasMore = true;
      return true;
    } else {
      throw new IOException("New segment did not have even one K/V");
    }
  }

  public void next() throws IOException {
    if (!hasNext()) {
      throw new NoSuchElementException("iterate past last value");
    }
    // Reset hasMore. See comment in hasNext()
    hasMore = false;
    currentKVOffset = nextKVOffset;
    nextKVOffset = -1;
  }

  public DataInputBuffer nextValue() {
    return  currentValue;
  }

  public DataInputBuffer nextKey() {
    return  currentKey;
  }

  public void reinitialize() throws IOException {
    if (segmentList.size() != 0) {
      clearSegmentList();
    }
    memCache.reinitialize(true);
    fileCache.reinitialize();
    readSegmentIndex = firstSegmentOffset = 0;
    currentKVOffset = 0;
    nextKVOffset = -1;
    hasMore = inReset = clearMarkFlag = false;
  }

  /**
   * This function is called the ValuesIterator when a mark is called
   * outside of a reset zone.  
   */
  public void exitResetMode() throws IOException { 
    inReset = false;
    if (clearMarkFlag ) {
      // If a flag was set to clear mark, do the reinit now.
      // See clearMark()
      reinitialize();
      return;
    }
    if (!fileCache.isActive) {
      memCache.reinitialize(false);
    }
  }

  /** For writing the first key and value bytes directly from the
   *  value iterators, pass the current underlying output stream
   *  @param length The length of the impending write
   */
  public DataOutputStream getOutputStream(int length) throws IOException {
    if (memCache.reserveSpace(length)) {
      return memCache.dataOut;
    } else {
      fileCache.activate();
      return fileCache.writer.getOutputStream();
    }
  }

  /** This method is called by the valueIterators after writing the first
   *  key and value bytes to the BackupStore
   * @param length 
   */
  public void updateCounters(int length) {
    if (fileCache.isActive) {
      fileCache.writer.updateCountersForExternalAppend(length);
    } else {
      memCache.usedSize += length;
    }
  }

  public void clearMark() throws IOException {
    if (inReset) {
      // If we are in the reset mode, we just mark a flag and come out
      // The actual re initialization would be done when we exit the reset
      // mode
      clearMarkFlag = true;
    } else {
      reinitialize();
    }
  }
  
  private void clearSegmentList() throws IOException {
    for (Segment<K,V> segment: segmentList) {
      long len = segment.getLength();
      segment.close();
      if (segment.inMemory()) {
       memCache.unreserve(len);
      }
    }
    segmentList.clear();
  }

  class MemoryCache {
    private DataOutputBuffer dataOut;
    private int blockSize;
    private int usedSize;
    private final BackupRamManager ramManager;

    // Memory cache is made up of blocks.
    private int defaultBlockSize = 1024 * 1024;

    public MemoryCache(int maxSize) {
      ramManager = new BackupRamManager(maxSize);
      if (maxSize < defaultBlockSize) {
        defaultBlockSize = maxSize;
      }
    }

    public void unreserve(long len) {
      ramManager.unreserve((int)len);
    }

    /**
     * Re-initialize the memory cache.
     * 
     * @param clearAll If true, re-initialize the ramManager also.
     */
    void reinitialize(boolean clearAll) {
      if (clearAll) {
        ramManager.reinitialize();
      }
      int allocatedSize = createNewMemoryBlock(defaultBlockSize, 
          defaultBlockSize);
      assert(allocatedSize == defaultBlockSize || allocatedSize == 0);
      LOG.debug("Created a new mem block of " + allocatedSize);
    }

    private int createNewMemoryBlock(int requestedSize, int minSize) {
      int allocatedSize = ramManager.reserve(requestedSize, minSize);
      usedSize = 0;
      if (allocatedSize == 0) {
        dataOut = null;
        blockSize = 0;
      } else {
        dataOut = new DataOutputBuffer(allocatedSize);
        blockSize = allocatedSize;
      }
      return allocatedSize;
    }

    /**
     * This method determines if there is enough space left in the 
     * memory cache to write to the requested length + space for
     * subsequent EOF makers.
     * @param length
     * @return true if enough space is available
     */
    boolean reserveSpace(int length) throws IOException {
      int availableSize = blockSize - usedSize;
      if (availableSize >= length + EOF_MARKER_SIZE) {
        return true;
      }
      // Not enough available. Close this block 
      assert (!inReset); 

      createInMemorySegment();
      
      // Create a new block
      int tmp = Math.max(length + EOF_MARKER_SIZE, defaultBlockSize);
      availableSize = createNewMemoryBlock(tmp, 
          (length + EOF_MARKER_SIZE));
      
      return (availableSize == 0) ? false : true;
    }

    boolean reserveSpace(DataInputBuffer key, DataInputBuffer value)
    throws IOException {
      int keyLength = key.getLength() - key.getPosition();
      int valueLength = value.getLength() - value.getPosition();

      int requestedSize = keyLength + valueLength + 
        WritableUtils.getVIntSize(keyLength) +
        WritableUtils.getVIntSize(valueLength);
      return reserveSpace(requestedSize);
    }
    
    /**
     * Write the key and value to the cache in the IFile format
     * @param key
     * @param value
     * @throws IOException
     */
    public void write(DataInputBuffer key, DataInputBuffer value)
    throws IOException {
      int keyLength = key.getLength() - key.getPosition();
      int valueLength = value.getLength() - value.getPosition();
      WritableUtils.writeVInt(dataOut, keyLength);
      WritableUtils.writeVInt(dataOut, valueLength);
      dataOut.write(key.getData(), key.getPosition(), keyLength);
      dataOut.write(value.getData(), value.getPosition(), valueLength);
      usedSize += keyLength + valueLength + 
        WritableUtils.getVIntSize(keyLength) +
        WritableUtils.getVIntSize(valueLength);
      LOG.debug("ID: " + segmentList.size() + " WRITE TO MEM");
    }

    /**
     * This method creates a memory segment from the existing buffer
     * @throws IOException
     */
    void createInMemorySegment () throws IOException {

      // If nothing was written in this block because the record size
      // was greater than the allocated block size, just return.
      if (usedSize == 0) {
        ramManager.unreserve(blockSize);
        return;
      }

      // spaceAvailable would have ensured that there is enough space
      // left for the EOF markers.
      assert ((blockSize - usedSize) >= EOF_MARKER_SIZE);
  
      WritableUtils.writeVInt(dataOut, IFile.EOF_MARKER);
      WritableUtils.writeVInt(dataOut, IFile.EOF_MARKER);

      usedSize += EOF_MARKER_SIZE;

      ramManager.unreserve(blockSize - usedSize);

      Reader<K, V> reader = 
        new org.apache.hadoop.mapreduce.task.reduce.InMemoryReader<K, V>(null, 
            (org.apache.hadoop.mapred.TaskAttemptID) tid, 
            dataOut.getData(), 0, usedSize, conf);
      Segment<K, V> segment = new Segment<K, V>(reader, false);
      segmentList.add(segment);
      LOG.debug("Added Memory Segment to List. List Size is " + 
          segmentList.size());
    }
  }

  class FileCache {
    private LocalDirAllocator lDirAlloc;
    private final Configuration conf;
    private final FileSystem fs;
    private boolean isActive = false;

    private Path file = null;
    private IFile.Writer<K,V> writer = null;
    private int spillNumber = 0;

    public FileCache(Configuration conf)
    throws IOException {
      this.conf = conf;
      this.fs = FileSystem.getLocal(conf);
      this.lDirAlloc = new LocalDirAllocator(MRConfig.LOCAL_DIR);
    }

    void write(DataInputBuffer key, DataInputBuffer value)
    throws IOException {
      if (writer == null) {
        // If spillNumber is 0, we should have called activate and not
        // come here at all
        assert (spillNumber != 0); 
        writer = createSpillFile();
      }
      writer.append(key, value);
      LOG.debug("ID: " + segmentList.size() + " WRITE TO DISK");
    }

    void reinitialize() {
      spillNumber = 0;
      writer = null;
      isActive = false;
    }

    void activate() throws IOException {
      isActive = true;
      writer = createSpillFile();
    }

    void createInDiskSegment() throws IOException {
      assert (writer != null);
      writer.close();
      Segment<K,V> s = new Segment<K, V>(conf, fs, file, null, true);
      writer = null;
      segmentList.add(s);
      LOG.debug("Disk Segment added to List. Size is "  + segmentList.size());
    }

    boolean isActive() { return isActive; }

    private Writer<K,V> createSpillFile() throws IOException {
      Path tmp =
          new Path(MRJobConfig.OUTPUT + "/backup_" + tid.getId() + "_"
              + (spillNumber++) + ".out");

      LOG.info("Created file: " + tmp);

      file = lDirAlloc.getLocalPathForWrite(tmp.toUri().getPath(), 
          -1, conf);
      FSDataOutputStream out = fs.create(file);
      out = IntermediateEncryptedStream.wrapIfNecessary(conf, out, tmp);
      return new Writer<K, V>(conf, out, null, null, null, null, true);
    }
  }

  static class BackupRamManager implements RamManager {

    private int availableSize = 0;
    private final int maxSize;

    public BackupRamManager(int size) {
      availableSize = maxSize = size;
    }

    public boolean reserve(int requestedSize, InputStream in) {
      // Not used
      LOG.warn("Reserve(int, InputStream) not supported by BackupRamManager");
      return false;
    }

    int reserve(int requestedSize) {
      if (availableSize == 0) {
        return 0;
      }
      int reservedSize = Math.min(requestedSize, availableSize);
      availableSize -= reservedSize;
      LOG.debug("Reserving: " + reservedSize + " Requested: " + requestedSize);
      return reservedSize;
    }

    int reserve(int requestedSize, int minSize) {
      if (availableSize < minSize) {
        LOG.debug("No space available. Available: " + availableSize + 
            " MinSize: " + minSize);
        return 0;
      } else {
        return reserve(requestedSize);
      }
    }

    public void unreserve(int requestedSize) {
      availableSize += requestedSize;
      LOG.debug("Unreserving: " + requestedSize +
          ". Available: " + availableSize);
    }
    
    void reinitialize() {
      availableSize = maxSize;
    }
  }
}
