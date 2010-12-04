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

package org.apache.hadoop.io;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Options;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.serial.RawComparator;
import org.apache.hadoop.io.serial.Serialization;
import org.apache.hadoop.io.serial.SerializationFactory;
import org.apache.hadoop.util.Progressable;

/** A file-based map from keys to values.
 * 
 * <p>A map is a directory containing two files, the <code>data</code> file,
 * containing all keys and values in the map, and a smaller <code>index</code>
 * file, containing a fraction of the keys.  The fraction is determined by
 * {@link Writer#getIndexInterval()}.
 *
 * <p>The index file is read entirely into memory.  Thus key implementations
 * should try to keep themselves small.
 *
 * <p>Map files are created by adding entries in-order.  To maintain a large
 * database, perform updates by copying the previous version of a database and
 * merging in a sorted change list, to create a new version of the database in
 * a new file.  Sorting large change lists can be done with {@link
 * SequenceFile.Sorter}.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MapFile {
  private static final Log LOG = LogFactory.getLog(MapFile.class);

  /** The name of the index file. */
  public static final String INDEX_FILE_NAME = "index";

  /** The name of the data file. */
  public static final String DATA_FILE_NAME = "data";

  protected MapFile() {}                          // no public ctor

  /** Writes a new map. */
  public static class Writer implements java.io.Closeable {
    private final SequenceFile.Writer data;
    private final SequenceFile.Writer index;
    private final Configuration conf;
    private final Serialization<Object> keySerialization;
    private final Serialization<Object> valueSerialization;

    final private static String INDEX_INTERVAL = "io.map.index.interval";
    private int indexInterval = 128;

    private long size;
    private LongWritable position = new LongWritable();

    // the following fields are used only for checking key order
    private final RawComparator comparator;
    private final DataInputBuffer inBuf = new DataInputBuffer();
    private DataOutputBuffer lastKey;
    private final DataOutputBuffer currentKey = new DataOutputBuffer();
    private final DataOutputBuffer currentValue = new DataOutputBuffer();

    /** What's the position (in bytes) we wrote when we got the last index */
    private long lastIndexPos = -1;

    /**
     * What was size when we last wrote an index. Set to MIN_VALUE to ensure that
     * we have an index at position zero -- midKey will throw an exception if this
     * is not the case
     */
    private long lastIndexKeyCount = Long.MIN_VALUE;


    /** Create the named map for keys of the named class. 
     * @deprecated Use Writer(Configuration, Path, Option...) instead.
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public Writer(Configuration conf, FileSystem fs, String dirName,
                  Class<? extends WritableComparable> keyClass, 
                  Class valClass) throws IOException {
      this(conf, new Path(dirName), keyClass(keyClass), valueClass(valClass));
    }

    /** Create the named map for keys of the named class. 
     * @deprecated Use Writer(Configuration, Path, Option...) instead.
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public Writer(Configuration conf, FileSystem fs, String dirName,
                  Class<? extends WritableComparable> keyClass, Class valClass,
                  CompressionType compress, 
                  Progressable progress) throws IOException {
      this(conf, new Path(dirName), keyClass(keyClass), valueClass(valClass),
           compression(compress), progressable(progress));
    }

    /** Create the named map for keys of the named class. 
     * @deprecated Use Writer(Configuration, Path, Option...) instead.
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public Writer(Configuration conf, FileSystem fs, String dirName,
                  Class<? extends WritableComparable> keyClass, Class valClass,
                  CompressionType compress, CompressionCodec codec,
                  Progressable progress) throws IOException {
      this(conf, new Path(dirName), keyClass(keyClass), valueClass(valClass),
           compression(compress, codec), progressable(progress));
    }

    /** Create the named map for keys of the named class. 
     * @deprecated Use Writer(Configuration, Path, Option...) instead.
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public Writer(Configuration conf, FileSystem fs, String dirName,
                  Class<? extends WritableComparable> keyClass, Class valClass,
                  CompressionType compress) throws IOException {
      this(conf, new Path(dirName), keyClass(keyClass),
           valueClass(valClass), compression(compress));
    }

    /** Create the named map using the named key comparator. 
     * @deprecated Use Writer(Configuration, Path, Option...) instead.
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public Writer(Configuration conf, FileSystem fs, String dirName,
                  WritableComparator comparator, Class valClass
                  ) throws IOException {
      this(conf, new Path(dirName), comparator(comparator), 
           valueClass(valClass));
    }

    /** Create the named map using the named key comparator. 
     * @deprecated Use Writer(Configuration, Path, Option...) instead.
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public Writer(Configuration conf, FileSystem fs, String dirName,
                  WritableComparator comparator, Class valClass,
                  SequenceFile.CompressionType compress) throws IOException {
      this(conf, new Path(dirName), comparator(comparator),
           valueClass(valClass), compression(compress));
    }

    /** Create the named map using the named key comparator. 
     * @deprecated Use Writer(Configuration, Path, Option...)} instead.
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public Writer(Configuration conf, FileSystem fs, String dirName,
                  WritableComparator comparator, Class valClass,
                  SequenceFile.CompressionType compress,
                  Progressable progress) throws IOException {
      this(conf, new Path(dirName), comparator(comparator),
           valueClass(valClass), compression(compress),
           progressable(progress));
    }

    /** Create the named map using the named key comparator. 
     * @deprecated Use Writer(Configuration, Path, Option...) instead.
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public Writer(Configuration conf, FileSystem fs, String dirName,
                  WritableComparator comparator, Class valClass,
                  SequenceFile.CompressionType compress, CompressionCodec codec,
                  Progressable progress) throws IOException {
      this(conf, new Path(dirName), comparator(comparator),
           valueClass(valClass), compression(compress, codec),
           progressable(progress));
    }
    
    // our options are a superset of sequence file writer options
    public static interface Option extends SequenceFile.Writer.Option { }
    
    private static class ComparatorOption extends Options.ComparatorOption 
                                          implements Option{
      ComparatorOption(RawComparator value) {
        super(value);
      }
    }

    public static SequenceFile.Writer.Option keyClass(Class<?> value) {
      return new SequenceFile.Writer.KeyClassOption(value);
    }
    
    public static Option comparator(RawComparator value) {
      return new ComparatorOption(value);
    }

    public static SequenceFile.Writer.Option valueClass(Class<?> value) {
      return SequenceFile.Writer.valueClass(value);
    }
    
    public static 
    SequenceFile.Writer.Option compression(CompressionType type) {
      return SequenceFile.Writer.compression(type);
    }

    public static 
    SequenceFile.Writer.Option compression(CompressionType type,
        CompressionCodec codec) {
      return SequenceFile.Writer.compression(type, codec);
    }

    public static SequenceFile.Writer.Option progressable(Progressable value) {
      return SequenceFile.Writer.progressable(value);
    }

    public static 
    SequenceFile.Writer.Option keySerialization(Serialization<?> value) {
      return SequenceFile.Writer.keySerialization(value);
    }

    public static 
    SequenceFile.Writer.Option valueSerialization(Serialization<?> value) {
      return SequenceFile.Writer.valueSerialization(value);
    }
 
    @SuppressWarnings("unchecked")
    public Writer(Configuration conf, 
                  Path dirName,
                  SequenceFile.Writer.Option... opts
                  ) throws IOException {
      this.conf = conf;
      ComparatorOption comparatorOption =
        Options.getOption(ComparatorOption.class, opts);
      
      this.indexInterval = conf.getInt(INDEX_INTERVAL, this.indexInterval);

      FileSystem fs = dirName.getFileSystem(conf);

      if (!fs.mkdirs(dirName)) {
        throw new IOException("Mkdirs failed to create directory " + dirName);
      }
      Path dataFile = new Path(dirName, DATA_FILE_NAME);
      Path indexFile = new Path(dirName, INDEX_FILE_NAME);

      SequenceFile.Writer.Option[] dataOptions =
        Options.prependOptions(opts, 
                               SequenceFile.Writer.file(dataFile));
      this.data = SequenceFile.createWriter(conf, dataOptions);
      keySerialization = (Serialization<Object>) data.getKeySerialization();
      valueSerialization = (Serialization<Object>) data.getValueSerialization();
      if (comparatorOption != null) {
        comparator = comparatorOption.getValue();
      } else {
        comparator = keySerialization.getRawComparator();
      }

      SequenceFile.Writer.Option[] indexOptions =
        Options.prependOptions(opts, SequenceFile.Writer.file(indexFile),
            SequenceFile.Writer.valueClass(LongWritable.class),
            SequenceFile.Writer.compression(CompressionType.BLOCK));
      this.index = SequenceFile.createWriter(conf, indexOptions);      
    }

    /** The number of entries that are added before an index entry is added.*/
    public int getIndexInterval() { return indexInterval; }

    /** Sets the index interval.
     * @see #getIndexInterval()
     */
    public void setIndexInterval(int interval) { indexInterval = interval; }

    /** Sets the index interval and stores it in conf
     * @see #getIndexInterval()
     */
    public static void setIndexInterval(Configuration conf, int interval) {
      conf.setInt(INDEX_INTERVAL, interval);
    }

    /**
     * Get the serialization used for the keys
     * @return the key serialization
     */
    public Serialization<?> getKeySerialization() {
      return data.getKeySerialization();
    }
    
    /**
     * Get the serialization used for the values
     * @return the value serialization
     */
    public Serialization<?> getValueSerialization() {
      return data.getValueSerialization();
    }
    
    /** Close the map. */
    public synchronized void close() throws IOException {
      data.close();
      index.close();
    }

    /** Append a key/value pair to the map.  The key must be greater or equal
     * to the previous key added to the map. */
    public synchronized void append(Object key, Object val)
      throws IOException {

      currentKey.reset();
      keySerialization.serialize(currentKey, key);
      checkKey(currentKey, key);
      currentValue.reset();
      valueSerialization.serialize(currentValue, val);

      long pos = data.getLength();      
      // Only write an index if we've changed positions. In a block compressed
      // file, this means we write an entry at the start of each block      
      if (size >= lastIndexKeyCount + indexInterval && pos > lastIndexPos) {
        position.set(pos);                        // point to current eof
        index.append(key, position);
        lastIndexPos = pos;
        lastIndexKeyCount = size;
      }

      data.append(key, val);                      // append key/value to data
      size++;
    }

    private void checkKey(DataOutputBuffer serialKey, Object key
                          ) throws IOException {
      // check that keys are well-ordered
      if (lastKey == null) {
        lastKey = new DataOutputBuffer();
      } else if (comparator.compare(lastKey.getData(), 0, lastKey.getLength(), 
                                    serialKey.getData(),0,serialKey.getLength()) 
                      > 0) {
        // rebuild the previous key so that we can explain what's wrong
        inBuf.reset(lastKey.getData(), 0, lastKey.getLength());
        Object prevKey = keySerialization.deserialize(inBuf, null, conf);
        throw new IOException("key out of order: "+ key +" after "+ prevKey);
      }
      lastKey.reset();
      lastKey.write(serialKey.getData(), 0, serialKey.getLength());
    }

  }
  
  /** Provide access to an existing map. */
  public static class Reader implements java.io.Closeable {
      
    /** Number of index entries to skip between each entry.  Zero by default.
     * Setting this to values larger than zero can facilitate opening large map
     * files using less memory. */
    private int INDEX_SKIP = 0;
      
    private RawComparator comparator;
    private Serialization<Object> keySerialization;
    private final Configuration conf;

    private DataOutputBuffer nextKey = new DataOutputBuffer();
    private DataInputBuffer inBuf = new DataInputBuffer();
    private long seekPosition = -1;
    private int seekIndex = -1;
    private long firstPosition;

    // the data, on disk
    private SequenceFile.Reader data;
    private SequenceFile.Reader index;

    // whether the index Reader was closed
    private boolean indexClosed = false;

    // the index, in memory
    private int count = -1;
    private byte[][] keys;
    private long[] positions;

    /** Returns the class of keys in this file. 
     * @deprecated Use {@link #getKeySerialization} instead.
     */
    @Deprecated
    public Class<?> getKeyClass() { return data.getKeyClass(); }

    /** Returns the class of values in this file. 
     * @deprecated Use {@link #getValueSerialization} instead.
     */
    @Deprecated
    public Class<?> getValueClass() { return data.getValueClass(); }

    /**
     * Get the key serialization for this map file.
     * @return the serialization for the key
     */
    public Serialization<?> getKeySerialization() {
      return keySerialization;
    }
    
    /**
     * Get the value serialization for this map file.
     * @return the serialization for the value
     */
    public Serialization<?> getValueSerialization() {
      return data.getValueSerialization();
    }
    public static interface Option extends SequenceFile.Reader.Option {}
    
    public static Option comparator(WritableComparator value) {
      return new ComparatorOption(value);
    }

    static class ComparatorOption extends Options.ComparatorOption 
                                          implements Option {
      ComparatorOption(RawComparator value) {
        super(value);
      }
    }

    public Reader(Path dir, Configuration conf,
                  SequenceFile.Reader.Option... opts) throws IOException {
      this.conf = conf;
      ComparatorOption comparatorOption = 
        Options.getOption(ComparatorOption.class, opts);
      RawComparator comparator =
        comparatorOption == null ? null : comparatorOption.getValue();
      INDEX_SKIP = conf.getInt("io.map.index.skip", 0);
      open(dir, comparator, conf, opts);
    }
 
    /** Construct a map reader for the named map.
     * @deprecated
     */
    @Deprecated
    public Reader(FileSystem fs, String dirName, 
                  Configuration conf) throws IOException {
      this(new Path(dirName), conf);
    }

    /** Construct a map reader for the named map using the named comparator.
     * @deprecated
     */
    @Deprecated
    public Reader(FileSystem fs, String dirName, WritableComparator comparator, 
                  Configuration conf) throws IOException {
      this(new Path(dirName), conf, comparator(comparator));
    }
    
    @SuppressWarnings("unchecked")
    protected synchronized void open(Path dir,
                                     RawComparator comparator,
                                     Configuration conf, 
                                     SequenceFile.Reader.Option... options
                                     ) throws IOException {
      Path dataFile = new Path(dir, DATA_FILE_NAME);
      Path indexFile = new Path(dir, INDEX_FILE_NAME);

      // open the data
      this.data = createDataFileReader(dataFile, conf, options);
      this.firstPosition = data.getPosition();
      keySerialization = (Serialization<Object>) data.getKeySerialization();

      if (comparator == null) {
        this.comparator = keySerialization.getRawComparator();
      } else {
        this.comparator = comparator;
      }

      // open the index
      SequenceFile.Reader.Option[] indexOptions =
        Options.prependOptions(options, SequenceFile.Reader.file(indexFile));
      this.index = new SequenceFile.Reader(conf, indexOptions);
    }

    /**
     * Override this method to specialize the type of
     * {@link SequenceFile.Reader} returned.
     */
    protected SequenceFile.Reader 
      createDataFileReader(Path dataFile, Configuration conf,
                           SequenceFile.Reader.Option... options
                           ) throws IOException {
      SequenceFile.Reader.Option[] newOptions =
        Options.prependOptions(options, SequenceFile.Reader.file(dataFile));
      return new SequenceFile.Reader(conf, newOptions);
    }

    private void readIndex() throws IOException {
      // read the index entirely into memory
      if (this.keys != null)
        return;
      this.count = 0;
      this.positions = new long[1024];

      try {
        int skip = INDEX_SKIP;
        LongWritable position = new LongWritable();
        byte[] lastKey = null;
        long lastIndex = -1;
        ArrayList<byte[]> keyBuilder = new ArrayList<byte[]>(1024);
        DataOutputBuffer key = new DataOutputBuffer();
        while (index.nextRawKey(key) > 0) {
          position = (LongWritable) index.getCurrentValue(position);

          // check order to make sure comparator is compatible
          if (lastKey != null && 
              comparator.compare(lastKey, 0, lastKey.length,
                                 key.getData(), 0 , key.getLength()) > 0) {
            inBuf.reset(lastKey, 0, lastKey.length);
            Object prevKey = keySerialization.deserialize(inBuf, null, conf);
            inBuf.reset(key.getData(), 0, key.getLength());
            Object curKey = keySerialization.deserialize(inBuf, null, conf);
            throw new IOException("key out of order: "+ curKey + " after " +
                                  prevKey);
          }
          lastKey = Arrays.copyOf(key.getData(), key.getLength());
          if (skip > 0) {
            skip--;
            continue;                             // skip this entry
          } else {
            skip = INDEX_SKIP;                    // reset skip
          }

          // don't read an index that is the same as the previous one. Block
          // compressed map files used to do this (multiple entries would point
          // at the same block)
          if (position.get() == lastIndex)
            continue;

          if (count == positions.length) {
            positions = Arrays.copyOf(positions, positions.length * 2);
          }

          keyBuilder.add(lastKey);
          positions[count] = position.get();
          count++;
        }

        this.keys = keyBuilder.toArray(new byte[count][]);
        positions = Arrays.copyOf(positions, count);
      } catch (EOFException e) {
        LOG.warn("Unexpected EOF reading " + index +
                 " at entry #" + count + ".  Ignoring.");
      } finally {
        indexClosed = true;
        index.close();
      }
    }

    /** Re-positions the reader before its first key. */
    public synchronized void reset() throws IOException {
      data.seek(firstPosition);
    }

    /** Get the key at approximately the middle of the file. Or null if the
     *  file is empty. 
     */
    public synchronized Object midKey() throws IOException {

      readIndex();
      if (count == 0) {
        return null;
      }
    
      byte[] rawKey = keys[(count -1) / 2];
      inBuf.reset(rawKey, 0, rawKey.length);
      return keySerialization.deserialize(inBuf, null, conf);
    }
    
    /** Reads the final key from the file.
     *
     * @param key key to read into
     */
    public synchronized Object finalKey(Object key) throws IOException {

      long originalPosition = data.getPosition(); // save position
      try {
        readIndex();                              // make sure index is valid
        if (count > 0) {
          data.seek(positions[count-1]);          // skip to last indexed entry
        } else {
          reset();                                // start at the beginning
        }
        Object prevKey = null;
        do {
          prevKey = key;
          key = data.nextKey(key);
        } while (key != null);
        return prevKey;
      } finally {
        data.seek(originalPosition);              // restore position
      }
    }

    /** Positions the reader at the named key, or if none such exists, at the
     * first entry after the named key.  Returns true iff the named key exists
     * in this map.
     */
    public synchronized boolean seek(Object key) throws IOException {
      return seekInternal(key) == 0;
    }

    /** 
     * Positions the reader at the named key, or if none such exists, at the
     * first entry after the named key.
     *
     * @return  0   - exact match found
     *          < 0 - positioned at next record
     *          1   - no more records in file
     */
    private synchronized int seekInternal(Object key)
      throws IOException {
      return seekInternal(key, false);
    }

    /** 
     * Positions the reader at the named key, or if none such exists, at the
     * key that falls just before or just after dependent on how the
     * <code>before</code> parameter is set.
     * 
     * @param before - IF true, and <code>key</code> does not exist, position
     * file at entry that falls just before <code>key</code>.  Otherwise,
     * position file at record that sorts just after.
     * @return  0   - exact match found
     *          < 0 - positioned at next record
     *          1   - no more records in file
     */
    private synchronized int seekInternal(Object key,
                                          final boolean before
                                          ) throws IOException {
      readIndex();                                // make sure index is read
      DataOutputBuffer keyBuffer = new DataOutputBuffer();
      keySerialization.serialize(keyBuffer, key);

      if (seekIndex != -1                         // seeked before
          && seekIndex+1 < count           
          && comparator.compare(keyBuffer.getData(), 0, keyBuffer.getLength(), 
                                keys[seekIndex+1], 0, keys[seekIndex+1].length)
                             < 0       // before next indexed
          && comparator.compare(keyBuffer.getData(), 0, keyBuffer.getLength(), 
                                nextKey.getData(), 0, nextKey.getLength())
                             >= 0) {   // but after last seeked
        // do nothing
      } else {
        seekIndex = binarySearch(keyBuffer.getData(), keyBuffer.getLength());
        if (seekIndex < 0)                        // decode insertion point
          seekIndex = -seekIndex-2;

        if (seekIndex == -1)                      // belongs before first entry
          seekPosition = firstPosition;           // use beginning of file
        else
          seekPosition = positions[seekIndex];    // else use index
      }
      data.seek(seekPosition);
      
      // If we're looking for the key before, we need to keep track
      // of the position we got the current key as well as the position
      // of the key before it.
      long prevPosition = -1;
      long curPosition = seekPosition;

      while (data.nextRawKey(nextKey) != -1) {
        int c = comparator.compare(keyBuffer.getData(), 0, keyBuffer.getLength(),
                                   nextKey.getData(), 0 , nextKey.getLength());
        if (c <= 0) {                             // at or beyond desired
          if (before && c != 0) {
            if (prevPosition == -1) {
              // We're on the first record of this index block
              // and we've already passed the search key. Therefore
              // we must be at the beginning of the file, so seek
              // to the beginning of this block and return c
              data.seek(curPosition);
            } else {
              // We have a previous record to back up to
              data.seek(prevPosition);
              data.nextRawKey(nextKey);
              // now that we've rewound, the search key must be greater than this key
              return 1;
            }
          }
          return c;
        }
        if (before) {
          prevPosition = curPosition;
          curPosition = data.getPosition();
        }
      }
      // if we have fallen off the end of the file and we want the before key
      // then back up to the previous key
      if (before && prevPosition != -1) {
        data.seek(prevPosition);
        data.nextRawKey(nextKey);
      }
      return 1;
    }

    private int binarySearch(byte[] key, int length) {
      int low = 0;
      int high = count-1;

      while (low <= high) {
        int mid = (low + high) >>> 1;
        byte[] midVal = keys[mid];
        int cmp = comparator.compare(midVal, 0, midVal.length,
                                     key, 0, length);

        if (cmp < 0)
          low = mid + 1;
        else if (cmp > 0)
          high = mid - 1;
        else
          return mid;                             // key found
      }
      return -(low + 1);                          // key not found.
    }

    /** Read the next key/value pair in the map into <code>key</code> and
     * <code>val</code>.  Returns true if such a pair exists and false when at
     * the end of the map 
     * @deprecated Use {@link #nextKey} and {@link #getCurrentValue} instead.
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public synchronized boolean next(WritableComparable key, Writable val)
      throws IOException {
      return data.next(key, val);
    }
    
    /**
     * Read the next key in the map.
     * @param reusable an object that may be re-used for holding the next key
     * @return the key that was read or null if there is not another key
     * @throws IOException
     */
    public Object nextKey(Object reusable) throws IOException {
      return data.nextKey(reusable);
    }

    /**
     * Get the current value in the map.
     * @param reusable an object that may be re-used for hold the value
     * @return the value that was read in
     * @throws IOException
     */
    public Object getCurrentValue(Object reusable) throws IOException {
      return data.getCurrentValue(reusable);
    }

    /**
     * Return the value for the named key, or null if none exists.
     * @param key the key to look for
     * @param value a object to read into
     * @return the value that was found or null if the key wasn't found
     * @throws IOException
     * @deprecated Use {@link #seek} and {@link #getCurrentValue} instead.
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public synchronized Writable get(WritableComparable key,
                                     Writable value) throws IOException {
      if (seek(key)) {
        return (Writable) data.getCurrentValue(value);
      } else {
        return null;
      }
    }

    /** Return the value for the named key, or null if none exists. */
    public synchronized Object get(Object key, Object val) throws IOException{
      if (seek(key)) {
        return data.getCurrentValue(val);
      } else
        return null;
    }

    /** 
     * Finds the record that is the closest match to the specified key.
     * Returns <code>key</code> or if it does not exist, at the first entry
     * after the named key.
     * 
-     * @param key       - key that we're trying to find
-     * @param val       - data value if key is found
-     * @return          - the key that was the closest match or null if eof.
     */
    public Object getClosest(Object key,
                             Object val)  throws IOException {
      return getClosest(key, val, false);
    }

    /** 
     * Finds the record that is the closest match to the specified key.
     * 
     * @param key       - key that we're trying to find
     * @param val       - data value if key is found
     * @param before    - IF true, and <code>key</code> does not exist, return
     * the first entry that falls just before the <code>key</code>.  Otherwise,
     * return the record that sorts just after.
     * @return          - the key that was the closest match or null if eof.
     */
    public synchronized Object getClosest(Object key,
                                          Object val, 
                                          final boolean before
                                          ) throws IOException {
     
      int c = seekInternal(key, before);

      // If we didn't get an exact match, and we ended up in the wrong
      // direction relative to the query key, return null since we
      // must be at the beginning or end of the file.
      if ((!before && c > 0) ||
          (before && c < 0)) {
        return null;
      }

      data.getCurrentValue(val);
      // deserialize the key
      inBuf.reset(nextKey.getData(), 0, nextKey.getLength());
      return keySerialization.deserialize(inBuf, null, conf);
    }

    /** Close the map. */
    public synchronized void close() throws IOException {
      if (!indexClosed) {
        index.close();
      }
      data.close();
    }

  }

  /** Renames an existing map directory. */
  public static void rename(FileSystem fs, String oldName, String newName)
    throws IOException {
    Path oldDir = new Path(oldName);
    Path newDir = new Path(newName);
    if (!fs.rename(oldDir, newDir)) {
      throw new IOException("Could not rename " + oldDir + " to " + newDir);
    }
  }

  /** Deletes the named map file. */
  public static void delete(FileSystem fs, String name) throws IOException {
    Path dir = new Path(name);
    Path data = new Path(dir, DATA_FILE_NAME);
    Path index = new Path(dir, INDEX_FILE_NAME);

    fs.delete(data, true);
    fs.delete(index, true);
    fs.delete(dir, true);
  }

  /**
   * This method attempts to fix a corrupt MapFile by re-creating its index.
   * @param fs filesystem
   * @param dir directory containing the MapFile data and index
   * @param keyClass key class (has to be a subclass of Writable)
   * @param valueClass value class (has to be a subclass of Writable)
   * @param dryrun do not perform any changes, just report what needs to be done
   * @return number of valid entries in this MapFile, or -1 if no fixing was needed
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  public static long fix(FileSystem fs, Path dir,
                         Class<?> keyClass,
                         Class<?> valueClass, boolean dryrun,
                         Configuration conf) throws IOException {
    String dr = (dryrun ? "[DRY RUN ] " : "");
    Path data = new Path(dir, DATA_FILE_NAME);
    Path index = new Path(dir, INDEX_FILE_NAME);
    int indexInterval = conf.getInt(Writer.INDEX_INTERVAL, 128);
    SerializationFactory factory = SerializationFactory.getInstance(conf);
    Serialization<Object> keySerialization = (Serialization<Object>)
      factory.getSerializationByType(keyClass);
    Serialization<Object> valueSerialization = (Serialization<Object>) 
      factory.getSerializationByType(valueClass);
    if (!fs.exists(data)) {
      // there's nothing we can do to fix this!
      throw new IOException(dr + "Missing data file in " + dir + 
                            ", impossible to fix this.");
    }
    if (fs.exists(index)) {
      // no fixing needed
      return -1;
    }
    SequenceFile.Reader dataReader = 
      new SequenceFile.Reader(conf, SequenceFile.Reader.file(data));
    if (!dataReader.getKeySerialization().equals(keySerialization)) {
      throw new IOException(dr + "Wrong key serialization in " + dir + 
                            ", expected" + keySerialization +
                            ", got " + dataReader.getKeySerialization());
    }
    if (!dataReader.getValueSerialization().equals(valueSerialization)) {
      throw new IOException(dr + "Wrong value serialization in " + dir + 
                            ", expected" + valueSerialization +
                            ", got " + dataReader.getValueSerialization());
    }
    long cnt = 0L;
    SequenceFile.Writer indexWriter = null;
    if (!dryrun) {
      indexWriter = 
        SequenceFile.createWriter(conf, 
                                  SequenceFile.Writer.file(index), 
                                  SequenceFile.Writer.keyClass(keyClass), 
                                  SequenceFile.Writer.valueClass
                                    (LongWritable.class));
    }
    try {
      long pos = 0L;
      LongWritable position = new LongWritable();
      Object key = null;
      Object value = null;
      while((key = dataReader.nextKey(key)) != null) {
        value = dataReader.getCurrentValue(value);
        cnt++;
        if (cnt % indexInterval == 0) {
          position.set(pos);
          if (!dryrun) indexWriter.append(key, position);
        }
        pos = dataReader.getPosition();
      }
    } catch(Throwable t) {
      // truncated data file. swallow it.
    }
    dataReader.close();
    if (!dryrun) indexWriter.close();
    return cnt;
  }


  public static void main(String[] args) throws Exception {
    String usage = "Usage: MapFile inFile outFile";
      
    if (args.length != 2) {
      System.err.println(usage);
      System.exit(-1);
    }
      
    String in = args[0];
    String out = args[1];

    Configuration conf = new Configuration();
    MapFile.Reader reader = new MapFile.Reader(new Path(in), conf);
    Serialization<?> keySerialization = reader.getKeySerialization();
    Serialization<?> valueSerialization = reader.getValueSerialization();
    MapFile.Writer writer =
      new MapFile.Writer(conf, new Path(out), 
                         Writer.keySerialization(keySerialization),
                         Writer.valueSerialization(valueSerialization));

    Object key = null;
    Object value = null;

    while ((key = reader.nextKey(key)) != null) {          // copy all entries
      value = reader.getCurrentValue(value);
      writer.append(key, value);
    }
    writer.close();
  }

}
