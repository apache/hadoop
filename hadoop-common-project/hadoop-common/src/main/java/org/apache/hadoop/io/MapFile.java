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
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Options;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_MAP_INDEX_SKIP_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_MAP_INDEX_SKIP_KEY;

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
    private SequenceFile.Writer data;
    private SequenceFile.Writer index;

    final private static String INDEX_INTERVAL = "io.map.index.interval";
    private int indexInterval = 128;

    private long size;
    private LongWritable position = new LongWritable();

    // the following fields are used only for checking key order
    private WritableComparator comparator;
    private DataInputBuffer inBuf = new DataInputBuffer();
    private DataOutputBuffer outBuf = new DataOutputBuffer();
    private WritableComparable lastKey;

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
    @Deprecated
    public Writer(Configuration conf, FileSystem fs, String dirName,
                  Class<? extends WritableComparable> keyClass, 
                  Class valClass) throws IOException {
      this(conf, new Path(dirName), keyClass(keyClass), valueClass(valClass));
    }

    /** Create the named map for keys of the named class. 
     * @deprecated Use Writer(Configuration, Path, Option...) instead.
     */
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
    
    private static class KeyClassOption extends Options.ClassOption
                                        implements Option {
      KeyClassOption(Class<?> value) {
        super(value);
      }
    }
    
    private static class ComparatorOption implements Option {
      private final WritableComparator value;
      ComparatorOption(WritableComparator value) {
        this.value = value;
      }
      WritableComparator getValue() {
        return value;
      }
    }

    public static Option keyClass(Class<? extends WritableComparable> value) {
      return new KeyClassOption(value);
    }
    
    public static Option comparator(WritableComparator value) {
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

    @SuppressWarnings("unchecked")
    public Writer(Configuration conf, 
                  Path dirName,
                  SequenceFile.Writer.Option... opts
                  ) throws IOException {
      KeyClassOption keyClassOption = 
        Options.getOption(KeyClassOption.class, opts);
      ComparatorOption comparatorOption =
        Options.getOption(ComparatorOption.class, opts);
      if ((keyClassOption == null) == (comparatorOption == null)) {
        throw new IllegalArgumentException("key class or comparator option "
                                           + "must be set");
      }
      this.indexInterval = conf.getInt(INDEX_INTERVAL, this.indexInterval);

      Class<? extends WritableComparable> keyClass;
      if (keyClassOption == null) {
        this.comparator = comparatorOption.getValue();
        keyClass = comparator.getKeyClass();
      } else {
        keyClass= 
          (Class<? extends WritableComparable>) keyClassOption.getValue();
        this.comparator = WritableComparator.get(keyClass, conf);
      }
      this.lastKey = comparator.newKey();
      FileSystem fs = dirName.getFileSystem(conf);

      if (!fs.mkdirs(dirName)) {
        throw new IOException("Mkdirs failed to create directory " + dirName);
      }
      Path dataFile = new Path(dirName, DATA_FILE_NAME);
      Path indexFile = new Path(dirName, INDEX_FILE_NAME);

      SequenceFile.Writer.Option[] dataOptions =
        Options.prependOptions(opts, 
                               SequenceFile.Writer.file(dataFile),
                               SequenceFile.Writer.keyClass(keyClass));
      this.data = SequenceFile.createWriter(conf, dataOptions);

      SequenceFile.Writer.Option[] indexOptions =
        Options.prependOptions(opts, SequenceFile.Writer.file(indexFile),
            SequenceFile.Writer.keyClass(keyClass),
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

    /** Close the map. */
    @Override
    public synchronized void close() throws IOException {
      data.close();
      index.close();
    }

    /** Append a key/value pair to the map.  The key must be greater or equal
     * to the previous key added to the map. */
    public synchronized void append(WritableComparable key, Writable val)
      throws IOException {

      checkKey(key);

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

    private void checkKey(WritableComparable key) throws IOException {
      // check that keys are well-ordered
      if (size != 0 && comparator.compare(lastKey, key) > 0)
        throw new IOException("key out of order: "+key+" after "+lastKey);
          
      // update lastKey with a copy of key by writing and reading
      outBuf.reset();
      key.write(outBuf);                          // write new key

      inBuf.reset(outBuf.getData(), outBuf.getLength());
      lastKey.readFields(inBuf);                  // read into lastKey
    }

  }
  
  /** Provide access to an existing map. */
  public static class Reader implements java.io.Closeable {
      
    /** Number of index entries to skip between each entry.  Zero by default.
     * Setting this to values larger than zero can facilitate opening large map
     * files using less memory. */
    private int INDEX_SKIP = 0;
      
    private WritableComparator comparator;

    private WritableComparable nextKey;
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
    private WritableComparable[] keys;
    private long[] positions;

    /** Returns the class of keys in this file. */
    public Class<?> getKeyClass() { return data.getKeyClass(); }

    /** Returns the class of values in this file. */
    public Class<?> getValueClass() { return data.getValueClass(); }

    public static interface Option extends SequenceFile.Reader.Option {}
    
    public static Option comparator(WritableComparator value) {
      return new ComparatorOption(value);
    }

    static class ComparatorOption implements Option {
      private final WritableComparator value;
      ComparatorOption(WritableComparator value) {
        this.value = value;
      }
      WritableComparator getValue() {
        return value;
      }
    }

    public Reader(Path dir, Configuration conf,
                  SequenceFile.Reader.Option... opts) throws IOException {
      ComparatorOption comparatorOption = 
        Options.getOption(ComparatorOption.class, opts);
      WritableComparator comparator =
        comparatorOption == null ? null : comparatorOption.getValue();
      INDEX_SKIP = conf.getInt(
          IO_MAP_INDEX_SKIP_KEY, IO_MAP_INDEX_SKIP_DEFAULT);
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
    
    protected synchronized void open(Path dir,
                                     WritableComparator comparator,
                                     Configuration conf, 
                                     SequenceFile.Reader.Option... options
                                     ) throws IOException {
      Path dataFile = new Path(dir, DATA_FILE_NAME);
      Path indexFile = new Path(dir, INDEX_FILE_NAME);

      // open the data
      this.data = createDataFileReader(dataFile, conf, options);
      this.firstPosition = data.getPosition();

      if (comparator == null) {
        Class<? extends WritableComparable> cls;
        cls = data.getKeyClass().asSubclass(WritableComparable.class);
        this.comparator = WritableComparator.get(cls, conf);
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
        WritableComparable lastKey = null;
        long lastIndex = -1;
        ArrayList<WritableComparable> keyBuilder = new ArrayList<WritableComparable>(1024);
        while (true) {
          WritableComparable k = comparator.newKey();

          if (!index.next(k, position))
            break;

          // check order to make sure comparator is compatible
          if (lastKey != null && comparator.compare(lastKey, k) > 0)
            throw new IOException("key out of order: "+k+" after "+lastKey);
          lastKey = k;
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

          keyBuilder.add(k);
          positions[count] = position.get();
          count++;
        }

        this.keys = keyBuilder.toArray(new WritableComparable[count]);
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
    public synchronized WritableComparable midKey() throws IOException {

      readIndex();
      if (count == 0) {
        return null;
      }
    
      return keys[(count - 1) / 2];
    }
    
    /** Reads the final key from the file.
     *
     * @param key key to read into
     */
    public synchronized void finalKey(WritableComparable key)
      throws IOException {

      long originalPosition = data.getPosition(); // save position
      try {
        readIndex();                              // make sure index is valid
        if (count > 0) {
          data.seek(positions[count-1]);          // skip to last indexed entry
        } else {
          reset();                                // start at the beginning
        }
        while (data.next(key)) {}                 // scan to eof

      } finally {
        data.seek(originalPosition);              // restore position
      }
    }

    /** Positions the reader at the named key, or if none such exists, at the
     * first entry after the named key.  Returns true iff the named key exists
     * in this map.
     */
    public synchronized boolean seek(WritableComparable key) throws IOException {
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
    private synchronized int seekInternal(WritableComparable key)
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
    private synchronized int seekInternal(WritableComparable key,
        final boolean before)
      throws IOException {
      readIndex();                                // make sure index is read

      if (seekIndex != -1                         // seeked before
          && seekIndex+1 < count           
          && comparator.compare(key, keys[seekIndex+1])<0 // before next indexed
          && comparator.compare(key, nextKey)
          >= 0) {                                 // but after last seeked
        // do nothing
      } else {
        seekIndex = binarySearch(key);
        if (seekIndex < 0)                        // decode insertion point
          seekIndex = -seekIndex-2;

        if (seekIndex == -1)                      // belongs before first entry
          seekPosition = firstPosition;           // use beginning of file
        else
          seekPosition = positions[seekIndex];    // else use index
      }
      data.seek(seekPosition);
      
      if (nextKey == null)
        nextKey = comparator.newKey();
     
      // If we're looking for the key before, we need to keep track
      // of the position we got the current key as well as the position
      // of the key before it.
      long prevPosition = -1;
      long curPosition = seekPosition;

      while (data.next(nextKey)) {
        int c = comparator.compare(key, nextKey);
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
              data.next(nextKey);
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

      return 1;
    }

    private int binarySearch(WritableComparable key) {
      int low = 0;
      int high = count-1;

      while (low <= high) {
        int mid = (low + high) >>> 1;
        WritableComparable midVal = keys[mid];
        int cmp = comparator.compare(midVal, key);

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
     * the end of the map */
    public synchronized boolean next(WritableComparable key, Writable val)
      throws IOException {
      return data.next(key, val);
    }

    /** Return the value for the named key, or null if none exists. */
    public synchronized Writable get(WritableComparable key, Writable val)
      throws IOException {
      if (seek(key)) {
        data.getCurrentValue(val);
        return val;
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
    public synchronized WritableComparable getClosest(WritableComparable key,
      Writable val)
    throws IOException {
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
    public synchronized WritableComparable getClosest(WritableComparable key,
        Writable val, final boolean before)
      throws IOException {
     
      int c = seekInternal(key, before);

      // If we didn't get an exact match, and we ended up in the wrong
      // direction relative to the query key, return null since we
      // must be at the beginning or end of the file.
      if ((!before && c > 0) ||
          (before && c < 0)) {
        return null;
      }

      data.getCurrentValue(val);
      return nextKey;
    }

    /** Close the map. */
    @Override
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
  public static long fix(FileSystem fs, Path dir,
                         Class<? extends Writable> keyClass,
                         Class<? extends Writable> valueClass, boolean dryrun,
                         Configuration conf) throws Exception {
    String dr = (dryrun ? "[DRY RUN ] " : "");
    Path data = new Path(dir, DATA_FILE_NAME);
    Path index = new Path(dir, INDEX_FILE_NAME);
    int indexInterval = conf.getInt(Writer.INDEX_INTERVAL, 128);
    if (!fs.exists(data)) {
      // there's nothing we can do to fix this!
      throw new Exception(dr + "Missing data file in " + dir + ", impossible to fix this.");
    }
    if (fs.exists(index)) {
      // no fixing needed
      return -1;
    }
    SequenceFile.Reader dataReader = 
      new SequenceFile.Reader(conf, SequenceFile.Reader.file(data));
    if (!dataReader.getKeyClass().equals(keyClass)) {
      throw new Exception(dr + "Wrong key class in " + dir + ", expected" + keyClass.getName() +
                          ", got " + dataReader.getKeyClass().getName());
    }
    if (!dataReader.getValueClass().equals(valueClass)) {
      throw new Exception(dr + "Wrong value class in " + dir + ", expected" + valueClass.getName() +
                          ", got " + dataReader.getValueClass().getName());
    }
    long cnt = 0L;
    Writable key = ReflectionUtils.newInstance(keyClass, conf);
    Writable value = ReflectionUtils.newInstance(valueClass, conf);
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
      while(dataReader.next(key, value)) {
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

  /**
   * Class to merge multiple MapFiles of same Key and Value types to one MapFile
   */
  public static class Merger {
    private Configuration conf;
    private WritableComparator comparator = null;
    private Reader[] inReaders;
    private Writer outWriter;
    private Class<Writable> valueClass = null;
    private Class<WritableComparable> keyClass = null;

    public Merger(Configuration conf) throws IOException {
      this.conf = conf;
    }

    /**
     * Merge multiple MapFiles to one Mapfile
     *
     * @param inMapFiles
     * @param outMapFile
     * @throws IOException
     */
    public void merge(Path[] inMapFiles, boolean deleteInputs,
        Path outMapFile) throws IOException {
      try {
        open(inMapFiles, outMapFile);
        mergePass();
      } finally {
        close();
      }
      if (deleteInputs) {
        for (int i = 0; i < inMapFiles.length; i++) {
          Path path = inMapFiles[i];
          delete(path.getFileSystem(conf), path.toString());
        }
      }
    }

    /*
     * Open all input files for reading and verify the key and value types. And
     * open Output file for writing
     */
    @SuppressWarnings("unchecked")
    private void open(Path[] inMapFiles, Path outMapFile) throws IOException {
      inReaders = new Reader[inMapFiles.length];
      for (int i = 0; i < inMapFiles.length; i++) {
        Reader reader = new Reader(inMapFiles[i], conf);
        if (keyClass == null || valueClass == null) {
          keyClass = (Class<WritableComparable>) reader.getKeyClass();
          valueClass = (Class<Writable>) reader.getValueClass();
        } else if (keyClass != reader.getKeyClass()
            || valueClass != reader.getValueClass()) {
          throw new HadoopIllegalArgumentException(
              "Input files cannot be merged as they"
                  + " have different Key and Value classes");
        }
        inReaders[i] = reader;
      }

      if (comparator == null) {
        Class<? extends WritableComparable> cls;
        cls = keyClass.asSubclass(WritableComparable.class);
        this.comparator = WritableComparator.get(cls, conf);
      } else if (comparator.getKeyClass() != keyClass) {
        throw new HadoopIllegalArgumentException(
            "Input files cannot be merged as they"
                + " have different Key class compared to"
                + " specified comparator");
      }

      outWriter = new MapFile.Writer(conf, outMapFile,
          MapFile.Writer.keyClass(keyClass),
          MapFile.Writer.valueClass(valueClass));
    }

    /**
     * Merge all input files to output map file.<br>
     * 1. Read first key/value from all input files to keys/values array. <br>
     * 2. Select the least key and corresponding value. <br>
     * 3. Write the selected key and value to output file. <br>
     * 4. Replace the already written key/value in keys/values arrays with the
     * next key/value from the selected input <br>
     * 5. Repeat step 2-4 till all keys are read. <br>
     */
    private void mergePass() throws IOException {
      // re-usable array
      WritableComparable[] keys = new WritableComparable[inReaders.length];
      Writable[] values = new Writable[inReaders.length];
      // Read first key/value from all inputs
      for (int i = 0; i < inReaders.length; i++) {
        keys[i] = ReflectionUtils.newInstance(keyClass, null);
        values[i] = ReflectionUtils.newInstance(valueClass, null);
        if (!inReaders[i].next(keys[i], values[i])) {
          // Handle empty files
          keys[i] = null;
          values[i] = null;
        }
      }

      do {
        int currentEntry = -1;
        WritableComparable currentKey = null;
        Writable currentValue = null;
        for (int i = 0; i < keys.length; i++) {
          if (keys[i] == null) {
            // Skip Readers reached EOF
            continue;
          }
          if (currentKey == null || comparator.compare(currentKey, keys[i]) > 0) {
            currentEntry = i;
            currentKey = keys[i];
            currentValue = values[i];
          }
        }
        if (currentKey == null) {
          // Merge Complete
          break;
        }
        // Write the selected key/value to merge stream
        outWriter.append(currentKey, currentValue);
        // Replace the already written key/value in keys/values arrays with the
        // next key/value from the selected input
        if (!inReaders[currentEntry].next(keys[currentEntry],
            values[currentEntry])) {
          // EOF for this file
          keys[currentEntry] = null;
          values[currentEntry] = null;
        }
      } while (true);
    }

    private void close() throws IOException {
      for (int i = 0; i < inReaders.length; i++) {
        IOUtils.closeStream(inReaders[i]);
        inReaders[i] = null;
      }
      if (outWriter != null) {
        outWriter.close();
        outWriter = null;
      }
    }
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
    FileSystem fs = FileSystem.getLocal(conf);
    MapFile.Reader reader = null;
    MapFile.Writer writer = null;
    try {
      reader = new MapFile.Reader(fs, in, conf);
      writer =
        new MapFile.Writer(conf, fs, out,
            reader.getKeyClass().asSubclass(WritableComparable.class),
            reader.getValueClass());

      WritableComparable<?> key = ReflectionUtils.newInstance(
          reader.getKeyClass().asSubclass(WritableComparable.class), conf);
      Writable value = ReflectionUtils.newInstance(reader.getValueClass()
        .asSubclass(Writable.class), conf);

      while (reader.next(key, value))               // copy all entries
        writer.append(key, value);
    } finally {
      IOUtils.cleanup(LOG, writer, reader);
    }
  }
}
