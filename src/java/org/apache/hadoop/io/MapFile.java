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

package org.apache.hadoop.io;

import java.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

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
public class MapFile {
  /** The name of the index file. */
  public static final String INDEX_FILE_NAME = "index";

  /** The name of the data file. */
  public static final String DATA_FILE_NAME = "data";

  protected MapFile() {}                          // no public ctor

  /** Writes a new map. */
  public static class Writer {
    private SequenceFile.Writer data;
    private SequenceFile.Writer index;

    private int indexInterval = 128;

    private long size;
    private LongWritable position = new LongWritable();

    // the following fields are used only for checking key order
    private WritableComparator comparator;
    private DataInputBuffer inBuf = new DataInputBuffer();
    private DataOutputBuffer outBuf = new DataOutputBuffer();
    private WritableComparable lastKey;


    /** Create the named map for keys of the named class. */
    public Writer(FileSystem fs, String dirName,
                  Class keyClass, Class valClass)
      throws IOException {
      this(fs, dirName, WritableComparator.get(keyClass), valClass, false);
    }

    /** Create the named map for keys of the named class. */
    public Writer(FileSystem fs, String dirName,
                  Class keyClass, Class valClass, boolean compress)
      throws IOException {
      this(fs, dirName, WritableComparator.get(keyClass), valClass, compress);
    }

    /** Create the named map using the named key comparator. */
    public Writer(FileSystem fs, String dirName,
                  WritableComparator comparator, Class valClass)
      throws IOException {
      this(fs, dirName, comparator, valClass, false);
    }
    /** Create the named map using the named key comparator. */
    public Writer(FileSystem fs, String dirName,
                  WritableComparator comparator, Class valClass,
                  boolean compress)
      throws IOException {

      this.comparator = comparator;
      this.lastKey = comparator.newKey();

      File dir = new File(dirName);
      fs.mkdirs(dir);

      File dataFile = new File(dir, DATA_FILE_NAME);
      File indexFile = new File(dir, INDEX_FILE_NAME);

      Class keyClass = comparator.getKeyClass();
      this.data =
        new SequenceFile.Writer(fs, dataFile.getPath(), keyClass, valClass,
                                compress);
      this.index =
        new SequenceFile.Writer(fs, indexFile.getPath(),
                                keyClass, LongWritable.class);
    }
    
    /** The number of entries that are added before an index entry is added.*/
    public int getIndexInterval() { return indexInterval; }

    /** Sets the index interval.
     * @see #getIndexInterval()
     */
    public void setIndexInterval(int interval) { indexInterval = interval; }

    /** Close the map. */
    public synchronized void close() throws IOException {
      data.close();
      index.close();
    }

    /** Append a key/value pair to the map.  The key must be greater or equal
     * to the previous key added to the map. */
    public synchronized void append(WritableComparable key, Writable val)
      throws IOException {

      checkKey(key);
      
      if (size % indexInterval == 0) {            // add an index entry
        position.set(data.getLength());           // point to current eof
        index.append(key, position);
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
  public static class Reader {
      
    /** Number of index entries to skip between each entry.  Zero by default.
    * Setting this to values larger than zero can facilitate opening large map
    * files using less memory. */
    private int INDEX_SKIP = 0;
      
    private WritableComparator comparator;

    private DataOutputBuffer keyBuf = new DataOutputBuffer();
    private DataOutputBuffer nextBuf = new DataOutputBuffer();
    private int nextKeyLen = -1;
    private long seekPosition = -1;
    private int seekIndex = -1;
    private long firstPosition;

    private WritableComparable getKey;

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
    public Class getKeyClass() { return data.getKeyClass(); }

    /** Returns the class of values in this file. */
    public Class getValueClass() { return data.getValueClass(); }

    /** Construct a map reader for the named map.*/
    public Reader(FileSystem fs, String dirName, Configuration conf) throws IOException {
      this(fs, dirName, null, conf);
      INDEX_SKIP = conf.getInt("io.map.index.skip", 0);
    }

    /** Construct a map reader for the named map using the named comparator.*/
    public Reader(FileSystem fs, String dirName, WritableComparator comparator, Configuration conf)
      throws IOException {
      File dir = new File(dirName);
      File dataFile = new File(dir, DATA_FILE_NAME);
      File indexFile = new File(dir, INDEX_FILE_NAME);

      // open the data
      this.data = new SequenceFile.Reader(fs, dataFile.getPath(),  conf);
      this.firstPosition = data.getPosition();

      if (comparator == null)
        this.comparator = WritableComparator.get(data.getKeyClass());
      else
        this.comparator = comparator;

      this.getKey = this.comparator.newKey();

      // open the index
      this.index = new SequenceFile.Reader(fs, indexFile.getPath(), conf);
    }

    private void readIndex() throws IOException {
      // read the index entirely into memory
      if (this.keys != null)
        return;
      this.count = 0;
      this.keys = new WritableComparable[1024];
      this.positions = new long[1024];
      try {
        int skip = INDEX_SKIP;
        LongWritable position = new LongWritable();
        WritableComparable lastKey = null;
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

          if (count == keys.length) {                // time to grow arrays
            int newLength = (keys.length*3)/2;
            WritableComparable[] newKeys = new WritableComparable[newLength];
            long[] newPositions = new long[newLength];
            System.arraycopy(keys, 0, newKeys, 0, count);
            System.arraycopy(positions, 0, newPositions, 0, count);
            keys = newKeys;
            positions = newPositions;
          }

          keys[count] = k;
          positions[count] = position.get();
          count++;
        }
      } catch (EOFException e) {
        SequenceFile.LOG.warning("Unexpected EOF reading " + index +
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
    public synchronized boolean seek(WritableComparable key)
      throws IOException {
      readIndex();                                // make sure index is read
      keyBuf.reset();                             // write key to keyBuf
      key.write(keyBuf);

      if (seekIndex != -1                         // seeked before
          && seekIndex+1 < count           
          && comparator.compare(key,keys[seekIndex+1])<0 // before next indexed
          && comparator.compare(keyBuf.getData(), 0, keyBuf.getLength(),
                                nextBuf.getData(), 0, nextKeyLen)
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
      
      while ((nextKeyLen = data.next(nextBuf.reset())) != -1) {
        int c = comparator.compare(keyBuf.getData(), 0, keyBuf.getLength(),
                                   nextBuf.getData(), 0, nextKeyLen);
        if (c <= 0) {                             // at or beyond desired
          data.seek(seekPosition);                // back off to previous
          return c == 0;
        }
        seekPosition = data.getPosition();
      }

      return false;
    }

    private int binarySearch(WritableComparable key) {
      int low = 0;
      int high = count-1;

      while (low <= high) {
        int mid = (low + high) >> 1;
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
        next(getKey, val);                        // don't smash key
        return val;
      } else
        return null;
    }

    /** Close the map. */
    public synchronized void close() throws IOException {
      if (! indexClosed) {
	index.close();
      }
      data.close();
    }

  }

  /** Renames an existing map directory. */
  public static void rename(FileSystem fs, String oldName, String newName)
    throws IOException {
    File oldDir = new File(oldName);
    File newDir = new File(newName);
    if (!fs.rename(oldDir, newDir)) {
      throw new IOException("Could not rename " + oldDir + " to " + newDir);
    }
  }

  /** Deletes the named map file. */
  public static void delete(FileSystem fs, String name) throws IOException {
    File dir = new File(name);
    File data = new File(dir, DATA_FILE_NAME);
    File index = new File(dir, INDEX_FILE_NAME);

    fs.delete(data);
    fs.delete(index);
    fs.delete(dir);
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
  public static long fix(FileSystem fs, File dir,
          Class keyClass, Class valueClass, boolean dryrun, Configuration conf) throws Exception {
    String dr = (dryrun ? "[DRY RUN ] " : "");
    File data = new File(dir, DATA_FILE_NAME);
    File index = new File(dir, INDEX_FILE_NAME);
    int indexInterval = 128;
    if (!fs.exists(data)) {
      // there's nothing we can do to fix this!
      throw new Exception(dr + "Missing data file in " + dir + ", impossible to fix this.");
    }
    if (fs.exists(index)) {
      // no fixing needed
      return -1;
    }
    SequenceFile.Reader dataReader = new SequenceFile.Reader(fs, data.toString(), conf);
    if (!dataReader.getKeyClass().equals(keyClass)) {
      throw new Exception(dr + "Wrong key class in " + dir + ", expected" + keyClass.getName() +
              ", got " + dataReader.getKeyClass().getName());
    }
    if (!dataReader.getValueClass().equals(valueClass)) {
      throw new Exception(dr + "Wrong value class in " + dir + ", expected" + valueClass.getName() +
              ", got " + dataReader.getValueClass().getName());
    }
    long cnt = 0L;
    Writable key = (Writable)keyClass.getConstructor(new Class[0]).newInstance(new Object[0]);
    Writable value = (Writable)valueClass.getConstructor(new Class[0]).newInstance(new Object[0]);
    SequenceFile.Writer indexWriter = null;
    if (!dryrun) indexWriter = new SequenceFile.Writer(fs, index.toString(), keyClass, LongWritable.class);
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


  public static void main(String[] args) throws Exception {
    String usage = "Usage: MapFile inFile outFile";
      
    if (args.length != 2) {
      System.err.println(usage);
      System.exit(-1);
    }
      
    String in = args[0];
    String out = args[1];

    Configuration conf = new Configuration();
    int ioFileBufferSize = conf.getInt("io.file.buffer.size", 4096);
    FileSystem fs = new LocalFileSystem(conf);
    MapFile.Reader reader = new MapFile.Reader(fs, in, conf);
    MapFile.Writer writer =
      new MapFile.Writer(fs, out, reader.getKeyClass(), reader.getValueClass());

    WritableComparable key =
      (WritableComparable)reader.getKeyClass().newInstance();
    Writable value = (Writable)reader.getValueClass().newInstance();

    while (reader.next(key, value))               // copy all entries
      writer.append(key, value);

    writer.close();
  }

}
