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
import java.util.*;
import java.util.zip.*;
import java.util.logging.*;
import java.net.InetAddress;
import java.rmi.server.UID;
import java.security.MessageDigest;
import org.apache.lucene.util.PriorityQueue;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.LogFormatter;

/** Support for flat files of binary key/value pairs. */
public class SequenceFile {
  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.hadoop.io.SequenceFile");

  private SequenceFile() {}                         // no public ctor

  private static byte[] VERSION = new byte[] {
    (byte)'S', (byte)'E', (byte)'Q', 3
  };

  private static final int SYNC_ESCAPE = -1;      // "length" of sync entries
  private static final int SYNC_HASH_SIZE = 16;   // number of bytes in hash 
  private static final int SYNC_SIZE = 4+SYNC_HASH_SIZE; // escape + hash

  /** The number of bytes between sync points.*/
  public static final int SYNC_INTERVAL = 100*SYNC_SIZE; 

  /** Write key/value pairs to a sequence-format file. */
  public static class Writer {
    private FSDataOutputStream out;
    private DataOutputBuffer buffer = new DataOutputBuffer();
    private FileSystem fs = null;
    private File target = null;

    private Class keyClass;
    private Class valClass;

    private boolean deflateValues;
    private Deflater deflater = new Deflater(Deflater.BEST_SPEED);
    private DeflaterOutputStream deflateFilter =
      new DeflaterOutputStream(buffer, deflater);
    private DataOutputStream deflateOut =
      new DataOutputStream(new BufferedOutputStream(deflateFilter));

    // Insert a globally unique 16-byte value every few entries, so that one
    // can seek into the middle of a file and then synchronize with record
    // starts and ends by scanning for this value.
    private long lastSyncPos;                     // position of last sync
    private byte[] sync;                          // 16 random bytes
    {
      try {                                       // use hash of uid + host
        MessageDigest digester = MessageDigest.getInstance("MD5");
        digester.update((new UID()+"@"+InetAddress.getLocalHost()).getBytes());
        sync = digester.digest();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    /** Create the named file. */
    public Writer(FileSystem fs, String name,
                  Class keyClass, Class valClass)
      throws IOException {
      this(fs, name, keyClass, valClass, false);
    }
    
    /** Create the named file.
     * @param compress if true, values are compressed.
     */
    public Writer(FileSystem fs, String name,
                  Class keyClass, Class valClass, boolean compress)
      throws IOException {
      this.fs = fs;
      this.target = new File(name);
      init(fs.create(target), keyClass, valClass, compress);
    }
    
    /** Write to an arbitrary stream using a specified buffer size. */
    private Writer(FSDataOutputStream out,
                   Class keyClass, Class valClass, boolean compress)
      throws IOException {
      init(out, keyClass, valClass, compress);
    }
    
    /** Write and flush the file header. */
    private void init(FSDataOutputStream out,
                      Class keyClass, Class valClass,
                      boolean compress) throws IOException {
      this.out = out;
      this.out.write(VERSION);

      this.keyClass = keyClass;
      this.valClass = valClass;

      this.deflateValues = compress;

      new UTF8(WritableName.getName(keyClass)).write(this.out);
      new UTF8(WritableName.getName(valClass)).write(this.out);

      this.out.writeBoolean(deflateValues);

      out.write(sync);                            // write the sync bytes

      this.out.flush();                           // flush header
    }
    

    /** Returns the class of keys in this file. */
    public Class getKeyClass() { return keyClass; }

    /** Returns the class of values in this file. */
    public Class getValueClass() { return valClass; }


    /** Close the file. */
    public synchronized void close() throws IOException {
      if (out != null) {
        out.close();
        out = null;
      }
    }

    /** Append a key/value pair. */
    public synchronized void append(Writable key, Writable val)
      throws IOException {
      if (key.getClass() != keyClass)
        throw new IOException("wrong key class: "+key+" is not "+keyClass);
      if (val.getClass() != valClass)
        throw new IOException("wrong value class: "+val+" is not "+valClass);

      buffer.reset();

      key.write(buffer);
      int keyLength = buffer.getLength();
      if (keyLength == 0)
        throw new IOException("zero length keys not allowed: " + key);

      if (deflateValues) {
        deflater.reset();
        val.write(deflateOut);
        deflateOut.flush();
        deflateFilter.finish();
      } else {
        val.write(buffer);
      }

      append(buffer.getData(), 0, buffer.getLength(), keyLength);
    }

    /** Append a key/value pair. */
    public synchronized void append(byte[] data, int start, int length,
                                    int keyLength) throws IOException {
      if (keyLength == 0)
        throw new IOException("zero length keys not allowed");

      if (sync != null &&
          out.getPos() >= lastSyncPos+SYNC_INTERVAL) { // time to emit sync
        lastSyncPos = out.getPos();               // update lastSyncPos
        //LOG.info("sync@"+lastSyncPos);
        out.writeInt(SYNC_ESCAPE);                // escape it
        out.write(sync);                          // write sync
      }

      out.writeInt(length);                       // total record length
      out.writeInt(keyLength);                    // key portion length
      out.write(data, start, length);             // data

    }

    /** Returns the current length of the output file. */
    public synchronized long getLength() throws IOException {
      return out.getPos();
    }

  }

  /** Writes key/value pairs from a sequence-format file. */
  public static class Reader {
    private String file;
    private FSDataInputStream in;
    private DataOutputBuffer outBuf = new DataOutputBuffer();
    private DataInputBuffer inBuf = new DataInputBuffer();
    private FileSystem fs = null;

    private byte[] version = new byte[VERSION.length];

    private Class keyClass;
    private Class valClass;

    private byte[] sync = new byte[SYNC_HASH_SIZE];
    private byte[] syncCheck = new byte[SYNC_HASH_SIZE];
    private boolean syncSeen;

    private long end;
    private int keyLength;

    private boolean inflateValues;
    private byte[] inflateIn = new byte[8192];
    private DataOutputBuffer inflateOut = new DataOutputBuffer();
    private Inflater inflater = new Inflater();
    private Configuration conf;

    /** Open the named file. */
    public Reader(FileSystem fs, String file, Configuration conf) throws IOException {
      this(fs, file, conf.getInt("io.file.buffer.size", 4096));
      this.conf = conf;
    }

    private Reader(FileSystem fs, String name, int bufferSize) throws IOException {
      this.fs = fs;
      this.file = name;
      File file = new File(name);
      this.in = fs.open(file, bufferSize);
      this.end = fs.getLength(file);
      init();
    }
    
    private Reader(FileSystem fs, String file, int bufferSize, long start, long length)
      throws IOException {
      this.fs = fs;
      this.file = file;
      this.in = fs.open(new File(file), bufferSize);
      seek(start);
      init();

      this.end = in.getPos() + length;
    }
    
    private void init() throws IOException {
      in.readFully(version);

      if ((version[0] != VERSION[0]) ||
          (version[1] != VERSION[1]) ||
          (version[2] != VERSION[2]))
        throw new IOException(file + " not a SequenceFile");

      if (version[3] > VERSION[3])
        throw new VersionMismatchException(VERSION[3], version[3]);

      UTF8 className = new UTF8();
      
      className.readFields(in);                   // read key class name
      this.keyClass = WritableName.getClass(className.toString());
      
      className.readFields(in);                   // read val class name
      this.valClass = WritableName.getClass(className.toString());

      if (version[3] > 2) {                       // if version > 2
        this.inflateValues = in.readBoolean();    // is compressed?
      }

      if (version[3] > 1) {                       // if version > 1
        in.readFully(sync);                       // read sync bytes
      }
    }
    
    /** Close the file. */
    public synchronized void close() throws IOException {
      in.close();
    }

    /** Returns the class of keys in this file. */
    public Class getKeyClass() { return keyClass; }

    /** Returns the class of values in this file. */
    public Class getValueClass() { return valClass; }

    /** Returns true if values are compressed. */
    public boolean isCompressed() { return inflateValues; }

    /** Read the next key in the file into <code>key</code>, skipping its
     * value.  True if another entry exists, and false at end of file. */
    public synchronized boolean next(Writable key) throws IOException {
      if (key.getClass() != keyClass)
        throw new IOException("wrong key class: "+key+" is not "+keyClass);

      outBuf.reset();

      keyLength = next(outBuf);
      if (keyLength < 0)
        return false;

      inBuf.reset(outBuf.getData(), outBuf.getLength());

      key.readFields(inBuf);
      if (inBuf.getPosition() != keyLength)
        throw new IOException(key + " read " + inBuf.getPosition()
                              + " bytes, should read " + keyLength);

      return true;
    }

    /** Read the next key/value pair in the file into <code>key</code> and
     * <code>val</code>.  Returns true if such a pair exists and false when at
     * end of file */
    public synchronized boolean next(Writable key, Writable val)
      throws IOException {
      if (val.getClass() != valClass)
        throw new IOException("wrong value class: "+val+" is not "+valClass);

      boolean more = next(key);

      if (more) {

        if (inflateValues) {
          inflater.reset();
          inflater.setInput(outBuf.getData(), keyLength,
                            outBuf.getLength()-keyLength);
          inflateOut.reset();
          while (!inflater.finished()) {
            try {
              int count = inflater.inflate(inflateIn);
              inflateOut.write(inflateIn, 0, count);
            } catch (DataFormatException e) {
              throw new IOException (e.toString());
            }
          }
          inBuf.reset(inflateOut.getData(), inflateOut.getLength());
        }
        if(val instanceof Configurable) {
          ((Configurable) val).setConf(this.conf);
        }
        val.readFields(inBuf);

        if (inBuf.getPosition() != inBuf.getLength())
          throw new IOException(val+" read "+(inBuf.getPosition()-keyLength)
                                + " bytes, should read " +
                                (inBuf.getLength()-keyLength));
      }

      return more;
    }

    /** Read the next key/value pair in the file into <code>buffer</code>.
     * Returns the length of the key read, or -1 if at end of file.  The length
     * of the value may be computed by calling buffer.getLength() before and
     * after calls to this method. */
    public synchronized int next(DataOutputBuffer buffer) throws IOException {
      if (in.getPos() >= end)
        return -1;

      try {
        int length = in.readInt();

        if (version[3] > 1 && sync != null &&
            length == SYNC_ESCAPE) {              // process a sync entry
          //LOG.info("sync@"+in.getPos());
          in.readFully(syncCheck);                // read syncCheck
          if (!Arrays.equals(sync, syncCheck))    // check it
            throw new IOException("File is corrupt!");
          syncSeen = true;
          length = in.readInt();                  // re-read length
        } else {
          syncSeen = false;
        }
        
        int keyLength = in.readInt();
        buffer.write(in, length);
        return keyLength;

      } catch (ChecksumException e) {             // checksum failure
        handleChecksumException(e);
        return next(buffer);
      }
    }

    private void handleChecksumException(ChecksumException e)
      throws IOException {
      if (this.conf.getBoolean("io.skip.checksum.errors", false)) {
        LOG.warning("Bad checksum at "+getPosition()+". Skipping entries.");
        sync(getPosition()+this.conf.getInt("io.bytes.per.checksum", 512));
      } else {
        throw e;
      }
    }

    /** Set the current byte position in the input file. */
    public synchronized void seek(long position) throws IOException {
      in.seek(position);
    }

    /** Seek to the next sync mark past a given position.*/
    public synchronized void sync(long position) throws IOException {
      if (position+SYNC_SIZE >= end) {
        seek(end);
        return;
      }

      try {
        seek(position+4);                         // skip escape
        in.readFully(syncCheck);
        int syncLen = sync.length;
        for (int i = 0; in.getPos() < end; i++) {
          int j = 0;
          for (; j < syncLen; j++) {
            if (sync[j] != syncCheck[(i+j)%syncLen])
              break;
          }
          if (j == syncLen) {
            in.seek(in.getPos() - SYNC_SIZE);     // position before sync
            return;
          }
          syncCheck[i%syncLen] = in.readByte();
        }
      } catch (ChecksumException e) {             // checksum failure
        handleChecksumException(e);
      }
    }

    /** Returns true iff the previous call to next passed a sync mark.*/
    public boolean syncSeen() { return syncSeen; }

    /** Return the current byte position in the input file. */
    public synchronized long getPosition() throws IOException {
      return in.getPos();
    }

    /** Returns the name of the file. */
    public String toString() {
      return file;
    }

  }

  /** Sorts key/value pairs in a sequence-format file.
   *
   * <p>For best performance, applications should make sure that the {@link
   * Writable#readFields(DataInput)} implementation of their keys is
   * very efficient.  In particular, it should avoid allocating memory.
   */
  public static class Sorter {

    private WritableComparator comparator;

    private String inFile;                        // when sorting
    private String[] inFiles;                     // when merging

    private String outFile;

    private int memory; // bytes
    private int factor; // merged per pass

    private FileSystem fs = null;

    private Class keyClass;
    private Class valClass;

    private Configuration conf;

    /** Sort and merge files containing the named classes. */
    public Sorter(FileSystem fs, Class keyClass, Class valClass, Configuration conf)  {
      this(fs, new WritableComparator(keyClass), valClass, conf);
    }

    /** Sort and merge using an arbitrary {@link WritableComparator}. */
    public Sorter(FileSystem fs, WritableComparator comparator, Class valClass, Configuration conf) {
      this.fs = fs;
      this.comparator = comparator;
      this.keyClass = comparator.getKeyClass();
      this.valClass = valClass;
      this.memory = conf.getInt("io.sort.mb", 100) * 1024 * 1024;
      this.factor = conf.getInt("io.sort.factor", 100);
      this.conf = conf;
    }

    /** Set the number of streams to merge at once.*/
    public void setFactor(int factor) { this.factor = factor; }

    /** Get the number of streams to merge at once.*/
    public int getFactor() { return factor; }

    /** Set the total amount of buffer memory, in bytes.*/
    public void setMemory(int memory) { this.memory = memory; }

    /** Get the total amount of buffer memory, in bytes.*/
    public int getMemory() { return memory; }

    /** Perform a file sort.*/
    public void sort(String inFile, String outFile) throws IOException {
      if (fs.exists(new File(outFile))) {
        throw new IOException("already exists: " + outFile);
      }

      this.inFile = inFile;
      this.outFile = outFile;

      int segments = sortPass();
      int pass = 1;
      while (segments > 1) {
        segments = mergePass(pass, segments <= factor);
        pass++;
      }
    }

    private int sortPass() throws IOException {
      LOG.fine("running sort pass");
      SortPass sortPass = new SortPass(this.conf);         // make the SortPass
      try {
        return sortPass.run();                    // run it
      } finally {
        sortPass.close();                         // close it
      }
    }

    private class SortPass {
      private int limit = memory/4;
      private DataOutputBuffer buffer = new DataOutputBuffer();
      private byte[] rawBuffer;

      private int[] starts = new int[1024];
      private int[] pointers = new int[starts.length];
      private int[] pointersCopy = new int[starts.length];
      private int[] keyLengths = new int[starts.length];
      private int[] lengths = new int[starts.length];
      
      private Reader in;
      private FSDataOutputStream out;
        private String outName;

      public SortPass(Configuration conf) throws IOException {
        in = new Reader(fs, inFile, conf);
      }
      
      public int run() throws IOException {
        int segments = 0;
        boolean atEof = false;
        while (!atEof) {
          int count = 0;
          buffer.reset();
          while (!atEof && buffer.getLength() < limit) {

            int start = buffer.getLength();       // read an entry into buffer
            int keyLength = in.next(buffer);
            int length = buffer.getLength() - start;

            if (keyLength == -1) {
              atEof = true;
              break;
            }

            if (count == starts.length)
              grow();

            starts[count] = start;                // update pointers
            pointers[count] = count;
            lengths[count] = length;
            keyLengths[count] = keyLength;

            count++;
          }

          // buffer is full -- sort & flush it
          LOG.finer("flushing segment " + segments);
          rawBuffer = buffer.getData();
          sort(count);
          flush(count, segments==0 && atEof);
          segments++;
        }
        return segments;
      }

      public void close() throws IOException {
        in.close();

        if (out != null) {
          out.close();
        }
      }

      private void grow() {
        int newLength = starts.length * 3 / 2;
        starts = grow(starts, newLength);
        pointers = grow(pointers, newLength);
        pointersCopy = new int[newLength];
        keyLengths = grow(keyLengths, newLength);
        lengths = grow(lengths, newLength);
      }

      private int[] grow(int[] old, int newLength) {
        int[] result = new int[newLength];
        System.arraycopy(old, 0, result, 0, old.length);
        return result;
      }

      private void flush(int count, boolean done) throws IOException {
        if (out == null) {
          outName = done ? outFile : outFile+".0";
          out = fs.create(new File(outName));
        }

        if (!done) {                              // an intermediate file

          long length = buffer.getLength();       // compute its size
          length += count*8;                      // allow for length/keyLength

          out.writeLong(length);                  // write size
          out.writeLong(count);                   // write count
        }

        Writer writer = new Writer(out, keyClass, valClass, in.isCompressed());
        if (!done) {
          writer.sync = null;                     // disable sync on temp files
        }

        for (int i = 0; i < count; i++) {         // write in sorted order
          int p = pointers[i];
          writer.append(rawBuffer, starts[p], lengths[p], keyLengths[p]);
        }
      }

      private void sort(int count) {
        System.arraycopy(pointers, 0, pointersCopy, 0, count);
        mergeSort(pointersCopy, pointers, 0, count);
      }

      private int compare(int i, int j) {
        return comparator.compare(rawBuffer, starts[i], keyLengths[i],
                                  rawBuffer, starts[j], keyLengths[j]);
      }

      private void mergeSort(int src[], int dest[], int low, int high) {
        int length = high - low;

        // Insertion sort on smallest arrays
        if (length < 7) {
          for (int i=low; i<high; i++)
            for (int j=i; j>low && compare(dest[j-1], dest[j])>0; j--)
              swap(dest, j, j-1);
          return;
        }

        // Recursively sort halves of dest into src
        int mid = (low + high) >> 1;
        mergeSort(dest, src, low, mid);
        mergeSort(dest, src, mid, high);

        // If list is already sorted, just copy from src to dest.  This is an
        // optimization that results in faster sorts for nearly ordered lists.
        if (compare(src[mid-1], src[mid]) <= 0) {
          System.arraycopy(src, low, dest, low, length);
          return;
        }

        // Merge sorted halves (now in src) into dest
        for(int i = low, p = low, q = mid; i < high; i++) {
          if (q>=high || p<mid && compare(src[p], src[q]) <= 0)
            dest[i] = src[p++];
          else
            dest[i] = src[q++];
        }
      }

      private void swap(int x[], int a, int b) {
	int t = x[a];
	x[a] = x[b];
	x[b] = t;
      }
    }

    private int mergePass(int pass, boolean last) throws IOException {
      LOG.fine("running merge pass=" + pass);
      MergePass mergePass = new MergePass(pass, last);
      try {                                       // make a merge pass
        return mergePass.run();                  // run it
      } finally {
        mergePass.close();                       // close it
      }
    }

    private class MergePass {
      private int pass;
      private boolean last;

      private MergeQueue queue;
      private FSDataInputStream in;
      private String inName;

      public MergePass(int pass, boolean last) throws IOException {
        this.pass = pass;
        this.last = last;

        this.queue =
          new MergeQueue(factor, last ? outFile : outFile+"."+pass, last);

        this.inName = outFile+"."+(pass-1);
        this.in = fs.open(new File(inName));
      }

      public void close() throws IOException {
        in.close();                               // close and delete input
        fs.delete(new File(inName));

        queue.close();                            // close queue
      }

      public int run() throws IOException {
        int segments = 0;
        long end = fs.getLength(new File(inName));

        while (in.getPos() < end) {
          LOG.finer("merging segment " + segments);
          long totalLength = 0;
          long totalCount = 0;
          while (in.getPos() < end && queue.size() < factor) {
            long length = in.readLong();
            long count = in.readLong();

            totalLength += length;

            totalCount+= count;

            Reader reader = new Reader(fs, inName, memory/(factor+1),
                                       in.getPos(), length);
            reader.sync = null;                   // disable sync on temp files

            MergeStream ms = new MergeStream(reader); // add segment to queue
            if (ms.next()) {
              queue.add(ms);
            }
            in.seek(reader.end);
          }

          if (!last) {                             // intermediate file
            queue.out.writeLong(totalLength);     // write size
            queue.out.writeLong(totalCount);      // write count
          }

          queue.merge();                          // do a merge

          segments++;
        }

        return segments;
      }
    }

    /** Merge the provided files.*/
    public void merge(String[] inFiles, String outFile) throws IOException {
      this.inFiles = inFiles;
      this.outFile = outFile;
      this.factor = inFiles.length;

      if (new File(outFile).exists()) {
        throw new IOException("already exists: " + outFile);
      }

      MergeFiles mergeFiles = new MergeFiles();
      try {                                       // make a merge pass
        mergeFiles.run();                         // run it
      } finally {
        mergeFiles.close();                       // close it
      }

    }

    private class MergeFiles {
      private MergeQueue queue;

      public MergeFiles() throws IOException {
        this.queue = new MergeQueue(factor, outFile, true);
      }

      public void close() throws IOException {
        queue.close();
      }

      public void run() throws IOException {
        LOG.finer("merging files=" + inFiles.length);
        for (int i = 0; i < inFiles.length; i++) {
          String inFile = inFiles[i];
          MergeStream ms =
            new MergeStream(new Reader(fs, inFile, memory/(factor+1)));
          if (ms.next())
            queue.put(ms);
        }

        queue.merge();
      }
    }

    private class MergeStream {
      private Reader in;

      private DataOutputBuffer buffer = new DataOutputBuffer();
      private int keyLength;
      
      public MergeStream(Reader reader) throws IOException {
        if (reader.keyClass != keyClass)
          throw new IOException("wrong key class: " + reader.getKeyClass() +
                                " is not " + keyClass);
        if (reader.valClass != valClass)
          throw new IOException("wrong value class: "+reader.getValueClass()+
                                " is not " + valClass);
        this.in = reader;
      }

      public boolean next() throws IOException {
        buffer.reset();
        keyLength = in.next(buffer);
        return keyLength >= 0;
      }
    }

    private class MergeQueue extends PriorityQueue {
      private FSDataOutputStream out;
      private boolean done;
      private boolean compress;

      public void add(MergeStream stream) throws IOException {
        if (size() == 0) {
          compress = stream.in.isCompressed();
        } else if (compress != stream.in.isCompressed()) {
          throw new IOException("All merged files must be compressed or not.");
        }
        put(stream);
      }

      public MergeQueue(int size, String outName, boolean done)
        throws IOException {
        initialize(size);
        this.out = fs.create(new File(outName), true, memory/(factor+1));
        this.done = done;
      }

      protected boolean lessThan(Object a, Object b) {
        MergeStream msa = (MergeStream)a;
        MergeStream msb = (MergeStream)b;
        return comparator.compare(msa.buffer.getData(), 0, msa.keyLength,
                                  msb.buffer.getData(), 0, msb.keyLength) < 0;
      }

      public void merge() throws IOException {
        Writer writer = new Writer(out, keyClass, valClass, compress);
        if (!done) {
          writer.sync = null;                     // disable sync on temp files
        }

        while (size() != 0) {
          MergeStream ms = (MergeStream)top();
          DataOutputBuffer buffer = ms.buffer;    // write top entry
          writer.append(buffer.getData(), 0, buffer.getLength(), ms.keyLength);
          
          if (ms.next()) {                        // has another entry
            adjustTop();
          } else {
            pop();                                // done with this file
            ms.in.close();
          }
        }
      }

      public void close() throws IOException {
        MergeStream ms;                           // close inputs
        while ((ms = (MergeStream)pop()) != null) {
          ms.in.close();
        }
        out.close();                              // close output
      }
    }
  }

}
