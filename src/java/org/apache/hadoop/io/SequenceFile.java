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
import java.net.InetAddress;
import java.rmi.server.UID;
import java.security.MessageDigest;
import org.apache.lucene.util.PriorityQueue;
import org.apache.commons.logging.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.Progressable;

/** Support for flat files of binary key/value pairs. */
public class SequenceFile {
  public static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.io.SequenceFile");

  private SequenceFile() {}                         // no public ctor

  private static final byte BLOCK_COMPRESS_VERSION = (byte)4;
  private static byte[] VERSION = new byte[] {
    (byte)'S', (byte)'E', (byte)'Q', BLOCK_COMPRESS_VERSION
  };

  private static final int SYNC_ESCAPE = -1;      // "length" of sync entries
  private static final int SYNC_HASH_SIZE = 16;   // number of bytes in hash 
  private static final int SYNC_SIZE = 4+SYNC_HASH_SIZE; // escape + hash

  /** The number of bytes between sync points.*/
  public static final int SYNC_INTERVAL = 100*SYNC_SIZE; 

  /** The type of compression.
   * @see SequenceFile#Writer
   */
  public static enum CompressionType {
    /** Do not compress records. */
    NONE, 
    /** Compress values only, each separately. */
    RECORD,
    /** Compress sequences of records together in blocks. */
    BLOCK
  }
  
  /**
   * Construct the preferred type of SequenceFile Writer.
   * @param fs The configured filesystem. 
   * @param conf The configuration.
   * @param name The name of the file. 
   * @param keyClass The 'key' type.
   * @param valClass The 'value' type.
   * @param compressionType The compression type.
   * @return Returns the handle to the constructed SequenceFile Writer.
   * @throws IOException
   */
  public static Writer 
  createWriter(FileSystem fs, Configuration conf, Path name, 
      Class keyClass, Class valClass, CompressionType compressionType) 
  throws IOException {
    Writer writer = null;
    
    if (compressionType == CompressionType.NONE) {
      writer = new Writer(fs, conf, name, keyClass, valClass);
    } else if (compressionType == CompressionType.RECORD) {
      writer = new RecordCompressWriter(fs, conf, name, keyClass, valClass);
    } else if (compressionType == CompressionType.BLOCK){
      writer = new BlockCompressWriter(fs, conf, name, keyClass, valClass);
    }
    
    return writer;
  }
  
  /**
   * Construct the preferred type of SequenceFile Writer.
   * @param fs The configured filesystem. 
   * @param conf The configuration.
   * @param name The name of the file. 
   * @param keyClass The 'key' type.
   * @param valClass The 'value' type.
   * @param compressionType The compression type.
   * @param progress The Progressable object to track progress.
   * @return Returns the handle to the constructed SequenceFile Writer.
   * @throws IOException
   */
  public static Writer
  createWriter(FileSystem fs, Configuration conf, Path name, 
      Class keyClass, Class valClass, CompressionType compressionType,
      Progressable progress) throws IOException {
    Writer writer = null;
    
    if (compressionType == CompressionType.NONE) {
      writer = new Writer(fs, conf, name, keyClass, valClass, progress);
    } else if (compressionType == CompressionType.RECORD) {
      writer = new RecordCompressWriter(fs, conf, name, 
          keyClass, valClass, progress);
    } else if (compressionType == CompressionType.BLOCK){
      writer = new BlockCompressWriter(fs, conf, name, 
          keyClass, valClass, progress);
    }
    
    return writer;
  }

  /**
   * Construct the preferred type of 'raw' SequenceFile Writer.
   * @param out The stream on top which the writer is to be constructed.
   * @param keyClass The 'key' type.
   * @param valClass The 'value' type.
   * @param compress Compress data?
   * @param blockCompress Compress blocks?
   * @return Returns the handle to the constructed SequenceFile Writer.
   * @throws IOException
   */
  private static Writer
  createWriter(FSDataOutputStream out, 
      Class keyClass, Class valClass, boolean compress, boolean blockCompress)
  throws IOException {
    Writer writer = null;

    if (!compress) {
      writer = new Writer(out, keyClass, valClass);
    } else if (compress && !blockCompress) {
      writer = new RecordCompressWriter(out, keyClass, valClass);
    } else {
      writer = new BlockCompressWriter(out, keyClass, valClass);
    }
    
    return writer;
  }

  /** The interface to 'raw' values of SequenceFiles. */
  public static interface ValueBytes {

    /** Writes the uncompressed bytes to the outStream.
     * @param outStream : Stream to write uncompressed bytes into.
     * @throws IOException
     */
    public void writeUncompressedBytes(DataOutputStream outStream)
    throws IOException;

    /** Write compressed bytes to outStream. 
     * Note: that it will NOT compress the bytes if they are not compressed.
     * @param outStream : Stream to write compressed bytes into.
     */
    public void writeCompressedBytes(DataOutputStream outStream) 
    throws IllegalArgumentException, IOException;
  }
  
  private static class UncompressedBytes implements ValueBytes {
    private int dataSize;
    private byte[] data;
    
    private UncompressedBytes() {
      data = null;
      dataSize = 0;
    }
    
    private void reset(DataInputStream in, int length) throws IOException {
      data = new byte[length];
      dataSize = -1;
      
      in.readFully(data);
      dataSize = data.length;
    }
    
    public int getSize() {
      return dataSize;
    }
    
    public void writeUncompressedBytes(DataOutputStream outStream)
    throws IOException {
      outStream.write(data, 0, dataSize);
    }

    public void writeCompressedBytes(DataOutputStream outStream) 
    throws IllegalArgumentException, IOException {
      throw 
      new IllegalArgumentException("UncompressedBytes cannot be compressed!");
    }

  } // UncompressedBytes
  
  private static class CompressedBytes implements ValueBytes {
    private int dataSize;
    private byte[] data;
    private Inflater zlibInflater = null;

    private CompressedBytes() {
      data = null;
      dataSize = 0;
    }

    private void reset(DataInputStream in, int length) throws IOException {
      data = new byte[length];
      dataSize = -1;

      in.readFully(data);
      dataSize = data.length;
    }
    
    public int getSize() {
      return dataSize;
    }
    
    public void writeUncompressedBytes(DataOutputStream outStream)
    throws IOException {
      if (zlibInflater == null) {
        zlibInflater = new Inflater();
      } else {
        zlibInflater.reset();
      }
      zlibInflater.setInput(data, 0, dataSize);

      byte[] buffer = new byte[8192];
      while (!zlibInflater.finished()) {
        try {
          int noDecompressedBytes = zlibInflater.inflate(buffer);
          outStream.write(buffer, 0, noDecompressedBytes);
        } catch (DataFormatException e) {
          throw new IOException (e.toString());
        }
      }
    }

    public void writeCompressedBytes(DataOutputStream outStream) 
    throws IllegalArgumentException, IOException {
      outStream.write(data, 0, dataSize);
    }

  } // CompressedBytes
  
  /** Write key/value pairs to a sequence-format file. */
  public static class Writer {
    FSDataOutputStream out;
    DataOutputBuffer buffer = new DataOutputBuffer();
    Path target = null;

    Class keyClass;
    Class valClass;

    private boolean compress;
    Deflater deflater = new Deflater(Deflater.BEST_SPEED);
    DeflaterOutputStream deflateFilter =
      new DeflaterOutputStream(buffer, deflater);
    DataOutputStream deflateOut =
      new DataOutputStream(new BufferedOutputStream(deflateFilter));

    // Insert a globally unique 16-byte value every few entries, so that one
    // can seek into the middle of a file and then synchronize with record
    // starts and ends by scanning for this value.
    long lastSyncPos;                     // position of last sync
    byte[] sync;                          // 16 random bytes
    {
      try {                                       // use hash of uid + host
        MessageDigest digester = MessageDigest.getInstance("MD5");
        digester.update((new UID()+"@"+InetAddress.getLocalHost()).getBytes());
        sync = digester.digest();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    /** @deprecated Call {@link #SequenceFile.Writer(FileSystem,Path,Class,Class)}. */
    public Writer(FileSystem fs, String name, Class keyClass, Class valClass)
      throws IOException {
      this(fs, new Path(name), keyClass, valClass, false);
    }

    /** Implicit constructor: needed for the period of transition!*/
    Writer()
    {}
    
    /** Create the named file. */
    /** @deprecated Call {@link #SequenceFile.Writer(FileSystem,Configuration,Path,Class,Class)}. */
    public Writer(FileSystem fs, Path name, Class keyClass, Class valClass)
      throws IOException {
      this(fs, name, keyClass, valClass, false);
    }
    
    /** Create the named file with write-progress reporter. */
    /** @deprecated Call {@link #SequenceFile.Writer(FileSystem,Configuration,Path,Class,Class,Progressable)}. */
    public Writer(FileSystem fs, Path name, Class keyClass, Class valClass,
            Progressable progress)
      throws IOException {
      this(fs, name, keyClass, valClass, false, progress);
    }
    
    /** Create the named file.
     * @param compress if true, values are compressed.
     */
    /** @deprecated Call {@link #SequenceFile.Writer(FileSystem,Configuration,Path,Class,Class)}. */
    public Writer(FileSystem fs, Path name,
                  Class keyClass, Class valClass, boolean compress)
      throws IOException {
      init(name, fs.create(name), keyClass, valClass, compress);

      initializeFileHeader();
      writeFileHeader();
      finalizeFileHeader();
    }
    
    /** Create the named file with write-progress reporter.
     * @param compress if true, values are compressed.
     */
    /** @deprecated Call {@link #SequenceFile.Writer(FileSystem,Configuration,Path,Class,Class,Progressable)}. */
    public Writer(FileSystem fs, Path name,
                  Class keyClass, Class valClass, boolean compress,
                  Progressable progress)
      throws IOException {
      init(name, fs.create(name, progress), keyClass, valClass, compress);
      
      initializeFileHeader();
      writeFileHeader();
      finalizeFileHeader();
    }
    
    /** Create the named file. */
    public Writer(FileSystem fs, Configuration conf, Path name, 
        Class keyClass, Class valClass)
      throws IOException {
      this(fs, name, keyClass, valClass, false);
    }
    
    /** Create the named file with write-progress reporter. */
    public Writer(FileSystem fs, Configuration conf, Path name, 
        Class keyClass, Class valClass, Progressable progress)
      throws IOException {
      this(fs, name, keyClass, valClass, false, progress);
    }

    /** Write to an arbitrary stream using a specified buffer size. */
    private Writer(FSDataOutputStream out, Class keyClass, Class valClass)
      throws IOException {
      init(null, out, keyClass, valClass, false);
      
      initializeFileHeader();
      writeFileHeader();
      finalizeFileHeader();
    }

    /** Write the initial part of file header. */
    void initializeFileHeader() 
    throws IOException{
      out.write(VERSION);
    }

    /** Write the final part of file header. */
    void finalizeFileHeader() 
    throws IOException{
      out.write(sync);                       // write the sync bytes
      out.flush();                           // flush header
    }
    
    boolean isCompressed() { return compress; }
    boolean isBlockCompressed() { return false; }
    
    /** Write and flush the file header. */
    void writeFileHeader() 
    throws IOException {
      Text.writeString(out, keyClass.getName());
      Text.writeString(out, valClass.getName());
      
      out.writeBoolean(this.isCompressed());
      out.writeBoolean(this.isBlockCompressed());
    }

    /** Initialize. */
    void init(Path name, FSDataOutputStream out,
                      Class keyClass, Class valClass,
                      boolean compress) 
    throws IOException {
      this.target = name;
      this.out = out;
      this.keyClass = keyClass;
      this.valClass = valClass;
      this.compress = compress;
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

    synchronized void checkAndWriteSync() throws IOException {
      if (sync != null &&
          out.getPos() >= lastSyncPos+SYNC_INTERVAL) { // time to emit sync
        lastSyncPos = out.getPos();               // update lastSyncPos
        //LOG.info("sync@"+lastSyncPos);
        out.writeInt(SYNC_ESCAPE);                // escape it
        out.write(sync);                          // write sync
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

      // Append the 'key'
      key.write(buffer);
      int keyLength = buffer.getLength();
      if (keyLength == 0)
        throw new IOException("zero length keys not allowed: " + key);

      // Append the 'value'
      if (compress) {
        deflater.reset();
        val.write(deflateOut);
        deflateOut.flush();
        deflateFilter.finish();
      } else {
        val.write(buffer);
      }

      // Write the record out
      checkAndWriteSync();                                // sync
      out.writeInt(buffer.getLength());                   // total record length
      out.writeInt(keyLength);                            // key portion length
      out.write(buffer.getData(), 0, buffer.getLength()); // data
    }

    /** 
     * Append a key/value pair. 
     * @deprecated Call {@link #appendRaw(byte[], int, int, SequenceFile.ValueBytes)}. 
     */
    public synchronized void append(byte[] data, int start, int length,
                                    int keyLength) throws IOException {
      if (keyLength == 0)
        throw new IOException("zero length keys not allowed");

      checkAndWriteSync();                        // sync
      out.writeInt(length);                       // total record length
      out.writeInt(keyLength);                    // key portion length
      out.write(data, start, length);             // data

    }
    
    public synchronized void appendRaw(
        byte[] keyData, int keyOffset, int keyLength, ValueBytes val) 
    throws IOException {
      if (keyLength == 0)
        throw new IOException("zero length keys not allowed: " + keyLength);

      UncompressedBytes value = (UncompressedBytes)val;
      int valLength = value.getSize();

      checkAndWriteSync();
      
      out.writeInt(keyLength+valLength);          // total record length
      out.writeInt(keyLength);                    // key portion length
      out.write(keyData, keyOffset, keyLength);   // key
      val.writeUncompressedBytes(out);            // value
    }

    /** Returns the current length of the output file. */
    public synchronized long getLength() throws IOException {
      return out.getPos();
    }

  } // class Writer

  /** Write key/compressed-value pairs to a sequence-format file. */
  static class RecordCompressWriter extends Writer {
    
    /** Create the named file. */
    public RecordCompressWriter(FileSystem fs, Configuration conf, Path name, 
        Class keyClass, Class valClass) throws IOException {
      super.init(name, fs.create(name), keyClass, valClass, true);
      
      initializeFileHeader();
      writeFileHeader();
      finalizeFileHeader();
    }
    
    /** Create the named file with write-progress reporter. */
    public RecordCompressWriter(FileSystem fs, Configuration conf, Path name, 
        Class keyClass, Class valClass, Progressable progress)
    throws IOException {
      super.init(name, fs.create(name, progress), keyClass, valClass, true);
      
      initializeFileHeader();
      writeFileHeader();
      finalizeFileHeader();
    }
    
    /** Write to an arbitrary stream using a specified buffer size. */
    private RecordCompressWriter(FSDataOutputStream out,
                   Class keyClass, Class valClass)
      throws IOException {
      super.init(null, out, keyClass, valClass, true);
      
      initializeFileHeader();
      writeFileHeader();
      finalizeFileHeader();
      
    }

    boolean isCompressed() { return true; }
    boolean isBlockCompressed() { return false; }

    /** Append a key/value pair. */
    public synchronized void append(Writable key, Writable val)
      throws IOException {
      if (key.getClass() != keyClass)
        throw new IOException("wrong key class: "+key+" is not "+keyClass);
      if (val.getClass() != valClass)
        throw new IOException("wrong value class: "+val+" is not "+valClass);

      buffer.reset();

      // Append the 'key'
      key.write(buffer);
      int keyLength = buffer.getLength();
      if (keyLength == 0)
        throw new IOException("zero length keys not allowed: " + key);

      // Compress 'value' and append it
      deflater.reset();
      val.write(deflateOut);
      deflateOut.flush();
      deflateFilter.finish();

      // Write the record out
      checkAndWriteSync();                                // sync
      out.writeInt(buffer.getLength());                   // total record length
      out.writeInt(keyLength);                            // key portion length
      out.write(buffer.getData(), 0, buffer.getLength()); // data
    }

    /** Append a key/value pair. */
    public synchronized void appendRaw(
        byte[] keyData, int keyOffset, int keyLength,
        ValueBytes val
        ) throws IOException {

      if (keyLength == 0)
        throw new IOException("zero length keys not allowed");

      CompressedBytes value = (CompressedBytes)val;
      int valLength = value.getSize();
      
      checkAndWriteSync();                        // sync
      out.writeInt(keyLength+valLength);          // total record length
      out.writeInt(keyLength);                    // key portion length
      out.write(keyData, keyOffset, keyLength);   // 'key' data
      val.writeCompressedBytes(out);              // 'value' data
    }
    
  } // RecordCompressionWriter

  /** Write compressed key/value blocks to a sequence-format file. */
  static class BlockCompressWriter extends Writer {
    
    private int noBufferedRecords = 0;
    
    private DataOutputBuffer keyLenBuffer = new DataOutputBuffer();
    private DataOutputBuffer keyBuffer = new DataOutputBuffer();

    private DataOutputBuffer valLenBuffer = new DataOutputBuffer();
    private DataOutputBuffer valBuffer = new DataOutputBuffer();

    private DataOutputBuffer compressedDataBuffer = new DataOutputBuffer();
    private Deflater deflater = new Deflater(Deflater.BEST_SPEED);
    private DeflaterOutputStream deflateFilter = 
      new DeflaterOutputStream(compressedDataBuffer, deflater);
    private DataOutputStream deflateOut = 
      new DataOutputStream(new BufferedOutputStream(deflateFilter));

    private int compressionBlockSize;
    
    /** Create the named file. */
    public BlockCompressWriter(FileSystem fs, Configuration conf, Path name, 
        Class keyClass, Class valClass) throws IOException {
      super.init(name, fs.create(name), keyClass, valClass, true);
      init(conf.getInt("mapred.seqfile.compress.blocksize", 1000000));
      
      initializeFileHeader();
      writeFileHeader();
      finalizeFileHeader();
    }
    
    /** Create the named file with write-progress reporter. */
    public BlockCompressWriter(FileSystem fs, Configuration conf, Path name, 
        Class keyClass, Class valClass, Progressable progress)
    throws IOException {
      super.init(name, fs.create(name, progress), keyClass, valClass, true);
      init(conf.getInt("mapred.seqfile.compress.blocksize", 1000000));
      
      initializeFileHeader();
      writeFileHeader();
      finalizeFileHeader();
    }
    
    /** Write to an arbitrary stream using a specified buffer size. */
    private BlockCompressWriter(FSDataOutputStream out,
                   Class keyClass, Class valClass)
      throws IOException {
      super.init(null, out, keyClass, valClass, true);
      init(1000000);
      
      initializeFileHeader();
      writeFileHeader();
      finalizeFileHeader();
    }

    boolean isCompressed() { return true; }
    boolean isBlockCompressed() { return true; }

    /** Initialize */
    void init(int compressionBlockSize) {
      this.compressionBlockSize = compressionBlockSize;
    }
    
    /** Workhorse to check and write out compressed data/lengths */
    private synchronized 
    void writeBuffer(DataOutputBuffer buffer) 
    throws IOException {
      deflater.reset();
      compressedDataBuffer.reset();
      deflateOut.write(buffer.getData(), 0, buffer.getLength());
      deflateOut.flush();
      deflateFilter.finish();
      
      WritableUtils.writeVInt(out, compressedDataBuffer.getLength());
      out.write(compressedDataBuffer.getData(), 
          0, compressedDataBuffer.getLength());
    }
    
    /** Compress and flush contents to dfs */
    private synchronized void writeBlock() throws IOException {
      if (noBufferedRecords > 0) {
        // Write 'sync' marker
        if (sync != null) {
          out.writeInt(SYNC_ESCAPE);
          out.write(sync);
        }
        
        // No. of records
        WritableUtils.writeVInt(out, noBufferedRecords);
        
        // Write 'keys' and lengths
        writeBuffer(keyLenBuffer);
        writeBuffer(keyBuffer);
        
        // Write 'values' and lengths
        writeBuffer(valLenBuffer);
        writeBuffer(valBuffer);
        
        // Flush the file-stream
        out.flush();
        
        // Reset internal states
        keyLenBuffer.reset();
        keyBuffer.reset();
        valLenBuffer.reset();
        valBuffer.reset();
        noBufferedRecords = 0;
      }
      
    }
    
    /** Close the file. */
    public synchronized void close() throws IOException {
      if (out != null) {
        writeBlock();
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

      // Save key/value into respective buffers 
      int oldKeyLength = keyBuffer.getLength();
      key.write(keyBuffer);
      int keyLength = keyBuffer.getLength() - oldKeyLength;
      if (keyLength == 0)
        throw new IOException("zero length keys not allowed: " + key);
      WritableUtils.writeVInt(keyLenBuffer, keyLength);

      int oldValLength = valBuffer.getLength();
      val.write(valBuffer);
      int valLength = valBuffer.getLength() - oldValLength;
      WritableUtils.writeVInt(valLenBuffer, valLength);
      
      // Added another key/value pair
      ++noBufferedRecords;
      
      // Compress and flush?
      int currentBlockSize = keyBuffer.getLength() + valBuffer.getLength();
      if (currentBlockSize >= compressionBlockSize) {
        writeBlock();
      }
    }
    
    /** Append a key/value pair. */
    public synchronized void appendRaw(
        byte[] keyData, int keyOffset, int keyLength,
        ValueBytes val
        ) throws IOException {
      
      if (keyLength == 0)
        throw new IOException("zero length keys not allowed");

      UncompressedBytes value = (UncompressedBytes)val;
      int valLength = value.getSize();
      
      // Save key/value data in relevant buffers
      WritableUtils.writeVInt(keyLenBuffer, keyLength);
      keyBuffer.write(keyData, keyOffset, keyLength);
      WritableUtils.writeVInt(valLenBuffer, valLength);
      val.writeUncompressedBytes(valBuffer);

      // Added another key/value pair
      ++noBufferedRecords;

      // Compress and flush?
      int currentBlockSize = keyBuffer.getLength() + valBuffer.getLength(); 
      if (currentBlockSize >= compressionBlockSize) {
        writeBlock();
      }
    }
  
  } // BlockCompressionWriter
  
  /** Reads key/value pairs from a sequence-format file. */
  public static class Reader {
    private Path file;
    private FSDataInputStream in;
    private DataOutputBuffer outBuf = new DataOutputBuffer();

    private byte version;

    private Class keyClass;
    private Class valClass;

    private byte[] sync = new byte[SYNC_HASH_SIZE];
    private byte[] syncCheck = new byte[SYNC_HASH_SIZE];
    private boolean syncSeen;

    private long end;
    private int keyLength;

    private boolean decompress;
    private boolean blockCompressed;
    
    private Configuration conf;

    private int noBufferedRecords = 0;
    private boolean lazyDecompress = true;
    private boolean valuesDecompressed = true;
    
    private int noBufferedKeys = 0;
    private int noBufferedValues = 0;
    
    private DataInputBuffer keyLenBuffer = null;
    private Inflater keyLenInflater = null;
    private DataInputStream keyLenIn = null;
    private DataInputBuffer keyBuffer = null;
    private Inflater keyInflater = null;
    private DataInputStream keyIn = null;

    private DataInputBuffer valLenBuffer = null;
    private Inflater valLenInflater = null;
    private DataInputStream valLenIn = null;
    private DataInputBuffer valBuffer = null;
    private Inflater valInflater = null;
    private DataInputStream valIn = null;

    /** @deprecated Call {@link #SequenceFile.Reader(FileSystem,Path,Configuration)}.*/
    public Reader(FileSystem fs, String file, Configuration conf)
      throws IOException {
      this(fs, new Path(file), conf);
    }

    /** Open the named file. */
    public Reader(FileSystem fs, Path file, Configuration conf)
      throws IOException {
      this(fs, file, conf.getInt("io.file.buffer.size", 4096), conf);
    }

    private Reader(FileSystem fs, Path name, int bufferSize,
                   Configuration conf) throws IOException {
      this.file = name;
      this.in = fs.open(file, bufferSize);
      this.end = fs.getLength(file);
      this.conf = conf;
      init();
    }
    
    private Reader(FileSystem fs, Path file, int bufferSize, long start,
                   long length, Configuration conf) throws IOException {
      this.file = file;
      this.in = fs.open(file, bufferSize);
      this.conf = conf;
      seek(start);
      this.end = in.getPos() + length;
      init();
    }
    
    private void init() throws IOException {
      byte[] versionBlock = new byte[VERSION.length];
      in.readFully(versionBlock);

      if ((versionBlock[0] != VERSION[0]) ||
          (versionBlock[1] != VERSION[1]) ||
          (versionBlock[2] != VERSION[2]))
        throw new IOException(file + " not a SequenceFile");

      // Set 'version'
      version = versionBlock[3];
      if (version > VERSION[3])
        throw new VersionMismatchException(VERSION[3], version);

      if (version < BLOCK_COMPRESS_VERSION) {
        UTF8 className = new UTF8();
        
        className.readFields(in);                   // read key class name
        this.keyClass = WritableName.getClass(className.toString(), conf);
        
        className.readFields(in);                   // read val class name
        this.valClass = WritableName.getClass(className.toString(), conf);
      } else {
        this.keyClass = WritableName.getClass(Text.readString(in), conf);
        this.valClass = WritableName.getClass(Text.readString(in), conf);
      }

      if (version > 2) {                          // if version > 2
        this.decompress = in.readBoolean();       // is compressed?
      }

      if (version >= BLOCK_COMPRESS_VERSION) {    // if version >= 4
        this.blockCompressed = in.readBoolean();  // is block-compressed?
      }
      
      if (version > 1) {                          // if version > 1
        in.readFully(sync);                       // read sync bytes
      }
      
      // Initialize
      valBuffer = new DataInputBuffer();
      if (decompress) {
        valInflater = new Inflater();
        valIn = new DataInputStream(new BufferedInputStream(
            new InflaterInputStream(valBuffer, valInflater))
        );
      } else {
        valIn = new DataInputStream(new BufferedInputStream(valBuffer));
      }
      
      if (blockCompressed) {
        keyLenBuffer = new DataInputBuffer();
        keyBuffer = new DataInputBuffer();
        valLenBuffer = new DataInputBuffer();
        
        keyLenInflater = new Inflater();
        keyLenIn = new DataInputStream(new BufferedInputStream(
            new InflaterInputStream(keyLenBuffer, keyLenInflater))
        );
        
        keyInflater = new Inflater();
        keyIn = new DataInputStream(new BufferedInputStream(
            new InflaterInputStream(keyBuffer, keyInflater)));
        
        valLenInflater = new Inflater();
        valLenIn = new DataInputStream(new BufferedInputStream(
            new InflaterInputStream(valLenBuffer, valLenInflater))
        );
      }
      

      lazyDecompress = conf.getBoolean("mapred.seqfile.lazydecompress", true);
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
    public boolean isCompressed() { return decompress; }
    
    /** Returns true if records are block-compressed. */
    public boolean isBlockCompressed() { return blockCompressed; }
    
    /** Read a compressed buffer */
    private synchronized void readBuffer(
        DataInputBuffer buffer, Inflater inflater, boolean castAway
        ) throws IOException {
      // Read data into a temporary buffer
      DataOutputBuffer dataBuffer = new DataOutputBuffer();
      int dataBufferLength = WritableUtils.readVInt(in);
      dataBuffer.write(in, dataBufferLength);
      
      if (false == castAway) {
        // Reset the inflater
        inflater.reset();
        
        // Set up 'buffer' connected to the input-stream
        buffer.reset(dataBuffer.getData(), 0, dataBuffer.getLength());
      }
    }
    
    /** Read the next 'compressed' block */
    private synchronized void readBlock() throws IOException {
      // Check if we need to throw away a whole block of 
      // 'values' due to 'lazy decompression' 
      if (lazyDecompress && !valuesDecompressed) {
        readBuffer(null, null, true);
        readBuffer(null, null, true);
      }
      
      // Reset internal states
      noBufferedKeys = 0; noBufferedValues = 0; noBufferedRecords = 0;
      valuesDecompressed = false;

      //Process sync
      if (sync != null) {
        in.readInt();
        in.readFully(syncCheck);                // read syncCheck
        if (!Arrays.equals(sync, syncCheck))    // check it
          throw new IOException("File is corrupt!");
      }
      syncSeen = true;

      // Read number of records in this block
      noBufferedRecords = WritableUtils.readVInt(in);
      
      // Read key lengths and keys
      readBuffer(keyLenBuffer, keyLenInflater, false);
      readBuffer(keyBuffer, keyInflater, false);
      noBufferedKeys = noBufferedRecords;
      
      // Read value lengths and values
      if (!lazyDecompress) {
        readBuffer(valLenBuffer, valLenInflater, false);
        readBuffer(valBuffer, valInflater, false);
        noBufferedValues = noBufferedRecords;
        valuesDecompressed = true;
      }
    }

    /** 
     * Position valLenIn/valIn to the 'value' 
     * corresponding to the 'current' key 
     */
    private synchronized void seekToCurrentValue() throws IOException {
      if (version < BLOCK_COMPRESS_VERSION || blockCompressed == false) {
        if (decompress) {
          valInflater.reset();
        }
      } else {
        // Check if this is the first value in the 'block' to be read
        if (lazyDecompress && !valuesDecompressed) {
          // Read the value lengths and values
          readBuffer(valLenBuffer, valLenInflater, false);
          readBuffer(valBuffer, valInflater, false);
          noBufferedValues = noBufferedRecords;
          valuesDecompressed = true;
        }
        
        // Calculate the no. of bytes to skip
        // Note: 'current' key has already been read!
        int skipValBytes = 0;
        int currentKey = noBufferedKeys + 1;          
        for (int i=noBufferedValues; i > currentKey; --i) {
          skipValBytes += WritableUtils.readVInt(valLenIn);
          --noBufferedValues;
        }
        
        // Skip to the 'val' corresponding to 'current' key
        if (skipValBytes > 0) {
          if (valIn.skipBytes(skipValBytes) != skipValBytes) {
            throw new IOException("Failed to seek to " + currentKey + 
                "(th) value!");
          }
        }
      }
    }

    /**
     * Get the 'value' corresponding to the last read 'key'.
     * @param val : The 'value' to be read.
     * @throws IOException
     */
    public synchronized void getCurrentValue(Writable val) 
    throws IOException {
      if (val instanceof Configurable) {
        ((Configurable) val).setConf(this.conf);
      }

      // Position stream to 'current' value
      seekToCurrentValue();

      if (version < BLOCK_COMPRESS_VERSION || blockCompressed == false) {
        val.readFields(valIn);
        
        if (valBuffer.getPosition() != valBuffer.getLength())
          throw new IOException(val+" read "+(valBuffer.getPosition()-keyLength)
              + " bytes, should read " +
              (valBuffer.getLength()-keyLength));
      } else {
        // Get the value
        int valLength = WritableUtils.readVInt(valLenIn);
        val.readFields(valIn);
        
        // Read another compressed 'value'
        --noBufferedValues;
        
        // Sanity check
        if (valLength < 0) {
          LOG.debug(val + " is a zero-length value");
        }
      }

    }
    
    /** Read the next key in the file into <code>key</code>, skipping its
     * value.  True if another entry exists, and false at end of file. */
    public synchronized boolean next(Writable key) throws IOException {
      if (key.getClass() != keyClass)
        throw new IOException("wrong key class: "+key+" is not "+keyClass);

      if (version < BLOCK_COMPRESS_VERSION || blockCompressed == false) {
        outBuf.reset();
        
        keyLength = next(outBuf);
        if (keyLength < 0)
          return false;
        
        valBuffer.reset(outBuf.getData(), outBuf.getLength());
        
        key.readFields(valBuffer);
        if (valBuffer.getPosition() != keyLength)
          throw new IOException(key + " read " + valBuffer.getPosition()
              + " bytes, should read " + keyLength);
      } else {
        //Reset syncSeen
        syncSeen = false;
        
        if (noBufferedKeys == 0) {
          try {
            readBlock();
          } catch (EOFException eof) {
            return false;
          }
        }
        
        int keyLength = WritableUtils.readVInt(keyLenIn);
        
        // Sanity check
        if (keyLength < 0) {
          return false;
        }
        
        //Read another compressed 'key'
        key.readFields(keyIn);
        --noBufferedKeys;
      }

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
        getCurrentValue(val);
      }

      return more;
    }
    
    private synchronized int checkAndReadSync(int length) 
    throws IOException {
      if (version > 1 && sync != null &&
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
      
      return length;
    }
    
    /** Read the next key/value pair in the file into <code>buffer</code>.
     * Returns the length of the key read, or -1 if at end of file.  The length
     * of the value may be computed by calling buffer.getLength() before and
     * after calls to this method. */
    /** @deprecated Call {@link #nextRaw(DataOutputBuffer,SequenceFile.ValueBytes)}. */
    public synchronized int next(DataOutputBuffer buffer) throws IOException {
      // Unsupported for block-compressed sequence files
      if (version >= BLOCK_COMPRESS_VERSION && blockCompressed) {
        throw new IOException("Unsupported call for block-compressed" +
            " SequenceFiles - use SequenceFile.Reader.next(DataOutputStream, ValueBytes)");
      }
      if (in.getPos() >= end)
        return -1;

      try {
        int length = checkAndReadSync(in.readInt());
        int keyLength = in.readInt();
        buffer.write(in, length);
        return keyLength;
      } catch (ChecksumException e) {             // checksum failure
        handleChecksumException(e);
        return next(buffer);
      }
    }
    
    public ValueBytes createValueBytes() {
      ValueBytes val = null;
      if (!decompress || blockCompressed) {
        val = new UncompressedBytes();
      } else {
        val = new CompressedBytes();
      }
      return val;
    }

    /**
     * Read 'raw' records.
     * @param key - The buffer into which the key is read
     * @param val - The 'raw' value
     * @return Returns the total record length
     * @throws IOException
     */
    public int nextRaw(DataOutputBuffer key, ValueBytes val) 
    throws IOException {
      if (version < BLOCK_COMPRESS_VERSION || blockCompressed == false) {
        if (in.getPos() >= end) 
          return -1;

        int length = checkAndReadSync(in.readInt());
        int keyLength = in.readInt();
        int valLength = length - keyLength;
        key.write(in, keyLength);
        if (decompress) {
          CompressedBytes value = (CompressedBytes)val;
          value.reset(in, valLength);
        } else {
          UncompressedBytes value = (UncompressedBytes)val;
          value.reset(in, valLength);
        }
        
        return length;
      } else {
        //Reset syncSeen
        syncSeen = false;
        
        // Read 'key'
        if (noBufferedKeys == 0) {
          if (in.getPos() >= end) 
            return -1;

          try { 
            readBlock();
          } catch (EOFException eof) {
            return -1;
          }
        }
        int keyLength = WritableUtils.readVInt(keyLenIn);
        if (keyLength < 0) {
          throw new IOException("zero length key found!");
        }
        key.write(keyIn, keyLength);
        --noBufferedKeys;
        
        // Read raw 'value'
        seekToCurrentValue();
        int valLength = WritableUtils.readVInt(valLenIn);
        UncompressedBytes rawValue = (UncompressedBytes)val;
        rawValue.reset(valIn, valLength);
        --noBufferedValues;
        
        return (keyLength+valLength);
      }
      
    }

    private void handleChecksumException(ChecksumException e)
      throws IOException {
      if (this.conf.getBoolean("io.skip.checksum.errors", false)) {
        LOG.warn("Bad checksum at "+getPosition()+". Skipping entries.");
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
      return file.toString();
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

    private Path[] inFiles;                     // when merging or sorting

    private Path outFile;

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
    public Sorter(FileSystem fs, WritableComparator comparator, Class valClass, 
        Configuration conf) {
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

    /** 
     * Perform a file sort from a set of input files into an output file.
     * @param inFiles the files to be sorted
     * @param outFile the sorted output file
     * @param deleteInput should the input files be deleted as they are read?
     */
    public void sort(Path[] inFiles, Path outFile,
                     boolean deleteInput) throws IOException {
      if (fs.exists(outFile)) {
        throw new IOException("already exists: " + outFile);
      }

      this.inFiles = inFiles;
      this.outFile = outFile;

      int segments = sortPass(deleteInput);
      int pass = 1;
      while (segments > 1) {
        segments = mergePass(pass, segments <= factor);
        pass++;
      }
      
      // Clean up intermediate files
      for (int i=0; i < pass; ++i) {
        fs.delete(new Path(outFile.toString() + "." + i));
        fs.delete(new Path(outFile.toString() + "." + i + ".index"));
      }
    }

    /**
     * The backwards compatible interface to sort.
     * @param inFile the input file to sort
     * @param outFile the sorted output file
     */
    public void sort(Path inFile, Path outFile) throws IOException {
      sort(new Path[]{inFile}, outFile, false);
    }
    
    private int sortPass(boolean deleteInput) throws IOException {
      LOG.debug("running sort pass");
      SortPass sortPass = new SortPass();         // make the SortPass
      try {
        return sortPass.run(deleteInput);         // run it
      } finally {
        sortPass.close();                         // close it
      }
    }

    private class SortPass {
      private int memoryLimit = memory/4;
      private int recordLimit = 1000000;
      
      private DataOutputBuffer rawKeys = new DataOutputBuffer();
      private byte[] rawBuffer;

      private int[] keyOffsets = new int[1024];
      private int[] pointers = new int[keyOffsets.length];
      private int[] pointersCopy = new int[keyOffsets.length];
      private int[] keyLengths = new int[keyOffsets.length];
      private ValueBytes[] rawValues = new ValueBytes[keyOffsets.length];
      
      private ArrayList segmentLengths = new ArrayList();
      
      private Reader in = null;
      private FSDataOutputStream out = null;
      private FSDataOutputStream indexOut = null;
      private Path outName;

      public int run(boolean deleteInput) throws IOException {
        int segments = 0;
        int currentFile = 0;
        boolean atEof = (currentFile >= inFiles.length);
        boolean isCompressed = false;
        boolean isBlockCompressed = false;
        segmentLengths.clear();
        if (atEof) {
          return 0;
        }
        
        // Initialize
        in = new Reader(fs, inFiles[currentFile], conf);
        isCompressed = in.isCompressed();
        isBlockCompressed = in.isBlockCompressed();
        for (int i=0; i < rawValues.length; ++i) {
          rawValues[i] = null;
        }
        
        while (!atEof) {
          int count = 0;
          int bytesProcessed = 0;
          rawKeys.reset();
          while (!atEof && 
              bytesProcessed < memoryLimit && count < recordLimit) {

            // Read a record into buffer
            // Note: Attempt to re-use 'rawValue' as far as possible
            int keyOffset = rawKeys.getLength();       
            ValueBytes rawValue = 
              (count == keyOffsets.length || rawValues[count] == null) ? 
                  in.createValueBytes() : 
                  rawValues[count];
            int recordLength = in.nextRaw(rawKeys, rawValue);
            if (recordLength == -1) {
              in.close();
              if (deleteInput) {
                fs.delete(inFiles[currentFile]);
              }
              currentFile += 1;
              atEof = currentFile >= inFiles.length;
              if (!atEof) {
                in = new Reader(fs, inFiles[currentFile], conf);
              } else {
                in = null;
              }
              continue;
            }
            //int length = buffer.getLength() - start;
            int keyLength = rawKeys.getLength() - keyOffset;

            if (count == keyOffsets.length)
              grow();

            keyOffsets[count] = keyOffset;                // update pointers
            pointers[count] = count;
            keyLengths[count] = keyLength;
            rawValues[count] = rawValue;

            bytesProcessed += recordLength; 
            count++;
          }

          // buffer is full -- sort & flush it
          LOG.debug("flushing segment " + segments);
          rawBuffer = rawKeys.getData();
          sort(count);
          flush(count, bytesProcessed, isCompressed, isBlockCompressed, 
              segments==0 && atEof);
          segments++;
        }
        return segments;
      }

      public void close() throws IOException {
        if (in != null) {
          in.close();
        }
        if (out != null) {
          out.close();
        }
        if (indexOut != null) {
          indexOut.close();
        }
      }

      private void grow() {
        int newLength = keyOffsets.length * 3 / 2;
        keyOffsets = grow(keyOffsets, newLength);
        pointers = grow(pointers, newLength);
        pointersCopy = new int[newLength];
        keyLengths = grow(keyLengths, newLength);
        rawValues = grow(rawValues, newLength);
      }

      private int[] grow(int[] old, int newLength) {
        int[] result = new int[newLength];
        System.arraycopy(old, 0, result, 0, old.length);
        return result;
      }
      
      private ValueBytes[] grow(ValueBytes[] old, int newLength) {
        ValueBytes[] result = new ValueBytes[newLength];
        System.arraycopy(old, 0, result, 0, old.length);
        for (int i=old.length; i < newLength; ++i) {
          result[i] = null;
        }
        return result;
      }

      private void flush(int count, int bytesProcessed, boolean isCompressed, 
          boolean isBlockCompressed, boolean done) throws IOException {
        if (out == null) {
          outName = done ? outFile : outFile.suffix(".0");
          out = fs.create(outName);
          if (!done) {
            indexOut = fs.create(outName.suffix(".index"));
          }
        }

        long segmentStart = out.getPos();
        Writer writer = createWriter(out, keyClass, valClass, 
            isCompressed, isBlockCompressed);
        
        if (!done) {
          writer.sync = null;                     // disable sync on temp files
        }

        for (int i = 0; i < count; i++) {         // write in sorted order
          int p = pointers[i];
          writer.appendRaw(rawBuffer, keyOffsets[p], keyLengths[p], rawValues[p]);
        }
        if (writer instanceof SequenceFile.BlockCompressWriter) {
          SequenceFile.BlockCompressWriter bcWriter = 
            (SequenceFile.BlockCompressWriter) writer;
          bcWriter.writeBlock();
        }
        writer.out.flush();
        
        
        if (!done) {
          // Save the segment length
          WritableUtils.writeVLong(indexOut, segmentStart);
          WritableUtils.writeVLong(indexOut, (writer.out.getPos()-segmentStart));
          indexOut.flush();
        }
      }

      private void sort(int count) {
        System.arraycopy(pointers, 0, pointersCopy, 0, count);
        mergeSort(pointersCopy, pointers, 0, count);
      }

      private int compare(int i, int j) {
        return comparator.compare(rawBuffer, keyOffsets[i], keyLengths[i],
                                  rawBuffer, keyOffsets[j], keyLengths[j]);
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
        for (int i = low, p = low, q = mid; i < high; i++) {
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
    } // SequenceFile.Sorter.SortPass

    private int mergePass(int pass, boolean last) throws IOException {
      LOG.debug("running merge pass=" + pass);
      MergePass mergePass = new MergePass(pass, last);
      try {                                       // make a merge pass
        return mergePass.run();                  // run it
      } finally {
        mergePass.close();                       // close it
      }
    }

    private class MergePass {
      private boolean last;

      private MergeQueue queue;
      private FSDataInputStream in = null;
      private Path inName;
      private FSDataInputStream indexIn = null;

      public MergePass(int pass, boolean last) throws IOException {
        this.last = last;

        this.queue =
          new MergeQueue(factor, last?outFile:outFile.suffix("."+pass), last);

        this.inName = outFile.suffix("."+(pass-1));
        this.in = fs.open(inName);
        this.indexIn = fs.open(inName.suffix(".index"));
      }

      public void close() throws IOException {
        in.close();                               // close and delete input
        fs.delete(inName);

        queue.close();                            // close queue
      }

      public int run() throws IOException {
        int segments = 0;
        long end = fs.getLength(inName);

        while (in.getPos() < end) {
          LOG.debug("merging segment " + segments);
          long segmentStart = queue.out.getPos();
          while (in.getPos() < end && queue.size() < factor) {
            long segmentOffset = WritableUtils.readVLong(indexIn);
            long segmentLength = WritableUtils.readVLong(indexIn);
            Reader reader = new Reader(fs, inName, memory/(factor+1),
                                        segmentOffset, segmentLength, conf);
            reader.sync = null;                   // disable sync on temp files

            MergeStream ms = new MergeStream(reader); // add segment to queue
            if (ms.next()) {
              queue.put(ms);
            }
            in.seek(reader.end);
          }

          queue.merge();                          // do a merge

          if (!last) {
            WritableUtils.writeVLong(queue.indexOut, segmentStart);
            WritableUtils.writeVLong(queue.indexOut, 
                (queue.out.getPos() - segmentStart));
          }
          
          segments++;
        }

        return segments;
      }
    } // SequenceFile.Sorter.MergePass

    /** Merge the provided files.*/
    public void merge(Path[] inFiles, Path outFile) throws IOException {
      this.inFiles = inFiles;
      this.outFile = outFile;
      this.factor = inFiles.length;

      if (fs.exists(outFile)) {
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
        LOG.debug("merging files=" + inFiles.length);
        for (int i = 0; i < inFiles.length; i++) {
          Path inFile = inFiles[i];
          MergeStream ms =
            new MergeStream(new Reader(fs, inFile, memory/(factor+1), conf));
          if (ms.next())
            queue.put(ms);
        }

        queue.merge();
      }
    } // SequenceFile.Sorter.MergeFiles

    private class MergeStream {
      private Reader in;

      private DataOutputBuffer rawKey = null;
      private ValueBytes rawValue = null;
      
      public MergeStream(Reader reader) throws IOException {
        if (reader.keyClass != keyClass)
          throw new IOException("wrong key class: " + reader.getKeyClass() +
                                " is not " + keyClass);
        if (reader.valClass != valClass)
          throw new IOException("wrong value class: "+reader.getValueClass()+
                                " is not " + valClass);
        this.in = reader;
        rawKey = new DataOutputBuffer();
        rawValue = in.createValueBytes();
      }

      public boolean next() throws IOException {
        rawKey.reset();
        int recordLength = 
          in.nextRaw(rawKey, rawValue);
        return (recordLength >= 0);
      }
    } // SequenceFile.Sorter.MergeStream

    private class MergeQueue extends PriorityQueue {
      private Path outName;
      private FSDataOutputStream out;
      private FSDataOutputStream indexOut;
      private boolean done;
      private boolean compress;
      private boolean blockCompress;

      public void put(MergeStream stream) throws IOException {
        if (size() == 0) {
          compress = stream.in.isCompressed();
          blockCompress = stream.in.isBlockCompressed();
        } else if (compress != stream.in.isCompressed() || 
            blockCompress != stream.in.isBlockCompressed()) {
          throw new IOException("All merged files must be compressed or not.");
        } 
        super.put(stream);
      }

      public MergeQueue(int size, Path outName, boolean done)
        throws IOException {
        initialize(size);
        this.outName = outName;
        this.out = fs.create(this.outName, true, memory/(factor+1));
        if (!done) {
          this.indexOut = fs.create(outName.suffix(".index"), true, 
              memory/(factor+1));
        }
        this.done = done;
      }

      protected boolean lessThan(Object a, Object b) {
        MergeStream msa = (MergeStream)a;
        MergeStream msb = (MergeStream)b;
        return comparator.compare(msa.rawKey.getData(), 0, msa.rawKey.getLength(),
            msb.rawKey.getData(), 0, msb.rawKey.getLength()) < 0;
      }

      public void merge() throws IOException {
        Writer writer = createWriter(out, keyClass, valClass, 
            compress, blockCompress);
        if (!done) {
          writer.sync = null;                     // disable sync on temp files
        }

        while (size() != 0) {
          MergeStream ms = (MergeStream)top();
          writer.appendRaw(ms.rawKey.getData(), 0, ms.rawKey.getLength(), 
              ms.rawValue);                       // write top entry
          
          if (ms.next()) {                        // has another entry
            adjustTop();
          } else {
            pop();                                // done with this file
            ms.in.close();
          }
        }

        if (writer instanceof SequenceFile.BlockCompressWriter) {
          SequenceFile.BlockCompressWriter bcWriter = 
            (SequenceFile.BlockCompressWriter) writer;
          bcWriter.writeBlock();
        }
        out.flush();
      }

      public void close() throws IOException {
        MergeStream ms;                           // close inputs
        while ((ms = (MergeStream)pop()) != null) {
          ms.in.close();
        }
        out.close();                              // close output
        if (indexOut != null) {
          indexOut.close();
        }
      }
      
    } // SequenceFile.Sorter.MergeQueue
    
  } // SequenceFile.Sorter

} // SequenceFile
