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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <code>IFile</code> is the simple &lt;key-len, value-len, key, value&gt; format
 * for the intermediate map-outputs in Map-Reduce.
 *
 * There is a <code>Writer</code> to write out map-outputs in this format and 
 * a <code>Reader</code> to read files of this format.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class IFile {
  private static final Logger LOG = LoggerFactory.getLogger(IFile.class);
  public static final int EOF_MARKER = -1; // End of File Marker
  private static final int ARRAY_MAX_SIZE = Integer.MAX_VALUE - 8;
  
  /**
   * <code>IFile.Writer</code> to write out intermediate map-outputs. 
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static class Writer<K extends Object, V extends Object> {
    FSDataOutputStream out;
    boolean ownOutputStream = false;
    long start = 0;
    FSDataOutputStream rawOut;
    
    CompressionOutputStream compressedOut;
    Compressor compressor;
    boolean compressOutput = false;
    
    long decompressedBytesWritten = 0;
    long compressedBytesWritten = 0;

    // Count records written to disk
    private long numRecordsWritten = 0;
    private final Counters.Counter writtenRecordsCounter;

    IFileOutputStream checksumOut;

    Class<K> keyClass;
    Class<V> valueClass;
    Serializer<K> keySerializer;
    Serializer<V> valueSerializer;
    
    DataOutputBuffer buffer = new DataOutputBuffer();

    public Writer(Configuration conf, FSDataOutputStream out,
        Class<K> keyClass, Class<V> valueClass,
        CompressionCodec codec, Counters.Counter writesCounter)
        throws IOException {
      this(conf, out, keyClass, valueClass, codec, writesCounter, false);
    }
    
    protected Writer(Counters.Counter writesCounter) {
      writtenRecordsCounter = writesCounter;
    }

    public Writer(Configuration conf, FSDataOutputStream out, 
        Class<K> keyClass, Class<V> valueClass,
        CompressionCodec codec, Counters.Counter writesCounter,
        boolean ownOutputStream)
        throws IOException {
      this.writtenRecordsCounter = writesCounter;
      this.checksumOut = new IFileOutputStream(out);
      this.rawOut = out;
      this.start = this.rawOut.getPos();
      if (codec != null) {
        this.compressor = CodecPool.getCompressor(codec);
        if (this.compressor != null) {
          this.compressor.reset();
          this.compressedOut = codec.createOutputStream(checksumOut, compressor);
          this.out = new FSDataOutputStream(this.compressedOut,  null);
          this.compressOutput = true;
        } else {
          LOG.warn("Could not obtain compressor from CodecPool");
          this.out = new FSDataOutputStream(checksumOut,null);
        }
      } else {
        this.out = new FSDataOutputStream(checksumOut,null);
      }
      
      this.keyClass = keyClass;
      this.valueClass = valueClass;

      if (keyClass != null) {
        SerializationFactory serializationFactory = 
          new SerializationFactory(conf);
        this.keySerializer = serializationFactory.getSerializer(keyClass);
        this.keySerializer.open(buffer);
        this.valueSerializer = serializationFactory.getSerializer(valueClass);
        this.valueSerializer.open(buffer);
      }
      this.ownOutputStream = ownOutputStream;
    }

    public void close() throws IOException {

      // When IFile writer is created by BackupStore, we do not have
      // Key and Value classes set. So, check before closing the
      // serializers
      if (keyClass != null) {
        keySerializer.close();
        valueSerializer.close();
      }

      // Write EOF_MARKER for key/value length
      WritableUtils.writeVInt(out, EOF_MARKER);
      WritableUtils.writeVInt(out, EOF_MARKER);
      decompressedBytesWritten += (long) 2 * WritableUtils.getVIntSize(EOF_MARKER);
      
      //Flush the stream
      out.flush();
  
      if (compressOutput) {
        // Flush
        compressedOut.finish();
        compressedOut.resetState();
      }
      
      // Close the underlying stream iff we own it...
      if (ownOutputStream) {
        out.close();
      }
      else {
        // Write the checksum
        checksumOut.finish();
      }

      compressedBytesWritten = rawOut.getPos() - start;

      if (compressOutput) {
        // Return back the compressor
        CodecPool.returnCompressor(compressor);
        compressor = null;
      }

      out = null;
      if(writtenRecordsCounter != null) {
        writtenRecordsCounter.increment(numRecordsWritten);
      }
    }

    public void append(K key, V value) throws IOException {
      if (key.getClass() != keyClass)
        throw new IOException("wrong key class: "+ key.getClass()
                              +" is not "+ keyClass);
      if (value.getClass() != valueClass)
        throw new IOException("wrong value class: "+ value.getClass()
                              +" is not "+ valueClass);

      // Append the 'key'
      keySerializer.serialize(key);
      int keyLength = buffer.getLength();
      if (keyLength < 0) {
        throw new IOException("Negative key-length not allowed: " + keyLength + 
                              " for " + key);
      }

      // Append the 'value'
      valueSerializer.serialize(value);
      int valueLength = buffer.getLength() - keyLength;
      if (valueLength < 0) {
        throw new IOException("Negative value-length not allowed: " + 
                              valueLength + " for " + value);
      }
      
      // Write the record out
      WritableUtils.writeVInt(out, keyLength);                  // key length
      WritableUtils.writeVInt(out, valueLength);                // value length
      out.write(buffer.getData(), 0, buffer.getLength());       // data

      // Reset
      buffer.reset();
      
      // Update bytes written
      decompressedBytesWritten += (long) keyLength + valueLength +
                                  WritableUtils.getVIntSize(keyLength) + 
                                  WritableUtils.getVIntSize(valueLength);
      ++numRecordsWritten;
    }
    
    public void append(DataInputBuffer key, DataInputBuffer value)
    throws IOException {
      int keyLength = key.getLength() - key.getPosition();
      if (keyLength < 0) {
        throw new IOException("Negative key-length not allowed: " + keyLength + 
                              " for " + key);
      }
      
      int valueLength = value.getLength() - value.getPosition();
      if (valueLength < 0) {
        throw new IOException("Negative value-length not allowed: " + 
                              valueLength + " for " + value);
      }

      WritableUtils.writeVInt(out, keyLength);
      WritableUtils.writeVInt(out, valueLength);
      out.write(key.getData(), key.getPosition(), keyLength); 
      out.write(value.getData(), value.getPosition(), valueLength); 

      // Update bytes written
      decompressedBytesWritten += (long) keyLength + valueLength +
                      WritableUtils.getVIntSize(keyLength) + 
                      WritableUtils.getVIntSize(valueLength);
      ++numRecordsWritten;
    }
    
    // Required for mark/reset
    public DataOutputStream getOutputStream () {
      return out;
    }
    
    // Required for mark/reset
    public void updateCountersForExternalAppend(long length) {
      ++numRecordsWritten;
      decompressedBytesWritten += length;
    }
    
    public long getRawLength() {
      return decompressedBytesWritten;
    }
    
    public long getCompressedLength() {
      return compressedBytesWritten;
    }
  }

  /**
   * <code>IFile.Reader</code> to read intermediate map-outputs. 
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static class Reader<K extends Object, V extends Object> {
    private static final int DEFAULT_BUFFER_SIZE = 128*1024;
    private static final int MAX_VINT_SIZE = 9;

    // Count records read from disk
    private long numRecordsRead = 0;
    private final Counters.Counter readRecordsCounter;

    final InputStream in;        // Possibly decompressed stream that we read
    Decompressor decompressor;
    public long bytesRead = 0;
    protected final long fileLength;
    protected boolean eof = false;
    final IFileInputStream checksumIn;
    
    protected byte[] buffer = null;
    protected int bufferSize = DEFAULT_BUFFER_SIZE;
    protected DataInputStream dataIn;

    protected int recNo = 1;
    protected int currentKeyLength;
    protected int currentValueLength;
    byte keyBytes[] = new byte[0];
    
    
    /**
     * Construct an IFile Reader.
     * 
     * @param conf Configuration File 
     * @param fs  FileSystem
     * @param file Path of the file to be opened. This file should have
     *             checksum bytes for the data at the end of the file.
     * @param codec codec
     * @param readsCounter Counter for records read from disk
     * @throws IOException
     */
    public Reader(Configuration conf, FileSystem fs, Path file,
                  CompressionCodec codec,
                  Counters.Counter readsCounter) throws IOException {
      this(conf, fs.open(file), 
           fs.getFileStatus(file).getLen(),
           codec, readsCounter);
    }

    /**
     * Construct an IFile Reader.
     * 
     * @param conf Configuration File 
     * @param in   The input stream
     * @param length Length of the data in the stream, including the checksum
     *               bytes.
     * @param codec codec
     * @param readsCounter Counter for records read from disk
     * @throws IOException
     */
    public Reader(Configuration conf, FSDataInputStream in, long length, 
                  CompressionCodec codec,
                  Counters.Counter readsCounter) throws IOException {
      readRecordsCounter = readsCounter;
      checksumIn = new IFileInputStream(in,length, conf);
      if (codec != null) {
        decompressor = CodecPool.getDecompressor(codec);
        if (decompressor != null) {
          this.in = codec.createInputStream(checksumIn, decompressor);
        } else {
          LOG.warn("Could not obtain decompressor from CodecPool");
          this.in = checksumIn;
        }
      } else {
        this.in = checksumIn;
      }
      this.dataIn = new DataInputStream(this.in);
      this.fileLength = length;
      
      if (conf != null) {
        bufferSize = conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
      }
    }
    
    public long getLength() { 
      return fileLength - checksumIn.getSize();
    }
    
    public long getPosition() throws IOException {    
      return checksumIn.getPosition(); 
    }
    
    /**
     * Read upto len bytes into buf starting at offset off.
     * 
     * @param buf buffer 
     * @param off offset
     * @param len length of buffer
     * @return the no. of bytes read
     * @throws IOException
     */
    private int readData(byte[] buf, int off, int len) throws IOException {
      int bytesRead = 0;
      while (bytesRead < len) {
        int n = IOUtils.wrappedReadForCompressedData(in, buf, off + bytesRead,
            len - bytesRead);
        if (n < 0) {
          return bytesRead;
        }
        bytesRead += n;
      }
      return len;
    }
    
    protected boolean positionToNextRecord(DataInput dIn) throws IOException {
      // Sanity check
      if (eof) {
        throw new EOFException("Completed reading " + bytesRead);
      }
      
      // Read key and value lengths
      currentKeyLength = WritableUtils.readVInt(dIn);
      currentValueLength = WritableUtils.readVInt(dIn);
      bytesRead += (long) WritableUtils.getVIntSize(currentKeyLength) +
                   WritableUtils.getVIntSize(currentValueLength);
      
      // Check for EOF
      if (currentKeyLength == EOF_MARKER && currentValueLength == EOF_MARKER) {
        eof = true;
        return false;
      }
      
      // Sanity check
      if (currentKeyLength < 0) {
        throw new IOException("Rec# " + recNo + ": Negative key-length: " + 
                              currentKeyLength);
      }
      if (currentValueLength < 0) {
        throw new IOException("Rec# " + recNo + ": Negative value-length: " + 
                              currentValueLength);
      }
            
      return true;
    }
    
    public boolean nextRawKey(DataInputBuffer key) throws IOException {
      if (!positionToNextRecord(dataIn)) {
        return false;
      }
      if (keyBytes.length < currentKeyLength) {
        keyBytes = new byte[currentKeyLength << 1];
      }
      int i = readData(keyBytes, 0, currentKeyLength);
      if (i != currentKeyLength) {
        throw new IOException ("Asked for " + currentKeyLength + " Got: " + i);
      }
      key.reset(keyBytes, currentKeyLength);
      bytesRead += currentKeyLength;
      return true;
    }
    
    public void nextRawValue(DataInputBuffer value) throws IOException {
      final int targetSize = currentValueLength << 1;

      final byte[] valBytes = (value.getData().length < currentValueLength)
        ? new byte[targetSize < 0 ? ARRAY_MAX_SIZE : targetSize]
        : value.getData();
      int i = readData(valBytes, 0, currentValueLength);
      if (i != currentValueLength) {
        throw new IOException ("Asked for " + currentValueLength + " Got: " + i);
      }
      value.reset(valBytes, currentValueLength);
      
      // Record the bytes read
      bytesRead += currentValueLength;

      ++recNo;
      ++numRecordsRead;
    }
    
    public void close() throws IOException {
      // Close the underlying stream
      in.close();
      
      // Release the buffer
      dataIn = null;
      buffer = null;
      if(readRecordsCounter != null) {
        readRecordsCounter.increment(numRecordsRead);
      }

      // Return the decompressor
      if (decompressor != null) {
        decompressor.reset();
        CodecPool.returnDecompressor(decompressor);
        decompressor = null;
      }
    }
    
    public void reset(int offset) {
      return;
    }

    public void disableChecksumValidation() {
      checksumIn.disableChecksumValidation();
    }

  }    
}
