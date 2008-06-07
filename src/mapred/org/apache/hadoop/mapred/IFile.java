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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;

/**
 * <code>IFile</code> is the simple <key-len, key, value-len, value> format
 * for the intermediate map-outputs in Map-Reduce.
 * 
 * There is a <code>Writer</code> to write out map-outputs in this format and 
 * a <code>Reader</code> to read files of this format.
 */
class IFile {

  private static int EOF_MARKER = -1;
  
  /**
   * <code>IFile.Writer</code> to write out intermediate map-outputs. 
   */
  public static class Writer<K extends Object, V extends Object> {
    FSDataOutputStream out;
    boolean ownOutputStream = false;
    long start = 0;
    
    CompressionOutputStream compressedOut;
    Compressor compressor;
    boolean compressOutput = false;
    
    long decompressedBytesWritten = 0;
    long compressedBytesWritten = 0;
    
    Class<K> keyClass;
    Class<V> valueClass;
    Serializer<K> keySerializer;
    Serializer<V> valueSerializer;
    
    DataOutputBuffer buffer = new DataOutputBuffer();

    public Writer(Configuration conf, FileSystem fs, Path file, 
                  Class<K> keyClass, Class<V> valueClass,
                  CompressionCodec codec) throws IOException {
      this(conf, fs.create(file), keyClass, valueClass, codec);
      ownOutputStream = true;
    }
    
    public Writer(Configuration conf, FSDataOutputStream out, 
        Class<K> keyClass, Class<V> valueClass,
        CompressionCodec codec) throws IOException {
      if (codec != null) {
        this.compressor = CodecPool.getCompressor(codec);
        this.compressor.reset();
        this.compressedOut = codec.createOutputStream(out, compressor);
        this.out = new FSDataOutputStream(this.compressedOut,  null);
        this.compressOutput = true;
      } else {
        this.out = out;
      }
      this.start = this.out.getPos();
      
      this.keyClass = keyClass;
      this.valueClass = valueClass;
      SerializationFactory serializationFactory = new SerializationFactory(conf);
      this.keySerializer = serializationFactory.getSerializer(keyClass);
      this.keySerializer.open(buffer);
      this.valueSerializer = serializationFactory.getSerializer(valueClass);
      this.valueSerializer.open(buffer);
    }
    
    public void close() throws IOException {
      // Write EOF_MARKER for key/value length
      WritableUtils.writeVInt(out, EOF_MARKER);
      WritableUtils.writeVInt(out, EOF_MARKER);
      decompressedBytesWritten += 2 * WritableUtils.getVIntSize(EOF_MARKER);
      
      if (compressOutput) {
        // Return the compressor
        compressedOut.finish();
        compressedOut.resetState();
        CodecPool.returnCompressor(compressor);
      }
      
      // Close the serializers
      keySerializer.close();
      valueSerializer.close();

      // Close the stream
      if (out != null) {
        out.flush();
        compressedBytesWritten = out.getPos() - start;
        
        // Close the underlying stream iff we own it...
        if (ownOutputStream) {
          out.close();
        }
        
        out = null;
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
      if (keyLength == 0)
        throw new IOException("zero length keys not allowed: " + key);

      // Append the 'value'
      valueSerializer.serialize(value);
      int valueLength = buffer.getLength() - keyLength;
      
      // Write the record out
      WritableUtils.writeVInt(out, keyLength);                  // key length
      WritableUtils.writeVInt(out, valueLength);                // value length
      out.write(buffer.getData(), 0, buffer.getLength());       // data

      // Reset
      buffer.reset();
      
      // Update bytes written
      decompressedBytesWritten += keyLength + valueLength + 
                                  WritableUtils.getVIntSize(keyLength) + 
                                  WritableUtils.getVIntSize(valueLength);
    }
    
    public void append(DataInputBuffer key, DataInputBuffer value)
    throws IOException {
      int keyLength = key.getLength() - key.getPosition();
      int valueLength = value.getLength() - value.getPosition();
      
      WritableUtils.writeVInt(out, keyLength);
      WritableUtils.writeVInt(out, valueLength);
      out.write(key.getData(), key.getPosition(), keyLength); 
      out.write(value.getData(), value.getPosition(), valueLength); 

      // Update bytes written
      decompressedBytesWritten += keyLength + valueLength + 
                      WritableUtils.getVIntSize(keyLength) + 
                      WritableUtils.getVIntSize(valueLength);
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
  public static class Reader<K extends Object, V extends Object> {
    private static final int DEFAULT_BUFFER_SIZE = 128*1024;
    private static final int MAX_VINT_SIZE = 5;

    InputStream in;
    Decompressor decompressor;
    long bytesRead = 0;
    long fileLength = 0;
    boolean eof = false;
    
    byte[] buffer = null;
    int bufferSize = DEFAULT_BUFFER_SIZE;
    DataInputBuffer dataIn = new DataInputBuffer();

    public Reader(Configuration conf, FileSystem fs, Path file,
                  CompressionCodec codec) throws IOException {
      this(conf, fs.open(file), fs.getFileStatus(file).getLen(), codec);
    }
    
    protected Reader() {}
    
    public Reader(Configuration conf, InputStream in, long length, 
                  CompressionCodec codec) throws IOException {
      if (codec != null) {
        decompressor = CodecPool.getDecompressor(codec);
        this.in = codec.createInputStream(in, decompressor);
      } else {
        this.in = in;
      }
      this.fileLength = length;
      
      this.bufferSize = conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
    }
    
    public long getLength() { return fileLength; }
    
    private int readData(byte[] buf, int off, int len) throws IOException {
      int bytesRead = 0;
      while (bytesRead < len) {
        int n = in.read(buf, off+bytesRead, len-bytesRead);
        if (n < 0) {
          return bytesRead;
        }
        bytesRead += n;
      }
      return len;
    }
    
    void readNextBlock(int minSize) throws IOException {
      if (buffer == null) {
        buffer = new byte[bufferSize];
        dataIn.reset(buffer, 0, 0);
      }
      buffer = 
        rejigData(buffer, 
                  (bufferSize < minSize) ? new byte[minSize << 1] : buffer);
      bufferSize = buffer.length;
    }
    
    private byte[] rejigData(byte[] source, byte[] destination) 
    throws IOException{
      // Copy remaining data into the destination array
      int bytesRemaining = dataIn.getLength()-dataIn.getPosition();
      if (bytesRemaining > 0) {
        System.arraycopy(source, dataIn.getPosition(), 
            destination, 0, bytesRemaining);
      }
      
      // Read as much data as will fit from the underlying stream 
      int n = readData(destination, bytesRemaining, 
                       (destination.length - bytesRemaining));
      dataIn.reset(destination, 0, (bytesRemaining + n));
      
      return destination;
    }
    
    public boolean next(DataInputBuffer key, DataInputBuffer value) 
    throws IOException {
      // Sanity check
      if (eof) {
        throw new EOFException("Completed reading " + bytesRead);
      }
      
      // Check if we have enough data to read lengths
      if ((dataIn.getLength() - dataIn.getPosition()) < 2*MAX_VINT_SIZE) {
        readNextBlock(2*MAX_VINT_SIZE);
      }
      
      // Read key and value lengths
      int oldPos = dataIn.getPosition();
      int keyLength = WritableUtils.readVInt(dataIn);
      int valueLength = WritableUtils.readVInt(dataIn);
      int pos = dataIn.getPosition();
      bytesRead += pos - oldPos;
      
      // Check for EOF
      if (keyLength == EOF_MARKER && valueLength == EOF_MARKER) {
        eof = true;
        return false;
      }
      
      final int recordLength = keyLength + valueLength;
      
      // Check if we have the raw key/value in the buffer
      if ((dataIn.getLength()-pos) < recordLength) {
        readNextBlock(recordLength);
        
        // Sanity check
        if ((dataIn.getLength() - dataIn.getPosition()) < recordLength) {
          throw new EOFException("Could read the next record");
        }
      }

      // Setup the key and value
      pos = dataIn.getPosition();
      byte[] data = dataIn.getData();
      key.reset(data, pos, keyLength);
      value.reset(data, (pos + keyLength), valueLength);
      
      // Position for the next record
      dataIn.skip(recordLength);
      bytesRead += recordLength;

      return true;
    }

    public void close() throws IOException {
      // Return the decompressor
      if (decompressor != null) {
        decompressor.reset();
        CodecPool.returnDecompressor(decompressor);
      }
      
      // Close the underlying stream
      if (in != null) {
        in.close();
      }
      
      // Release the buffer
      dataIn = null;
      buffer = null;
    }
  }    
  
  /**
   * <code>IFile.InMemoryReader</code> to read map-outputs present in-memory.
   */
  public static class InMemoryReader<K, V> extends Reader<K, V> {
    RamManager ramManager;
    
    public InMemoryReader(RamManager ramManager, 
                          byte[] data, int start, int length) {
      this.ramManager = ramManager;
      
      buffer = data;
      fileLength = bufferSize = (length - start);
      dataIn.reset(buffer, start, length);
    }
    
    public boolean next(DataInputBuffer key, DataInputBuffer value) 
    throws IOException {
      // Sanity check
      if (eof) {
        throw new EOFException("Completed reading " + bytesRead);
      }
      
      // Read key and value lengths
      int oldPos = dataIn.getPosition();
      int keyLength = WritableUtils.readVInt(dataIn);
      int valueLength = WritableUtils.readVInt(dataIn);
      int pos = dataIn.getPosition();
      bytesRead += pos - oldPos;
      
      // Check for EOF
      if (keyLength == EOF_MARKER && valueLength == EOF_MARKER) {
        eof = true;
        return false;
      }
      
      final int recordLength = keyLength + valueLength;
      
      // Setup the key and value
      pos = dataIn.getPosition();
      byte[] data = dataIn.getData();
      key.reset(data, pos, keyLength);
      value.reset(data, (pos + keyLength), valueLength);
      
      // Position for the next record
      long skipped = dataIn.skip(recordLength);
      if (skipped != recordLength) {
        throw new IOException("Failed to skip past record of length: " + 
                              recordLength);
      }
      
      // Record the byte
      bytesRead += recordLength;

      return true;
    }
      
    public void close() {
      // Release
      dataIn = null;
      buffer = null;
      
      // Inform the RamManager
      ramManager.unreserve(bufferSize);
    }
  }
}
