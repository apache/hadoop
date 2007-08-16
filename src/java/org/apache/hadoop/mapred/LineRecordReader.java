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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

/**
 * Treats keys as offset in file and value as line. 
 */
public class LineRecordReader implements RecordReader<LongWritable, Text> {
  private CompressionCodecFactory compressionCodecs = null;
  private long start; 
  private long pos;
  private long end;
  private BufferedInputStream in;
  private ByteArrayOutputStream buffer = new ByteArrayOutputStream(256);
  /**
   * Provide a bridge to get the bytes from the ByteArrayOutputStream
   * without creating a new byte array.
   */
  private static class TextStuffer extends OutputStream {
    public Text target;
    public void write(int b) {
      throw new UnsupportedOperationException("write(byte) not supported");
    }
    public void write(byte[] data, int offset, int len) throws IOException {
      target.set(data, offset, len);
    }      
  }
  private TextStuffer bridge = new TextStuffer();

  public LineRecordReader(Configuration job, FileSplit split)
    throws IOException {
    long start = split.getStart();
    long end = start + split.getLength();
    final Path file = split.getPath();
    compressionCodecs = new CompressionCodecFactory(job);
    final CompressionCodec codec = compressionCodecs.getCodec(file);

    // open the file and seek to the start of the split
    FileSystem fs = file.getFileSystem(job);
    FSDataInputStream fileIn = fs.open(split.getPath());
    InputStream in = fileIn;
    boolean skipFirstLine = false;
    if (codec != null) {
      in = codec.createInputStream(fileIn);
      end = Long.MAX_VALUE;
    } else if (start != 0) {
      skipFirstLine = true;  // wait till BufferedInputStream to skip
      --start;
      fileIn.seek(start);
    }

    this.in = new BufferedInputStream(in);
    if (skipFirstLine) {  // skip first line and re-establish "start".
      start += LineRecordReader.readLine(this.in, null);
    }
    this.start = start;
    this.pos = start;
    this.end = end;
  }
  
  public LineRecordReader(InputStream in, long offset, long endOffset) 
    throws IOException{
    this.in = new BufferedInputStream(in);
    this.start = offset;
    this.pos = offset;
    this.end = endOffset;    
    //    readLine(in, null); 
  }
  
  public LongWritable createKey() {
    return new LongWritable();
  }
  
  public Text createValue() {
    return new Text();
  }
  
  /** Read a line. */
  public synchronized boolean next(LongWritable key, Text value)
    throws IOException {
    if (pos >= end)
      return false;

    key.set(pos);           // key is position
    buffer.reset();
    long bytesRead = readLine();
    if (bytesRead == 0) {
      return false;
    }
    pos += bytesRead;
    bridge.target = value;
    buffer.writeTo(bridge);
    return true;
  }
  
  protected long readLine() throws IOException {
    return LineRecordReader.readLine(in, buffer);
  }

  public static long readLine(InputStream in, 
                              OutputStream out) throws IOException {
    long bytes = 0;
    while (true) {
      
      int b = in.read();
      if (b == -1) {
        break;
      }
      bytes += 1;
      
      byte c = (byte)b;
      if (c == '\n') {
        break;
      }
      
      if (c == '\r') {
        in.mark(1);
        byte nextC = (byte)in.read();
        if (nextC != '\n') {
          in.reset();
        } else {
          bytes += 1;
        }
        break;
      }
      
      if (out != null) {
        out.write(c);
      }
    }
    return bytes;
  }
  
  /**
   * Get the progress within the split
   */
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float)(end - start));
    }
  }
  
  public  synchronized long getPos() throws IOException {
    return pos;
  }

  public synchronized void close() throws IOException { 
    in.close(); 
  }
}
