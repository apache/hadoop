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

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;

/** An {@link InputFormat} for plain text files.  Files are broken into lines.
 * Either linefeed or carriage-return are used to signal end of line.  Keys are
 * the position in the file, and values are the line of text.. */
public class TextInputFormat extends InputFormatBase implements JobConfigurable {

  private CompressionCodecFactory compressionCodecs = null;
  
  public void configure(JobConf conf) {
    compressionCodecs = new CompressionCodecFactory(conf);
  }
  
  protected boolean isSplitable(FileSystem fs, Path file) {
    return compressionCodecs.getCodec(file) == null;
  }
  
  protected static class LineRecordReader implements RecordReader {
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

    public LineRecordReader(InputStream in, long offset, long endOffset) {
      this.in = new BufferedInputStream(in);
      this.pos = offset;
      this.end = endOffset;
    }
    
    public WritableComparable createKey() {
      return new LongWritable();
    }
    
    public Writable createValue() {
      return new Text();
    }
    
    /** Read a line. */
    public synchronized boolean next(Writable key, Writable value)
      throws IOException {
      if (pos >= end)
        return false;

      ((LongWritable)key).set(pos);           // key is position
      buffer.reset();
      long bytesRead = readLine(in, buffer);
      if (bytesRead == 0) {
        return false;
      }
      pos += bytesRead;
      bridge.target = (Text) value;
      buffer.writeTo(bridge);
      return true;
    }
    
    public  synchronized long getPos() throws IOException {
      return pos;
    }

    public synchronized void close() throws IOException { 
      in.close(); 
    }  

  }
  
  public RecordReader getRecordReader(FileSystem fs, FileSplit split,
                                      JobConf job, Reporter reporter)
    throws IOException {

    reporter.setStatus(split.toString());

    long start = split.getStart();
    long end = start + split.getLength();
    final Path file = split.getPath();
    final CompressionCodec codec = compressionCodecs.getCodec(file);

    // open the file and seek to the start of the split
    FSDataInputStream fileIn = fs.open(split.getPath());
    InputStream in = fileIn;
    
    if (codec != null) {
      in = codec.createInputStream(fileIn);
      end = Long.MAX_VALUE;
    } else if (start != 0) {
      fileIn.seek(start-1);
      readLine(fileIn, null);
      start = fileIn.getPos();
    }
    
    return new LineRecordReader(in, start, end);
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

}

