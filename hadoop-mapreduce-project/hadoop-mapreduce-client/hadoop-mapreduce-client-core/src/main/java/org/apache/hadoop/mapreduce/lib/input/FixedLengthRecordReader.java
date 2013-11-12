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

package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

/**
 * A reader to read fixed length records from a split.  Record offset is
 * returned as key and the record as bytes is returned in value.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FixedLengthRecordReader
    extends RecordReader<LongWritable, BytesWritable> {
  private static final Log LOG 
      = LogFactory.getLog(FixedLengthRecordReader.class);

  private int recordLength;
  private long start;
  private long pos;
  private long end;
  private long  numRecordsRemainingInSplit;
  private FSDataInputStream fileIn;
  private Seekable filePosition;
  private LongWritable key;
  private BytesWritable value;
  private boolean isCompressedInput;
  private Decompressor decompressor;
  private InputStream inputStream;

  public FixedLengthRecordReader(int recordLength) {
    this.recordLength = recordLength;
  }

  @Override
  public void initialize(InputSplit genericSplit,
                         TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();
    final Path file = split.getPath();
    initialize(job, split.getStart(), split.getLength(), file);
  }

  // This is also called from the old FixedLengthRecordReader API implementation
  public void initialize(Configuration job, long splitStart, long splitLength,
                         Path file) throws IOException {
    start = splitStart;
    end = start + splitLength;
    long partialRecordLength = start % recordLength;
    long numBytesToSkip = 0;
    if (partialRecordLength != 0) {
      numBytesToSkip = recordLength - partialRecordLength;
    }

    // open the file and seek to the start of the split
    final FileSystem fs = file.getFileSystem(job);
    fileIn = fs.open(file);

    CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
    if (null != codec) {
      isCompressedInput = true;	
      decompressor = CodecPool.getDecompressor(codec);
      CompressionInputStream cIn
          = codec.createInputStream(fileIn, decompressor);
      filePosition = cIn;
      inputStream = cIn;
      numRecordsRemainingInSplit = Long.MAX_VALUE;
      LOG.info(
          "Compressed input; cannot compute number of records in the split");
    } else {
      fileIn.seek(start);
      filePosition = fileIn;
      inputStream = fileIn;
      long splitSize = end - start - numBytesToSkip;
      numRecordsRemainingInSplit = (splitSize + recordLength - 1)/recordLength;
      if (numRecordsRemainingInSplit < 0) {
        numRecordsRemainingInSplit = 0;
      }
      LOG.info("Expecting " + numRecordsRemainingInSplit
          + " records each with a length of " + recordLength
          + " bytes in the split with an effective size of "
          + splitSize + " bytes");
    }
    if (numBytesToSkip != 0) {
      start += inputStream.skip(numBytesToSkip);
    }
    this.pos = start;
  }

  @Override
  public synchronized boolean nextKeyValue() throws IOException {
    if (key == null) {
      key = new LongWritable();
    }
    if (value == null) {
      value = new BytesWritable(new byte[recordLength]);
    }
    boolean dataRead = false;
    value.setSize(recordLength);
    byte[] record = value.getBytes();
    if (numRecordsRemainingInSplit > 0) {
      key.set(pos);
      int offset = 0;
      int numBytesToRead = recordLength;
      int numBytesRead = 0;
      while (numBytesToRead > 0) {
        numBytesRead = inputStream.read(record, offset, numBytesToRead);
        if (numBytesRead == -1) {
          // EOF
          break;
        }
        offset += numBytesRead;
        numBytesToRead -= numBytesRead;
      }
      numBytesRead = recordLength - numBytesToRead;
      pos += numBytesRead;
      if (numBytesRead > 0) {
        dataRead = true;
        if (numBytesRead >= recordLength) {
          if (!isCompressedInput) {
            numRecordsRemainingInSplit--;
          }
        } else {
          throw new IOException("Partial record(length = " + numBytesRead
              + ") found at the end of split.");
        }
      } else {
        numRecordsRemainingInSplit = 0L; // End of input.
      }
    }
    return dataRead;
  }

  @Override
  public LongWritable getCurrentKey() {
    return key;
  }

  @Override
  public BytesWritable getCurrentValue() {
    return value;
  }

  @Override
  public synchronized float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
    }
  }
  
  @Override
  public synchronized void close() throws IOException {
    try {
      if (inputStream != null) {
        inputStream.close();
        inputStream = null;
      }
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
        decompressor = null;
      }
    }
  }

  // This is called from the old FixedLengthRecordReader API implementation.
  public long getPos() {
    return pos;
  }

  private long getFilePosition() throws IOException {
    long retVal;
    if (isCompressedInput && null != filePosition) {
      retVal = filePosition.getPos();
    } else {
      retVal = pos;
    }
    return retVal;
  }

}
