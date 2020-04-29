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

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests sync based seek reads/write intervals inside SequenceFiles. */
public class TestSequenceFileSync {
  private static final int NUMRECORDS = 2000;
  private static final int RECORDSIZE = 80;
  private static final Random RAND = new Random();

  private final static String REC_FMT = "%d RECORDID %d : ";


  private static void forOffset(SequenceFile.Reader reader,
      IntWritable key, Text val, int iter, long off, int expectedRecord)
      throws IOException {
    val.clear();
    reader.sync(off);
    reader.next(key, val);
    assertThat(key.get()).isEqualTo(expectedRecord);
    final String test = String.format(REC_FMT, expectedRecord, expectedRecord);
    assertThat(val.find(test, 0)).withFailMessage(
        "Invalid value in iter " + iter + ": " + val).isZero();
  }

  @Test
  public void testDefaultSyncInterval() throws IOException {
    // Uses the default sync interval of 100 KB
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.getLocal(conf);
    final Path path = new Path(GenericTestUtils.getTempPath(
            "sequencefile.sync.test"));
    final IntWritable input = new IntWritable();
    final Text val = new Text();
    SequenceFile.Writer writer = new SequenceFile.Writer(
        conf,
        SequenceFile.Writer.file(path),
        SequenceFile.Writer.compression(CompressionType.NONE),
        SequenceFile.Writer.keyClass(IntWritable.class),
        SequenceFile.Writer.valueClass(Text.class)
    );
    try {
      writeSequenceFile(writer, NUMRECORDS*4);
      for (int i = 0; i < 5; i++) {
        final SequenceFile.Reader reader;

        //try different SequenceFile.Reader constructors
        if (i % 2 == 0) {
          final int buffersize = conf.getInt("io.file.buffer.size", 4096);
          reader = new SequenceFile.Reader(conf,
              SequenceFile.Reader.file(path),
              SequenceFile.Reader.bufferSize(buffersize));
        } else {
          final FSDataInputStream in = fs.open(path);
          final long length = fs.getFileStatus(path).getLen();
          reader = new SequenceFile.Reader(conf,
              SequenceFile.Reader.stream(in),
              SequenceFile.Reader.start(0L),
              SequenceFile.Reader.length(length));
        }

        try {
          forOffset(reader, input, val, i, 0, 0);
          forOffset(reader, input, val, i, 65, 0);
          // There would be over 1000 records within
          // this sync interval
          forOffset(reader, input, val, i, 2000, 1101);
          forOffset(reader, input, val, i, 0, 0);
        } finally {
          reader.close();
        }
      }
    } finally {
      fs.delete(path, false);
    }
  }

  @Test
  public void testLowSyncpoint() throws IOException {
    // Uses a smaller sync interval of 2000 bytes
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.getLocal(conf);
    final Path path = new Path(GenericTestUtils.getTempPath(
        "sequencefile.sync.test"));
    final IntWritable input = new IntWritable();
    final Text val = new Text();
    SequenceFile.Writer writer = new SequenceFile.Writer(
        conf,
        SequenceFile.Writer.file(path),
        SequenceFile.Writer.compression(CompressionType.NONE),
        SequenceFile.Writer.keyClass(IntWritable.class),
        SequenceFile.Writer.valueClass(Text.class),
        SequenceFile.Writer.syncInterval(20*100)
    );
    // Ensure the custom sync interval value is set
    assertThat(writer.syncInterval).isEqualTo(20*100);
    try {
      writeSequenceFile(writer, NUMRECORDS);
      for (int i = 0; i < 5; i++) {
        final SequenceFile.Reader reader;
       
        //try different SequenceFile.Reader constructors
        if (i % 2 == 0) {
          final int bufferSize = conf.getInt("io.file.buffer.size", 4096);
          reader = new SequenceFile.Reader(
              conf,
              SequenceFile.Reader.file(path),
              SequenceFile.Reader.bufferSize(bufferSize));
        } else {
          final FSDataInputStream in = fs.open(path);
          final long length = fs.getFileStatus(path).getLen();
          reader = new SequenceFile.Reader(
              conf,
              SequenceFile.Reader.stream(in),
              SequenceFile.Reader.start(0L),
              SequenceFile.Reader.length(length));
        }

        try {
          forOffset(reader, input, val, i, 0, 0);
          forOffset(reader, input, val, i, 65, 0);
          // There would be only a few records within
          // this sync interval
          forOffset(reader, input, val, i, 2000, 21);
          forOffset(reader, input, val, i, 0, 0);
        } finally {
          reader.close();
        }
      }
    } finally {
      fs.delete(path, false);
    }
  }

  private static void writeSequenceFile(SequenceFile.Writer writer,
      int numRecords) throws IOException {
    final IntWritable key = new IntWritable();
    final Text val = new Text();
    for (int numWritten = 0; numWritten < numRecords; ++numWritten) {
      key.set(numWritten);
      randomText(val, numWritten, RECORDSIZE);
      writer.append(key, val);
    }
    writer.close();
  }

  private static void randomText(Text val, int id, int recordSize) {
    val.clear();
    final StringBuilder ret = new StringBuilder(recordSize);
    ret.append(String.format(REC_FMT, id, id));
    recordSize -= ret.length();
    for (int i = 0; i < recordSize; ++i) {
      ret.append(RAND.nextInt(9));
    }
    val.set(ret.toString());
  }
}
