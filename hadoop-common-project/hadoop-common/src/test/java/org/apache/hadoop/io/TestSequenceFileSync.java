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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

public class TestSequenceFileSync {
  private static final int NUMRECORDS = 2000;
  private static final int RECORDSIZE = 80;
  private static final Random rand = new Random();

  private final static String REC_FMT = "%d RECORDID %d : ";


  private static void forOffset(SequenceFile.Reader reader,
      IntWritable key, Text val, int iter, long off, int expectedRecord)
      throws IOException {
    val.clear();
    reader.sync(off);
    reader.next(key, val);
    assertEquals(key.get(), expectedRecord);
    final String test = String.format(REC_FMT, expectedRecord, expectedRecord);
    assertEquals("Invalid value " + val, 0, val.find(test, 0));
  }

  @Test
  public void testLowSyncpoint() throws IOException {
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.getLocal(conf);
    final Path path = new Path(GenericTestUtils.getTempPath(
        "sequencefile.sync.test"));
    final IntWritable input = new IntWritable();
    final Text val = new Text();
    SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path,
        IntWritable.class, Text.class);
    try {
      writeSequenceFile(writer, NUMRECORDS);
      for (int i = 0; i < 5 ; i++) {
       final SequenceFile.Reader reader;
       
       //try different SequenceFile.Reader constructors
       if (i % 2 == 0) {
         reader = new SequenceFile.Reader(fs, path, conf);
       } else {
         final FSDataInputStream in = fs.open(path);
         final long length = fs.getFileStatus(path).getLen();
         final int buffersize = conf.getInt("io.file.buffer.size", 4096);
         reader = new SequenceFile.Reader(in, buffersize, 0L, length, conf);
       }

       try {
          forOffset(reader, input, val, i, 0, 0);
          forOffset(reader, input, val, i, 65, 0);
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

  public static void writeSequenceFile(SequenceFile.Writer writer,
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

  static void randomText(Text val, int id, int recordSize) {
    val.clear();
    final StringBuilder ret = new StringBuilder(recordSize);
    ret.append(String.format(REC_FMT, id, id));
    recordSize -= ret.length();
    for (int i = 0; i < recordSize; ++i) {
      ret.append(rand.nextInt(9));
    }
    val.set(ret.toString());
  }
}
