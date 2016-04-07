/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.io.file.tfile;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.file.tfile.TFile.Writer;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestTFileComparator2 {
  private static String ROOT = GenericTestUtils.getTestDir().getAbsolutePath();
  private static final String name = "test-tfile-comparator2";
  private final static int BLOCK_SIZE = 512;
  private static final String VALUE = "value";
  private static final String jClassLongWritableComparator = "jclass:"
      + LongWritable.Comparator.class.getName();
  private static final long NENTRY = 10000;

  private static long cube(long n) {
    return n*n*n;
  }
  
  private static String buildValue(long i) {
    return String.format("%s-%d", VALUE, i);
  }
  
  @Test
  public void testSortedLongWritable() throws IOException {
    Configuration conf = new Configuration();
    Path path = new Path(ROOT, name);
    FileSystem fs = path.getFileSystem(conf);
    FSDataOutputStream out = fs.create(path);
    try {
    TFile.Writer writer = new Writer(out, BLOCK_SIZE, "gz",
        jClassLongWritableComparator, conf);
      try {
        LongWritable key = new LongWritable(0);
        for (long i=0; i<NENTRY; ++i) {
          key.set(cube(i-NENTRY/2));
          DataOutputStream dos = writer.prepareAppendKey(-1);
          try {
            key.write(dos);
          } finally {
            dos.close();
          }
          dos = writer.prepareAppendValue(-1);
          try {
            dos.write(buildValue(i).getBytes());
          } finally {
            dos.close();
          }
        }
      } finally {
        writer.close();
      } 
    } finally {
      out.close();
    }
    
    FSDataInputStream in = fs.open(path);
    try {
      TFile.Reader reader = new TFile.Reader(in, fs.getFileStatus(path)
          .getLen(), conf);
      try {
        TFile.Reader.Scanner scanner = reader.createScanner();
        long i=0;
        BytesWritable value = new BytesWritable();
        for (; !scanner.atEnd(); scanner.advance()) {
          scanner.entry().getValue(value);
          assertEquals(buildValue(i), new String(value.getBytes(), 0, value
              .getLength()));
          ++i;
        }
      } finally {
        reader.close();
      }
    } finally {
      in.close();
    }
  }
}
