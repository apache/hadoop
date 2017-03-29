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
package org.apache.hadoop.hdfs.server.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.TextFileRegionFormat.*;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.compress.CompressionCodec;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test for the text based block format for provided block maps.
 */
public class TestTextBlockFormat {

  static final Path OUTFILE = new Path("hdfs://dummyServer:0000/dummyFile.txt");

  void check(TextWriter.Options opts, final Path vp,
      final Class<? extends CompressionCodec> vc) throws IOException {
    TextFileRegionFormat mFmt = new TextFileRegionFormat() {
      @Override
      public TextWriter createWriter(Path file, CompressionCodec codec,
          String delim, Configuration conf) throws IOException {
        assertEquals(vp, file);
        if (null == vc) {
          assertNull(codec);
        } else {
          assertEquals(vc, codec.getClass());
        }
        return null; // ignored
      }
    };
    mFmt.getWriter(opts);
  }

  @Test
  public void testWriterOptions() throws Exception {
    TextWriter.Options opts = TextWriter.defaults();
    assertTrue(opts instanceof WriterOptions);
    WriterOptions wopts = (WriterOptions) opts;
    Path def = new Path(DFSConfigKeys.DFS_PROVIDED_BLOCK_MAP_PATH_DEFAULT);
    assertEquals(def, wopts.getFile());
    assertNull(wopts.getCodec());

    opts.filename(OUTFILE);
    check(opts, OUTFILE, null);

    opts.filename(OUTFILE);
    opts.codec("gzip");
    Path cp = new Path(OUTFILE.getParent(), OUTFILE.getName() + ".gz");
    check(opts, cp, org.apache.hadoop.io.compress.GzipCodec.class);

  }

  @Test
  public void testCSVReadWrite() throws Exception {
    final DataOutputBuffer out = new DataOutputBuffer();
    FileRegion r1 = new FileRegion(4344L, OUTFILE, 0, 1024);
    FileRegion r2 = new FileRegion(4345L, OUTFILE, 1024, 1024);
    FileRegion r3 = new FileRegion(4346L, OUTFILE, 2048, 512);
    try (TextWriter csv = new TextWriter(new OutputStreamWriter(out), ",")) {
      csv.store(r1);
      csv.store(r2);
      csv.store(r3);
    }
    Iterator<FileRegion> i3;
    try (TextReader csv = new TextReader(null, null, null, ",") {
      @Override
      public InputStream createStream() {
        DataInputBuffer in = new DataInputBuffer();
        in.reset(out.getData(), 0, out.getLength());
        return in;
        }}) {
      Iterator<FileRegion> i1 = csv.iterator();
      assertEquals(r1, i1.next());
      Iterator<FileRegion> i2 = csv.iterator();
      assertEquals(r1, i2.next());
      assertEquals(r2, i2.next());
      assertEquals(r3, i2.next());
      assertEquals(r2, i1.next());
      assertEquals(r3, i1.next());

      assertFalse(i1.hasNext());
      assertFalse(i2.hasNext());
      i3 = csv.iterator();
    }
    try {
      i3.next();
    } catch (IllegalStateException e) {
      return;
    }
    fail("Invalid iterator");
  }

  @Test
  public void testCSVReadWriteTsv() throws Exception {
    final DataOutputBuffer out = new DataOutputBuffer();
    FileRegion r1 = new FileRegion(4344L, OUTFILE, 0, 1024);
    FileRegion r2 = new FileRegion(4345L, OUTFILE, 1024, 1024);
    FileRegion r3 = new FileRegion(4346L, OUTFILE, 2048, 512);
    try (TextWriter csv = new TextWriter(new OutputStreamWriter(out), "\t")) {
      csv.store(r1);
      csv.store(r2);
      csv.store(r3);
    }
    Iterator<FileRegion> i3;
    try (TextReader csv = new TextReader(null, null, null, "\t") {
      @Override
      public InputStream createStream() {
        DataInputBuffer in = new DataInputBuffer();
        in.reset(out.getData(), 0, out.getLength());
        return in;
      }}) {
      Iterator<FileRegion> i1 = csv.iterator();
      assertEquals(r1, i1.next());
      Iterator<FileRegion> i2 = csv.iterator();
      assertEquals(r1, i2.next());
      assertEquals(r2, i2.next());
      assertEquals(r3, i2.next());
      assertEquals(r2, i1.next());
      assertEquals(r3, i1.next());

      assertFalse(i1.hasNext());
      assertFalse(i2.hasNext());
      i3 = csv.iterator();
    }
    try {
      i3.next();
    } catch (IllegalStateException e) {
      return;
    }
    fail("Invalid iterator");
  }

}
