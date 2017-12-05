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
package org.apache.hadoop.hdfs.server.common.blockaliasmap.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap.*;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.compress.CompressionCodec;

import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.junit.Test;

import static org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap.fileNameFromBlockPoolID;
import static org.junit.Assert.*;

/**
 * Test for the text based block format for provided block maps.
 */
public class TestTextBlockAliasMap {

  static final String OUTFILE_PATH = "hdfs://dummyServer:0000/";
  static final String OUTFILE_BASENAME = "dummyFile";
  static final Path OUTFILE = new Path(OUTFILE_PATH, OUTFILE_BASENAME + "txt");
  static final String BPID = "BPID-0";

  void check(TextWriter.Options opts, final Path vp,
      final Class<? extends CompressionCodec> vc) throws IOException {
    TextFileRegionAliasMap mFmt = new TextFileRegionAliasMap() {
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
    mFmt.getWriter(opts, BPID);
  }

  void check(TextReader.Options opts, final Path vp,
      final Class<? extends CompressionCodec> vc) throws IOException {
    TextFileRegionAliasMap aliasMap = new TextFileRegionAliasMap() {
      @Override
      public TextReader createReader(Path file, String delim, Configuration cfg,
          String blockPoolID) throws IOException {
        assertEquals(vp, file);
        if (null != vc) {
          CompressionCodecFactory factory = new CompressionCodecFactory(cfg);
          CompressionCodec codec = factory.getCodec(file);
          assertEquals(vc, codec.getClass());
        }
        return null; // ignored
      }
    };
    aliasMap.getReader(opts, BPID);
  }

  @Test
  public void testWriterOptions() throws Exception {
    TextWriter.Options opts = TextWriter.defaults();
    assertTrue(opts instanceof WriterOptions);
    WriterOptions wopts = (WriterOptions) opts;
    Path def =
        new Path(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_WRITE_DIR_DEFAULT);
    assertEquals(def, wopts.getDir());
    assertNull(wopts.getCodec());

    Path cp = new Path(OUTFILE_PATH, "blocks_" + BPID + ".csv");
    opts.dirName(new Path(OUTFILE_PATH));
    check(opts, cp, null);

    opts.codec("gzip");
    cp = new Path(OUTFILE_PATH, "blocks_" + BPID + ".csv.gz");
    check(opts, cp, org.apache.hadoop.io.compress.GzipCodec.class);
  }

  @Test
  public void testReaderOptions() throws Exception {
    TextReader.Options opts = TextReader.defaults();
    assertTrue(opts instanceof ReaderOptions);
    ReaderOptions ropts = (ReaderOptions) opts;

    Path cp = new Path(OUTFILE_PATH, fileNameFromBlockPoolID(BPID));
    opts.filename(cp);
    check(opts, cp, null);

    cp = new Path(OUTFILE_PATH, "blocks_" + BPID + ".csv.gz");
    opts.filename(cp);
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
