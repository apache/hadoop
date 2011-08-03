/**
 * Copyright 2009 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.io.hfile;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Test {@link HFileScanner#seekTo(byte[])} and its variants.
 */
public class TestSeekTo extends HBaseTestCase {

  static KeyValue toKV(String row) {
    return new KeyValue(Bytes.toBytes(row), Bytes.toBytes("family"), Bytes
        .toBytes("qualifier"), Bytes.toBytes("value"));
  }

  static String toRowStr(KeyValue kv) {
    return Bytes.toString(kv.getRow());
  }

  Path makeNewFile() throws IOException {
    Path ncTFile = new Path(this.testDir, "basic.hfile");
    FSDataOutputStream fout = this.fs.create(ncTFile);
    int blocksize = toKV("a").getLength() * 3;
    HFile.Writer writer = HFile.getWriterFactory(conf).createWriter(fout,
        blocksize, "none", null);
    // 4 bytes * 3 * 2 for each key/value +
    // 3 for keys, 15 for values = 42 (woot)
    writer.append(toKV("c"));
    writer.append(toKV("e"));
    writer.append(toKV("g"));
    // block transition
    writer.append(toKV("i"));
    writer.append(toKV("k"));
    writer.close();
    fout.close();
    return ncTFile;
  }

  public void testSeekBefore() throws Exception {
    Path p = makeNewFile();
    HFile.Reader reader = HFile.createReader(fs, p, null, false, false);
    reader.loadFileInfo();
    HFileScanner scanner = reader.getScanner(false, true);
    assertEquals(false, scanner.seekBefore(toKV("a").getKey()));

    assertEquals(false, scanner.seekBefore(toKV("c").getKey()));

    assertEquals(true, scanner.seekBefore(toKV("d").getKey()));
    assertEquals("c", toRowStr(scanner.getKeyValue()));

    assertEquals(true, scanner.seekBefore(toKV("e").getKey()));
    assertEquals("c", toRowStr(scanner.getKeyValue()));

    assertEquals(true, scanner.seekBefore(toKV("f").getKey()));
    assertEquals("e", toRowStr(scanner.getKeyValue()));

    assertEquals(true, scanner.seekBefore(toKV("g").getKey()));
    assertEquals("e", toRowStr(scanner.getKeyValue()));

    assertEquals(true, scanner.seekBefore(toKV("h").getKey()));
    assertEquals("g", toRowStr(scanner.getKeyValue()));
    assertEquals(true, scanner.seekBefore(toKV("i").getKey()));
    assertEquals("g", toRowStr(scanner.getKeyValue()));
    assertEquals(true, scanner.seekBefore(toKV("j").getKey()));
    assertEquals("i", toRowStr(scanner.getKeyValue()));
    assertEquals(true, scanner.seekBefore(toKV("k").getKey()));
    assertEquals("i", toRowStr(scanner.getKeyValue()));
    assertEquals(true, scanner.seekBefore(toKV("l").getKey()));
    assertEquals("k", toRowStr(scanner.getKeyValue()));
  }

  public void testSeekTo() throws Exception {
    Path p = makeNewFile();
    HFile.Reader reader = HFile.createReader(fs, p, null, false, false);
    reader.loadFileInfo();
    assertEquals(2, reader.getDataBlockIndexReader().getRootBlockCount());
    HFileScanner scanner = reader.getScanner(false, true);
    // lies before the start of the file.
    assertEquals(-1, scanner.seekTo(toKV("a").getKey()));

    assertEquals(1, scanner.seekTo(toKV("d").getKey()));
    assertEquals("c", toRowStr(scanner.getKeyValue()));

    // Across a block boundary now.
    assertEquals(1, scanner.seekTo(toKV("h").getKey()));
    assertEquals("g", toRowStr(scanner.getKeyValue()));

    assertEquals(1, scanner.seekTo(toKV("l").getKey()));
    assertEquals("k", toRowStr(scanner.getKeyValue()));
  }

  public void testBlockContainingKey() throws Exception {
    Path p = makeNewFile();
    HFile.Reader reader = HFile.createReader(fs, p, null, false, false);
    reader.loadFileInfo();
    HFileBlockIndex.BlockIndexReader blockIndexReader = 
      reader.getDataBlockIndexReader();
    System.out.println(blockIndexReader.toString());
    int klen = toKV("a").getKey().length;
    // falls before the start of the file.
    assertEquals(-1, blockIndexReader.rootBlockContainingKey(
        toKV("a").getKey(), 0, klen));
    assertEquals(0, blockIndexReader.rootBlockContainingKey(
        toKV("c").getKey(), 0, klen));
    assertEquals(0, blockIndexReader.rootBlockContainingKey(
        toKV("d").getKey(), 0, klen));
    assertEquals(0, blockIndexReader.rootBlockContainingKey(
        toKV("e").getKey(), 0, klen));
    assertEquals(0, blockIndexReader.rootBlockContainingKey(
        toKV("g").getKey(), 0, klen));
    assertEquals(0, blockIndexReader.rootBlockContainingKey(
        toKV("h").getKey(), 0, klen));
    assertEquals(1, blockIndexReader.rootBlockContainingKey(
        toKV("i").getKey(), 0, klen));
    assertEquals(1, blockIndexReader.rootBlockContainingKey(
        toKV("j").getKey(), 0, klen));
    assertEquals(1, blockIndexReader.rootBlockContainingKey(
        toKV("k").getKey(), 0, klen));
    assertEquals(1, blockIndexReader.rootBlockContainingKey(
        toKV("l").getKey(), 0, klen));
 }
}
