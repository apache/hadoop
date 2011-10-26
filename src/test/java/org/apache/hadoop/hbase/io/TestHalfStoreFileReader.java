/*
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.io;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;


public class TestHalfStoreFileReader {

  /**
   * Test the scanner and reseek of a half hfile scanner. The scanner API
   * demands that seekTo and reseekTo() only return < 0 if the key lies
   * before the start of the file (with no position on the scanner). Returning
   * 0 if perfect match (rare), and return > 1 if we got an imperfect match.
   *
   * The latter case being the most common, we should generally be returning 1,
   * and if we do, there may or may not be a 'next' in the scanner/file.
   *
   * A bug in the half file scanner was returning -1 at the end of the bottom
   * half, and that was causing the infrastructure above to go null causing NPEs
   * and other problems.  This test reproduces that failure, and also tests
   * both the bottom and top of the file while we are at it.
   *
   * @throws IOException
   */
  @Test
  public void testHalfScanAndReseek() throws IOException {
    HBaseTestingUtility test_util = new HBaseTestingUtility();
    String root_dir = test_util.getDataTestDir("TestHalfStoreFile").toString();
    Path p = new Path(root_dir, "test");

    Configuration conf = test_util.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    CacheConfig cacheConf = new CacheConfig(conf);

    HFile.Writer w =
      HFile.getWriterFactory(conf, cacheConf).createWriter(fs, p, 1024,
        "none", KeyValue.KEY_COMPARATOR);

    // write some things.
    List<KeyValue> items = genSomeKeys();
    for (KeyValue kv : items) {
      w.append(kv);
    }
    w.close();

    HFile.Reader r = HFile.createReader(fs, p, cacheConf);
    r.loadFileInfo();
    byte [] midkey = r.midkey();
    KeyValue midKV = KeyValue.createKeyValueFromKey(midkey);
    midkey = midKV.getRow();

    //System.out.println("midkey: " + midKV + " or: " + Bytes.toStringBinary(midkey));

    Reference bottom = new Reference(midkey, Reference.Range.bottom);
    doTestOfScanAndReseek(p, fs, bottom, cacheConf);

    Reference top = new Reference(midkey, Reference.Range.top);
    doTestOfScanAndReseek(p, fs, top, cacheConf);
  }

  private void doTestOfScanAndReseek(Path p, FileSystem fs, Reference bottom,
      CacheConfig cacheConf)
      throws IOException {
    final HalfStoreFileReader halfreader =
        new HalfStoreFileReader(fs, p, cacheConf, bottom);
    halfreader.loadFileInfo();
    final HFileScanner scanner = halfreader.getScanner(false, false);

    scanner.seekTo();
    KeyValue curr;
    do {
      curr = scanner.getKeyValue();
      KeyValue reseekKv =
          getLastOnCol(curr);
      int ret = scanner.reseekTo(reseekKv.getKey());
      assertTrue("reseek to returned: " + ret, ret > 0);
      //System.out.println(curr + ": " + ret);
    } while (scanner.next());

    int ret = scanner.reseekTo(getLastOnCol(curr).getKey());
    //System.out.println("Last reseek: " + ret);
    assertTrue( ret > 0 );
  }

  private KeyValue getLastOnCol(KeyValue curr) {
    return KeyValue.createLastOnRow(
        curr.getBuffer(), curr.getRowOffset(), curr.getRowLength(),
        curr.getBuffer(), curr.getFamilyOffset(), curr.getFamilyLength(),
        curr.getBuffer(), curr.getQualifierOffset(), curr.getQualifierLength());
  }

  static final int SIZE = 1000;

  static byte[] _b(String s) {
    return Bytes.toBytes(s);
  }

  List<KeyValue> genSomeKeys() {
    List<KeyValue> ret = new ArrayList<KeyValue>(SIZE);
    for (int i = 0 ; i < SIZE; i++) {
      KeyValue kv =
          new KeyValue(
              _b(String.format("row_%04d", i)),
              _b("family"),
              _b("qualifier"),
              1000, // timestamp
              _b("value"));
      ret.add(kv);
    }
    return ret;
  }


}
