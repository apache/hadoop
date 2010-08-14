/**
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
package org.apache.hadoop.hbase.io.hfile;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test {@link HFileScanner#reseekTo(byte[])}
 */
public class TestReseekTo {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Test
  public void testReseekTo() throws Exception {

    Path ncTFile = new Path(HBaseTestingUtility.getTestDir(), "basic.hfile");
    FSDataOutputStream fout = TEST_UTIL.getTestFileSystem().create(ncTFile);
    HFile.Writer writer = new HFile.Writer(fout, 4000, "none", null);
    int numberOfKeys = 1000;

    String valueString = "Value";

    List<Integer> keyList = new ArrayList<Integer>();
    List<String> valueList = new ArrayList<String>();

    for (int key = 0; key < numberOfKeys; key++) {
      String value = valueString + key;
      keyList.add(key);
      valueList.add(value);
      writer.append(Bytes.toBytes(key), Bytes.toBytes(value));
    }
    writer.close();
    fout.close();

    HFile.Reader reader = new HFile.Reader(TEST_UTIL.getTestFileSystem(),
        ncTFile, null, false);
    reader.loadFileInfo();
    HFileScanner scanner = reader.getScanner(false, true);

    scanner.seekTo();
    for (int i = 0; i < keyList.size(); i++) {
      Integer key = keyList.get(i);
      String value = valueList.get(i);
      long start = System.nanoTime();
      scanner.seekTo(Bytes.toBytes(key));
      System.out.println("Seek Finished in: " + (System.nanoTime() - start)/1000 + " micro s");
      assertEquals(value, scanner.getValueString());
    }

    scanner.seekTo();
    for (int i = 0; i < keyList.size(); i += 10) {
      Integer key = keyList.get(i);
      String value = valueList.get(i);
      long start = System.nanoTime();
      scanner.reseekTo(Bytes.toBytes(key));
      System.out.println("Reseek Finished in: " + (System.nanoTime() - start)/1000 + " micro s");
      assertEquals(value, scanner.getValueString());
    }
  }

}