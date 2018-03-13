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
package org.apache.hadoop.mapred.nativetask.kvtest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.nativetask.testutil.BytesFactory;
import org.apache.hadoop.mapred.nativetask.testutil.ScenarioConfiguration;
import org.apache.hadoop.mapred.nativetask.testutil.TestConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestInputFile {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestInputFile.class);

  public static class KVSizeScope {
    private static final int DefaultMinNum = 1;
    private static final int DefaultMaxNum = 64;

    public int minBytesNum;
    public int maxBytesNum;

    public KVSizeScope() {
      this.minBytesNum = DefaultMinNum;
      this.maxBytesNum = DefaultMaxNum;
    }

    public KVSizeScope(int min, int max) {
      this.minBytesNum = min;
      this.maxBytesNum = max;
    }
  }

  private static HashMap<String, KVSizeScope> map = new HashMap<String, KVSizeScope>();

  private byte[] databuf = null;
  private final String keyClsName, valueClsName;
  private int filesize = 0;
  private int keyMaxBytesNum, keyMinBytesNum;
  private int valueMaxBytesNum, valueMinBytesNum;
  private SequenceFile.Writer writer = null;
  Random r = new Random();
  public static final int DATABUFSIZE = 1 << 22; // 4M

  private enum State {
    KEY, VALUE
  };
  
  static {
    map.put(BooleanWritable.class.getName(), new KVSizeScope(1, 1));
    map.put(DoubleWritable.class.getName(), new KVSizeScope(8, 8));
    map.put(FloatWritable.class.getName(), new KVSizeScope(4, 4));
    map.put(VLongWritable.class.getName(), new KVSizeScope(8, 8));
    map.put(ByteWritable.class.getName(), new KVSizeScope(1, 1));
    map.put(LongWritable.class.getName(), new KVSizeScope(8, 8));
    map.put(VIntWritable.class.getName(), new KVSizeScope(4, 4));
    map.put(IntWritable.class.getName(), new KVSizeScope(4, 4));
  }
  
  public TestInputFile(int filesize, String keytype, String valuetype,
                       Configuration conf) throws Exception {
    this.filesize = filesize;
    this.databuf = new byte[DATABUFSIZE];
    this.keyClsName = keytype;
    this.valueClsName = valuetype;
    final int defaultMinBytes = conf.getInt(TestConstants.NATIVETASK_KVSIZE_MIN, 1);
    final int defaultMaxBytes = conf.getInt(TestConstants.NATIVETASK_KVSIZE_MAX, 64);

    if (map.get(keytype) != null) {
      keyMinBytesNum = map.get(keytype).minBytesNum;
      keyMaxBytesNum = map.get(keytype).maxBytesNum;
    } else {
      keyMinBytesNum = defaultMinBytes;
      keyMaxBytesNum = defaultMaxBytes;
    }

    if (map.get(valuetype) != null) {
      valueMinBytesNum = map.get(valuetype).minBytesNum;
      valueMaxBytesNum = map.get(valuetype).maxBytesNum;
    } else {
      valueMinBytesNum = defaultMinBytes;
      valueMaxBytesNum = defaultMaxBytes;
    }
  }

  public void createSequenceTestFile(String filepath) throws Exception {
    int FULL_BYTE_SPACE = 256;
    createSequenceTestFile(filepath, FULL_BYTE_SPACE);
  }

  public void createSequenceTestFile(String filepath, int base) throws Exception {
    createSequenceTestFile(filepath, base, (byte)0);
  }
  
  public void createSequenceTestFile(String filepath, int base,  byte start) throws Exception {
    LOG.info("creating file " + filepath + "(" + filesize + " bytes)");
    LOG.info(keyClsName + " " + valueClsName);
    Class<?> tmpkeycls, tmpvaluecls;
    try {
      tmpkeycls = Class.forName(keyClsName);
    } catch (final ClassNotFoundException e) {
      throw new Exception("key class not found: ", e);
    }
    try {
      tmpvaluecls = Class.forName(valueClsName);
    } catch (final ClassNotFoundException e) {
      throw new Exception("key class not found: ", e);
    }
    try {
      final Path outputfilepath = new Path(filepath);
      final ScenarioConfiguration conf= new ScenarioConfiguration();
      writer = SequenceFile.createWriter(
        conf,
        SequenceFile.Writer.file(outputfilepath),
        SequenceFile.Writer.keyClass(tmpkeycls),
        SequenceFile.Writer.valueClass(tmpvaluecls));
    } catch (final Exception e) {
      e.printStackTrace();
    }

    int tmpfilesize = this.filesize;
    while (tmpfilesize > DATABUFSIZE) {
      nextRandomBytes(databuf, base, start);
      final int size = flushBuf(DATABUFSIZE);
      tmpfilesize -= size;
    }
    nextRandomBytes(databuf, base, start);
    flushBuf(tmpfilesize);

    if (writer != null) {
      IOUtils.closeStream(writer);
    } else {
      throw new Exception("no writer to create sequenceTestFile!");
    }
  }
  
  private void nextRandomBytes(byte[] buf, int base) {
    nextRandomBytes(buf, base, (byte)0);
  }
  
  private void nextRandomBytes(byte[] buf, int base, byte start) {
    r.nextBytes(buf);
    for (int i = 0; i < buf.length; i++) {
      buf[i] = (byte) ((buf[i] & 0xFF) % base + start);
    }
  }

  private int flushBuf(int buflen) throws Exception {
    final Random r = new Random();
    int keybytesnum = 0;
    int valuebytesnum = 0;
    int offset = 0;

    Writable keyWritable = BytesFactory.newObject(null, keyClsName);
    Writable valWritable = BytesFactory.newObject(null, valueClsName);

    while (offset < buflen) {
      final int remains = buflen - offset;
      keybytesnum = keyMaxBytesNum;
      if (keyMaxBytesNum != keyMinBytesNum) {
        keybytesnum = keyMinBytesNum + r.nextInt(keyMaxBytesNum - keyMinBytesNum);
      }

      valuebytesnum = valueMaxBytesNum;
      if (valueMaxBytesNum != valueMinBytesNum) {
        valuebytesnum = valueMinBytesNum + r.nextInt(valueMaxBytesNum - valueMinBytesNum);
      }

      if (keybytesnum + valuebytesnum > remains) {
        break;
      }

      final byte[] key = new byte[keybytesnum];
      final byte[] value = new byte[valuebytesnum];

      System.arraycopy(databuf, offset, key, 0, keybytesnum);
      offset += keybytesnum;

      System.arraycopy(databuf, offset, value, 0, valuebytesnum);
      offset += valuebytesnum;

      BytesFactory.updateObject(keyWritable, key);
      BytesFactory.updateObject(valWritable, value);
     
      try {
        writer.append(keyWritable, valWritable);
      } catch (final IOException e) {
        e.printStackTrace();
        throw new Exception("sequence file create failed", e);
      }
    }
    return offset;
  }

}
