/**
 * Copyright 2007 The Apache Software Foundation
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.filter.StopRowFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

public class TestHbaseObjectWritable extends TestCase {

  @Override
  protected void setUp() throws Exception {
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  @SuppressWarnings("boxing")
  public void testReadObjectDataInputConfiguration() throws IOException {
    HBaseConfiguration conf = new HBaseConfiguration();
    // Do primitive type
    final int COUNT = 101;
    assertTrue(doType(conf, COUNT, int.class).equals(COUNT));
    // Do array
    final byte [] testing = "testing".getBytes();
    byte [] result = (byte [])doType(conf, testing, testing.getClass());
    assertTrue(WritableComparator.compareBytes(testing, 0, testing.length,
       result, 0, result.length) == 0);
    // Do unsupported type.
    boolean exception = false;
    try {
      doType(conf, new File("a"), File.class);
    } catch (UnsupportedOperationException uoe) {
      exception = true;
    }
    assertTrue(exception);
    // Try odd types
    final byte A = 'A';
    byte [] bytes = new byte[1];
    bytes[0] = A;
    Object obj = doType(conf, bytes, byte [].class);
    assertTrue(((byte [])obj)[0] == A);
    // Do 'known' Writable type.
    obj = doType(conf, new Text(""), Text.class);
    assertTrue(obj instanceof Text);
    // Try type that should get transferred old fashion way.
    obj = doType(conf, new StopRowFilter(HConstants.EMPTY_BYTE_ARRAY),
        RowFilterInterface.class);
    assertTrue(obj instanceof StopRowFilter);
  }
  
  private Object doType(final HBaseConfiguration conf, final Object value,
      final Class<?> clazz)
  throws IOException {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(byteStream);
    HbaseObjectWritable.writeObject(out, value, clazz, conf);
    out.close();
    ByteArrayInputStream bais =
      new ByteArrayInputStream(byteStream.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    Object product = HbaseObjectWritable.readObject(dis, conf);
    dis.close();
    return product;
  }
}
