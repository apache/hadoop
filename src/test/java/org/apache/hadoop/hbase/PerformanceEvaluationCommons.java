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
package org.apache.hadoop.hbase;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Code shared by PE tests.
 */
public class PerformanceEvaluationCommons {
  static final Log LOG =
    LogFactory.getLog(PerformanceEvaluationCommons.class.getName());

  public static void assertValueSize(final int expectedSize, final int got) {
    if (got != expectedSize) {
      throw new AssertionError("Expected " + expectedSize + " but got " + got);
    }
  }

  public static void assertKey(final byte [] expected, final ByteBuffer got) {
    byte [] b = new byte[got.limit()];
    got.get(b, 0, got.limit());
    assertKey(expected, b);
  }

  public static void assertKey(final byte [] expected, final byte [] got) {
    if (!org.apache.hadoop.hbase.util.Bytes.equals(expected, got)) {
      throw new AssertionError("Expected " +
        org.apache.hadoop.hbase.util.Bytes.toString(expected) +
        " but got " + org.apache.hadoop.hbase.util.Bytes.toString(got));
    }
  }

  public static void concurrentReads(final Runnable r) {
    final int count = 1;
    long now = System.currentTimeMillis();
    List<Thread> threads = new ArrayList<Thread>(count);
    for (int i = 0; i < count; i++) {
      Thread t = new Thread(r);
      t.setName("" + i);
      threads.add(t);
    }
    for (Thread t: threads) {
      t.start();
    }
    for (Thread t: threads) {
      try {
        t.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    LOG.info("Test took " + (System.currentTimeMillis() - now));
  }
}