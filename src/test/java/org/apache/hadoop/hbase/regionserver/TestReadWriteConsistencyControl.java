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
package org.apache.hadoop.hbase.regionserver;

import junit.framework.TestCase;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class TestReadWriteConsistencyControl extends TestCase {
  static class Writer implements Runnable {
    final AtomicBoolean finished;
    final ReadWriteConsistencyControl rwcc;
    final AtomicBoolean status;

    Writer(AtomicBoolean finished, ReadWriteConsistencyControl rwcc, AtomicBoolean status) {
      this.finished = finished;
      this.rwcc = rwcc;
      this.status = status;
    }
    private Random rnd = new Random();
    public boolean failed = false;

    public void run() {
      while (!finished.get()) {
        ReadWriteConsistencyControl.WriteEntry e = rwcc.beginMemstoreInsert();
//        System.out.println("Begin write: " + e.getWriteNumber());
        // 10 usec - 500usec (including 0)
        int sleepTime = rnd.nextInt(500);
        // 500 * 1000 = 500,000ns = 500 usec
        // 1 * 100 = 100ns = 1usec
        try {
          if (sleepTime > 0)
            Thread.sleep(0, sleepTime * 1000);
        } catch (InterruptedException e1) {
        }
        try {
          rwcc.completeMemstoreInsert(e);
        } catch (RuntimeException ex) {
          // got failure
          System.out.println(ex.toString());
          ex.printStackTrace();
          status.set(false);
          return;
          // Report failure if possible.
        }
      }
    }
  }

  public void testParallelism() throws Exception {
    final ReadWriteConsistencyControl rwcc = new ReadWriteConsistencyControl();

    final AtomicBoolean finished = new AtomicBoolean(false);

    // fail flag for the reader thread
    final AtomicBoolean readerFailed = new AtomicBoolean(false);
    final AtomicLong failedAt = new AtomicLong();
    Runnable reader = new Runnable() {
      public void run() {
        long prev = rwcc.memstoreReadPoint();
        while (!finished.get()) {
          long newPrev = rwcc.memstoreReadPoint();
          if (newPrev < prev) {
            // serious problem.
            System.out.println("Reader got out of order, prev: " +
            prev + " next was: " + newPrev);
            readerFailed.set(true);
            // might as well give up
            failedAt.set(newPrev);
            return;
          }
        }
      }
    };

    // writer thread parallelism.
    int n = 20;
    Thread [] writers = new Thread[n];
    AtomicBoolean [] statuses = new AtomicBoolean[n];
    Thread readThread = new Thread(reader);

    for (int i = 0 ; i < n ; ++i ) {
      statuses[i] = new AtomicBoolean(true);
      writers[i] = new Thread(new Writer(finished, rwcc, statuses[i]));
      writers[i].start();
    }
    readThread.start();

    try {
      Thread.sleep(10 * 1000);
    } catch (InterruptedException ex) {
    }

    finished.set(true);

    readThread.join();
    for (int i = 0; i < n; ++i) {
      writers[i].join();
    }

    // check failure.
    assertFalse(readerFailed.get());
    for (int i = 0; i < n; ++i) {
      assertTrue(statuses[i].get());
    }


  }
}
