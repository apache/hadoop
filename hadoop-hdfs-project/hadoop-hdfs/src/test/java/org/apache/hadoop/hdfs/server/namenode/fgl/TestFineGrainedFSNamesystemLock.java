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
package org.apache.hadoop.hdfs.server.namenode.fgl;

import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class TestFineGrainedFSNamesystemLock {

  private final Logger log = LoggerFactory.getLogger(TestFineGrainedFSNamesystemLock.class);

  private int getLoopNumber() {
    return ThreadLocalRandom.current().nextInt(2000, 3000);
  }

  /**
   * Test read/write lock of Global, FS and BM model through multi-threading.
   */
  @Test(timeout=120000)
  public void testMultipleThreadsUsingLocks()
      throws InterruptedException, ExecutionException {
    FineGrainedFSNamesystemLock fsn = new FineGrainedFSNamesystemLock(new Configuration(), null);
    ExecutorService service = HadoopExecutors.newFixedThreadPool(1000);

    AtomicLong globalCount = new AtomicLong(0);
    AtomicLong fsCount = new AtomicLong(0);
    AtomicLong bmCount = new AtomicLong(0);
    AtomicLong globalNumber = new AtomicLong(0);
    AtomicLong fsNumber = new AtomicLong(0);
    AtomicLong bmNumber = new AtomicLong(0);

    List<Callable<Boolean>> callableList = new ArrayList<>(1000);
    for (int i = 0; i < 1000; i++) {
      int index = i % 12;
      String opName = Integer.toString(i);
      if (index == 0) { // Test the global write lock via multiple threads.
        callableList.add(() -> {
          for (int startIndex = 0; startIndex < getLoopNumber(); startIndex++) {
            writeLock(fsn, RwLockMode.GLOBAL, opName, globalCount);
            globalNumber.incrementAndGet();
          }
          return true;
        });
      } else if (index == 1) { // Test the fs write lock via multiple threads.
        callableList.add(() -> {
          for (int startIndex = 0; startIndex < getLoopNumber(); startIndex++) {
            writeLock(fsn, RwLockMode.FS, opName, fsCount);
            fsNumber.incrementAndGet();
          }
          return true;
        });
      } else if (index == 2) { // Test the bm write lock via multiple threads.
        callableList.add(() -> {
          for (int startIndex = 0; startIndex < getLoopNumber(); startIndex++) {
            writeLock(fsn, RwLockMode.BM, opName, bmCount);
            bmNumber.incrementAndGet();
          }
          return true;
        });
      } else if (index == 3) { // Test the bm read lock via multiple threads.
        callableList.add(() -> {
          for (int startIndex = 0; startIndex < getLoopNumber(); startIndex++) {
            readLock(fsn, RwLockMode.BM, opName, bmCount);
            bmNumber.incrementAndGet();
          }
          return true;
        });
      } else if (index == 4) { // Test the fs read lock via multiple threads.
        callableList.add(() -> {
          for (int startIndex = 0; startIndex < getLoopNumber(); startIndex++) {
            readLock(fsn, RwLockMode.FS, opName, fsCount);
            fsNumber.incrementAndGet();
          }
          return true;
        });
      } else if (index == 5) { // Test the global read lock via multiple threads.
        callableList.add(() -> {
          for (int startIndex = 0; startIndex < getLoopNumber(); startIndex++) {
            readLock(fsn, RwLockMode.GLOBAL, opName, globalCount);
            globalNumber.incrementAndGet();
          }
          return true;
        });
      } else if (index == 6) { // Test the global interruptable write lock via multiple threads.
        callableList.add(() -> {
          for (int startIndex = 0; startIndex < getLoopNumber(); startIndex++) {
            writeLockInterruptibly(fsn, RwLockMode.GLOBAL, opName, globalCount);
            globalNumber.incrementAndGet();
          }
          return true;
        });
      } else if (index == 7) { // Test the fs interruptable write lock via multiple threads.
        callableList.add(() -> {
          for (int startIndex = 0; startIndex < getLoopNumber(); startIndex++) {
            writeLockInterruptibly(fsn, RwLockMode.FS, opName, fsCount);
            fsNumber.incrementAndGet();
          }
          return true;
        });
      } else if (index == 8) { // Test the bm interruptable write lock via multiple threads.
        callableList.add(() -> {
          for (int startIndex = 0; startIndex < getLoopNumber(); startIndex++) {
            writeLockInterruptibly(fsn, RwLockMode.BM, opName, bmCount);
            bmNumber.incrementAndGet();
          }
          return true;
        });
      } else if (index == 9) { // Test the bm interruptable read lock via multiple threads.
        callableList.add(() -> {
          for (int startIndex = 0; startIndex < getLoopNumber(); startIndex++) {
            readLockInterruptibly(fsn, RwLockMode.BM, opName, bmCount);
            bmNumber.incrementAndGet();
          }
          return true;
        });
      } else if (index == 10) { // Test the fs interruptable read lock via multiple threads.
        callableList.add(() -> {
          for (int startIndex = 0; startIndex < getLoopNumber(); startIndex++) {
            readLockInterruptibly(fsn, RwLockMode.FS, opName, fsCount);
            fsNumber.incrementAndGet();
          }
          return true;
        });
      } else { // Test the global interruptable read lock via multiple threads.
        callableList.add(() -> {
          for (int startIndex = 0; startIndex < getLoopNumber(); startIndex++) {
            readLockInterruptibly(fsn, RwLockMode.GLOBAL, opName, globalCount);
            globalNumber.incrementAndGet();
          }
          return true;
        });
      }
    }

    List<Future<Boolean>> futures = service.invokeAll(callableList);
    for (Future<Boolean> f : futures) {
      f.get();
    }
    log.info("Global executed {} times, FS executed {} times, BM executed {} times.",
        globalNumber.get(), fsNumber.get(), bmNumber.get());
    assert globalCount.get() == 0;
    assert fsCount.get() == 0;
    assert bmCount.get() == 0;
  }

  /**
   * Test write lock for the input lock mode.
   * @param fsn FSNLockManager
   * @param mode LockMode
   * @param opName operation name
   * @param counter counter to trace this lock mode
   */
  private void writeLock(FSNLockManager fsn, RwLockMode mode,
      String opName, AtomicLong counter)  {
    fsn.writeLock(mode);
    try {
      counter.incrementAndGet();
    } finally {
      fsn.writeUnlock(mode, opName);
    }
    fsn.writeLock(mode);
    try {
      counter.decrementAndGet();
    } finally {
      fsn.writeUnlock(mode, opName);
    }
  }

  /**
   * Test read lock for the input lock mode.
   * @param fsn FSNLockManager
   * @param mode LockMode
   * @param opName operation name
   * @param counter counter to trace this lock mode
   */
  private void readLock(FSNLockManager fsn, RwLockMode mode,
      String opName, AtomicLong counter)  {
    fsn.readLock(mode);
    try {
      counter.get();
    } finally {
      fsn.readUnlock(mode, opName);
    }
  }

  /**
   * Test write lock for the input lock mode.
   * @param fsn FSNLockManager
   * @param mode LockMode
   * @param opName operation name
   * @param counter counter to trace this lock mode
   */
  private void writeLockInterruptibly(FSNLockManager fsn, RwLockMode mode,
      String opName, AtomicLong counter)  {
    boolean success = false;
    try {
      fsn.writeLockInterruptibly(mode);
      try {
        counter.incrementAndGet();
        success = true;
      } finally {
        fsn.writeUnlock(mode, opName);
      }
    } catch (InterruptedException e) {
      log.info("InterruptedException happens in thread {}" +
          " during increasing the Count.", opName);
      // ignore;
    }
    while (success) {
      try {
        fsn.writeLockInterruptibly(mode);
        try {
          counter.decrementAndGet();
          success = false;
        } finally {
          fsn.writeUnlock(mode, opName);
        }
      } catch (InterruptedException e) {
        log.info("InterruptedException happens in thread {}" +
            " during decreasing the Count.", opName);
        // ignore.
      }
    }
  }

  /**
   * Test read lock for the input lock mode.
   * @param fsn FSNLockManager
   * @param mode LockMode
   * @param opName operation name
   * @param counter counter to trace this lock mode
   */
  private void readLockInterruptibly(FSNLockManager fsn, RwLockMode mode,
      String opName, AtomicLong counter)  {
    try {
      fsn.readLockInterruptibly(mode);
      try {
        counter.get();
      } finally {
        fsn.readUnlock(mode, opName);
      }
    } catch (InterruptedException e) {
      log.info("InterruptedException happens in thread {}" +
          " during getting the Count.", opName);
      // ignore
    }
  }
}
