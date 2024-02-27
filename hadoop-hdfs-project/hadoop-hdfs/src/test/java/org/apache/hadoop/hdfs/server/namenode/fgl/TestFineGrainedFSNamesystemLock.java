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

  private final Logger LOG = LoggerFactory.getLogger(TestFineGrainedFSNamesystemLock.class);

  private int getLoopNumber() {
    return ThreadLocalRandom.current().nextInt(2000, 3000);
  }

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
      int finalI = i;
      int index = finalI % 12;
      if (index == 0) {
        callableList.add(() -> {
          for (int startIndex = 0; startIndex < getLoopNumber(); startIndex++) {
            fsn.writeLock(FSNamesystemLockMode.GLOBAL);
            try {
              globalCount.incrementAndGet();
            } finally {
              fsn.writeUnlock(FSNamesystemLockMode.GLOBAL, Integer.toString(finalI));
            }
            fsn.writeLock(FSNamesystemLockMode.GLOBAL);
            try {
              globalCount.decrementAndGet();
            } finally {
              fsn.writeUnlock(FSNamesystemLockMode.GLOBAL, Integer.toString(finalI));
            }
            globalNumber.incrementAndGet();
          }
          return true;
        });
      } else if (index == 1) {
        callableList.add(() -> {
          for (int startIndex = 0; startIndex < getLoopNumber(); startIndex++) {
            fsn.writeLock(FSNamesystemLockMode.FS);
            try {
              fsCount.incrementAndGet();
            } finally {
              fsn.writeUnlock(FSNamesystemLockMode.FS, Integer.toString(finalI));
            }
            fsn.writeLock(FSNamesystemLockMode.FS);
            try {
              fsCount.decrementAndGet();
            } finally {
              fsn.writeUnlock(FSNamesystemLockMode.FS, Integer.toString(finalI));
            }
            fsNumber.incrementAndGet();
          }
          return true;
        });
      } else if (index == 2) {
        callableList.add(() -> {
          for (int startIndex = 0; startIndex < getLoopNumber(); startIndex++) {
            fsn.writeLock(FSNamesystemLockMode.BM);
            try {
              bmCount.incrementAndGet();
            } finally {
              fsn.writeUnlock(FSNamesystemLockMode.BM, Integer.toString(finalI));
            }
            fsn.writeLock(FSNamesystemLockMode.BM);
            try {
              bmCount.decrementAndGet();
            } finally {
              fsn.writeUnlock(FSNamesystemLockMode.BM, Integer.toString(finalI));
            }
            bmNumber.incrementAndGet();
          }
          return true;
        });
      } else if (index == 3) {
        callableList.add(() -> {
          for (int startIndex = 0; startIndex < getLoopNumber(); startIndex++) {
            fsn.readLock(FSNamesystemLockMode.BM);
            try {
              bmCount.get();
            } finally {
              fsn.readUnlock(FSNamesystemLockMode.BM, Integer.toString(finalI));
            }
            bmNumber.incrementAndGet();
          }
          return true;
        });
      } else if (index == 4) {
        callableList.add(() -> {
          for (int startIndex = 0; startIndex < getLoopNumber(); startIndex++) {
            fsn.readLock(FSNamesystemLockMode.FS);
            try {
              fsCount.get();
            } finally {
              fsn.readUnlock(FSNamesystemLockMode.FS, Integer.toString(finalI));
            }
            fsNumber.incrementAndGet();
          }
          return true;
        });
      } else if (index == 5) {
        callableList.add(() -> {
          for (int startIndex = 0; startIndex < getLoopNumber(); startIndex++) {
            fsn.readLock(FSNamesystemLockMode.GLOBAL);
            try {
              globalCount.get();
            } finally {
              fsn.readUnlock(FSNamesystemLockMode.GLOBAL, Integer.toString(finalI));
            }
            globalNumber.incrementAndGet();
          }
          return true;
        });
      } else if (index == 6) {
        callableList.add(() -> {
          for (int startIndex = 0; startIndex < getLoopNumber(); startIndex++) {
            boolean success = false;
            try {
              fsn.writeLockInterruptibly(FSNamesystemLockMode.GLOBAL);
              try {
                globalCount.incrementAndGet();
                success = true;
              } finally {
                fsn.writeUnlock(FSNamesystemLockMode.GLOBAL, Integer.toString(finalI));
              }
            } catch (InterruptedException e) {
              LOG.info("InterruptedException happens in thread {}" +
                  " during increasing the globalCount.", finalI);
              // ignore;
            }
            while (success) {
              try {
                fsn.writeLockInterruptibly(FSNamesystemLockMode.GLOBAL);
                try {
                  globalCount.decrementAndGet();
                  success = false;
                } finally {
                  fsn.writeUnlock(FSNamesystemLockMode.GLOBAL, Integer.toString(finalI));
                }
              } catch (InterruptedException e) {
                LOG.info("InterruptedException happens in thread {}" +
                    " during decreasing the globalCount.", finalI);
                // ignore.
              }
            }
            globalNumber.incrementAndGet();
          }
          return true;
        });
      } else if (index == 7) {
        callableList.add(() -> {
          for (int startIndex = 0; startIndex < getLoopNumber(); startIndex++) {
            boolean success = false;
            try {
              fsn.writeLockInterruptibly(FSNamesystemLockMode.FS);
              try {
                fsCount.incrementAndGet();
                success = true;
              } finally {
                fsn.writeUnlock(FSNamesystemLockMode.FS, Integer.toString(finalI));
              }
            } catch (InterruptedException e) {
              LOG.info("InterruptedException happens in thread {}" +
                  " during increasing the fsCount.", finalI);
              // ignore;
            }
            while (success) {
              try {
                fsn.writeLockInterruptibly(FSNamesystemLockMode.FS);
                try {
                  fsCount.decrementAndGet();
                  success = false;
                } finally {
                  fsn.writeUnlock(FSNamesystemLockMode.FS, Integer.toString(finalI));
                }
              } catch (InterruptedException e) {
                LOG.info("InterruptedException happens in thread {}" +
                    " during decreasing the fsCount.", finalI);
                // ignore.
              }
            }
            fsNumber.incrementAndGet();
          }
          return true;
        });
      } else if (index == 8) {
        callableList.add(() -> {
          for (int startIndex = 0; startIndex < getLoopNumber(); startIndex++) {
            boolean success = false;
            try {
              fsn.writeLockInterruptibly(FSNamesystemLockMode.BM);
              try {
                bmCount.incrementAndGet();
                success = true;
              } finally {
                fsn.writeUnlock(FSNamesystemLockMode.BM, Integer.toString(finalI));
              }
            } catch (InterruptedException e) {
              LOG.info("InterruptedException happens in thread {}" +
                  " during increasing the bmCount.", finalI);
              // ignore;
            }

            while (success) {
              try {
                fsn.writeLockInterruptibly(FSNamesystemLockMode.BM);
                try {
                  bmCount.decrementAndGet();
                  success = false;
                } finally {
                  fsn.writeUnlock(FSNamesystemLockMode.BM, Integer.toString(finalI));
                }
              } catch (InterruptedException e) {
                LOG.info("InterruptedException happens in thread {}" +
                    " during decreasing the bmCount.", finalI);
                // ignore.
              }
            }
            bmNumber.incrementAndGet();
          }
          return true;
        });
      } else if (index == 9) {
        callableList.add(() -> {
          for (int startIndex = 0; startIndex < getLoopNumber(); startIndex++) {
            try {
              fsn.readLockInterruptibly(FSNamesystemLockMode.BM);
              try {
                bmCount.get();
              } finally {
                fsn.readUnlock(FSNamesystemLockMode.BM, Integer.toString(finalI));
              }
            } catch (InterruptedException e) {
              LOG.info("InterruptedException happens in thread {}" +
                  " during getting the globalCount.", finalI);
              // ignore
            }
            bmNumber.incrementAndGet();
          }
          return true;
        });
      } else if (index == 10) {
        callableList.add(() -> {
          for (int startIndex = 0; startIndex < getLoopNumber(); startIndex++) {
            try {
              fsn.readLockInterruptibly(FSNamesystemLockMode.FS);
              try {
                fsCount.get();
              } finally {
                fsn.readUnlock(FSNamesystemLockMode.FS, Integer.toString(finalI));
              }
            } catch (InterruptedException e) {
              LOG.info("InterruptedException happens in thread {}" +
                  " during getting the fsCount.", finalI);
              // ignore
            }
            fsNumber.incrementAndGet();
          }
          return true;
        });
      } else {
        callableList.add(() -> {
          for (int startIndex = 0; startIndex < getLoopNumber(); startIndex++) {
            try {
              fsn.readLockInterruptibly(FSNamesystemLockMode.GLOBAL);
              try {
                globalCount.get();
              } finally {
                fsn.readUnlock(FSNamesystemLockMode.GLOBAL, Integer.toString(finalI));
              }
            } catch (InterruptedException e) {
              LOG.info("InterruptedException happens in thread {}" +
                  " during getting the bmCount.", finalI);
              // ignore
            }
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
    LOG.info("Global executed {} times, FS executed {} times, BM executed {} times.",
        globalNumber.get(), fsNumber.get(), bmNumber.get());
    assert globalCount.get() == 0;
    assert fsCount.get() == 0;
    assert bmCount.get() == 0;
  }
}
