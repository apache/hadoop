/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.cosn;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used by {@link CosNFileSystem} as an task that submitted
 * to the thread pool to accelerate the copy progress.
 * Each task is responsible for copying the source key to the destination.
 */
public class CosNCopyFileTask implements Runnable {
  private static final Logger LOG =
      LoggerFactory.getLogger(CosNCopyFileTask.class);

  private NativeFileSystemStore store;
  private String srcKey;
  private String dstKey;
  private CosNCopyFileContext cosCopyFileContext;

  public CosNCopyFileTask(NativeFileSystemStore store, String srcKey,
      String dstKey, CosNCopyFileContext cosCopyFileContext) {
    this.store = store;
    this.srcKey = srcKey;
    this.dstKey = dstKey;
    this.cosCopyFileContext = cosCopyFileContext;
  }

  @Override
  public void run() {
    boolean fail = false;
    LOG.info(Thread.currentThread().getName() + "copying...");
    try {
      this.store.copy(srcKey, dstKey);
    } catch (IOException e) {
      LOG.warn("Exception thrown when copy from {} to {}, exception:{}",
          this.srcKey, this.dstKey, e);
      fail = true;
    } finally {
      this.cosCopyFileContext.lock();
      if (fail) {
        cosCopyFileContext.setCopySuccess(false);
      }
      cosCopyFileContext.incCopiesFinish();
      cosCopyFileContext.signalAll();
      this.cosCopyFileContext.unlock();
    }
  }

}
