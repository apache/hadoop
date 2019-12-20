/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.aliyun.oss;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used by {@link AliyunOSSFileSystem} as an task that submitted
 * to the thread pool to accelerate the copy progress.
 * Each AliyunOSSCopyFileTask copies one file from src path to dst path
 */
public class AliyunOSSCopyFileTask implements Runnable {
  public static final Logger LOG =
      LoggerFactory.getLogger(AliyunOSSCopyFileTask.class);

  private AliyunOSSFileSystemStore store;
  private String srcKey;
  private long srcLen;
  private String dstKey;
  private AliyunOSSCopyFileContext copyFileContext;

  public AliyunOSSCopyFileTask(AliyunOSSFileSystemStore store,
      String srcKey, long srcLen,
      String dstKey, AliyunOSSCopyFileContext copyFileContext) {
    this.store = store;
    this.srcKey = srcKey;
    this.srcLen = srcLen;
    this.dstKey = dstKey;
    this.copyFileContext = copyFileContext;
  }

  @Override
  public void run() {
    boolean fail = false;
    try {
      fail = !store.copyFile(srcKey, srcLen, dstKey);
    } catch (Exception e) {
      LOG.warn("Exception thrown when copy from "
          + srcKey + " to " + dstKey +  ", exception: " + e);
      fail = true;
    } finally {
      copyFileContext.lock();
      if (fail) {
        copyFileContext.setCopyFailure(fail);
      }
      copyFileContext.incCopiesFinish();
      copyFileContext.signalAll();
      copyFileContext.unlock();
    }
  }
}
