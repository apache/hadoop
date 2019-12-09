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

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Used by {@link AliyunOSSInputStream} as an task that submitted
 * to the thread pool.
 * Each AliyunOSSFileReaderTask reads one part of the file so that
 * we can accelerate the sequential read.
 */
public class AliyunOSSFileReaderTask implements Runnable {
  public static final Logger LOG =
      LoggerFactory.getLogger(AliyunOSSFileReaderTask.class);

  private String key;
  private AliyunOSSFileSystemStore store;
  private ReadBuffer readBuffer;
  private static final int MAX_RETRIES = 3;
  private RetryPolicy retryPolicy;

  public AliyunOSSFileReaderTask(String key, AliyunOSSFileSystemStore store,
      ReadBuffer readBuffer) {
    this.key = key;
    this.store = store;
    this.readBuffer = readBuffer;
    RetryPolicy defaultPolicy =
        RetryPolicies.retryUpToMaximumCountWithFixedSleep(
            MAX_RETRIES, 3, TimeUnit.SECONDS);
    Map<Class<? extends Exception>, RetryPolicy> policies = new HashMap<>();
    policies.put(IOException.class, defaultPolicy);
    policies.put(IndexOutOfBoundsException.class,
        RetryPolicies.TRY_ONCE_THEN_FAIL);
    policies.put(NullPointerException.class,
        RetryPolicies.TRY_ONCE_THEN_FAIL);

    this.retryPolicy = RetryPolicies.retryByException(defaultPolicy, policies);
  }

  @Override
  public void run() {
    int retries = 0;
    readBuffer.lock();
    try {
      while (true) {
        try (InputStream in = store.retrieve(
            key, readBuffer.getByteStart(), readBuffer.getByteEnd())) {
          IOUtils.readFully(in, readBuffer.getBuffer(),
              0, readBuffer.getBuffer().length);
          readBuffer.setStatus(ReadBuffer.STATUS.SUCCESS);
          break;
        } catch (Exception e) {
          LOG.warn("Exception thrown when retrieve key: "
              + this.key + ", exception: " + e);
          try {
            RetryPolicy.RetryAction rc = retryPolicy.shouldRetry(
                e, retries++, 0, true);
            if (rc.action == RetryPolicy.RetryAction.RetryDecision.RETRY) {
              Thread.sleep(rc.delayMillis);
            } else {
              //should not retry
              break;
            }
          } catch (Exception ex) {
            //FAIL
            LOG.warn("Exception thrown when call shouldRetry, exception " + ex);
            break;
          }
        }
      }

      if (readBuffer.getStatus() != ReadBuffer.STATUS.SUCCESS) {
        readBuffer.setStatus(ReadBuffer.STATUS.ERROR);
      }

      //notify main thread which wait for this buffer
      readBuffer.signalAll();
    } finally {
      readBuffer.unlock();
    }
  }
}
