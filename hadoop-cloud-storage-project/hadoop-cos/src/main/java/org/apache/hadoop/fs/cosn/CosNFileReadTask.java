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
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;

/**
 * Used by {@link CosNInputStream} as an asynchronous task
 * submitted to the thread pool.
 * Each task is responsible for reading a part of a large file.
 * It is used to pre-read the data from COS to accelerate file reading process.
 */
public class CosNFileReadTask implements Runnable {
  private static final Logger LOG =
      LoggerFactory.getLogger(CosNFileReadTask.class);

  private final String key;
  private final NativeFileSystemStore store;
  private final CosNInputStream.ReadBuffer readBuffer;

  private RetryPolicy retryPolicy;

  public CosNFileReadTask(
      Configuration conf,
      String key, NativeFileSystemStore store,
      CosNInputStream.ReadBuffer readBuffer) {
    this.key = key;
    this.store = store;
    this.readBuffer = readBuffer;

    RetryPolicy defaultPolicy =
        RetryPolicies.retryUpToMaximumCountWithFixedSleep(
            conf.getInt(
                CosNConfigKeys.COSN_MAX_RETRIES_KEY,
                CosNConfigKeys.DEFAULT_MAX_RETRIES),
            conf.getLong(
                CosNConfigKeys.COSN_RETRY_INTERVAL_KEY,
                CosNConfigKeys.DEFAULT_RETRY_INTERVAL),
            TimeUnit.SECONDS);
    Map<Class<? extends Exception>, RetryPolicy> retryPolicyMap =
        new HashMap<>();
    retryPolicyMap.put(IOException.class, defaultPolicy);
    retryPolicyMap.put(
        IndexOutOfBoundsException.class, RetryPolicies.TRY_ONCE_THEN_FAIL);
    retryPolicyMap.put(
        NullPointerException.class, RetryPolicies.TRY_ONCE_THEN_FAIL);

    this.retryPolicy = RetryPolicies.retryByException(
        defaultPolicy, retryPolicyMap);
  }

  @Override
  public void run() {
    int retries = 0;
    RetryPolicy.RetryAction retryAction;
    try {
      this.readBuffer.lock();
      do {
        try {
          InputStream inputStream = this.store.retrieveBlock(this.key,
              this.readBuffer.getStart(), this.readBuffer.getEnd());
          IOUtils.readFully(inputStream, this.readBuffer.getBuffer(), 0,
              readBuffer.getBuffer().length);
          inputStream.close();
          this.readBuffer.setStatus(CosNInputStream.ReadBuffer.SUCCESS);
          break;
        } catch (IOException e) {
          this.readBuffer.setStatus(CosNInputStream.ReadBuffer.ERROR);
          LOG.warn(
              "Exception occurs when retrieve the block range start: "
                  + String.valueOf(this.readBuffer.getStart()) + " end: "
                  + this.readBuffer.getEnd());
          try {
            retryAction = this.retryPolicy.shouldRetry(
                e, retries++, 0, true);
            if (retryAction.action
                == RetryPolicy.RetryAction.RetryDecision.RETRY) {
              Thread.sleep(retryAction.delayMillis);
            }
          } catch (Exception e1) {
            String errMsg = String.format("Exception occurs when retry[%s] "
                    + "to retrieve the block range start: %s, end:%s",
                this.retryPolicy.toString(),
                String.valueOf(this.readBuffer.getStart()),
                String.valueOf(this.readBuffer.getEnd()));
            LOG.error(errMsg, e1);
            break;
          }
        }
      } while (retryAction.action ==
          RetryPolicy.RetryAction.RetryDecision.RETRY);
      this.readBuffer.signalAll();
    } finally {
      this.readBuffer.unLock();
    }
  }
}
