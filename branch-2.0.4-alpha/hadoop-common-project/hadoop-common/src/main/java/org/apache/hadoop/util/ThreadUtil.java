/*
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
package org.apache.hadoop.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceStability;

@InterfaceStability.Evolving
public class ThreadUtil {
  
  private static final Log LOG = LogFactory.getLog(ThreadUtil.class);

  /**
   * Cause the current thread to sleep as close as possible to the provided
   * number of milliseconds. This method will log and ignore any
   * {@link InterruptedException} encountered.
   * 
   * @param millis the number of milliseconds for the current thread to sleep
   */
  public static void sleepAtLeastIgnoreInterrupts(long millis) {
    long start = Time.now();
    while (Time.now() - start < millis) {
      long timeToSleep = millis -
          (Time.now() - start);
      try {
        Thread.sleep(timeToSleep);
      } catch (InterruptedException ie) {
        LOG.warn("interrupted while sleeping", ie);
      }
    }
  }
}
