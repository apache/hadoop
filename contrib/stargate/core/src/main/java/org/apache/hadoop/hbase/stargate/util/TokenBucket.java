/*
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

package org.apache.hadoop.hbase.stargate.util;

public class TokenBucket {

  private int tokens;
  private int rate;
  private int size;
  private long lastUpdated;

  /**
   * Constructor
   * @param rate limit in units per second
   * @param size maximum burst in units per second
   */
  public TokenBucket(int rate, int size) {
    this.rate = rate;
    this.tokens = this.size = size;
  }

  /**
   * @return the number of remaining tokens in the bucket
   */
  public int available() {
    long now = System.currentTimeMillis();
    long elapsed = now - lastUpdated;
    lastUpdated = now;
    tokens += elapsed * rate;
    if (tokens > size) {
      tokens = size;
    }
    return tokens;
  }

  /**
   * @param t the number of tokens to consume from the bucket
   */
  public void remove(int t) {
    tokens -= t;
  }

}
