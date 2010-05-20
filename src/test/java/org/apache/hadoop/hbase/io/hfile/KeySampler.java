/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hbase.io.hfile;

import java.util.Random;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.hbase.io.hfile.RandomDistribution.DiscreteRNG;

/*
* <p>
* Copied from
* <a href="https://issues.apache.org/jira/browse/HADOOP-3315">hadoop-3315 tfile</a>.
* Remove after tfile is committed and use the tfile version of this class
* instead.</p>
*/
class KeySampler {
  Random random;
  int min, max;
  DiscreteRNG keyLenRNG;
  private static final int MIN_KEY_LEN = 4;

  public KeySampler(Random random, byte [] first, byte [] last,
      DiscreteRNG keyLenRNG) {
    this.random = random;
    min = keyPrefixToInt(first);
    max = keyPrefixToInt(last);
    this.keyLenRNG = keyLenRNG;
  }

  private int keyPrefixToInt(byte [] key) {
    byte[] b = key;
    int o = 0;
    return (b[o] & 0xff) << 24 | (b[o + 1] & 0xff) << 16
        | (b[o + 2] & 0xff) << 8 | (b[o + 3] & 0xff);
  }

  public void next(BytesWritable key) {
    key.setSize(Math.max(MIN_KEY_LEN, keyLenRNG.nextInt()));
    random.nextBytes(key.get());
    int n = random.nextInt(max - min) + min;
    byte[] b = key.get();
    b[0] = (byte) (n >> 24);
    b[1] = (byte) (n >> 16);
    b[2] = (byte) (n >> 8);
    b[3] = (byte) n;
  }
}
