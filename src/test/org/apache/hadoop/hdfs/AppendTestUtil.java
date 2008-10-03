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
package org.apache.hadoop.hdfs;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/** Utilities for append-related tests */ 
class AppendTestUtil {
  /** For specifying the random number generator seed,
   *  change the following value:
   */
  static final Long RANDOM_NUMBER_GENERATOR_SEED = null;

  static final Log LOG = LogFactory.getLog(AppendTestUtil.class);

  private static final Random SEED = new Random();
  static {
    final long seed = RANDOM_NUMBER_GENERATOR_SEED == null?
        SEED.nextLong(): RANDOM_NUMBER_GENERATOR_SEED;
    LOG.info("seed=" + seed);
    SEED.setSeed(seed);
  }

  private static final ThreadLocal<Random> RANDOM = new ThreadLocal<Random>() {
    protected Random initialValue() {
      final Random r =  new Random();
      synchronized(SEED) { 
        final long seed = SEED.nextLong();
        r.setSeed(seed);
        LOG.info(Thread.currentThread().getName() + ": seed=" + seed);
      }
      return r;
    }
  };
  
  static int nextInt() {return RANDOM.get().nextInt();}
  static int nextInt(int n) {return RANDOM.get().nextInt(n);}
  static int nextLong() {return RANDOM.get().nextInt();}

  static byte[] randomBytes(long seed, int size) {
    LOG.info("seed=" + seed + ", size=" + size);
    final byte[] b = new byte[size];
    final Random rand = new Random(seed);
    rand.nextBytes(b);
    return b;
  }
}