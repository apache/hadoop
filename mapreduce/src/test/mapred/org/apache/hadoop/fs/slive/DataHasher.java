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

package org.apache.hadoop.fs.slive;

import java.util.Random;

/**
 * Class which is used to create the data to write for a given path and offset
 * into that file for writing and later verification that the expected value is
 * read at that file bytes offset
 */
class DataHasher {

  private Random rnd;

  DataHasher(long mixIn) {
    this.rnd = new Random(mixIn);
  }

  /**
   * @param offSet
   *          the byte offset into the file
   * 
   * @return the data to be expected at that offset
   */
  long generate(long offSet) {
    return ((offSet * 47) ^ (rnd.nextLong() * 97)) * 37;
  }

}
