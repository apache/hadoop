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
package org.apache.hadoop.hdfs.qjournal.server;

/**
 * Utilities for testing {@link Journal} instances.
 */
public class JournalTestUtil {

  /**
   * Corrupt the cache of a {@link Journal} to simulate some corrupt entries
   * being present.
   *
   * @param txid The transaction ID whose containing buffer in the cache
   *             should be corrupted.
   * @param journal The journal whose cache should be corrupted.
   */
  public static void corruptJournaledEditsCache(long txid, Journal journal) {
    JournaledEditsCache cache = journal.getJournaledEditsCache();
    byte[] buf = cache.getRawDataForTests(txid);
    // Change a few arbitrary bytes in the buffer
    for (int i = 0; i < buf.length; i += 9) {
      buf[i] = 0;
    }
    for (int i = 3; i < buf.length; i += 9) {
      buf[i] += 10;
    }
    for (int i = 6; i < buf.length; i += 9) {
      buf[i] -= 10;
    }
  }

}
