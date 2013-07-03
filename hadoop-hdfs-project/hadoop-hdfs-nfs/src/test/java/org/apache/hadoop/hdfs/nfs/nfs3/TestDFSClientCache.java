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
package org.apache.hadoop.hdfs.nfs.nfs3;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.junit.Test;
import org.mockito.Mockito;

public class TestDFSClientCache {
  @Test
  public void testLruTable() throws IOException {
    DFSClientCache cache = new DFSClientCache(new Configuration(), 3);
    DFSClient client = Mockito.mock(DFSClient.class);
    cache.put("a", client);
    assertTrue(cache.containsKey("a"));

    cache.put("b", client);
    cache.put("c", client);
    cache.put("d", client);
    assertTrue(cache.usedSize() == 3);
    assertFalse(cache.containsKey("a"));

    // Cache should have d,c,b in LRU order
    assertTrue(cache.containsKey("b"));
    // Do a lookup to make b the most recently used
    assertTrue(cache.get("b") != null);

    cache.put("e", client);
    assertTrue(cache.usedSize() == 3);
    // c should be replaced with e, and cache has e,b,d
    assertFalse(cache.containsKey("c"));
    assertTrue(cache.containsKey("e"));
    assertTrue(cache.containsKey("b"));
    assertTrue(cache.containsKey("d"));
  }
}
