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
package org.apache.hadoop.hdfs.qjournal.client;

import static org.junit.Assert.*;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hdfs.qjournal.client.QuorumCall;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SettableFuture;

public class TestQuorumCall {
  @Test(timeout=10000)
  public void testQuorums() throws Exception {
    Map<String, SettableFuture<String>> futures = ImmutableMap.of(
        "f1", SettableFuture.<String>create(),
        "f2", SettableFuture.<String>create(),
        "f3", SettableFuture.<String>create());
    
    QuorumCall<String, String> q = QuorumCall.create(futures);
    assertEquals(0, q.countResponses());
    
    futures.get("f1").set("first future");
    q.waitFor(1, 0, 0, 100000, "test"); // wait for 1 response
    q.waitFor(0, 1, 0, 100000, "test"); // wait for 1 success
    assertEquals(1, q.countResponses());
    
    
    futures.get("f2").setException(new Exception("error"));
    assertEquals(2, q.countResponses());
    
    futures.get("f3").set("second future");
    q.waitFor(3, 0, 100, 100000, "test"); // wait for 3 responses
    q.waitFor(0, 2, 100, 100000, "test"); // 2 successes

    assertEquals(3, q.countResponses());
    assertEquals("f1=first future,f3=second future",
        Joiner.on(",").withKeyValueSeparator("=").join(
            new TreeMap<String, String>(q.getResults())));
    
    try {
      q.waitFor(0, 4, 100, 10, "test");
      fail("Didn't time out waiting for more responses than came back");
    } catch (TimeoutException te) {
      // expected
    }
  }
  @Test(timeout=10000)
  public void testQuorumFailsWithoutResponse() throws Exception {
    Map<String, SettableFuture<String>> futures = ImmutableMap.of(
        "f1", SettableFuture.<String>create());

    QuorumCall<String, String> q = QuorumCall.create(futures);
    assertEquals("The number of quorum calls for which a response has been"
            + " received should be 0", 0, q.countResponses());

    try {
      q.waitFor(0, 1, 100, 10, "test");
      fail("Didn't time out waiting for more responses than came back");
    } catch (TimeoutException te) {
      // expected
    }
  }

}
