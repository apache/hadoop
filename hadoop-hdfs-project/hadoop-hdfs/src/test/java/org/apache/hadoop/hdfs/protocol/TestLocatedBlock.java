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

package org.apache.hadoop.hdfs.protocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.junit.Test;

import static org.junit.Assert.fail;

public class TestLocatedBlock {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestLocatedBlock.class);

  @Test(timeout = 10000)
  public void testAddCachedLocWhenEmpty() {
    DatanodeInfo[] ds = new DatanodeInfo[0];
    ExtendedBlock b1 = new ExtendedBlock("bpid", 1, 1, 1);
    LocatedBlock l1 = new LocatedBlock(b1, ds);
    DatanodeDescriptor dn = new DatanodeDescriptor(
        new DatanodeID("127.0.0.1", "localhost", "abcd",
            5000, 5001, 5002, 5003));
    try {
      l1.addCachedLoc(dn);
      fail("Adding dn when block is empty should throw");
    } catch (IllegalArgumentException e) {
      LOG.info("Expected exception:", e);
    }
  }
}
