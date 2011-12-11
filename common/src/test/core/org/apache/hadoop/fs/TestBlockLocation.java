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
package org.apache.hadoop.fs;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.io.DataOutputBuffer;

public class TestBlockLocation extends TestCase {
  // Verify fix of bug identified in HADOOP-6004
  public void testDeserialization() throws IOException {
    // Create a test BlockLocation
    String[] names = {"one", "two" };
    String[] hosts = {"three", "four" };
    String[] topologyPaths = {"five", "six"};
    long offset = 25l;
    long length = 55l;
    
    BlockLocation bl = new BlockLocation(names, hosts, topologyPaths, 
                                         offset, length);
    
    DataOutputBuffer dob = new DataOutputBuffer();
    
    // Serialize it
    try {
      bl.write(dob);
    } catch (IOException e) {
      fail("Unable to serialize data: " + e.getMessage());
    }

    byte[] bytes = dob.getData();
    DataInput da = new DataInputStream(new ByteArrayInputStream(bytes));

    // Try to re-create the BlockLocation the same way as is done during
    // deserialization
    BlockLocation bl2 = new BlockLocation();
    
    try {
      bl2.readFields(da);
    } catch (IOException e) {
      fail("Unable to deserialize BlockLocation: " + e.getMessage());
    }
    
    // Check that we got back what we started with
    verifyDeserialization(bl2.getHosts(), hosts);
    verifyDeserialization(bl2.getNames(), names);
    verifyDeserialization(bl2.getTopologyPaths(), topologyPaths);
    assertEquals(bl2.getOffset(), offset);
    assertEquals(bl2.getLength(), length);
  }

  private void verifyDeserialization(String[] ar1, String[] ar2) {
    assertEquals(ar1.length, ar2.length);
    
    for(int i = 0; i < ar1.length; i++)
      assertEquals(ar1[i], ar2[i]);
  }
}
