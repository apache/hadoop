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
package org.apache.hadoop.hdfs.client;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;

import org.apache.commons.lang.SystemUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.SharedFileDescriptorFactory;
import org.apache.hadoop.hdfs.client.ShortCircuitSharedMemorySegment.Slot;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

public class TestShortCircuitSharedMemorySegment {
  public static final Log LOG =
      LogFactory.getLog(TestShortCircuitSharedMemorySegment.class);
  
  private static final File TEST_BASE =
      new File(System.getProperty("test.build.data", "/tmp"));

  @Before
  public void before() {
    Assume.assumeTrue(NativeIO.isAvailable());
    Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
  }

  @Test(timeout=60000)
  public void testStartupShutdown() throws Exception {
    File path = new File(TEST_BASE, "testStartupShutdown");
    path.mkdirs();
    SharedFileDescriptorFactory factory =
        new SharedFileDescriptorFactory("shm_", path.getAbsolutePath());
    FileInputStream stream = factory.createDescriptor(4096);
    ShortCircuitSharedMemorySegment shm = 
        new ShortCircuitSharedMemorySegment(stream);
    shm.close();
    stream.close();
    FileUtil.fullyDelete(path);
  }

  @Test(timeout=60000)
  public void testAllocateSlots() throws Exception {
    File path = new File(TEST_BASE, "testAllocateSlots");
    path.mkdirs();
    SharedFileDescriptorFactory factory =
        new SharedFileDescriptorFactory("shm_", path.getAbsolutePath());
    FileInputStream stream = factory.createDescriptor(4096);
    ShortCircuitSharedMemorySegment shm = 
        new ShortCircuitSharedMemorySegment(stream);
    int numSlots = 0;
    ArrayList<Slot> slots = new ArrayList<Slot>();
    while (true) {
      Slot slot = shm.allocateNextSlot();
      if (slot == null) {
        LOG.info("allocated " + numSlots + " slots before running out.");
        break;
      }
      slots.add(slot);
      numSlots++;
    }
    int slotIdx = 0;
    for (Slot slot : slots) {
      Assert.assertFalse(slot.addAnchor());
      Assert.assertEquals(slotIdx++, slot.getIndex());
    }
    for (Slot slot : slots) {
      slot.makeAnchorable();
    }
    for (Slot slot : slots) {
      Assert.assertTrue(slot.addAnchor());
    }
    for (Slot slot : slots) {
      slot.removeAnchor();
    }
    shm.close();
    for (Slot slot : slots) {
      slot.close();
    }
    stream.close();
    FileUtil.fullyDelete(path);
  }
}
