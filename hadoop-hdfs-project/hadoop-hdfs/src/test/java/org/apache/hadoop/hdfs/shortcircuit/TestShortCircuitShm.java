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
package org.apache.hadoop.hdfs.shortcircuit;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.ShmId;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.Slot;
import org.apache.hadoop.io.nativeio.SharedFileDescriptorFactory;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class TestShortCircuitShm {
  public static final Log LOG = LogFactory.getLog(TestShortCircuitShm.class);
  
  private static final File TEST_BASE =
      new File(System.getProperty("test.build.data", "/tmp"));

  @Before
  public void before() {
    Assume.assumeTrue(null == 
        SharedFileDescriptorFactory.getLoadingFailureReason());
  }

  @Test(timeout=60000)
  public void testStartupShutdown() throws Exception {
    File path = new File(TEST_BASE, "testStartupShutdown");
    path.mkdirs();
    SharedFileDescriptorFactory factory =
        SharedFileDescriptorFactory.create("shm_",
            new String[] { path.getAbsolutePath() } );
    FileInputStream stream =
        factory.createDescriptor("testStartupShutdown", 4096);
    ShortCircuitShm shm = new ShortCircuitShm(ShmId.createRandom(), stream);
    shm.free();
    stream.close();
    FileUtil.fullyDelete(path);
  }

  @Test(timeout=60000)
  public void testAllocateSlots() throws Exception {
    File path = new File(TEST_BASE, "testAllocateSlots");
    path.mkdirs();
    SharedFileDescriptorFactory factory =
        SharedFileDescriptorFactory.create("shm_", 
            new String[] { path.getAbsolutePath() });
    FileInputStream stream =
        factory.createDescriptor("testAllocateSlots", 4096);
    ShortCircuitShm shm = new ShortCircuitShm(ShmId.createRandom(), stream);
    int numSlots = 0;
    ArrayList<Slot> slots = new ArrayList<Slot>();
    while (!shm.isFull()) {
      Slot slot = shm.allocAndRegisterSlot(new ExtendedBlockId(123L, "test_bp1"));
      slots.add(slot);
      numSlots++;
    }
    LOG.info("allocated " + numSlots + " slots before running out.");
    int slotIdx = 0;
    for (Iterator<Slot> iter = shm.slotIterator();
        iter.hasNext(); ) {
      Assert.assertTrue(slots.contains(iter.next()));
    }
    for (Slot slot : slots) {
      Assert.assertFalse(slot.addAnchor());
      Assert.assertEquals(slotIdx++, slot.getSlotIdx());
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
    for (Slot slot : slots) {
      shm.unregisterSlot(slot.getSlotIdx());
      slot.makeInvalid();
    }
    shm.free();
    stream.close();
    FileUtil.fullyDelete(path);
  }
}
