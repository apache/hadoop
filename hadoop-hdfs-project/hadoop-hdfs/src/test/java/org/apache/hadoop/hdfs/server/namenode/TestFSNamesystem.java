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

package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.mockito.Mockito;

public class TestFSNamesystem {
  @Test
  /**
   * Test that isInStartupSafemode returns true only during startup safemode
   * and not also during low-resource safemode
   */
  public void testStartupSafemode() throws IOException {
    Configuration conf = new Configuration();
    FSImage fsImage = Mockito.mock(FSImage.class);
    FSEditLog fsEditLog = Mockito.mock(FSEditLog.class);
    Mockito.when(fsImage.getEditLog()).thenReturn(fsEditLog);
    FSNamesystem fsn = new FSNamesystem(fsImage, conf);

    fsn.leaveSafeMode(false);
    assertTrue("After leaving safemode FSNamesystem.isInStartupSafeMode still "
      + "returned true", !fsn.isInStartupSafeMode());
    assertTrue("After leaving safemode FSNamesystem.isInSafeMode still returned"
      + " true", !fsn.isInSafeMode() );

    fsn.enterSafeMode(true);
    assertTrue("After entering safemode due to low resources FSNamesystem."
      + "isInStartupSafeMode still returned true", !fsn.isInStartupSafeMode());
    assertTrue("After entering safemode due to low resources FSNamesystem."
      + "isInSafeMode still returned false", fsn.isInSafeMode());
  }

  @Test
  public void testReplQueuesActiveAfterStartupSafemode() throws IOException, InterruptedException{
    Configuration conf = new Configuration();

    FSEditLog fsEditLog = Mockito.mock(FSEditLog.class);
    FSImage fsImage = Mockito.mock(FSImage.class);
    Mockito.when(fsImage.getEditLog()).thenReturn(fsEditLog);

    FSNamesystem fsNamesystem = new FSNamesystem(fsImage, conf);
    FSNamesystem fsn = Mockito.spy(fsNamesystem);

    fsn.leaveSafeMode(false);
    assertTrue("FSNamesystem didn't leave safemode", !fsn.isInSafeMode());
    assertTrue("Replication queues weren't being populated even after leaving "
      + "safemode", fsn.isPopulatingReplQueues());
    fsn.enterSafeMode(false);
    assertTrue("FSNamesystem didn't enter safemode", fsn.isInSafeMode());
    assertTrue("Replication queues weren't being populated after entering "
      + "safemode 2nd time", fsn.isPopulatingReplQueues());
  }
  
  @Test
  public void testFsLockFairness() throws IOException, InterruptedException{
    Configuration conf = new Configuration();

    FSEditLog fsEditLog = Mockito.mock(FSEditLog.class);
    FSImage fsImage = Mockito.mock(FSImage.class);
    Mockito.when(fsImage.getEditLog()).thenReturn(fsEditLog);

    conf.setBoolean("dfs.namenode.fslock.fair", true);
    FSNamesystem fsNamesystem = new FSNamesystem(fsImage, conf);
    assertTrue(fsNamesystem.getFsLockForTests().isFair());
    
    conf.setBoolean("dfs.namenode.fslock.fair", false);
    fsNamesystem = new FSNamesystem(fsImage, conf);
    assertFalse(fsNamesystem.getFsLockForTests().isFair());
  }  
}
