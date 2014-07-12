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

package org.apache.hadoop.fs.azure.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

import java.util.Date;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azure.AzureBlobStorageTestAccount;
import org.junit.Assume;
import org.junit.Test;

public class TestBandwidthGaugeUpdater {
  @Test
  public void testSingleThreaded() throws Exception {
    AzureFileSystemInstrumentation instrumentation =
        new AzureFileSystemInstrumentation(new Configuration());
    BandwidthGaugeUpdater updater =
        new BandwidthGaugeUpdater(instrumentation, 1000, true);
    updater.triggerUpdate(true);
    assertEquals(0, AzureMetricsTestUtil.getCurrentBytesWritten(instrumentation));
    updater.blockUploaded(new Date(), new Date(), 150);
    updater.triggerUpdate(true);
    assertEquals(150, AzureMetricsTestUtil.getCurrentBytesWritten(instrumentation));
    updater.blockUploaded(new Date(new Date().getTime() - 10000),
        new Date(), 200);
    updater.triggerUpdate(true);
    long currentBytes = AzureMetricsTestUtil.getCurrentBytesWritten(instrumentation);
    assertTrue(
        "We expect around (200/10 = 20) bytes written as the gauge value." +
        "Got " + currentBytes,
        currentBytes > 18 && currentBytes < 22);
    updater.close();
  }

  @Test
  public void testMultiThreaded() throws Exception {
    final AzureFileSystemInstrumentation instrumentation =
        new AzureFileSystemInstrumentation(new Configuration());
    final BandwidthGaugeUpdater updater =
        new BandwidthGaugeUpdater(instrumentation, 1000, true);
    Thread[] threads = new Thread[10];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          updater.blockDownloaded(new Date(), new Date(), 10);
          updater.blockDownloaded(new Date(0), new Date(0), 10);
        }
      });
    }
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    updater.triggerUpdate(false);
    assertEquals(10 * threads.length, AzureMetricsTestUtil.getCurrentBytesRead(instrumentation));
    updater.close();
  }
  
  @Test
  public void testFinalizerThreadShutdown() throws Exception {
    
    // force cleanup of any existing wasb filesystems
    System.gc();
    System.runFinalization();
    
    int nUpdaterThreadsStart = getWasbThreadCount();
    assertTrue("Existing WASB threads have not been cleared", nUpdaterThreadsStart == 0);
    
    final int nFilesystemsToSpawn = 10;
    AzureBlobStorageTestAccount testAccount = null;
    
    for(int i = 0; i < nFilesystemsToSpawn; i++){
      testAccount = AzureBlobStorageTestAccount.createMock();
      testAccount.getFileSystem();
    }

    int nUpdaterThreadsAfterSpawn = getWasbThreadCount();
    Assume.assumeTrue("Background threads should have spawned.", nUpdaterThreadsAfterSpawn == 10);
   
    testAccount = null;  //clear the last reachable reference
    
    // force cleanup
    System.gc();
    System.runFinalization();
    
    int nUpdaterThreadsAfterCleanup = getWasbThreadCount();
    assertTrue("Finalizers should have reduced the thread count.  ", nUpdaterThreadsAfterCleanup == 0 );
  }

  private int getWasbThreadCount() {
    int c = 0;
    Map<Thread, StackTraceElement[]> stacksStart = Thread.getAllStackTraces();
    for (Thread t : stacksStart.keySet()){
      if(t.getName().equals(BandwidthGaugeUpdater.THREAD_NAME))
      {
        c++;
      }
    }
    return c;
  }
}
