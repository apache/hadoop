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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;


import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestLeaseRenewer {
  private String FAKE_AUTHORITY="hdfs://nn1/";
  private UserGroupInformation FAKE_UGI_A =
    UserGroupInformation.createUserForTesting(
      "myuser", new String[]{"group1"});
  private UserGroupInformation FAKE_UGI_B =
    UserGroupInformation.createUserForTesting(
      "myuser", new String[]{"group1"});

  private DFSClient MOCK_DFSCLIENT;
  private LeaseRenewer renewer;
  
  /** Cause renewals often so test runs quickly. */
  private static final long FAST_GRACE_PERIOD = 100L;
  
  @Before
  public void setupMocksAndRenewer() throws IOException {
    MOCK_DFSCLIENT = Mockito.mock(DFSClient.class);
    Mockito.doReturn(true)
      .when(MOCK_DFSCLIENT).isClientRunning();
    Mockito.doReturn((int)FAST_GRACE_PERIOD)
      .when(MOCK_DFSCLIENT).getHdfsTimeout();
    Mockito.doReturn("myclient")
      .when(MOCK_DFSCLIENT).getClientName();
    
    renewer = LeaseRenewer.getInstance(
        FAKE_AUTHORITY, FAKE_UGI_A, MOCK_DFSCLIENT);
    renewer.setGraceSleepPeriod(FAST_GRACE_PERIOD);
}
 
  @Test
  public void testInstanceSharing() throws IOException {
    // Two lease renewers with the same UGI should return
    // the same instance
    LeaseRenewer lr = LeaseRenewer.getInstance(
        FAKE_AUTHORITY, FAKE_UGI_A, MOCK_DFSCLIENT);
    LeaseRenewer lr2 = LeaseRenewer.getInstance(
        FAKE_AUTHORITY, FAKE_UGI_A, MOCK_DFSCLIENT);
    Assert.assertSame(lr, lr2);
    
    // But a different UGI should return a different instance
    LeaseRenewer lr3 = LeaseRenewer.getInstance(
        FAKE_AUTHORITY, FAKE_UGI_B, MOCK_DFSCLIENT);
    Assert.assertNotSame(lr, lr3);
    
    // A different authority with same UGI should also be a different
    // instance.
    LeaseRenewer lr4 = LeaseRenewer.getInstance(
        "someOtherAuthority", FAKE_UGI_B, MOCK_DFSCLIENT);
    Assert.assertNotSame(lr, lr4);
    Assert.assertNotSame(lr3, lr4);
  }
  
  @Test
  public void testClientName() throws IOException {
    String clientName = renewer.getClientName("NONMAPREDUCE");
    Assert.assertTrue("bad client name: " + clientName,
        clientName.startsWith("DFSClient_NONMAPREDUCE_"));
  }
  
  @Test
  public void testRenewal() throws Exception {
    // Keep track of how many times the lease gets renewed
    final AtomicInteger leaseRenewalCount = new AtomicInteger();
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        leaseRenewalCount.incrementAndGet();
        return null;
      }
    }).when(MOCK_DFSCLIENT).renewLease();

    
    // Set up a file so that we start renewing our lease.
    DFSOutputStream mockStream = Mockito.mock(DFSOutputStream.class);
    String filePath = "/foo";
    renewer.put(filePath, mockStream, MOCK_DFSCLIENT);

    // Wait for lease to get renewed
    long failTime = System.currentTimeMillis() + 5000;
    while (System.currentTimeMillis() < failTime &&
        leaseRenewalCount.get() == 0) {
      Thread.sleep(50);
    }
    if (leaseRenewalCount.get() == 0) {
      Assert.fail("Did not renew lease at all!");
    }

    renewer.closeFile(filePath, MOCK_DFSCLIENT);
  }
  
  @Test
  public void testThreadName() throws Exception {
    DFSOutputStream mockStream = Mockito.mock(DFSOutputStream.class);
    String filePath = "/foo";
    Assert.assertFalse("Renewer not initially running",
        renewer.isRunning());
    
    // Pretend to open a file
    Mockito.doReturn(false)
      .when(MOCK_DFSCLIENT).isFilesBeingWrittenEmpty();
    renewer.put(filePath, mockStream, MOCK_DFSCLIENT);
    
    Assert.assertTrue("Renewer should have started running",
        renewer.isRunning());
    
    // Check the thread name is reasonable
    String threadName = renewer.getDaemonName();
    Assert.assertEquals("LeaseRenewer:myuser@hdfs://nn1/", threadName);
    
    // Pretend to close the file
    Mockito.doReturn(true)
      .when(MOCK_DFSCLIENT).isFilesBeingWrittenEmpty();
    renewer.closeFile(filePath, MOCK_DFSCLIENT);
    
    // Should stop the renewer running within a few seconds
    long failTime = System.currentTimeMillis() + 5000;
    while (renewer.isRunning() && System.currentTimeMillis() < failTime) {
      Thread.sleep(50);
    }
    Assert.assertFalse(renewer.isRunning());
  }
  
}
