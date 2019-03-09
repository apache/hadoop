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

package org.apache.hadoop.yarn.server.resourcemanager.security;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SystemCredentialsForAppsProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatResponsePBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.TestRMRestart.TestSecurityMockRM;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer.DelegationTokenToRenew;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.YarnServerBuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Supplier;

/**
 * unit test - 
 * tests addition/deletion/cancellation of renewals of delegation tokens
 *
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class TestDelegationTokenRenewer {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDelegationTokenRenewer.class);
  private static final Text KIND =
      DelegationTokenRenewer.HDFS_DELEGATION_KIND;
  
  private static BlockingQueue<Event> eventQueue;
  private static volatile AtomicInteger counter;
  private static AsyncDispatcher dispatcher;
  public static class Renewer extends TokenRenewer {
    private static int counter = 0;
    private static Token<?> lastRenewed = null;
    private static Token<?> tokenToRenewIn2Sec = null;
    private static boolean cancelled = false; 
    private static void reset() {
      counter = 0;
      lastRenewed = null;
      tokenToRenewIn2Sec = null;
      cancelled = false;
    }

    @Override
    public boolean handleKind(Text kind) {
      return KIND.equals(kind);
    }

    @Override
    public boolean isManaged(Token<?> token) throws IOException {
      return true;
    }

    @Override
    public long renew(Token<?> t, Configuration conf) throws IOException {
      if ( !(t instanceof MyToken)) {
        if(conf.get("override_token_expire_time") != null) {
          return System.currentTimeMillis() +
              Long.parseLong(conf.get("override_token_expire_time"));
        } else {
          // renew in 3 seconds
          return System.currentTimeMillis() + 3000;
        }
      }
      MyToken token = (MyToken)t;
      if(token.isCanceled()) {
        throw new InvalidToken("token has been canceled");
      }
      lastRenewed = token;
      counter ++;
      LOG.info("Called MYDFS.renewdelegationtoken " + token + 
          ";this dfs=" + this.hashCode() + ";c=" + counter);
      if(tokenToRenewIn2Sec == token) { 
        // this token first renewal in 2 seconds
        LOG.info("RENEW in 2 seconds");
        tokenToRenewIn2Sec=null;
        return 2*1000 + System.currentTimeMillis();
      } else {
        return 86400*1000 + System.currentTimeMillis();
      }
    }

    @Override
    public void cancel(Token<?> t, Configuration conf) {
      cancelled = true;
      if (t instanceof MyToken) {
        MyToken token = (MyToken) t;
        LOG.info("Cancel token " + token);
        token.cancelToken();
      }
   }
    
  }

  private static Configuration conf;
  DelegationTokenRenewer delegationTokenRenewer;
 
  @BeforeClass
  public static void setUpClass() throws Exception {
    conf = new Configuration();
    
    // create a fake FileSystem (MyFS) and assosiate it
    // with "hdfs" schema.
    URI uri = new URI(DelegationTokenRenewer.SCHEME+"://localhost:0");
    System.out.println("scheme is : " + uri.getScheme());
    conf.setClass("fs." + uri.getScheme() + ".impl", MyFS.class, DistributedFileSystem.class);
    FileSystem.setDefaultUri(conf, uri);
    LOG.info("filesystem uri = " + FileSystem.getDefaultUri(conf).toString());
  }
  

  @Before
  public void setUp() throws Exception {
    counter = new AtomicInteger(0);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    conf.set("override_token_expire_time", "3000");
    UserGroupInformation.setConfiguration(conf);
    eventQueue = new LinkedBlockingQueue<Event>();
    dispatcher = new AsyncDispatcher(eventQueue);
    Renewer.reset();
    delegationTokenRenewer = createNewDelegationTokenRenewer(conf, counter);
    RMContext mockContext =  mock(RMContext.class);
    ClientRMService mockClientRMService = mock(ClientRMService.class);
    when(mockContext.getSystemCredentialsForApps()).thenReturn(
        new ConcurrentHashMap<ApplicationId, SystemCredentialsForAppsProto>());
    when(mockContext.getDelegationTokenRenewer()).thenReturn(
        delegationTokenRenewer);
    when(mockContext.getDispatcher()).thenReturn(dispatcher);
    when(mockContext.getClientRMService()).thenReturn(mockClientRMService);
    InetSocketAddress sockAddr =
        InetSocketAddress.createUnresolved("localhost", 1234);
    when(mockClientRMService.getBindAddress()).thenReturn(sockAddr);
    delegationTokenRenewer.setRMContext(mockContext);
    delegationTokenRenewer.init(conf);
    delegationTokenRenewer.start();
  }
  
  @After
  public void tearDown() {
    delegationTokenRenewer.stop();
  }
  
  private static class MyDelegationTokenSecretManager extends DelegationTokenSecretManager {

    public MyDelegationTokenSecretManager(long delegationKeyUpdateInterval,
        long delegationTokenMaxLifetime, long delegationTokenRenewInterval,
        long delegationTokenRemoverScanInterval, FSNamesystem namesystem) {
      super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
          delegationTokenRenewInterval, delegationTokenRemoverScanInterval,
          namesystem);
    }
    
    @Override //DelegationTokenSecretManager
    public void logUpdateMasterKey(DelegationKey key) throws IOException {
      return;
    }
  }
  
  /**
   * add some extra functionality for testing
   * 1. toString();
   * 2. cancel() and isCanceled()
   */
  private static class MyToken extends Token<DelegationTokenIdentifier> {
    public String status = "GOOD";
    public static final String CANCELED = "CANCELED";

    public MyToken(DelegationTokenIdentifier dtId1,
        MyDelegationTokenSecretManager sm) {
      super(dtId1, sm);
      setKind(KIND);
      status = "GOOD";
    }
    
    public boolean isCanceled() {return status.equals(CANCELED);}
    
    public void cancelToken() {this.status=CANCELED;}

    @Override
    public long renew(Configuration conf) throws IOException,
        InterruptedException {
      return super.renew(conf);
    }

    public String toString() {
      StringBuilder sb = new StringBuilder(1024);
      
      sb.append("id=");
      String id = StringUtils.byteToHexString(this.getIdentifier());
      int idLen = id.length();
      sb.append(id.substring(idLen-6));
      sb.append(";k=");
      sb.append(this.getKind());
      sb.append(";s=");
      sb.append(this.getService());
      return sb.toString();
    }
  }

  /**
   * fake FileSystem 
   * overwrites three methods
   * 1. getDelegationToken() - generates a token
   * 2. renewDelegataionToken - counts number of calls, and remembers 
   * most recently renewed token.
   * 3. cancelToken -cancels token (subsequent renew will cause IllegalToken 
   * exception
   */
  static class MyFS extends DistributedFileSystem {
    private static AtomicInteger instanceCounter = new AtomicInteger();
    public MyFS() {
      instanceCounter.incrementAndGet();
    }
    public void close() {
      instanceCounter.decrementAndGet();
    }
    public static int getInstanceCounter() {
      return instanceCounter.get();
    }
    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {}
    
    @Override 
    public MyToken getDelegationToken(String renewer) throws IOException {
      MyToken result = createTokens(new Text(renewer));
      LOG.info("Called MYDFS.getdelegationtoken " + result);
      return result;
    }

    public Token<?>[] addDelegationTokens(
        final String renewer, Credentials credentials) throws IOException {
      return new Token<?>[0];
    }
  }
  
  /**
   * Auxiliary - create token
   * @param renewer
   * @return
   * @throws IOException
   */
  static MyToken createTokens(Text renewer) 
    throws IOException {
    Text user1= new Text("user1");
    
    MyDelegationTokenSecretManager sm = new MyDelegationTokenSecretManager(
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT,
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT,
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT,
        3600000, null);
    sm.startThreads();
    
    DelegationTokenIdentifier dtId1 = 
      new DelegationTokenIdentifier(user1, renewer, user1);
    
    MyToken token1 = new MyToken(dtId1, sm);
   
    token1.setService(new Text("localhost:0"));
    return token1;
  }
  
  
  /**
   * Basic idea of the test:
   * 1. create tokens.
   * 2. Mark one of them to be renewed in 2 seconds (instead of
   * 24 hours)
   * 3. register them for renewal
   * 4. sleep for 3 seconds
   * 5. count number of renewals (should 3 initial ones + one extra)
   * 6. register another token for 2 seconds 
   * 7. cancel it immediately
   * 8. Sleep and check that the 2 seconds renew didn't happen 
   * (totally 5 renewals)
   * 9. check cancellation
   * @throws IOException
   * @throws URISyntaxException
   */
  @Test(timeout=60000)
  public void testDTRenewal () throws Exception {
    MyFS dfs = (MyFS)FileSystem.get(conf);
    LOG.info("dfs="+(Object)dfs.hashCode() + ";conf="+conf.hashCode());
    // Test 1. - add three tokens - make sure exactly one get's renewed
    
    // get the delegation tokens
    MyToken token1, token2, token3;
    token1 = dfs.getDelegationToken("user1");
    token2 = dfs.getDelegationToken("user2");
    token3 = dfs.getDelegationToken("user3");

    //to cause this one to be set for renew in 2 secs
    Renewer.tokenToRenewIn2Sec = token1;
    LOG.info("token="+token1+" should be renewed for 2 secs");
    
    // three distinct Namenodes
    String nn1 = DelegationTokenRenewer.SCHEME + "://host1:0";
    String nn2 = DelegationTokenRenewer.SCHEME + "://host2:0";
    String nn3 = DelegationTokenRenewer.SCHEME + "://host3:0";
    
    Credentials ts = new Credentials();
    
    // register the token for renewal
    ts.addToken(new Text(nn1), token1);
    ts.addToken(new Text(nn2), token2);
    ts.addToken(new Text(nn3), token3);
    
    // register the tokens for renewal
    ApplicationId applicationId_0 = 
        BuilderUtils.newApplicationId(0, 0);
    delegationTokenRenewer.addApplicationAsync(applicationId_0, ts, true, "user",
        new Configuration());
    waitForEventsToGetProcessed(delegationTokenRenewer);

    // first 3 initial renewals + 1 real
    int numberOfExpectedRenewals = 3+1; 
    
    int attempts = 10;
    while(attempts-- > 0) {
      try {
        Thread.sleep(3*1000); // sleep 3 seconds, so it has time to renew
      } catch (InterruptedException e) {}
      
      // since we cannot guarantee timely execution - let's give few chances
      if(Renewer.counter==numberOfExpectedRenewals)
        break;
    }
    
    LOG.info("dfs=" + dfs.hashCode() + 
        ";Counter = " + Renewer.counter + ";t="+  Renewer.lastRenewed);
    assertEquals("renew wasn't called as many times as expected(4):",
        numberOfExpectedRenewals, Renewer.counter);
    assertEquals("most recently renewed token mismatch", Renewer.lastRenewed, 
        token1);
    
    // Test 2. 
    // add another token ( that expires in 2 secs). Then remove it, before
    // time is up.
    // Wait for 3 secs , and make sure no renews were called
    ts = new Credentials();
    MyToken token4 = dfs.getDelegationToken("user4");
    
    //to cause this one to be set for renew in 2 secs
    Renewer.tokenToRenewIn2Sec = token4; 
    LOG.info("token="+token4+" should be renewed for 2 secs");
    
    String nn4 = DelegationTokenRenewer.SCHEME + "://host4:0";
    ts.addToken(new Text(nn4), token4);
    

    ApplicationId applicationId_1 = BuilderUtils.newApplicationId(0, 1);
    delegationTokenRenewer.addApplicationAsync(applicationId_1, ts, true, "user",
        new Configuration());
    waitForEventsToGetProcessed(delegationTokenRenewer);
    delegationTokenRenewer.applicationFinished(applicationId_1);
    waitForEventsToGetProcessed(delegationTokenRenewer);
    numberOfExpectedRenewals = Renewer.counter; // number of renewals so far
    try {
      Thread.sleep(6*1000); // sleep 6 seconds, so it has time to renew
    } catch (InterruptedException e) {}
    LOG.info("Counter = " + Renewer.counter + ";t="+ Renewer.lastRenewed);
    
    // counter and the token should stil be the old ones
    assertEquals("renew wasn't called as many times as expected",
        numberOfExpectedRenewals, Renewer.counter);
    
    // also renewing of the cancelled token should fail
    try {
      token4.renew(conf);
      fail("Renewal of cancelled token should have failed");
    } catch (InvalidToken ite) {
      //expected
    }
  }
  
  @Test(timeout=60000)
  public void testAppRejectionWithCancelledDelegationToken() throws Exception {
    MyFS dfs = (MyFS)FileSystem.get(conf);
    LOG.info("dfs="+(Object)dfs.hashCode() + ";conf="+conf.hashCode());

    MyToken token = dfs.getDelegationToken("user1");
    token.cancelToken();

    Credentials ts = new Credentials();
    ts.addToken(token.getKind(), token);
    
    // register the tokens for renewal
    ApplicationId appId =  BuilderUtils.newApplicationId(0, 0);
    delegationTokenRenewer.addApplicationAsync(appId, ts, true, "user",
        new Configuration());
    int waitCnt = 20;
    while (waitCnt-- >0) {
      if (!eventQueue.isEmpty()) {
        Event evt = eventQueue.take();
        if (evt.getType() == RMAppEventType.APP_REJECTED) {
          Assert.assertTrue(
              ((RMAppEvent) evt).getApplicationId().equals(appId));
          return;
        }
      } else {
        Thread.sleep(500);
      }
    }
    fail("App submission with a cancelled token should have failed");
  }

  // Testcase for YARN-3021, let RM skip renewing token if the renewer string
  // is empty
  @Test(timeout=60000)
  public void testAppTokenWithNonRenewer() throws Exception {
    MyFS dfs = (MyFS)FileSystem.get(conf);
    LOG.info("dfs="+(Object)dfs.hashCode() + ";conf="+conf.hashCode());

    // Test would fail if using non-empty renewer string here
    MyToken token = dfs.getDelegationToken("");
    token.cancelToken();

    Credentials ts = new Credentials();
    ts.addToken(token.getKind(), token);
    
    // register the tokens for renewal
    ApplicationId appId =  BuilderUtils.newApplicationId(0, 0);
    delegationTokenRenewer.addApplicationSync(appId, ts, true, "user");
  }

  /**
   * Basic idea of the test:
   * 1. register a token for 2 seconds with no cancel at the end
   * 2. cancel it immediately
   * 3. Sleep and check that the 2 seconds renew didn't happen 
   * (totally 5 renewals)
   * 4. check cancellation
   * @throws IOException
   * @throws URISyntaxException
   */
  @Test(timeout=60000)
  public void testDTRenewalWithNoCancel () throws Exception {
    MyFS dfs = (MyFS)FileSystem.get(conf);
    LOG.info("dfs="+(Object)dfs.hashCode() + ";conf="+conf.hashCode());

    Credentials ts = new Credentials();
    MyToken token1 = dfs.getDelegationToken("user1");

    //to cause this one to be set for renew in 2 secs
    Renewer.tokenToRenewIn2Sec = token1; 
    LOG.info("token="+token1+" should be renewed for 2 secs");
    
    String nn1 = DelegationTokenRenewer.SCHEME + "://host1:0";
    ts.addToken(new Text(nn1), token1);
    

    ApplicationId applicationId_1 = BuilderUtils.newApplicationId(0, 1);
    delegationTokenRenewer.addApplicationAsync(applicationId_1, ts, false, "user",
        new Configuration());
    waitForEventsToGetProcessed(delegationTokenRenewer);
    delegationTokenRenewer.applicationFinished(applicationId_1);
    waitForEventsToGetProcessed(delegationTokenRenewer);
    int numberOfExpectedRenewals = Renewer.counter; // number of renewals so far
    try {
      Thread.sleep(6*1000); // sleep 6 seconds, so it has time to renew
    } catch (InterruptedException e) {}
    LOG.info("Counter = " + Renewer.counter + ";t="+ Renewer.lastRenewed);
    
    // counter and the token should still be the old ones
    assertEquals("renew wasn't called as many times as expected",
        numberOfExpectedRenewals, Renewer.counter);
    
    // also renewing of the canceled token should not fail, because it has not
    // been canceled
    token1.renew(conf);
  }
  
  /**
   * Basic idea of the test:
   * 0. Setup token KEEP_ALIVE
   * 1. create tokens.
   * 2. register them for renewal - to be cancelled on app complete
   * 3. Complete app.
   * 4. Verify token is alive within the KEEP_ALIVE time
   * 5. Verify token has been cancelled after the KEEP_ALIVE_TIME
   * @throws IOException
   * @throws URISyntaxException
   */
  @Test(timeout=60000)
  public void testDTKeepAlive1 () throws Exception {
    Configuration lconf = new Configuration(conf);
    lconf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
    //Keep tokens alive for 6 seconds.
    lconf.setLong(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS, 6000l);
    //Try removing tokens every second.
    lconf.setLong(
        YarnConfiguration.RM_DELAYED_DELEGATION_TOKEN_REMOVAL_INTERVAL_MS,
        1000l);
    DelegationTokenRenewer localDtr =
        createNewDelegationTokenRenewer(lconf, counter);
    RMContext mockContext = mock(RMContext.class);
    when(mockContext.getSystemCredentialsForApps()).thenReturn(
        new ConcurrentHashMap<ApplicationId, SystemCredentialsForAppsProto>());
    ClientRMService mockClientRMService = mock(ClientRMService.class);
    when(mockContext.getClientRMService()).thenReturn(mockClientRMService);
    when(mockContext.getDelegationTokenRenewer()).thenReturn(
        localDtr);
    when(mockContext.getDispatcher()).thenReturn(dispatcher);
    InetSocketAddress sockAddr =
        InetSocketAddress.createUnresolved("localhost", 1234);
    when(mockClientRMService.getBindAddress()).thenReturn(sockAddr);
    localDtr.setRMContext(mockContext);
    localDtr.init(lconf);
    localDtr.start();
    
    MyFS dfs = (MyFS)FileSystem.get(lconf);
    LOG.info("dfs="+(Object)dfs.hashCode() + ";conf="+lconf.hashCode());
    
    Credentials ts = new Credentials();
    // get the delegation tokens
    MyToken token1 = dfs.getDelegationToken("user1");

    String nn1 = DelegationTokenRenewer.SCHEME + "://host1:0";
    ts.addToken(new Text(nn1), token1);

    // register the tokens for renewal
    ApplicationId applicationId_0 =  BuilderUtils.newApplicationId(0, 0);
    localDtr.addApplicationAsync(applicationId_0, ts, true, "user",
        new Configuration());
    waitForEventsToGetProcessed(localDtr);
    if (!eventQueue.isEmpty()){
      Event evt = eventQueue.take();
      if (evt instanceof RMAppEvent) {
        Assert.assertEquals(((RMAppEvent)evt).getType(), RMAppEventType.START);
      } else {
        fail("RMAppEvent.START was expected!!");
      }
    }
    
    localDtr.applicationFinished(applicationId_0);
    waitForEventsToGetProcessed(localDtr);

    //Token should still be around. Renewal should not fail.
    token1.renew(lconf);

    //Allow the keepalive time to run out
    Thread.sleep(10000l);

    //The token should have been cancelled at this point. Renewal will fail.
    try {
      token1.renew(lconf);
      fail("Renewal of cancelled token should have failed");
    } catch (InvalidToken ite) {}
  }

  /**
   * Basic idea of the test:
   * 0. Setup token KEEP_ALIVE
   * 1. create tokens.
   * 2. register them for renewal - to be cancelled on app complete
   * 3. Complete app.
   * 4. Verify token is alive within the KEEP_ALIVE time
   * 5. Send an explicity KEEP_ALIVE_REQUEST
   * 6. Verify token KEEP_ALIVE time is renewed.
   * 7. Verify token has been cancelled after the renewed KEEP_ALIVE_TIME.
   * @throws IOException
   * @throws URISyntaxException
   */
  @Test(timeout=60000)
  public void testDTKeepAlive2() throws Exception {
    Configuration lconf = new Configuration(conf);
    lconf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
    //Keep tokens alive for 6 seconds.
    lconf.setLong(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS, 6000l);
    //Try removing tokens every second.
    lconf.setLong(
        YarnConfiguration.RM_DELAYED_DELEGATION_TOKEN_REMOVAL_INTERVAL_MS,
        1000l);
    DelegationTokenRenewer localDtr =
        createNewDelegationTokenRenewer(conf, counter);
    RMContext mockContext = mock(RMContext.class);
    when(mockContext.getSystemCredentialsForApps()).thenReturn(
        new ConcurrentHashMap<ApplicationId, SystemCredentialsForAppsProto>());
    ClientRMService mockClientRMService = mock(ClientRMService.class);
    when(mockContext.getClientRMService()).thenReturn(mockClientRMService);
    when(mockContext.getDelegationTokenRenewer()).thenReturn(
        localDtr);
    when(mockContext.getDispatcher()).thenReturn(dispatcher);
    InetSocketAddress sockAddr =
        InetSocketAddress.createUnresolved("localhost", 1234);
    when(mockClientRMService.getBindAddress()).thenReturn(sockAddr);
    localDtr.setRMContext(mockContext);
    localDtr.init(lconf);
    localDtr.start();
    
    MyFS dfs = (MyFS)FileSystem.get(lconf);
    LOG.info("dfs="+(Object)dfs.hashCode() + ";conf="+lconf.hashCode());

    Credentials ts = new Credentials();
    // get the delegation tokens
    MyToken token1 = dfs.getDelegationToken("user1");
    
    String nn1 = DelegationTokenRenewer.SCHEME + "://host1:0";
    ts.addToken(new Text(nn1), token1);

    // register the tokens for renewal
    ApplicationId applicationId_0 =  BuilderUtils.newApplicationId(0, 0);
    localDtr.addApplicationAsync(applicationId_0, ts, true, "user",
        new Configuration());
    localDtr.applicationFinished(applicationId_0);
    waitForEventsToGetProcessed(delegationTokenRenewer);
    //Send another keep alive.
    localDtr.updateKeepAliveApplications(Collections
        .singletonList(applicationId_0));
    //Renewal should not fail.
    token1.renew(lconf);
    //Token should be around after this. 
    Thread.sleep(4500l);
    //Renewal should not fail. - ~1.5 seconds for keepalive timeout.
    token1.renew(lconf);
    //Allow the keepalive time to run out
    Thread.sleep(3000l);
    //The token should have been cancelled at this point. Renewal will fail.
    try {
      token1.renew(lconf);
      fail("Renewal of cancelled token should have failed");
    } catch (InvalidToken ite) {}
  }

  private DelegationTokenRenewer createNewDelegationTokenRenewer(
      Configuration conf, final AtomicInteger counter) {
    DelegationTokenRenewer renew =  new DelegationTokenRenewer() {

      @Override
      protected ThreadPoolExecutor
          createNewThreadPoolService(Configuration conf) {
        ThreadPoolExecutor pool =
            new ThreadPoolExecutor(5, 5, 3L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>()) {

              @Override
              protected void afterExecute(Runnable r, Throwable t) {
                counter.decrementAndGet();
                super.afterExecute(r, t);
              }

              @Override
              public void execute(Runnable command) {
                counter.incrementAndGet();
                super.execute(command);
              }
            };
        return pool;
      }
    };
    renew.setRMContext(TestUtils.getMockRMContext());
    return renew;
  }

  private void waitForEventsToGetProcessed(DelegationTokenRenewer dtr)
      throws InterruptedException {
    int wait = 40;
    while (wait-- > 0
        && counter.get() > 0) {
      Thread.sleep(200);
    }
  }

  @Test(timeout=20000)
  public void testDTRonAppSubmission()
      throws IOException, InterruptedException, BrokenBarrierException {
    final Credentials credsx = new Credentials();
    final Token<DelegationTokenIdentifier> tokenx = mock(Token.class);
    when(tokenx.getKind()).thenReturn(KIND);
    DelegationTokenIdentifier dtId1 = 
        new DelegationTokenIdentifier(new Text("user1"), new Text("renewer"),
          new Text("user1"));
    when(tokenx.decodeIdentifier()).thenReturn(dtId1);
    credsx.addToken(new Text("token"), tokenx);
    doReturn(true).when(tokenx).isManaged();
    doThrow(new IOException("boom"))
        .when(tokenx).renew(any(Configuration.class));
      // fire up the renewer
    final DelegationTokenRenewer dtr =
         createNewDelegationTokenRenewer(conf, counter);
    RMContext mockContext = mock(RMContext.class);
    when(mockContext.getSystemCredentialsForApps()).thenReturn(
        new ConcurrentHashMap<ApplicationId, SystemCredentialsForAppsProto>());
    ClientRMService mockClientRMService = mock(ClientRMService.class);
    when(mockContext.getClientRMService()).thenReturn(mockClientRMService);
    InetSocketAddress sockAddr =
        InetSocketAddress.createUnresolved("localhost", 1234);
    when(mockClientRMService.getBindAddress()).thenReturn(sockAddr);
    dtr.setRMContext(mockContext);
    when(mockContext.getDelegationTokenRenewer()).thenReturn(dtr);
    dtr.init(conf);
    dtr.start();

    try {
      dtr.addApplicationSync(mock(ApplicationId.class), credsx, false, "user");
      fail("Catch IOException on app submission");
    } catch (IOException e){
      Assert.assertTrue(e.getMessage().contains(tokenx.toString()));
      Assert.assertTrue(e.getCause().toString().contains("boom"));
    }

  }

  @Test(timeout=20000)                                                         
  public void testConcurrentAddApplication()                                  
      throws IOException, InterruptedException, BrokenBarrierException {       
    final CyclicBarrier startBarrier = new CyclicBarrier(2);                   
    final CyclicBarrier endBarrier = new CyclicBarrier(2);                     
                                                                               
    // this token uses barriers to block during renew                          
    final Credentials creds1 = new Credentials();                              
    final Token<DelegationTokenIdentifier> token1 = mock(Token.class);    
    when(token1.getKind()).thenReturn(KIND);
    DelegationTokenIdentifier dtId1 = 
        new DelegationTokenIdentifier(new Text("user1"), new Text("renewer"),
          new Text("user1"));
    when(token1.decodeIdentifier()).thenReturn(dtId1);
    creds1.addToken(new Text("token"), token1);                                
    doReturn(true).when(token1).isManaged();                                   
    doAnswer(new Answer<Long>() {                                              
      public Long answer(InvocationOnMock invocation)                          
          throws InterruptedException, BrokenBarrierException { 
        startBarrier.await();                                                  
        endBarrier.await();                                                    
        return Long.MAX_VALUE;                                                 
      }}).when(token1).renew(any(Configuration.class));                        
                                                                               
    // this dummy token fakes renewing                                         
    final Credentials creds2 = new Credentials();                              
    final Token<DelegationTokenIdentifier> token2 = mock(Token.class);           
    when(token2.getKind()).thenReturn(KIND);
    when(token2.decodeIdentifier()).thenReturn(dtId1);
    creds2.addToken(new Text("token"), token2);                                
    doReturn(true).when(token2).isManaged();                                   
    doReturn(Long.MAX_VALUE).when(token2).renew(any(Configuration.class));     
                                                                               
    // fire up the renewer                                                     
    final DelegationTokenRenewer dtr =
        createNewDelegationTokenRenewer(conf, counter);           
    RMContext mockContext = mock(RMContext.class);
    when(mockContext.getSystemCredentialsForApps()).thenReturn(
        new ConcurrentHashMap<ApplicationId, SystemCredentialsForAppsProto>());
    ClientRMService mockClientRMService = mock(ClientRMService.class);         
    when(mockContext.getClientRMService()).thenReturn(mockClientRMService);    
    InetSocketAddress sockAddr =                                               
        InetSocketAddress.createUnresolved("localhost", 1234);                 
    when(mockClientRMService.getBindAddress()).thenReturn(sockAddr);           
    dtr.setRMContext(mockContext);  
    when(mockContext.getDelegationTokenRenewer()).thenReturn(dtr);
    dtr.init(conf);
    dtr.start();                                                                           
    // submit a job that blocks during renewal                                 
    Thread submitThread = new Thread() {                                       
      @Override                                                                
      public void run() {
        dtr.addApplicationAsync(mock(ApplicationId.class), creds1, false, "user",
            new Configuration());
      }                                                                        
    };                                                                         
    submitThread.start();                                                      
                                                                               
    // wait till 1st submit blocks, then submit another
    startBarrier.await();                           
    dtr.addApplicationAsync(mock(ApplicationId.class), creds2, false, "user",
        new Configuration());
    // signal 1st to complete                                                  
    endBarrier.await();                                                        
    submitThread.join(); 
  }
  
  @Test(timeout=20000)
  public void testAppSubmissionWithInvalidDelegationToken() throws Exception {
    Configuration conf = new Configuration();
    conf.set(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    UserGroupInformation.setConfiguration(conf);
    MockRM rm = new MockRM(conf) {
      @Override
      protected void doSecureLogin() throws IOException {
        // Skip the login.
      }
    };
    ByteBuffer tokens = ByteBuffer.wrap("BOGUS".getBytes()); 
    ContainerLaunchContext amContainer =
        ContainerLaunchContext.newInstance(
            new HashMap<String, LocalResource>(), new HashMap<String, String>(),
            new ArrayList<String>(), new HashMap<String, ByteBuffer>(), tokens,
            new HashMap<ApplicationAccessType, String>());
    ApplicationSubmissionContext appSubContext =
        ApplicationSubmissionContext.newInstance(
            ApplicationId.newInstance(1234121, 0),
            "BOGUS", "default", Priority.UNDEFINED, amContainer, false,
            true, 1, Resource.newInstance(1024, 1), "BOGUS");
    SubmitApplicationRequest request =
        SubmitApplicationRequest.newInstance(appSubContext);
    try {
      rm.getClientRMService().submitApplication(request);
      fail("Error was excepted.");
    } catch (YarnException e) {
      Assert.assertTrue(e.getMessage().contains(
          "Bad header found in token storage"));
    }
  }


  @Test(timeout = 30000)
  public void testReplaceExpiringDelegationToken() throws Exception {
    conf.setBoolean(YarnConfiguration.RM_PROXY_USER_PRIVILEGES_ENABLED, true);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
      "kerberos");
    UserGroupInformation.setConfiguration(conf);

    // create Token1:
    Text userText1 = new Text("user1");
    DelegationTokenIdentifier dtId1 =
        new DelegationTokenIdentifier(userText1, new Text("renewer1"),
          userText1);
    // set max date to 0 to simulate an expiring token;
    dtId1.setMaxDate(0);
    final Token<DelegationTokenIdentifier> token1 =
        new Token<DelegationTokenIdentifier>(dtId1.getBytes(),
          "password1".getBytes(), dtId1.getKind(), new Text("service1"));

    // create token2
    Text userText2 = new Text("user2");
    DelegationTokenIdentifier dtId2 =
        new DelegationTokenIdentifier(userText1, new Text("renewer2"),
          userText2);
    final Token<DelegationTokenIdentifier> expectedToken =
        new Token<DelegationTokenIdentifier>(dtId2.getBytes(),
          "password2".getBytes(), dtId2.getKind(), new Text("service2"));

    final MockRM rm = new TestSecurityMockRM(conf, null) {
      @Override
      protected DelegationTokenRenewer createDelegationTokenRenewer() {
        return new DelegationTokenRenewer() {
          @Override
          protected Token<?>[] obtainSystemTokensForUser(String user,
              final Credentials credentials) throws IOException {
            credentials.addToken(expectedToken.getService(), expectedToken);
            return new Token<?>[] { expectedToken };
          }
        };
      }
    };
    rm.start();
    Credentials credentials = new Credentials();
    credentials.addToken(userText1, token1);

    RMApp app =
        rm.submitApp(200, "name", "user",
          new HashMap<ApplicationAccessType, String>(), false, "default", 1,
          credentials);

    // wait for the initial expiring hdfs token to be removed from allTokens
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      public Boolean get() {
        return
            rm.getRMContext().getDelegationTokenRenewer().getAllTokens()
            .get(token1) == null;
      }
    }, 1000, 20000);

    // wait for the initial expiring hdfs token to be removed from appTokens
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      public Boolean get() {
        return !rm.getRMContext().getDelegationTokenRenewer()
          .getDelegationTokens().contains(token1);
      }
    }, 1000, 20000);

    // wait for the new retrieved hdfs token.
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      public Boolean get() {
        return rm.getRMContext().getDelegationTokenRenewer()
          .getDelegationTokens().contains(expectedToken);
      }
    }, 1000, 20000);

    // check nm can retrieve the token
    final MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm.getResourceTrackerService());
    nm1.registerNode();
    NodeHeartbeatResponse response = nm1.nodeHeartbeat(true);

    NodeHeartbeatResponse proto = new NodeHeartbeatResponsePBImpl(
        ((NodeHeartbeatResponsePBImpl) response).getProto());

    ByteBuffer tokenBuffer =
        YarnServerBuilderUtils
            .convertFromProtoFormat(proto.getSystemCredentialsForApps())
            .get(app.getApplicationId());
    Assert.assertNotNull(tokenBuffer);
    Credentials appCredentials = new Credentials();
    DataInputByteBuffer buf = new DataInputByteBuffer();
    tokenBuffer.rewind();
    buf.reset(tokenBuffer);
    appCredentials.readTokenStorageStream(buf);
    Assert.assertTrue(appCredentials.getAllTokens().contains(expectedToken));
  }


  // 1. token is expired before app completes.
  // 2. RM shutdown.
  // 3. When RM recovers the app, token renewal will fail as token expired.
  //    RM should request a new token and sent it to NM for log-aggregation.
  @Test
  public void testRMRestartWithExpiredToken() throws Exception {
    Configuration yarnConf = new YarnConfiguration();
    yarnConf
        .setBoolean(YarnConfiguration.RM_PROXY_USER_PRIVILEGES_ENABLED, true);
    yarnConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    yarnConf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    yarnConf
        .set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    UserGroupInformation.setConfiguration(yarnConf);

    // create Token1:
    Text userText1 = new Text("user1");
    DelegationTokenIdentifier dtId1 = new DelegationTokenIdentifier(userText1,
        new Text("renewer1"), userText1);
    final Token<DelegationTokenIdentifier> originalToken =
        new Token<>(dtId1.getBytes(), "password1".getBytes(), dtId1.getKind(),
            new Text("service1"));
    Credentials credentials = new Credentials();
    credentials.addToken(userText1, originalToken);

    MockRM rm1 = new TestSecurityMockRM(yarnConf);
    MemoryRMStateStore memStore = (MemoryRMStateStore) rm1.getRMStateStore();
    rm1.start();
    RMApp app = rm1.submitApp(200, "name", "user",
        new HashMap<ApplicationAccessType, String>(), false, "default", 1,
        credentials);

    // create token2
    Text userText2 = new Text("user1");
    DelegationTokenIdentifier dtId2 =
        new DelegationTokenIdentifier(userText1, new Text("renewer2"),
            userText2);
    final Token<DelegationTokenIdentifier> updatedToken =
        new Token<DelegationTokenIdentifier>(dtId2.getBytes(),
            "password2".getBytes(), dtId2.getKind(), new Text("service2"));
    AtomicBoolean firstRenewInvoked = new AtomicBoolean(false);
    AtomicBoolean secondRenewInvoked = new AtomicBoolean(false);
    MockRM rm2 = new TestSecurityMockRM(yarnConf, memStore) {
      @Override
      protected DelegationTokenRenewer createDelegationTokenRenewer() {
        return new DelegationTokenRenewer() {

          @Override
          protected void renewToken(final DelegationTokenToRenew dttr)
              throws IOException {

            if (dttr.token.equals(updatedToken)) {
              secondRenewInvoked.set(true);
              super.renewToken(dttr);
            } else if (dttr.token.equals(originalToken)){
              firstRenewInvoked.set(true);
              throw new InvalidToken("Failed to renew");
            } else {
              throw new IOException("Unexpected");
            }
          }

          @Override
          protected Token<?>[] obtainSystemTokensForUser(String user,
              final Credentials credentials) throws IOException {
            credentials.addToken(updatedToken.getService(), updatedToken);
            return new Token<?>[] { updatedToken };
          }
        };
      }
    };

    // simulating restart the rm
    rm2.start();

    // check nm can retrieve the token
    final MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm2.getResourceTrackerService());
    nm1.registerNode();
    NodeHeartbeatResponse response = nm1.nodeHeartbeat(true);

    NodeHeartbeatResponse proto = new NodeHeartbeatResponsePBImpl(
        ((NodeHeartbeatResponsePBImpl) response).getProto());

    ByteBuffer tokenBuffer =
        YarnServerBuilderUtils
            .convertFromProtoFormat(proto.getSystemCredentialsForApps())
            .get(app.getApplicationId());
    Assert.assertNotNull(tokenBuffer);
    Credentials appCredentials = new Credentials();
    DataInputByteBuffer buf = new DataInputByteBuffer();
    tokenBuffer.rewind();
    buf.reset(tokenBuffer);
    appCredentials.readTokenStorageStream(buf);
    Assert.assertTrue(firstRenewInvoked.get() && secondRenewInvoked.get());
    Assert.assertTrue(appCredentials.getAllTokens().contains(updatedToken));
  }

  // YARN will get the token for the app submitted without the delegation token.
  @Test
  public void testAppSubmissionWithoutDelegationToken() throws Exception {
    conf.setBoolean(YarnConfiguration.RM_PROXY_USER_PRIVILEGES_ENABLED, true);
    // create token2
    Text userText2 = new Text("user2");
    DelegationTokenIdentifier dtId2 =
        new DelegationTokenIdentifier(new Text("user2"), new Text("renewer2"),
          userText2);
    final Token<DelegationTokenIdentifier> token2 =
        new Token<DelegationTokenIdentifier>(dtId2.getBytes(),
          "password2".getBytes(), dtId2.getKind(), new Text("service2"));
    final MockRM rm = new TestSecurityMockRM(conf, null) {
      @Override
      protected DelegationTokenRenewer createDelegationTokenRenewer() {
        return new DelegationTokenRenewer() {
          @Override
          protected Token<?>[] obtainSystemTokensForUser(String user,
              final Credentials credentials) throws IOException {
            credentials.addToken(token2.getService(), token2);
            return new Token<?>[] { token2 };
          }
        };
      }
    };
    rm.start();

    // submit an app without delegationToken
    RMApp app = rm.submitApp(200);

    // wait for the new retrieved hdfs token.
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      public Boolean get() {
        return rm.getRMContext().getDelegationTokenRenewer()
          .getDelegationTokens().contains(token2);
      }
    }, 1000, 20000);

    // check nm can retrieve the token
    final MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm.getResourceTrackerService());
    nm1.registerNode();
    NodeHeartbeatResponse response = nm1.nodeHeartbeat(true);

    NodeHeartbeatResponse proto = new NodeHeartbeatResponsePBImpl(
        ((NodeHeartbeatResponsePBImpl) response).getProto());

    ByteBuffer tokenBuffer =
        YarnServerBuilderUtils
            .convertFromProtoFormat(proto.getSystemCredentialsForApps())
            .get(app.getApplicationId());
    Assert.assertNotNull(tokenBuffer);
    Credentials appCredentials = new Credentials();
    DataInputByteBuffer buf = new DataInputByteBuffer();
    tokenBuffer.rewind();
    buf.reset(tokenBuffer);
    appCredentials.readTokenStorageStream(buf);
    Assert.assertTrue(appCredentials.getAllTokens().contains(token2));
  }

  // Test submitting an application with the token obtained by a previously
  // submitted application.
  @Test (timeout = 30000)
  public void testAppSubmissionWithPreviousToken() throws Exception{
    MockRM rm = new TestSecurityMockRM(conf, null);
    rm.start();
    final MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm.getResourceTrackerService());
    nm1.registerNode();

    // create Token1:
    Text userText1 = new Text("user");
    DelegationTokenIdentifier dtId1 =
        new DelegationTokenIdentifier(userText1, new Text("renewer1"),
          userText1);
    final Token<DelegationTokenIdentifier> token1 =
        new Token<DelegationTokenIdentifier>(dtId1.getBytes(),
          "password1".getBytes(), dtId1.getKind(), new Text("service1"));

    Credentials credentials = new Credentials();
    credentials.addToken(userText1, token1);

    // submit app1 with a token, set cancelTokenWhenComplete to false;
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemorySize(200);
    RMApp app1 = rm.submitApp(resource, "name", "user", null, false, null, 2,
        credentials, null, true, false, false, null, 0, null, false, null);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);
    rm.waitForState(app1.getApplicationId(), RMAppState.RUNNING);
    DelegationTokenRenewer renewer =
        rm.getRMContext().getDelegationTokenRenewer();
    DelegationTokenToRenew dttr = renewer.getAllTokens().get(token1);
    Assert.assertNotNull(dttr);

    // submit app2 with the same token, set cancelTokenWhenComplete to true;
    RMApp app2 = rm.submitApp(resource, "name", "user", null, false, null, 2,
        credentials, null, true, false, false, null, 0, null, true, null);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm1);
    rm.waitForState(app2.getApplicationId(), RMAppState.RUNNING);
    finishAMAndWaitForComplete(app2, rm, nm1, am2, dttr);
    Assert.assertTrue(rm.getRMContext().getDelegationTokenRenewer()
      .getAllTokens().containsKey(token1));

    finishAMAndWaitForComplete(app1, rm, nm1, am1, dttr);
    // app2 completes, app1 is still running, check the token is not cancelled
    Assert.assertFalse(Renewer.cancelled);
  }

  // Test FileSystem memory leak in obtainSystemTokensForUser.
  @Test
  public void testFSLeakInObtainSystemTokensForUser() throws Exception{
    Credentials credentials = new Credentials();
    String user = "test";
    int oldCounter = MyFS.getInstanceCounter();
    delegationTokenRenewer.obtainSystemTokensForUser(user, credentials);
    delegationTokenRenewer.obtainSystemTokensForUser(user, credentials);
    delegationTokenRenewer.obtainSystemTokensForUser(user, credentials);
    Assert.assertEquals(oldCounter, MyFS.getInstanceCounter());
  }
  
  // Test submitting an application with the token obtained by a previously
  // submitted application that is set to be cancelled.  Token should be
  // renewed while all apps are running, and then cancelled when all apps
  // complete
  @Test (timeout = 30000)
  public void testCancelWithMultipleAppSubmissions() throws Exception{
    MockRM rm = new TestSecurityMockRM(conf, null);
    rm.start();
    final MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm.getResourceTrackerService());
    nm1.registerNode();

    // create Token1:
    Text userText1 = new Text("user");
    DelegationTokenIdentifier dtId1 =
        new DelegationTokenIdentifier(userText1, new Text("renewer1"),
          userText1);
    final Token<DelegationTokenIdentifier> token1 =
        new Token<DelegationTokenIdentifier>(dtId1.getBytes(),
          "password1".getBytes(), dtId1.getKind(), new Text("service1"));

    Credentials credentials = new Credentials();
    credentials.addToken(token1.getService(), token1);

    DelegationTokenRenewer renewer =
        rm.getRMContext().getDelegationTokenRenewer();
    Assert.assertTrue(renewer.getAllTokens().isEmpty());
    Assert.assertFalse(Renewer.cancelled);

    Resource resource = Records.newRecord(Resource.class);
    resource.setMemorySize(200);
    RMApp app1 =
        rm.submitApp(resource, "name", "user", null, false, null, 2, credentials,
          null, true, false, false, null, 0, null, true, null);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);
    rm.waitForState(app1.getApplicationId(), RMAppState.RUNNING);

    DelegationTokenToRenew dttr = renewer.getAllTokens().get(token1);
    Assert.assertNotNull(dttr);
    Assert.assertTrue(dttr.referringAppIds.contains(app1.getApplicationId()));
    RMApp app2 = rm.submitApp(resource, "name", "user", null, false, null, 2,
        credentials, null, true, false, false, null, 0, null, true, null);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm1);
    rm.waitForState(app2.getApplicationId(), RMAppState.RUNNING);
    Assert.assertTrue(renewer.getAllTokens().containsKey(token1));
    Assert.assertTrue(dttr.referringAppIds.contains(app2.getApplicationId()));
    Assert.assertTrue(dttr.referringAppIds.contains(app2.getApplicationId()));
    Assert.assertFalse(Renewer.cancelled);

    finishAMAndWaitForComplete(app2, rm, nm1, am2, dttr);
    // app2 completes, app1 is still running, check the token is not cancelled
    Assert.assertTrue(renewer.getAllTokens().containsKey(token1));
    Assert.assertTrue(dttr.referringAppIds.contains(app1.getApplicationId()));
    Assert.assertFalse(dttr.referringAppIds.contains(app2.getApplicationId()));
    Assert.assertFalse(dttr.isTimerCancelled());
    Assert.assertFalse(Renewer.cancelled);

    RMApp app3 = rm.submitApp(resource, "name", "user", null, false, null, 2,
        credentials, null, true, false, false, null, 0, null, true, null);
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm, nm1);
    rm.waitForState(app3.getApplicationId(), RMAppState.RUNNING);
    Assert.assertTrue(renewer.getAllTokens().containsKey(token1));
    Assert.assertTrue(dttr.referringAppIds.contains(app1.getApplicationId()));
    Assert.assertTrue(dttr.referringAppIds.contains(app3.getApplicationId()));
    Assert.assertFalse(dttr.isTimerCancelled());
    Assert.assertFalse(Renewer.cancelled);

    finishAMAndWaitForComplete(app1, rm, nm1, am1, dttr);
    Assert.assertTrue(renewer.getAllTokens().containsKey(token1));
    Assert.assertFalse(dttr.referringAppIds.contains(app1.getApplicationId()));
    Assert.assertTrue(dttr.referringAppIds.contains(app3.getApplicationId()));
    Assert.assertFalse(dttr.isTimerCancelled());
    Assert.assertFalse(Renewer.cancelled);

    finishAMAndWaitForComplete(app3, rm, nm1, am3, dttr);
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return !renewer.getAllTokens().containsKey(token1);
      }
    }, 10, 5000);
    Assert.assertFalse(renewer.getAllTokens().containsKey(token1));
    Assert.assertTrue(dttr.referringAppIds.isEmpty());
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return dttr.isTimerCancelled();
      }
    }, 10, 5000);
    Assert.assertTrue(dttr.isTimerCancelled());
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return Renewer.cancelled;
      }
    }, 10, 5000);
    Assert.assertTrue(Renewer.cancelled);

    // make sure the token also has been removed from appTokens
    Assert.assertFalse(renewer.getDelegationTokens().contains(token1));
  }

  private void finishAMAndWaitForComplete(final RMApp app, MockRM rm,
      MockNM nm, MockAM am, final DelegationTokenToRenew dttr)
          throws Exception {
    MockRM.finishAMAndVerifyAppState(app, rm, nm, am);
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      public Boolean get() {
        return !dttr.referringAppIds.contains(app.getApplicationId());
      }
    }, 10, 10000);
  }

  // Test DelegationTokenRenewer uses the tokenConf provided by application
  // for token renewal.
  @Test
  public void testRenewTokenUsingTokenConfProvidedByApp() throws Exception{
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    UserGroupInformation.setConfiguration(conf);

    MockRM rm = new TestSecurityMockRM(conf, null);
    rm.start();
    final MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm.getResourceTrackerService());
    nm1.registerNode();

    // create a token
    Text userText1 = new Text("user1");
    DelegationTokenIdentifier dtId1 =
        new DelegationTokenIdentifier(userText1, new Text("renewer1"),
            userText1);
    final Token<DelegationTokenIdentifier> token1 =
        new Token<DelegationTokenIdentifier>(dtId1.getBytes(),
            "password1".getBytes(), dtId1.getKind(), new Text("service1"));
    Credentials credentials = new Credentials();
    credentials.addToken(userText1, token1);

    // create token conf for renewal
    Configuration appConf = new Configuration(false);
    appConf.set("dfs.nameservices", "mycluster1,mycluster2");
    appConf.set("dfs.namenode.rpc-address.mycluster2.nn1", "123.0.0.1");
    appConf.set("dfs.namenode.rpc-address.mycluster2.nn2", "123.0.0.2");
    appConf.set("dfs.ha.namenodes.mycluster2", "nn1,nn2");
    appConf.set("dfs.client.failover.proxy.provider.mycluster2", "provider");
    DataOutputBuffer dob = new DataOutputBuffer();
    appConf.write(dob);
    ByteBuffer tokenConf = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    final int confSize = appConf.size();

    // submit app
    RMApp app = rm.submitApp(credentials, tokenConf);

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      public Boolean get() {
        DelegationTokenToRenew toRenew =
            rm.getRMContext().getDelegationTokenRenewer().getAllTokens()
                .get(token1);
        // check app conf size equals to original size and it does contain
        // the specific config we added.
        return toRenew != null && toRenew.conf != null
            && toRenew.conf.size() == confSize && toRenew.conf
            .get("dfs.namenode.rpc-address.mycluster2.nn1").equals("123.0.0.1");
      }
    }, 200, 10000);
  }

  // Test if app's token conf exceeds RM_DELEGATION_TOKEN_MAX_CONF_SIZE,
  // app should fail
  @Test
  public void testTokensConfExceedLimit() throws Exception {
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    UserGroupInformation.setConfiguration(conf);
    // limit 100 bytes
    conf.setInt(YarnConfiguration.RM_DELEGATION_TOKEN_MAX_CONF_SIZE, 100);
    MockRM rm = new TestSecurityMockRM(conf, null);
    rm.start();
    final MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm.getResourceTrackerService());
    nm1.registerNode();

    // create a token
    Text userText1 = new Text("user1");
    DelegationTokenIdentifier dtId1 =
        new DelegationTokenIdentifier(userText1, new Text("renewer1"),
            userText1);
    final Token<DelegationTokenIdentifier> token1 =
        new Token<DelegationTokenIdentifier>(dtId1.getBytes(),
            "password1".getBytes(), dtId1.getKind(), new Text("service1"));
    Credentials credentials = new Credentials();
    credentials.addToken(userText1, token1);

    // create token conf for renewal, total size (512 bytes) > limit (100 bytes)
    // By experiment, it's roughly 128 bytes per key-value pair.
    Configuration appConf = new Configuration(false);
    appConf.clear();
    appConf.set("dfs.nameservices", "mycluster1,mycluster2"); // 128 bytes
    appConf.set("dfs.namenode.rpc-address.mycluster2.nn1", "123.0.0.1"); //128 bytes
    appConf.set("dfs.namenode.rpc-address.mycluster3.nn2", "123.0.0.2"); // 128 bytes

    DataOutputBuffer dob = new DataOutputBuffer();
    appConf.write(dob);
    ByteBuffer tokenConf = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

    try {
      rm.submitApp(credentials, tokenConf);
      Assert.fail();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(e.getCause().getMessage()
          .contains(YarnConfiguration.RM_DELEGATION_TOKEN_MAX_CONF_SIZE));
    }
  }

  // Test if the token renewer returned an invalid expiration time, that token's
  // renewal should be ignored.
  @Test
  public void testTokenRenewerInvalidReturn() throws Exception {
    DelegationTokenToRenew mockDttr = mock(DelegationTokenToRenew.class);
    mockDttr.expirationDate = 0;
    delegationTokenRenewer.setTimerForTokenRenewal(mockDttr);
    assertNull(mockDttr.timerTask);

    mockDttr.expirationDate = -1;
    delegationTokenRenewer.setTimerForTokenRenewal(mockDttr);
    assertNull(mockDttr.timerTask);

    mockDttr.expirationDate = System.currentTimeMillis() - 1;
    delegationTokenRenewer.setTimerForTokenRenewal(mockDttr);
    assertNull(mockDttr.timerTask);
  }

  /**
   * Test that the DelegationTokenRenewer class can gracefully handle
   * interactions that occur when it has been stopped.
   */
  @Test
  public void testShutDown() {
    DelegationTokenRenewer dtr = createNewDelegationTokenRenewer(conf, counter);
    RMContext mockContext = mock(RMContext.class);
    when(mockContext.getSystemCredentialsForApps()).thenReturn(
        new ConcurrentHashMap<ApplicationId, SystemCredentialsForAppsProto>());
    ClientRMService mockClientRMService = mock(ClientRMService.class);
    when(mockContext.getClientRMService()).thenReturn(mockClientRMService);
    InetSocketAddress sockAddr =
        InetSocketAddress.createUnresolved("localhost", 1234);
    when(mockClientRMService.getBindAddress()).thenReturn(sockAddr);
    dtr.setRMContext(mockContext);
    when(mockContext.getDelegationTokenRenewer()).thenReturn(dtr);
    dtr.init(conf);
    dtr.start();
    delegationTokenRenewer.stop();
    delegationTokenRenewer.applicationFinished(
        BuilderUtils.newApplicationId(0, 1));
  }

  @Test(timeout = 10000)
  public void testTokenSequenceNoAfterNewTokenAndRenewal() throws Exception {
    conf.setBoolean(YarnConfiguration.RM_PROXY_USER_PRIVILEGES_ENABLED, true);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    UserGroupInformation.setConfiguration(conf);

    final Credentials credsx = new Credentials();

    DelegationTokenIdentifier dtId1 = new DelegationTokenIdentifier(
        new Text("user1"), new Text("renewer"), new Text("user1"));
    final Token<DelegationTokenIdentifier> expectedToken =
        new Token<DelegationTokenIdentifier>(dtId1.getBytes(),
            "password2".getBytes(), dtId1.getKind(), new Text("service2"));

    // fire up the renewer
    final DelegationTokenRenewer dtr = new DelegationTokenRenewer() {
      @Override
      protected Token<?>[] obtainSystemTokensForUser(String user,
          final Credentials credentials) throws IOException {
        credentials.addToken(expectedToken.getService(), expectedToken);
        return new Token<?>[] {expectedToken};
      }
    };

    RMContext mockContext = mock(RMContext.class);
    when(mockContext.getSystemCredentialsForApps()).thenReturn(
        new ConcurrentHashMap<ApplicationId, SystemCredentialsForAppsProto>());
    ClientRMService mockClientRMService = mock(ClientRMService.class);
    when(mockContext.getClientRMService()).thenReturn(mockClientRMService);
    InetSocketAddress sockAddr =
        InetSocketAddress.createUnresolved("localhost", 1234);
    when(mockClientRMService.getBindAddress()).thenReturn(sockAddr);
    dtr.setRMContext(mockContext);
    when(mockContext.getDelegationTokenRenewer()).thenReturn(dtr);
    dtr.init(conf);
    dtr.start();

    final ApplicationId appId1 = ApplicationId.newInstance(1234, 1);

    Collection<ApplicationId> appIds = new ArrayList<ApplicationId>(1);
    appIds.add(appId1);

    dtr.addApplicationSync(appId1, credsx, false, "user1");

    // Ensure incrTokenSequenceNo has been called for new token request
    Mockito.verify(mockContext, Mockito.times(1)).incrTokenSequenceNo();

    DelegationTokenToRenew dttr = new DelegationTokenToRenew(appIds,
        expectedToken, conf, 1000, false, "user1");

    dtr.requestNewHdfsDelegationTokenIfNeeded(dttr);

    // Ensure incrTokenSequenceNo has been called for token renewal as well.
    Mockito.verify(mockContext, Mockito.times(2)).incrTokenSequenceNo();
  }
}
