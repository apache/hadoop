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
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.security.token.delegation.DelegationKey;
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
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * unit test - 
 * tests addition/deletion/cancellation of renewals of delegation tokens
 *
 */
@SuppressWarnings("rawtypes")
public class TestDelegationTokenRenewer {
  private static final Log LOG = 
      LogFactory.getLog(TestDelegationTokenRenewer.class);
  private static final Text KIND = new Text("TestDelegationTokenRenewer.Token");
  
  private static BlockingQueue<Event> eventQueue;
  private static volatile AtomicInteger counter;
  private static AsyncDispatcher dispatcher;
  public static class Renewer extends TokenRenewer {
    private static int counter = 0;
    private static Token<?> lastRenewed = null;
    private static Token<?> tokenToRenewIn2Sec = null;

    private static void reset() {
      counter = 0;
      lastRenewed = null;
      tokenToRenewIn2Sec = null;
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
      MyToken token = (MyToken)t;
      LOG.info("Cancel token " + token);
      token.cancelToken();
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
    UserGroupInformation.setConfiguration(conf);
    eventQueue = new LinkedBlockingQueue<Event>();
    dispatcher = new AsyncDispatcher(eventQueue);
    Renewer.reset();
    delegationTokenRenewer = createNewDelegationTokenRenewer(conf, counter);
    RMContext mockContext = mock(RMContext.class);
    ClientRMService mockClientRMService = mock(ClientRMService.class);
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
    
    public MyFS() {}
    public void close() {}
    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {}
    
    @Override 
    public MyToken getDelegationToken(String renewer) throws IOException {
      MyToken result = createTokens(new Text(renewer));
      LOG.info("Called MYDFS.getdelegationtoken " + result);
      return result;
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
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT,
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT,
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
    delegationTokenRenewer.addApplicationAsync(applicationId_0, ts, true);
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
    delegationTokenRenewer.addApplicationAsync(applicationId_1, ts, true);
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
    delegationTokenRenewer.addApplicationAsync(appId, ts, true);
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
    delegationTokenRenewer.addApplicationAsync(applicationId_1, ts, false);
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
    localDtr.addApplicationAsync(applicationId_0, ts, true);
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
    localDtr.addApplicationAsync(applicationId_0, ts, true);
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
    return new DelegationTokenRenewer() {

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
    final Token<?> tokenx = mock(Token.class);
    credsx.addToken(new Text("token"), tokenx);
    doReturn(true).when(tokenx).isManaged();
    doThrow(new IOException("boom"))
        .when(tokenx).renew(any(Configuration.class));
      // fire up the renewer
    final DelegationTokenRenewer dtr =
         createNewDelegationTokenRenewer(conf, counter);
    RMContext mockContext = mock(RMContext.class);
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
      dtr.addApplicationSync(mock(ApplicationId.class), credsx, false);
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
    final Token<?> token1 = mock(Token.class);                                 
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
    final Token<?> token2 = mock(Token.class);                                 
    creds2.addToken(new Text("token"), token2);                                
    doReturn(true).when(token2).isManaged();                                   
    doReturn(Long.MAX_VALUE).when(token2).renew(any(Configuration.class));     
                                                                               
    // fire up the renewer                                                     
    final DelegationTokenRenewer dtr =
        createNewDelegationTokenRenewer(conf, counter);           
    RMContext mockContext = mock(RMContext.class);                             
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
        dtr.addApplicationAsync(mock(ApplicationId.class), creds1, false);
      }                                                                        
    };                                                                         
    submitThread.start();                                                      
                                                                               
    // wait till 1st submit blocks, then submit another
    startBarrier.await();                           
    dtr.addApplicationAsync(mock(ApplicationId.class), creds2, false);
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
    MockRM rm = new MockRM(conf);
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
}
