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
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * unit test - 
 * tests addition/deletion/cancelation of renewals of delegation tokens
 *
 */
public class TestDelegationTokenRenewer {
  private static final Log LOG = 
      LogFactory.getLog(TestDelegationTokenRenewer.class);
  private static final Text KIND = new Text("TestDelegationTokenRenewer.Token");
  
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
    Renewer.reset();
    delegationTokenRenewer = new DelegationTokenRenewer();
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
    public MyToken getDelegationToken(Text renewer) throws IOException {
      MyToken result = createTokens(renewer);
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
  @Test
  public void testDTRenewal () throws Exception {
    MyFS dfs = (MyFS)FileSystem.get(conf);
    LOG.info("dfs="+(Object)dfs.hashCode() + ";conf="+conf.hashCode());
    // Test 1. - add three tokens - make sure exactly one get's renewed
    
    // get the delegation tokens
    MyToken token1, token2, token3;
    token1 = dfs.getDelegationToken(new Text("user1"));
    token2 = dfs.getDelegationToken(new Text("user2"));
    token3 = dfs.getDelegationToken(new Text("user3"));

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
    delegationTokenRenewer.addApplication(applicationId_0, ts, true);
    
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
    MyToken token4 = dfs.getDelegationToken(new Text("user4"));
    
    //to cause this one to be set for renew in 2 secs
    Renewer.tokenToRenewIn2Sec = token4; 
    LOG.info("token="+token4+" should be renewed for 2 secs");
    
    String nn4 = DelegationTokenRenewer.SCHEME + "://host4:0";
    ts.addToken(new Text(nn4), token4);
    

    ApplicationId applicationId_1 = BuilderUtils.newApplicationId(0, 1);
    delegationTokenRenewer.addApplication(applicationId_1, ts, true);
    delegationTokenRenewer.applicationFinished(applicationId_1);
    
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
  
  @Test
  public void testInvalidDTWithAddApplication() throws Exception {
    MyFS dfs = (MyFS)FileSystem.get(conf);
    LOG.info("dfs="+(Object)dfs.hashCode() + ";conf="+conf.hashCode());

    MyToken token = dfs.getDelegationToken(new Text("user1"));
    token.cancelToken();

    Credentials ts = new Credentials();
    ts.addToken(token.getKind(), token);
    
    // register the tokens for renewal
    ApplicationId appId =  BuilderUtils.newApplicationId(0, 0);
    try {
      delegationTokenRenewer.addApplication(appId, ts, true);
      fail("App submission with a cancelled token should have failed");
    } catch (InvalidToken e) {
      // expected
    }
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
  @Test
  public void testDTRenewalWithNoCancel () throws Exception {
    MyFS dfs = (MyFS)FileSystem.get(conf);
    LOG.info("dfs="+(Object)dfs.hashCode() + ";conf="+conf.hashCode());

    Credentials ts = new Credentials();
    MyToken token1 = dfs.getDelegationToken(new Text("user1"));

    //to cause this one to be set for renew in 2 secs
    Renewer.tokenToRenewIn2Sec = token1; 
    LOG.info("token="+token1+" should be renewed for 2 secs");
    
    String nn1 = DelegationTokenRenewer.SCHEME + "://host1:0";
    ts.addToken(new Text(nn1), token1);
    

    ApplicationId applicationId_1 = BuilderUtils.newApplicationId(0, 1);
    delegationTokenRenewer.addApplication(applicationId_1, ts, false);
    delegationTokenRenewer.applicationFinished(applicationId_1);
    
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
  @Test
  public void testDTKeepAlive1 () throws Exception {
    DelegationTokenRenewer localDtr = new DelegationTokenRenewer();
    Configuration lconf = new Configuration(conf);
    lconf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
    //Keep tokens alive for 6 seconds.
    lconf.setLong(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS, 6000l);
    //Try removing tokens every second.
    lconf.setLong(
        YarnConfiguration.RM_DELAYED_DELEGATION_TOKEN_REMOVAL_INTERVAL_MS,
        1000l);
    localDtr.init(lconf);
    localDtr.start();
    
    MyFS dfs = (MyFS)FileSystem.get(lconf);
    LOG.info("dfs="+(Object)dfs.hashCode() + ";conf="+lconf.hashCode());
    
    Credentials ts = new Credentials();
    // get the delegation tokens
    MyToken token1 = dfs.getDelegationToken(new Text("user1"));

    String nn1 = DelegationTokenRenewer.SCHEME + "://host1:0";
    ts.addToken(new Text(nn1), token1);

    // register the tokens for renewal
    ApplicationId applicationId_0 =  BuilderUtils.newApplicationId(0, 0);
    localDtr.addApplication(applicationId_0, ts, true);
    localDtr.applicationFinished(applicationId_0);
 
    Thread.sleep(3000l);

    //Token should still be around. Renewal should not fail.
    token1.renew(lconf);

    //Allow the keepalive time to run out
    Thread.sleep(6000l);

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
  @Test
  public void testDTKeepAlive2() throws Exception {
    DelegationTokenRenewer localDtr = new DelegationTokenRenewer();
    Configuration lconf = new Configuration(conf);
    lconf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
    //Keep tokens alive for 6 seconds.
    lconf.setLong(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS, 6000l);
    //Try removing tokens every second.
    lconf.setLong(
        YarnConfiguration.RM_DELAYED_DELEGATION_TOKEN_REMOVAL_INTERVAL_MS,
        1000l);
    localDtr.init(lconf);
    localDtr.start();
    
    MyFS dfs = (MyFS)FileSystem.get(lconf);
    LOG.info("dfs="+(Object)dfs.hashCode() + ";conf="+lconf.hashCode());

    Credentials ts = new Credentials();
    // get the delegation tokens
    MyToken token1 = dfs.getDelegationToken(new Text("user1"));
    
    String nn1 = DelegationTokenRenewer.SCHEME + "://host1:0";
    ts.addToken(new Text(nn1), token1);

    // register the tokens for renewal
    ApplicationId applicationId_0 =  BuilderUtils.newApplicationId(0, 0);
    localDtr.addApplication(applicationId_0, ts, true);
    localDtr.applicationFinished(applicationId_0);

    Thread.sleep(4000l);

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
  
  @Test(timeout=2000)
  public void testConncurrentAddApplication()
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
    final DelegationTokenRenewer dtr = new DelegationTokenRenewer();
    dtr.init(conf);
    dtr.start();
    
    // submit a job that blocks during renewal
    Thread submitThread = new Thread() {
      @Override
      public void run() {
        try {
          dtr.addApplication(mock(ApplicationId.class), creds1, false);
        } catch (IOException e) {}        
      }
    };
    submitThread.start();
    
    // wait till 1st submit blocks, then submit another
    startBarrier.await();
    dtr.addApplication(mock(ApplicationId.class), creds2, false);
    // signal 1st to complete
    endBarrier.await();
    submitThread.join();
  }
}
