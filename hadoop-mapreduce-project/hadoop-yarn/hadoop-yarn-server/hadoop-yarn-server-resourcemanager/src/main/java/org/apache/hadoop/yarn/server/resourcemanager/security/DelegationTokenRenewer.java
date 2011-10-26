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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.service.AbstractService;

/**
 * Service to renew application delegation tokens.
 */
@Private
@Unstable
public class DelegationTokenRenewer extends AbstractService {
  
  private static final Log LOG = 
      LogFactory.getLog(DelegationTokenRenewer.class);
  
  public static final String SCHEME = "hdfs";

  // global single timer (daemon)
  private Timer renewalTimer;
  
  // delegation token canceler thread
  private DelegationTokenCancelThread dtCancelThread =
    new DelegationTokenCancelThread();

  // managing the list of tokens using Map
  // appId=>List<tokens>
  private Set<DelegationTokenToRenew> delegationTokens = 
    Collections.synchronizedSet(new HashSet<DelegationTokenToRenew>());

  public DelegationTokenRenewer() {
    super(DelegationTokenRenewer.class.getName());
  }

  @Override
  public synchronized void init(Configuration conf) {
    super.init(conf);
  }

  @Override
  public synchronized void start() {
    super.start();
    
    dtCancelThread.start();
    renewalTimer = new Timer(true);
  }

  @Override
  public synchronized void stop() {
    renewalTimer.cancel();
    delegationTokens.clear();

    dtCancelThread.interrupt();
    try {
      dtCancelThread.join(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    
    super.stop();
  }

  /**
   * class that is used for keeping tracks of DT to renew
   *
   */
  private static class DelegationTokenToRenew {
    public final Token<?> token;
    public final ApplicationId applicationId;
    public final Configuration conf;
    public long expirationDate;
    public TimerTask timerTask;
    
    public DelegationTokenToRenew(
        ApplicationId jId, Token<?> token, 
        Configuration conf, long expirationDate) {
      this.token = token;
      this.applicationId = jId;
      this.conf = conf;
      this.expirationDate = expirationDate;
      this.timerTask = null;
      if (this.token==null || this.applicationId==null || this.conf==null) {
        throw new IllegalArgumentException("Invalid params to renew token" +
            ";token=" + this.token +
            ";appId=" + this.applicationId +
            ";conf=" + this.conf);
      }
    }
    
    public void setTimerTask(TimerTask tTask) {
      timerTask = tTask;
    }
    
    @Override
    public String toString() {
      return token + ";exp=" + expirationDate;
    }
    
    @Override
    public boolean equals(Object obj) {
      return obj instanceof DelegationTokenToRenew &&
        token.equals(((DelegationTokenToRenew)obj).token);
    }
    
    @Override
    public int hashCode() {
      return token.hashCode();
    }
  }
  
  
  private static class DelegationTokenCancelThread extends Thread {
    private static class TokenWithConf {
      Token<?> token;
      Configuration conf;
      TokenWithConf(Token<?> token, Configuration conf) {
        this.token = token;
        this.conf = conf;
      }
    }
    private LinkedBlockingQueue<TokenWithConf> queue =  
      new LinkedBlockingQueue<TokenWithConf>();
     
    public DelegationTokenCancelThread() {
      super("Delegation Token Canceler");
      setDaemon(true);
    }
    public void cancelToken(Token<?> token,  
        Configuration conf) {
      TokenWithConf tokenWithConf = new TokenWithConf(token, conf);
      while (!queue.offer(tokenWithConf)) {
        LOG.warn("Unable to add token " + token + " for cancellation. " +
        		 "Will retry..");
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    public void run() {
      TokenWithConf tokenWithConf = null;
      while (true) {
        try {
          tokenWithConf = queue.take();
          final TokenWithConf current = tokenWithConf;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Canceling token " + tokenWithConf.token.getService());
          }
          // need to use doAs so that http can find the kerberos tgt
          UserGroupInformation.getLoginUser()
            .doAs(new PrivilegedExceptionAction<Void>(){

              @Override
              public Void run() throws Exception {
                current.token.cancel(current.conf);
                return null;
              }
            });
        } catch (IOException e) {
          LOG.warn("Failed to cancel token " + tokenWithConf.token + " " +  
              StringUtils.stringifyException(e));
        } catch (InterruptedException ie) {
          return;
        } catch (Throwable t) {
          LOG.warn("Got exception " + StringUtils.stringifyException(t) + 
                   ". Exiting..");
          System.exit(-1);
        }
      }
    }
  }
  //adding token
  private void addTokenToList(DelegationTokenToRenew t) {
    delegationTokens.add(t);
  }
  
  /**
   * Add application tokens for renewal.
   * @param applicationId added application
   * @param ts tokens
   * @throws IOException
   */
  public synchronized void addApplication(
      ApplicationId applicationId, Credentials ts) 
  throws IOException {
    if (ts == null) {
      return; //nothing to add
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Registering tokens for renewal for:" + 
          " appId = " + applicationId);
    }
    
    Collection <Token<?>> tokens = ts.getAllTokens();
    long now = System.currentTimeMillis();
    
    for(Token<?> token : tokens) {
      // first renew happens immediately
      if (token.isManaged()) {
        DelegationTokenToRenew dtr = 
          new DelegationTokenToRenew(applicationId, token, getConfig(), now); 

        addTokenToList(dtr);
      
        setTimerForTokenRenewal(dtr, true);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Registering token for renewal for:" +
              " service = " + token.getService() + 
              " for appId = " + applicationId);
        }
      }
    }
  }
  
  /**
   * Task - to renew a token
   *
   */
  private class RenewalTimerTask extends TimerTask {
    private DelegationTokenToRenew dttr;
    
    RenewalTimerTask(DelegationTokenToRenew t) {  
      dttr = t;  
    }
    
    @Override
    public void run() {
      Token<?> token = dttr.token;
      try {
        // need to use doAs so that http can find the kerberos tgt
        dttr.expirationDate = UserGroupInformation.getLoginUser()
          .doAs(new PrivilegedExceptionAction<Long>(){

          @Override
          public Long run() throws Exception {
            return dttr.token.renew(dttr.conf);
          }
        });

        if (LOG.isDebugEnabled()) {
          LOG.debug("Renewing delegation-token for:" + token.getService() + 
              "; new expiration;" + dttr.expirationDate);
        }
        
        setTimerForTokenRenewal(dttr, false);// set the next one
      } catch (Exception e) {
        LOG.error("Exception renewing token" + token + ". Not rescheduled", e);
        removeFailedDelegationToken(dttr);
      }
    }
  }
  
  /**
   * set task to renew the token
   */
  private 
  void setTimerForTokenRenewal(DelegationTokenToRenew token, 
                               boolean firstTime) throws IOException {
      
    // calculate timer time
    long now = System.currentTimeMillis();
    long renewIn;
    if(firstTime) {
      renewIn = now;
    } else {
      long expiresIn = (token.expirationDate - now); 
      renewIn = now + expiresIn - expiresIn/10; // little bit before the expiration
    }
    
    // need to create new task every time
    TimerTask tTask = new RenewalTimerTask(token);
    token.setTimerTask(tTask); // keep reference to the timer

    renewalTimer.schedule(token.timerTask, new Date(renewIn));
  }

  // cancel a token
  private void cancelToken(DelegationTokenToRenew t) {
    dtCancelThread.cancelToken(t.token, t.conf);
  }
  
  /**
   * removing failed DT
   * @param applicationId
   */
  private void removeFailedDelegationToken(DelegationTokenToRenew t) {
    ApplicationId applicationId = t.applicationId;
    if (LOG.isDebugEnabled())
      LOG.debug("removing failed delegation token for appid=" + applicationId + 
          ";t=" + t.token.getService());
    delegationTokens.remove(t);
    // cancel the timer
    if(t.timerTask!=null)
      t.timerTask.cancel();
  }
  
  /**
   * Removing delegation token for completed applications.
   * @param applicationId completed application
   */
  public void removeApplication(ApplicationId applicationId) {
    synchronized (delegationTokens) {
      Iterator<DelegationTokenToRenew> it = delegationTokens.iterator();
      while(it.hasNext()) {
        DelegationTokenToRenew dttr = it.next();
        if (dttr.applicationId.equals(applicationId)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Removing delegation token for appId=" + applicationId + 
                "; token=" + dttr.token.getService());
          }

          // cancel the timer
          if(dttr.timerTask!=null)
            dttr.timerTask.cancel();

          // cancel the token
          cancelToken(dttr);

          it.remove();
        }
      }
    }
  }
}
