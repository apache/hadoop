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

package org.apache.hadoop.mapreduce.security.token;

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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;


//@InterfaceAudience.Private
public class DelegationTokenRenewal {
  private static final Log LOG = LogFactory.getLog(DelegationTokenRenewal.class);
  public static final String SCHEME = "hdfs";
  
  /**
   * class that is used for keeping tracks of DT to renew
   *
   */
  private static class DelegationTokenToRenew {
    public final Token<?> token;
    public final JobID jobId;
    public final Configuration conf;
    public long expirationDate;
    public TimerTask timerTask;
    
    public DelegationTokenToRenew(
        JobID jId, Token<?> t, 
        Configuration newConf, long newExpirationDate) {
      token = t;
      jobId = jId;
      conf = newConf;
      expirationDate = newExpirationDate;
      timerTask = null;
      if(token==null || jobId==null || conf==null) {
        throw new IllegalArgumentException("invalid params for Renew Token" +
            ";t="+token+";j="+jobId+";c="+conf);
      }
    }
    public void setTimerTask(TimerTask tTask) {
      timerTask = tTask;
    }
    @Override
    public String toString() {
      return token + ";exp="+expirationDate;
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
  
  // global single timer (daemon)
  private static Timer renewalTimer = new Timer(true);
  
  //delegation token canceler thread
  private static DelegationTokenCancelThread dtCancelThread =
    new DelegationTokenCancelThread();
  static {
    dtCancelThread.start();
  }

  
  //managing the list of tokens using Map
  // jobId=>List<tokens>
  private static Set<DelegationTokenToRenew> delegationTokens = 
    Collections.synchronizedSet(new HashSet<DelegationTokenToRenew>());
  
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
  private static void addTokenToList(DelegationTokenToRenew t) {
    delegationTokens.add(t);
  }
  
  public static synchronized void registerDelegationTokensForRenewal(
      JobID jobId, Credentials ts, Configuration conf) throws IOException {
    if(ts==null)
      return; //nothing to add
    
    Collection <Token<?>> tokens = ts.getAllTokens();
    long now = System.currentTimeMillis();
    
    for(Token<?> t : tokens) {
      // first renew happens immediately
      if (t.isManaged()) {
        DelegationTokenToRenew dtr = 
          new DelegationTokenToRenew(jobId, t, conf, now); 

        addTokenToList(dtr);
      
        setTimerForTokenRenewal(dtr, true);
        LOG.info("registering token for renewal for service =" + t.getService()+
                 " and jobID = " + jobId);
      }
    }
  }
  
  /**
   * Task - to renew a token
   *
   */
  private static class RenewalTimerTask extends TimerTask {
    private DelegationTokenToRenew dttr;
    
    RenewalTimerTask(DelegationTokenToRenew t) {  dttr = t;  }
    
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
          LOG.debug("renewing for:" + token.getService() + ";newED=" + 
                    dttr.expirationDate);
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
  private static 
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

  /**
   * removing all tokens renewals
   */
  static public void close() {
    renewalTimer.cancel();
    delegationTokens.clear();
  }
  
  // cancel a token
  private static void cancelToken(DelegationTokenToRenew t) {
    dtCancelThread.cancelToken(t.token, t.conf);
  }
  
  /**
   * removing failed DT
   * @param jobId
   */
  private static void removeFailedDelegationToken(DelegationTokenToRenew t) {
    JobID jobId = t.jobId;
    if (LOG.isDebugEnabled())
      LOG.debug("removing failed delegation token for jobid=" + jobId + 
          ";t=" + t.token.getService());
    delegationTokens.remove(t);
    // cancel the timer
    if(t.timerTask!=null)
      t.timerTask.cancel();
  }
  
  /**
   * removing DT for completed jobs
   * @param jobId
   */
  public static void removeDelegationTokenRenewalForJob(JobID jobId) {
    synchronized (delegationTokens) {
      Iterator<DelegationTokenToRenew> it = delegationTokens.iterator();
      while(it.hasNext()) {
        DelegationTokenToRenew dttr = it.next();
        if (dttr.jobId.equals(jobId)) {
          if (LOG.isDebugEnabled())
            LOG.debug("removing delegation token for jobid=" + jobId + 
                ";t=" + dttr.token.getService());

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
