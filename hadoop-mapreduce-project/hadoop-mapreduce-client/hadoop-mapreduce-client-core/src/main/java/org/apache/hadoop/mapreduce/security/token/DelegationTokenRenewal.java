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
import java.net.InetAddress;
import java.net.URI;
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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.tools.DelegationTokenFetcher;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.StringUtils;


@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DelegationTokenRenewal {
  private static final Log LOG = LogFactory.getLog(DelegationTokenRenewal.class);
  public static final String SCHEME = "hdfs";
  
  /**
   * class that is used for keeping tracks of DT to renew
   *
   */
  private static class DelegationTokenToRenew {
    public final Token<DelegationTokenIdentifier> token;
    public final JobID jobId;
    public final Configuration conf;
    public long expirationDate;
    public TimerTask timerTask;
    
    public DelegationTokenToRenew(
        JobID jId, Token<DelegationTokenIdentifier> t, 
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
    public boolean equals (Object obj) {
      if (obj == this) {
        return true;
      } else if (obj == null || getClass() != obj.getClass()) {
        return false;
      } else {
        return token.equals(((DelegationTokenToRenew)obj).token);
      }
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
      Token<DelegationTokenIdentifier> token;
      Configuration conf;
      TokenWithConf(Token<DelegationTokenIdentifier> token,  
          Configuration conf) {
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
    public void cancelToken(Token<DelegationTokenIdentifier> token,  
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
      while (true) {
        TokenWithConf tokenWithConf = null;
        try {
          tokenWithConf = queue.take();
          DistributedFileSystem dfs = null;
          try {
            // do it over rpc. For that we need DFS object
            dfs = getDFSForToken(tokenWithConf.token, tokenWithConf.conf);
          } catch (Exception e) {
            LOG.info("couldn't get DFS to cancel. Will retry over HTTPS");
            dfs = null;
          }
      
          if(dfs != null) {
            dfs.cancelDelegationToken(tokenWithConf.token);
          } else {
            cancelDelegationTokenOverHttps(tokenWithConf.token, 
                                           tokenWithConf.conf);
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Canceling token " + tokenWithConf.token.getService() +  
                " for dfs=" + dfs);
          }
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
  
  // kind of tokens we currently renew
  private static final Text kindHdfs = 
    DelegationTokenIdentifier.HDFS_DELEGATION_KIND;
  
  @SuppressWarnings("unchecked")
  public static synchronized void registerDelegationTokensForRenewal(
      JobID jobId, Credentials ts, Configuration conf) {
    if(ts==null)
      return; //nothing to add
    
    Collection <Token<? extends TokenIdentifier>> tokens = ts.getAllTokens();
    long now = System.currentTimeMillis();
    
    for(Token<? extends TokenIdentifier> t : tokens) {
      // currently we only check for HDFS delegation tokens
      // later we can add more different types.
      if(! t.getKind().equals(kindHdfs)) {
        continue; 
      }
      Token<DelegationTokenIdentifier> dt = 
        (Token<DelegationTokenIdentifier>)t;
      
      // first renew happens immediately
      DelegationTokenToRenew dtr = 
        new DelegationTokenToRenew(jobId, dt, conf, now); 

      addTokenToList(dtr);
      
      setTimerForTokenRenewal(dtr, true);
      LOG.info("registering token for renewal for service =" + dt.getService()+
          " and jobID = " + jobId);
    }
  }
  
  private static String getHttpAddressForToken(
      Token<DelegationTokenIdentifier> token, final Configuration conf) 
  throws IOException {

    String[] ipaddr = token.getService().toString().split(":");

    InetAddress iaddr = InetAddress.getByName(ipaddr[0]);
    String dnsName = iaddr.getCanonicalHostName();
    
    // in case it is a different cluster it may have a different port
    String httpsPort = conf.get("dfs.hftp.https.port");
    if(httpsPort == null) {
      // get from this cluster
      httpsPort = conf.get(DFSConfigKeys.DFS_HTTPS_PORT_KEY, 
          "" + DFSConfigKeys.DFS_HTTPS_PORT_DEFAULT);
    }

    // always use https (it is for security only)
    return "https://" + dnsName+":"+httpsPort;
  }

  protected static long renewDelegationTokenOverHttps(
      final Token<DelegationTokenIdentifier> token, final Configuration conf) 
  throws InterruptedException, IOException{
    final String httpAddress = getHttpAddressForToken(token, conf);
    // will be chaged to debug
    LOG.info("address to renew=" + httpAddress + "; tok=" + token.getService());
    Long expDate = (Long) UserGroupInformation.getLoginUser().doAs(
        new PrivilegedExceptionAction<Long>() {
          public Long run() throws IOException {
            return DelegationTokenFetcher.renewDelegationToken(httpAddress, token);  
          }
        });
    LOG.info("Renew over HTTP done. addr="+httpAddress+";res="+expDate);
    return expDate;
  }
  
  private static long renewDelegationToken(DelegationTokenToRenew dttr) 
  throws Exception {
    long newExpirationDate=System.currentTimeMillis()+3600*1000;
    Token<DelegationTokenIdentifier> token = dttr.token;
    Configuration conf = dttr.conf;
    if(token.getKind().equals(kindHdfs)) {
      DistributedFileSystem dfs=null;
    
      try {
        // do it over rpc. For that we need DFS object
        dfs = getDFSForToken(token, conf);
      } catch (IOException e) {
        LOG.info("couldn't get DFS to renew. Will retry over HTTPS");
        dfs = null;
      }
      
      try {
        if(dfs != null)
          newExpirationDate = dfs.renewDelegationToken(token);
        else {
          // try HTTP
          newExpirationDate = renewDelegationTokenOverHttps(token, conf);
        }
      } catch (InvalidToken ite) {
        LOG.warn("invalid token - not scheduling for renew");
        removeFailedDelegationToken(dttr);
        throw new IOException("failed to renew token", ite);
      } catch (AccessControlException ioe) {
        LOG.warn("failed to renew token:"+token, ioe);
        removeFailedDelegationToken(dttr);
        throw new IOException("failed to renew token", ioe);
      } catch (Exception e) {
        LOG.warn("failed to renew token:"+token, e);
        // returns default expiration date
      }
    } else {
      throw new Exception("unknown token type to renew:"+token.getKind());
    }
    return newExpirationDate;
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
      Token<DelegationTokenIdentifier> token = dttr.token;
      long newExpirationDate=0;
      try {
        newExpirationDate = renewDelegationToken(dttr);
      } catch (Exception e) {
        return; // message logged in renewDT method
      }
      if (LOG.isDebugEnabled())
        LOG.debug("renewing for:"+token.getService()+";newED=" + 
            newExpirationDate);
      
      // new expiration date
      dttr.expirationDate = newExpirationDate;
      setTimerForTokenRenewal(dttr, false);// set the next one
    }
  }
  
  private static DistributedFileSystem getDFSForToken(
      Token<DelegationTokenIdentifier> token, final Configuration conf) 
  throws Exception {
    DistributedFileSystem dfs = null;
    try {
      final URI uri = new URI (SCHEME + "://" + token.getService().toString());
      dfs = 
      UserGroupInformation.getLoginUser().doAs(
          new PrivilegedExceptionAction<DistributedFileSystem>() {
        public DistributedFileSystem run() throws IOException {
          return (DistributedFileSystem) FileSystem.get(uri, conf);  
        }
      });
    } catch (Exception e) {
      LOG.warn("Failed to create a dfs to renew/cancel for:" + token.getService(), e);
      throw e;
    } 
    return dfs;
  }
  
  /**
   * find the soonest expiring token and set it for renew
   */
  private static void setTimerForTokenRenewal(
      DelegationTokenToRenew token, boolean firstTime) {
      
    // calculate timer time
    long now = System.currentTimeMillis();
    long renewIn;
    if(firstTime) {
      renewIn = now;
    } else {
      long expiresIn = (token.expirationDate - now); 
      renewIn = now + expiresIn - expiresIn/10; // little before expiration
    }
    
    try {
      // need to create new timer every time
      TimerTask tTask = new RenewalTimerTask(token);
      token.setTimerTask(tTask); // keep reference to the timer

      renewalTimer.schedule(token.timerTask, new Date(renewIn));
    } catch (Exception e) {
      LOG.warn("failed to schedule a task, token will not renew more", e);
    }
  }

  /**
   * removing all tokens renewals
   */
  static public void close() {
    renewalTimer.cancel();
    delegationTokens.clear();
  }
  
  
  protected static void cancelDelegationTokenOverHttps(
      final Token<DelegationTokenIdentifier> token, final Configuration conf) 
  throws InterruptedException, IOException{
    final String httpAddress = getHttpAddressForToken(token, conf);
    // will be chaged to debug
    LOG.info("address to cancel=" + httpAddress + "; tok=" + token.getService());
    
    UserGroupInformation.getLoginUser().doAs(
        new PrivilegedExceptionAction<Void>() {
          public Void run() throws IOException {
            DelegationTokenFetcher.cancelDelegationToken(httpAddress, token);
            return null;
          }
        });
    LOG.info("Cancel over HTTP done. addr="+httpAddress);
  }
  
  
  // cancel a token
  private static void cancelToken(DelegationTokenToRenew t) {
    Token<DelegationTokenIdentifier> token = t.token;
    Configuration conf = t.conf;
    
    if(token.getKind().equals(kindHdfs)) {
      dtCancelThread.cancelToken(token, conf);
    }
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
