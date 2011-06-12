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

package org.apache.hadoop.mapreduce.security;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.TokenStorage;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;


/**
 * This class provides user facing APIs for transferring secrets from
 * the job client to the tasks.
 * The secrets can be stored just before submission of jobs and read during
 * the task execution.  
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TokenCache {
  
  private static final Log LOG = LogFactory.getLog(TokenCache.class);

  private static TokenStorage tokenStorage;
  
  /**
   * auxiliary method to get user's secret keys..
   * @param alias
   * @return secret key from the storage
   */
  public static byte[] getSecretKey(Text alias) {
    if(tokenStorage == null)
      return null;
    return tokenStorage.getSecretKey(alias);
  }
  
  /**
   * auxiliary methods to store user'  s secret keys
   * @param alias
   * @param key
   */
  public static void addSecretKey(Text alias, byte[] key) {
    getTokenStorage().addSecretKey(alias, key);
  }
  
  /**
   * auxiliary method to add a delegation token
   */
  public static void addDelegationToken(
      String namenode, Token<? extends TokenIdentifier> t) {
    getTokenStorage().addToken(new Text(namenode), t);
  }

  /**
   * auxiliary method 
   * @return all the available tokens
   */
  public static Collection<Token<? extends TokenIdentifier>> getAllTokens() {
    return getTokenStorage().getAllTokens();
  }
  /**
   * Convenience method to obtain delegation tokens from namenodes 
   * corresponding to the paths passed.
   * @param ps array of paths
   * @param conf configuration
   * @throws IOException
   */
  public static void obtainTokensForNamenodes(Path [] ps, Configuration conf) 
  throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }
    obtainTokensForNamenodesInternal(ps, conf);
  }
    
  static void obtainTokensForNamenodesInternal(Path [] ps, Configuration conf)
  throws IOException {
    // get jobtracker principal id (for the renewer)
    Text jtCreds = new Text(conf.get(MRJobConfig.JOB_JOBTRACKER_ID, ""));
    
    for(Path p: ps) {
      FileSystem fs = FileSystem.get(p.toUri(), conf);
      if(fs instanceof DistributedFileSystem) {
        DistributedFileSystem dfs = (DistributedFileSystem)fs;
        URI uri = fs.getUri();
        String fs_addr = buildDTServiceName(uri);
        
        // see if we already have the token
        Token<DelegationTokenIdentifier> token = 
          TokenCache.getDelegationToken(fs_addr); 
        if(token != null) {
          LOG.debug("DT for " + token.getService()  + " is already present");
          continue;
        }
        // get the token
        token = dfs.getDelegationToken(jtCreds);
        if(token==null) 
          throw new IOException("Token from " + fs_addr + " is null");

        token.setService(new Text(fs_addr));
        TokenCache.addDelegationToken(fs_addr, token);
        LOG.info("getting dt for " + p.toString() + ";uri="+ fs_addr + 
            ";t.service="+token.getService());
      }
    }
  }
  
  /**
   * file name used on HDFS for generated job token
   */
  @InterfaceAudience.Private
  public static final String JOB_TOKEN_HDFS_FILE = "jobToken";

  /**
   * conf setting for job tokens cache file name
   */
  @InterfaceAudience.Private
  public static final String JOB_TOKENS_FILENAME = "mapreduce.job.jobTokenFile";
  private static final Text JOB_TOKEN = new Text("ShuffleAndJobToken");
  
  /**
   * 
   * @param namenode
   * @return delegation token
   */
  @SuppressWarnings("unchecked")
  @InterfaceAudience.Private
  public static Token<DelegationTokenIdentifier> 
  getDelegationToken(String namenode) {
    return (Token<DelegationTokenIdentifier>)getTokenStorage().
            getToken(new Text(namenode));
  }

  /**
   * @return TokenStore object
   */
  @InterfaceAudience.Private
  public static TokenStorage getTokenStorage() {
    if(tokenStorage==null)
      tokenStorage = new TokenStorage();
    
    return tokenStorage;
  }
  
  /**
   * sets TokenStorage
   * @param ts
   */
  @InterfaceAudience.Private
  public static void setTokenStorage(TokenStorage ts) {
    if(tokenStorage != null)
      LOG.warn("Overwriting existing token storage with # keys=" + 
          tokenStorage.numberOfSecretKeys());
    tokenStorage = ts;
  }
  
  /**
   * load token storage and stores it
   * @param conf
   * @return Loaded TokenStorage object
   * @throws IOException
   */
  @InterfaceAudience.Private
  public static TokenStorage loadTaskTokenStorage(String fileName, JobConf conf)
  throws IOException {
    if(tokenStorage != null)
      return tokenStorage;
    
    tokenStorage = loadTokens(fileName, conf);
    
    return tokenStorage;
  }
  
  /**
   * load job token from a file
   * @param conf
   * @throws IOException
   */
  @InterfaceAudience.Private
  public static TokenStorage loadTokens(String jobTokenFile, JobConf conf) 
  throws IOException {
    Path localJobTokenFile = new Path (jobTokenFile);
    FileSystem localFS = FileSystem.getLocal(conf);
    FSDataInputStream in = localFS.open(localJobTokenFile);
    
    TokenStorage ts = new TokenStorage();
    ts.readFields(in);

    if(LOG.isDebugEnabled()) {
      LOG.debug("Task: Loaded jobTokenFile from: "+localJobTokenFile.toUri().getPath() 
        +"; num of sec keys  = " + ts.numberOfSecretKeys());
    }
    in.close();
    return ts;
  }
  /**
   * store job token
   * @param t
   */
  @InterfaceAudience.Private
  public static void setJobToken(Token<? extends TokenIdentifier> t, 
      TokenStorage ts) {
    ts.addToken(JOB_TOKEN, t);
  }
  /**
   * 
   * @return job token
   */
  @SuppressWarnings("unchecked")
  @InterfaceAudience.Private
  public static Token<JobTokenIdentifier> getJobToken(TokenStorage ts) {
    return (Token<JobTokenIdentifier>) ts.getToken(JOB_TOKEN);
  }
  
  static String buildDTServiceName(URI uri) {
    int port = uri.getPort();
    if(port == -1) 
      port = NameNode.DEFAULT_PORT;
    
    // build the service name string "ip:port"
    StringBuffer sb = new StringBuffer();
    sb.append(NetUtils.normalizeHostName(uri.getHost())).append(":").append(port);
    return sb.toString();
  }
}
