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
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * this class keeps static references to TokenStorage object
 * also it provides auxiliary methods for setting and getting secret keys  
 */
//@InterfaceStability.Evolving
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
  public static void setSecretKey(Text alias, byte[] key) {
    getTokenStorage().addSecretKey(alias, key);
  }
  
  /**
   * auxiliary method to add a delegation token
   */
  public static void addDelegationToken(
      String namenode, Token<? extends TokenIdentifier> t) {
    getTokenStorage().setToken(new Text(namenode), t);
  }
  
  /**
   * auxiliary method 
   * @return all the available tokens
   */
  public static Collection<Token<? extends TokenIdentifier>> getAllTokens() {
    return getTokenStorage().getAllTokens();
  }
  
  /**
   * @return TokenStore object
   */
  //@InterfaceAudience.Private
  public static TokenStorage getTokenStorage() {
    if(tokenStorage==null)
      tokenStorage = new TokenStorage();
    
    return tokenStorage;
  }
  
  /**
   * sets TokenStorage
   * @param ts
   */
  //@InterfaceAudience.Private
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
  //@InterfaceAudience.Private
  public static TokenStorage loadTaskTokenStorage(JobConf conf)
  throws IOException {
    if(tokenStorage != null)
      return tokenStorage;
    
    tokenStorage = loadTokens(conf);
    
    return tokenStorage;
  }
  
  /**
   * load job token from a file
   * @param conf
   * @throws IOException
   */
  //@InterfaceAudience.Private
  public static TokenStorage loadTokens(JobConf conf) 
  throws IOException {
    String jobTokenFile = conf.get(JobContext.JOB_TOKEN_FILE);
    Path localJobTokenFile = new Path (jobTokenFile);
    FileSystem localFS = FileSystem.getLocal(conf);
    FSDataInputStream in = localFS.open(localJobTokenFile);
    
    TokenStorage ts = new TokenStorage();
    ts.readFields(in);

    LOG.info("Task: Loaded jobTokenFile from: "+localJobTokenFile.toUri().getPath() 
        +"; num of sec keys  = " + ts.numberOfSecretKeys());
    in.close();
    return ts;
  }
}
