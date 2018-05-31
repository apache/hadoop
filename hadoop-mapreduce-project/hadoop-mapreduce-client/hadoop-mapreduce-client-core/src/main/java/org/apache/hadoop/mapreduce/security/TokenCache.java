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
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Master;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class provides user facing APIs for transferring secrets from
 * the job client to the tasks.
 * The secrets can be stored just before submission of jobs and read during
 * the task execution.  
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TokenCache {
  
  private static final Logger LOG = LoggerFactory.getLogger(TokenCache.class);

  
  /**
   * auxiliary method to get user's secret keys..
   * @param alias
   * @return secret key from the storage
   */
  public static byte[] getSecretKey(Credentials credentials, Text alias) {
    if(credentials == null)
      return null;
    return credentials.getSecretKey(alias);
  }
  
  /**
   * Convenience method to obtain delegation tokens from namenodes 
   * corresponding to the paths passed.
   * @param credentials
   * @param ps array of paths
   * @param conf configuration
   * @throws IOException
   */
  public static void obtainTokensForNamenodes(Credentials credentials,
      Path[] ps, Configuration conf) throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }
    obtainTokensForNamenodesInternal(credentials, ps, conf);
  }

  /**
   * Remove jobtoken referrals which don't make sense in the context
   * of the task execution.
   *
   * @param conf
   */
  public static void cleanUpTokenReferral(Configuration conf) {
    conf.unset(MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY);
  }

  static void obtainTokensForNamenodesInternal(Credentials credentials,
      Path[] ps, Configuration conf) throws IOException {
    Set<FileSystem> fsSet = new HashSet<FileSystem>();
    for(Path p: ps) {
      fsSet.add(p.getFileSystem(conf));
    }
    String masterPrincipal = Master.getMasterPrincipal(conf);
    for (FileSystem fs : fsSet) {
      obtainTokensForNamenodesInternal(fs, credentials, conf, masterPrincipal);
    }
  }

  static boolean isTokenRenewalExcluded(FileSystem fs, Configuration conf) {
    String [] nns =
        conf.getStrings(MRJobConfig.JOB_NAMENODES_TOKEN_RENEWAL_EXCLUDE);
    if (nns != null) {
      String host = fs.getUri().getHost();
      for(int i=0; i< nns.length; i++) {
        if (nns[i].equals(host)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * get delegation token for a specific FS
   * @param fs
   * @param credentials
   * @param conf
   * @throws IOException
   */
  static void obtainTokensForNamenodesInternal(FileSystem fs,
      Credentials credentials, Configuration conf, String renewer)
      throws IOException {
    // RM skips renewing token with empty renewer
    String delegTokenRenewer = "";
    if (!isTokenRenewalExcluded(fs, conf)) {
      if (StringUtils.isEmpty(renewer)) {
        throw new IOException(
            "Can't get Master Kerberos principal for use as renewer");
      } else {
        delegTokenRenewer = renewer;
      }
    }

    mergeBinaryTokens(credentials, conf);

    final Token<?> tokens[] = fs.addDelegationTokens(delegTokenRenewer,
                                                     credentials);
    if (tokens != null) {
      for (Token<?> token : tokens) {
        LOG.info("Got dt for " + fs.getUri() + "; "+token);
      }
    }
  }

  private static void mergeBinaryTokens(Credentials creds, Configuration conf) {
    String binaryTokenFilename =
        conf.get(MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY);
    if (binaryTokenFilename != null) {
      Credentials binary;
      try {
        binary = Credentials.readTokenStorageFile(
            FileSystem.getLocal(conf).makeQualified(
                new Path(binaryTokenFilename)),
            conf);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      // supplement existing tokens with the tokens in the binary file
      creds.mergeAll(binary);
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
  private static final Text JOB_TOKEN = new Text("JobToken");
  private static final Text SHUFFLE_TOKEN = new Text("MapReduceShuffleToken");
  private static final Text ENC_SPILL_KEY = new Text("MapReduceEncryptedSpillKey");
  
  /**
   * load job token from a file
   * @deprecated Use {@link Credentials#readTokenStorageFile} instead,
   * this method is included for compatibility against Hadoop-1.
   * @param conf
   * @throws IOException
   */
  @InterfaceAudience.Private
  @Deprecated
  public static Credentials loadTokens(String jobTokenFile, JobConf conf)
  throws IOException {
    Path localJobTokenFile = new Path ("file:///" + jobTokenFile);

    Credentials ts = Credentials.readTokenStorageFile(localJobTokenFile, conf);

    if(LOG.isDebugEnabled()) {
      LOG.debug("Task: Loaded jobTokenFile from: "+
          localJobTokenFile.toUri().getPath() 
          +"; num of sec keys  = " + ts.numberOfSecretKeys() +
          " Number of tokens " +  ts.numberOfTokens());
    }
    return ts;
  }
  
  /**
   * load job token from a file
   * @deprecated Use {@link Credentials#readTokenStorageFile} instead,
   * this method is included for compatibility against Hadoop-1.
   * @param conf
   * @throws IOException
   */
  @InterfaceAudience.Private
  @Deprecated
  public static Credentials loadTokens(String jobTokenFile, Configuration conf)
      throws IOException {
    return loadTokens(jobTokenFile, new JobConf(conf));
  }
  
  /**
   * store job token
   * @param t
   */
  @InterfaceAudience.Private
  public static void setJobToken(Token<? extends TokenIdentifier> t, 
      Credentials credentials) {
    credentials.addToken(JOB_TOKEN, t);
  }
  /**
   * 
   * @return job token
   */
  @SuppressWarnings("unchecked")
  @InterfaceAudience.Private
  public static Token<JobTokenIdentifier> getJobToken(Credentials credentials) {
    return (Token<JobTokenIdentifier>) credentials.getToken(JOB_TOKEN);
  }

  @InterfaceAudience.Private
  public static void setShuffleSecretKey(byte[] key, Credentials credentials) {
    credentials.addSecretKey(SHUFFLE_TOKEN, key);
  }

  @InterfaceAudience.Private
  public static byte[] getShuffleSecretKey(Credentials credentials) {
    return getSecretKey(credentials, SHUFFLE_TOKEN);
  }

  @InterfaceAudience.Private
  public static void setEncryptedSpillKey(byte[] key, Credentials credentials) {
    credentials.addSecretKey(ENC_SPILL_KEY, key);
  }

  @InterfaceAudience.Private
  public static byte[] getEncryptedSpillKey(Credentials credentials) {
    return getSecretKey(credentials, ENC_SPILL_KEY);
  }
  /**
   * @deprecated Use {@link Credentials#getToken(org.apache.hadoop.io.Text)}
   * instead, this method is included for compatibility against Hadoop-1
   * @param namenode
   * @return delegation token
   */
  @InterfaceAudience.Private
  @Deprecated
  public static
      Token<?> getDelegationToken(
          Credentials credentials, String namenode) {
    return (Token<?>) credentials.getToken(new Text(
      namenode));
  }
}
