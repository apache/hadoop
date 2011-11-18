/*
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

package org.apache.hadoop.hbase.security.token;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

/**
 * Utility methods for obtaining authentication tokens.
 */
public class TokenUtil {
  private static Log LOG = LogFactory.getLog(TokenUtil.class);

  /**
   * Obtain and return an authentication token for the current user.
   * @param conf The configuration for connecting to the cluster
   * @return the authentication token instance
   */
  public static Token<AuthenticationTokenIdentifier> obtainToken(
      Configuration conf) throws IOException {
    HTable meta = null;
    try {
      meta = new HTable(conf, ".META.");
      AuthenticationProtocol prot = meta.coprocessorProxy(
          AuthenticationProtocol.class, HConstants.EMPTY_START_ROW);
      return prot.getAuthenticationToken();
    } finally {
      if (meta != null) {
        meta.close();
      }
    }
  }

  private static Text getClusterId(Token<AuthenticationTokenIdentifier> token)
      throws IOException {
    return token.getService() != null
        ? token.getService() : new Text("default");
  }

  /**
   * Obtain an authentication token for the given user and add it to the
   * user's credentials.
   * @param conf The configuration for connecting to the cluster
   * @param user The user for whom to obtain the token
   * @throws IOException If making a remote call to the {@link TokenProvider} fails
   * @throws InterruptedException If executing as the given user is interrupted
   */
  public static void obtainAndCacheToken(final Configuration conf,
      UserGroupInformation user)
      throws IOException, InterruptedException {
    try {
      Token<AuthenticationTokenIdentifier> token =
          user.doAs(new PrivilegedExceptionAction<Token<AuthenticationTokenIdentifier>>() {
            public Token<AuthenticationTokenIdentifier> run() throws Exception {
              return obtainToken(conf);
            }
          });

      if (token == null) {
        throw new IOException("No token returned for user "+user.getUserName());
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Obtained token "+token.getKind().toString()+" for user "+
            user.getUserName());
      }
      user.addToken(token);
    } catch (IOException ioe) {
      throw ioe;
    } catch (InterruptedException ie) {
      throw ie;
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new UndeclaredThrowableException(e,
          "Unexpected exception obtaining token for user "+user.getUserName());
    }
  }

  /**
   * Obtain an authentication token on behalf of the given user and add it to
   * the credentials for the given map reduce job.
   * @param conf The configuration for connecting to the cluster
   * @param user The user for whom to obtain the token
   * @param job The job instance in which the token should be stored
   * @throws IOException If making a remote call to the {@link TokenProvider} fails
   * @throws InterruptedException If executing as the given user is interrupted
   */
  public static void obtainTokenForJob(final Configuration conf,
      UserGroupInformation user, Job job)
      throws IOException, InterruptedException {
    try {
      Token<AuthenticationTokenIdentifier> token =
          user.doAs(new PrivilegedExceptionAction<Token<AuthenticationTokenIdentifier>>() {
            public Token<AuthenticationTokenIdentifier> run() throws Exception {
              return obtainToken(conf);
            }
          });

      if (token == null) {
        throw new IOException("No token returned for user "+user.getUserName());
      }
      Text clusterId = getClusterId(token);
      LOG.info("Obtained token "+token.getKind().toString()+" for user "+
          user.getUserName() + " on cluster "+clusterId.toString());
      job.getCredentials().addToken(clusterId, token);
    } catch (IOException ioe) {
      throw ioe;
    } catch (InterruptedException ie) {
      throw ie;
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new UndeclaredThrowableException(e,
          "Unexpected exception obtaining token for user "+user.getUserName());
    }
  }

  /**
   * Obtain an authentication token on behalf of the given user and add it to
   * the credentials for the given map reduce job.
   * @param user The user for whom to obtain the token
   * @param job The job configuration in which the token should be stored
   * @throws IOException If making a remote call to the {@link TokenProvider} fails
   * @throws InterruptedException If executing as the given user is interrupted
   */
  public static void obtainTokenForJob(final JobConf job,
      UserGroupInformation user)
      throws IOException, InterruptedException {
    try {
      Token<AuthenticationTokenIdentifier> token =
          user.doAs(new PrivilegedExceptionAction<Token<AuthenticationTokenIdentifier>>() {
            public Token<AuthenticationTokenIdentifier> run() throws Exception {
              return obtainToken(job);
            }
          });

      if (token == null) {
        throw new IOException("No token returned for user "+user.getUserName());
      }
      Text clusterId = getClusterId(token);
      LOG.info("Obtained token "+token.getKind().toString()+" for user "+
          user.getUserName()+" on cluster "+clusterId.toString());
      job.getCredentials().addToken(clusterId, token);
    } catch (IOException ioe) {
      throw ioe;
    } catch (InterruptedException ie) {
      throw ie;
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new UndeclaredThrowableException(e,
          "Unexpected exception obtaining token for user "+user.getUserName());
    }
  }
}
