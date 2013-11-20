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
package org.apache.hadoop.hdfs.web;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegationTokenRenewer;
import org.apache.hadoop.fs.DelegationTokenRenewer.Renewable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSelector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSelector;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class implements the aspects that relate to delegation tokens for all
 * HTTP-based file system.
 */
final class TokenAspect<T extends FileSystem & Renewable> {
  @InterfaceAudience.Private
  public static class TokenManager extends TokenRenewer {

    @Override
    public void cancel(Token<?> token, Configuration conf) throws IOException {
      getInstance(token, conf).cancelDelegationToken(token);
    }

    @Override
    public boolean handleKind(Text kind) {
      return kind.equals(HftpFileSystem.TOKEN_KIND)
          || kind.equals(HsftpFileSystem.TOKEN_KIND)
          || kind.equals(WebHdfsFileSystem.TOKEN_KIND)
          || kind.equals(SWebHdfsFileSystem.TOKEN_KIND);
    }

    @Override
    public boolean isManaged(Token<?> token) throws IOException {
      return true;
    }

    @Override
    public long renew(Token<?> token, Configuration conf) throws IOException {
      return getInstance(token, conf).renewDelegationToken(token);
    }

    private TokenManagementDelegator getInstance(Token<?> token,
        Configuration conf) throws IOException {
      final InetSocketAddress address = SecurityUtil.getTokenServiceAddr(token);
      Text kind = token.getKind();
      final URI uri;

      if (kind.equals(HftpFileSystem.TOKEN_KIND)) {
        uri = DFSUtil.createUri(HftpFileSystem.SCHEME, address);
      } else if (kind.equals(HsftpFileSystem.TOKEN_KIND)) {
        uri = DFSUtil.createUri(HsftpFileSystem.SCHEME, address);
      } else if (kind.equals(WebHdfsFileSystem.TOKEN_KIND)) {
        uri = DFSUtil.createUri(WebHdfsFileSystem.SCHEME, address);
      } else if (kind.equals(SWebHdfsFileSystem.TOKEN_KIND)) {
        uri = DFSUtil.createUri(SWebHdfsFileSystem.SCHEME, address);
      } else {
        throw new IllegalArgumentException("Unsupported scheme");
      }
      return (TokenManagementDelegator) FileSystem.get(uri, conf);
    }
  }

  static class DTSelecorByKind extends
      AbstractDelegationTokenSelector<DelegationTokenIdentifier> {
    private static final DelegationTokenSelector selector = new DelegationTokenSelector();

    public DTSelecorByKind(final Text kind) {
      super(kind);
    }

    Token<DelegationTokenIdentifier> selectToken(URI nnUri,
        Collection<Token<?>> tokens, Configuration conf) {
      Token<DelegationTokenIdentifier> token = selectToken(
          SecurityUtil.buildTokenService(nnUri), tokens);
      if (token == null) {
        token = selector.selectToken(nnUri, tokens, conf);
      }
      return token;
    }
  }

  /**
   * Callbacks for token management
   */
  interface TokenManagementDelegator {
    void cancelDelegationToken(final Token<?> token) throws IOException;

    URI getCanonicalUri();

    long renewDelegationToken(final Token<?> token) throws IOException;
  }

  private DelegationTokenRenewer.RenewAction<?> action;
  private DelegationTokenRenewer dtRenewer = null;
  private final DTSelecorByKind dtSelector;
  private final T fs;
  private boolean hasInitedToken;
  private final Log LOG;

  TokenAspect(T fs, final Text kind) {
    this.LOG = LogFactory.getLog(fs.getClass());
    this.fs = fs;
    this.dtSelector = new DTSelecorByKind(kind);
  }

  synchronized void ensureTokenInitialized() throws IOException {
    // we haven't inited yet, or we used to have a token but it expired
    if (!hasInitedToken || (action != null && !action.isValid())) {
      //since we don't already have a token, go get one
      Token<?> token = fs.getDelegationToken(null);
      // security might be disabled
      if (token != null) {
        fs.setDelegationToken(token);
        addRenewAction(fs);
        LOG.debug("Created new DT for " + token.getService());
      }
      hasInitedToken = true;
    }
  }

  public synchronized void reset() {
    hasInitedToken = false;
  }

  synchronized void initDelegationToken(UserGroupInformation ugi) {
    Token<?> token = selectDelegationToken(ugi);
    if (token != null) {
      LOG.debug("Found existing DT for " + token.getService());
      fs.setDelegationToken(token);
      hasInitedToken = true;
    }
  }

  synchronized void removeRenewAction() throws IOException {
    if (dtRenewer != null) {
      dtRenewer.removeRenewAction(fs);
    }
  }

  @VisibleForTesting
  Token<DelegationTokenIdentifier> selectDelegationToken(
      UserGroupInformation ugi) {
    return dtSelector.selectToken(
        ((TokenManagementDelegator)fs).getCanonicalUri(), ugi.getTokens(),
        fs.getConf());
  }

  private synchronized void addRenewAction(final T webhdfs) {
    if (dtRenewer == null) {
      dtRenewer = DelegationTokenRenewer.getInstance();
    }

    action = dtRenewer.addRenewAction(webhdfs);
  }
}
