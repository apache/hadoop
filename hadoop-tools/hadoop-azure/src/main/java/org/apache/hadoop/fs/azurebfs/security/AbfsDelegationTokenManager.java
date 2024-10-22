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


package org.apache.hadoop.fs.azurebfs.security;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.extensions.BoundDTExtension;
import org.apache.hadoop.fs.azurebfs.extensions.CustomDelegationTokenManager;
import org.apache.hadoop.fs.azurebfs.extensions.ExtensionHelper;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ReflectionUtils;

import static java.util.Objects.requireNonNull;

/**
 * Class for delegation token Manager.
 *
 * Instantiates the class declared in
 * {@link ConfigurationKeys#FS_AZURE_DELEGATION_TOKEN_PROVIDER_TYPE} and
 * issues tokens from it.
 */
public class AbfsDelegationTokenManager implements BoundDTExtension {

  private CustomDelegationTokenManager tokenManager;
  private static final Logger LOG =
          LoggerFactory.getLogger(AbfsDelegationTokenManager.class);
  private URI fsURI;

  /**
   * Create the custom delegation token manager and call its
   * {@link CustomDelegationTokenManager#initialize(Configuration)} method.
   * @param conf configuration
   * @throws IOException failure during initialization.
   * @throws RuntimeException classloading problems.
   */
  public AbfsDelegationTokenManager(final Configuration conf) throws IOException {

    Preconditions.checkNotNull(conf, "conf");

    Class<? extends CustomDelegationTokenManager> customDelegationTokenMgrClass =
            conf.getClass(ConfigurationKeys.FS_AZURE_DELEGATION_TOKEN_PROVIDER_TYPE, null,
                    CustomDelegationTokenManager.class);

    if (customDelegationTokenMgrClass == null) {
      throw new IllegalArgumentException(
              "The value for \"fs.azure.delegation.token.provider.type\" is not defined.");
    }

    CustomDelegationTokenManager customTokenMgr = ReflectionUtils
            .newInstance(customDelegationTokenMgrClass, conf);
    Preconditions.checkArgument(customTokenMgr != null,
        "Failed to initialize %s.", customDelegationTokenMgrClass);
    customTokenMgr.initialize(conf);
    tokenManager = customTokenMgr;
  }

  /**
   * Bind to a filesystem instance by passing the binding information down
   * to any token manager which implements {@link BoundDTExtension}.
   *
   * This is not invoked before renew or cancel operations, but is guaranteed
   * to be invoked before calls to {@link #getDelegationToken(String)}.
   * @param fsURI URI of the filesystem.
   * @param conf configuration of this extension.
   * @throws IOException bind failure.
   */
  @Override
  public void bind(final URI fsURI, final Configuration conf)
      throws IOException {
    this.fsURI = requireNonNull(fsURI, "Null Filesystem URI");
    ExtensionHelper.bind(tokenManager, fsURI, conf);
  }

  /**
   * Query the token manager for the service name; if it does not implement
   * the extension interface, one is generated from the FS URI.
   * @return the canonical service name.
   */
  @Override
  public String getCanonicalServiceName() {
    String name = ExtensionHelper.getCanonicalServiceName(tokenManager, null);
    if (name == null) {
      // no name from DT plugin. Generate one from the fs
      name = ExtensionHelper.buildCanonicalServiceName(fsURI);
      LOG.debug("No canonical service name from DT implementation; using {}", name);
    }
    return name;
  }

  /**
   * Close.
   * If the token manager is closeable, it has its {@link Closeable#close()}
   * method (quietly) invoked.
   */
  @Override
  public void close() {
    if (tokenManager instanceof Closeable) {
      IOUtils.cleanupWithLogger(LOG, (Closeable) tokenManager);
    }
  }

  /**
   * Get a delegation token by invoking
   * {@link CustomDelegationTokenManager#getDelegationToken(String)}.
   * If the token returned already has a Kind; that is used.
   * If not, then the token kind is set to
   * {@link AbfsDelegationTokenIdentifier#TOKEN_KIND}, which implicitly
   * resets any token renewer class.
   * @param renewer the principal permitted to renew the token.
   * @return a token for the filesystem.
   * @throws IOException failure.
   */
  public Token<DelegationTokenIdentifier> getDelegationToken(
      String renewer) throws IOException {

    LOG.debug("Requesting Delegation token for {}", renewer);
    Token<DelegationTokenIdentifier> token = tokenManager.getDelegationToken(renewer);

    if (token.getKind() == null) {
      // if a token type is not set, use the default.
      // note: this also sets the renewer to null.

      LOG.warn("Setting token kind of {} -renewer {} may be lost", token, renewer);
      token.setKind(AbfsDelegationTokenIdentifier.TOKEN_KIND);
    }
    if (token.getService() == null || token.getService().getLength() == 0) {
      // set the service name
      final Text service = new Text(getCanonicalServiceName());
      LOG.debug("Setting service name to {}", service);
      token.setService(service);
    }
    return token;
  }

  public long renewDelegationToken(Token<?> token)
      throws IOException {

    return tokenManager.renewDelegationToken(token);
  }

  public void cancelDelegationToken(Token<?> token)
          throws IOException {

    tokenManager.cancelDelegationToken(token);
  }

  @VisibleForTesting
  public CustomDelegationTokenManager getTokenManager() {
    return tokenManager;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "AbfsDelegationTokenManager{");
    sb.append("tokenManager=").append(tokenManager);
    sb.append('}');
    return sb.toString();
  }
}
