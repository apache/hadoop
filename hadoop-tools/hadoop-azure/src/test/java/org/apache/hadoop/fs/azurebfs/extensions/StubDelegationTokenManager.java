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

package org.apache.hadoop.fs.azurebfs.extensions;

import java.io.IOException;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_DELEGATION_TOKEN_PROVIDER_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_DELEGATION_TOKEN;

/**
 * This is a Stub DT manager which adds support for {@link BoundDTExtension}
 * to {@link ClassicDelegationTokenManager}.
 */
public class StubDelegationTokenManager extends ClassicDelegationTokenManager
    implements BoundDTExtension {

  private static final Logger LOG = LoggerFactory.getLogger(
      StubDelegationTokenManager.class);

  /**
   * Classname.
   */
  public static final String NAME
      = "org.apache.hadoop.fs.azurebfs.extensions.StubDelegationTokenManager";

  /**
   * Instantiate.
   */
  public StubDelegationTokenManager() {
  }

  @Override
  public void bind(final URI uri, final Configuration conf)
      throws IOException {
    super.innerBind(uri, conf);
  }

  /**
   * Create a token.
   *
   * @param sequenceNumber sequence number.
   * @param uri FS URI
   * @param owner FS owner
   * @param renewer renewer
   * @return a token.
   */
  public static Token<DelegationTokenIdentifier> createToken(
      final int sequenceNumber,
      final URI uri,
      final Text owner,
      final Text renewer) {
    return ClassicDelegationTokenManager.createToken(sequenceNumber, uri, owner,
        renewer);
  }

  /**
   * Patch a configuration to declare this the DT provider for a filesystem
   * built off the given configuration.
   * The ABFS Filesystem still needs to come up with security enabled.
   * @param conf configuration.
   * @return the patched configuration.
   */
  public static Configuration useStubDTManager(Configuration conf) {
    conf.setBoolean(FS_AZURE_ENABLE_DELEGATION_TOKEN, true);
    conf.set(FS_AZURE_DELEGATION_TOKEN_PROVIDER_TYPE,
        StubDelegationTokenManager.NAME);
    return conf;
  }

}
