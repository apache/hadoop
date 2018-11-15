/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.security;

import java.util.Collection;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A delegation token selector that is specialized for Ozone.
 */
@InterfaceAudience.Private
public class OzoneDelegationTokenSelector
    extends AbstractDelegationTokenSelector<OzoneTokenIdentifier> {

  public OzoneDelegationTokenSelector() {
    super(OzoneTokenIdentifier.KIND_NAME);
  }

  private static final Logger LOG = LoggerFactory
      .getLogger(OzoneDelegationTokenSelector.class);

  @Override
  public Token selectToken(Text service,
      Collection<Token<? extends TokenIdentifier>> tokens) {
    LOG.trace("Getting token for service {}", service);
    return super.selectToken(service, tokens);
  }

}

