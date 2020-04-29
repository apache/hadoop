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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security;

import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;

public class LocalizerTokenSelector implements
    TokenSelector<LocalizerTokenIdentifier> {

  private static final Logger LOG =
       LoggerFactory.getLogger(LocalizerTokenSelector.class);

  @SuppressWarnings("unchecked")
  @Override
  public Token<LocalizerTokenIdentifier> selectToken(Text service,
      Collection<Token<? extends TokenIdentifier>> tokens) {

    LOG.debug("Using localizerTokenSelector.");

    for (Token<? extends TokenIdentifier> token : tokens) {
      LOG.debug("Token of kind {} is found", token.getKind());
      if (LocalizerTokenIdentifier.KIND.equals(token.getKind())) {
        return (Token<LocalizerTokenIdentifier>) token;
      }
    }
    LOG.debug("Returning null.");
    return null;
  }
}
