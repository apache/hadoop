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
package org.apache.hadoop.crypto.key.kms;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;

/**
 * Holder class for KMS delegation tokens.
 */
@InterfaceAudience.Private
public final class KMSDelegationToken {

  public static final String TOKEN_LEGACY_KIND_STR = "kms-dt";
  public static final Text TOKEN_LEGACY_KIND = new Text(TOKEN_LEGACY_KIND_STR);

  public static final String TOKEN_KIND_STR = "KMS_DELEGATION_TOKEN";
  public static final Text TOKEN_KIND = new Text(TOKEN_KIND_STR);

  // Utility class is not supposed to be instantiated.
  private KMSDelegationToken() {
  }

  /**
   * DelegationTokenIdentifier used for the KMS.
   */
  public static class KMSDelegationTokenIdentifier
      extends DelegationTokenIdentifier {

    public KMSDelegationTokenIdentifier() {
      super(TOKEN_KIND);
    }

    @Override
    public Text getKind() {
      return TOKEN_KIND;
    }
  }

  /**
   * DelegationTokenIdentifier used for the KMS for legacy tokens.
   */
  @Deprecated
  public static class KMSLegacyDelegationTokenIdentifier
      extends DelegationTokenIdentifier {

    public KMSLegacyDelegationTokenIdentifier() {
      super(TOKEN_LEGACY_KIND);
    }

    @Override
    public Text getKind() {
      return TOKEN_LEGACY_KIND;
    }
  }
}