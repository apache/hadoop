/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.policies;

import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;

/**
 * Helper class used to factor out common validation steps for policies.
 */
public final class FederationPolicyInitializationContextValidator {

  private FederationPolicyInitializationContextValidator() {
    // disable constructor per checkstyle
  }

  public static void validate(
      FederationPolicyInitializationContext policyContext, String myType)
      throws FederationPolicyInitializationException {

    if (myType == null) {
      throw new FederationPolicyInitializationException(
          "The myType parameter" + " should not be null.");
    }

    if (policyContext == null) {
      throw new FederationPolicyInitializationException(
          "The FederationPolicyInitializationContext provided is null. Cannot"
              + " reinitalize " + "successfully.");
    }

    if (policyContext.getFederationStateStoreFacade() == null) {
      throw new FederationPolicyInitializationException(
          "The FederationStateStoreFacade provided is null. Cannot"
              + " reinitalize successfully.");
    }

    if (policyContext.getFederationSubclusterResolver() == null) {
      throw new FederationPolicyInitializationException(
          "The FederationSubclusterResolver provided is null. Cannot"
              + " reinitalize successfully.");
    }

    if (policyContext.getSubClusterPolicyConfiguration() == null) {
      throw new FederationPolicyInitializationException(
          "The SubClusterPolicyConfiguration provided is null. Cannot "
              + "reinitalize successfully.");
    }

    String intendedType =
        policyContext.getSubClusterPolicyConfiguration().getType();

    if (!myType.equals(intendedType)) {
      throw new FederationPolicyInitializationException(
          "The FederationPolicyConfiguration carries a type (" + intendedType
              + ") different then mine (" + myType
              + "). Cannot reinitialize successfully.");
    }

  }

}
