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

package org.apache.hadoop.fs.s3a.auth.delegation;

import org.apache.hadoop.fs.s3a.impl.StoreContext;

/**
 * Class with nothing but binding configuration info.
 * This is passed down to the DTBinding implementation before
 * the service is started.
 * <p></p>
 * This is clearly over-engineered, but given
 * that previous changes have caused linkage problems with external
 * implementations, doing it this way avoids changing the
 * signatures of implementations if new information
 * is passed in, while the builder does the same for tests.
 */
public final class ExtensionBindingData {

  /**
   * Flag to indicate this token binding was deployed as a secondary
   * token.
   * This is not expected to be of any importance to a token binding, but
   * it is passed down so that if an implementation does need to care,
   * it knows.
   */
  private final boolean secondaryBinding;

  private final StoreContext storeContext;

  private final DelegationOperations delegationOperations;

  private ExtensionBindingData(final Builder builder) {
    this.secondaryBinding = builder.secondaryBinding;
    this.delegationOperations = builder.delegationOperations;
    this.storeContext = builder.storeContext;
  }


  public boolean isSecondaryBinding() {
    return secondaryBinding;
  }

  public DelegationOperations getDelegationOperations() {
    return delegationOperations;
  }

  public StoreContext getStoreContext() {
    return storeContext;
  }

  /**
   * Create the builder.
   * @return the builder for this class
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for the binding information.
   */
  public static final class Builder {

    private Builder() {
    }

    private boolean secondaryBinding;

    private StoreContext storeContext;

    private DelegationOperations delegationOperations;


    public Builder withSecondaryBinding(final boolean sb) {
      this.secondaryBinding = sb;
      return this;
    }


    public Builder withStoreContext(final StoreContext context) {
      this.storeContext = context;
      return this;
    }

    public Builder withDelegationOperations(
        final DelegationOperations operations) {
      this.delegationOperations = operations;
      return this;
    }

    public ExtensionBindingData build() {
      return new ExtensionBindingData(this);
    }
  }

}
