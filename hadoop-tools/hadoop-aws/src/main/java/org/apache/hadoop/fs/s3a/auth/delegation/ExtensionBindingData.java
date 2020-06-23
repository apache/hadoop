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

/**
 * Class with nothing but binding configuration info.
 * This is passed down to the DTBinding implementation before
 * the service is started.
 * <p></p>
 * This is clearly overenginered for a single boolean, but given
 * that previous changes have caused linkage problems with external
 * implementations, doing it this way avoids changing the
 * signatures of implementations if new information
 * is passed in, while the builder does the same for tests.
 */
public class ExtensionBindingData {

  private ExtensionBindingData(final boolean secondaryBinding) {
    this.secondaryBinding = secondaryBinding;
  }

  /**
   * Flag to indicate this token binding was deployed as a secondary
   * token.
   * This is not expected to be of any importance to a token binding, but
   * it is passed down so that if an implementation does need to care,
   * it knows.
   */

  public final boolean secondaryBinding;

  public boolean isSecondaryBinding() {
    return secondaryBinding;
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
  public static class Builder {

    private Builder() {
    }

    private boolean secondaryBinding;

    public Builder setSecondaryBinding(final boolean sb) {
      this.secondaryBinding = sb;
      return this;
    }

    public ExtensionBindingData build() {
      return new ExtensionBindingData(secondaryBinding);
    }
  }

}
