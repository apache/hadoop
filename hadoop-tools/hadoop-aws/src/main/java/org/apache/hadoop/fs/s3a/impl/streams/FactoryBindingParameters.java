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

package org.apache.hadoop.fs.s3a.impl.streams;

import static java.util.Objects.requireNonNull;

/**
 * Parameters passed down to {@link ObjectInputStreamFactory#bind}.
 */
public class FactoryBindingParameters {

  /**
   * Callbacks which may be invoked by a stream factory directly.
   */
  private final ObjectInputStreamFactory.StreamFactoryCallbacks callbacks;

  /**
   * @param callbacks callback implementation.
   */
  public FactoryBindingParameters(final ObjectInputStreamFactory.StreamFactoryCallbacks callbacks) {
    this.callbacks = requireNonNull(callbacks);
  }

  /**
   * Callbacks which may be invoked by a stream factory directly.
   */
  ObjectInputStreamFactory.StreamFactoryCallbacks callbacks() {
    return callbacks;
  }
}
