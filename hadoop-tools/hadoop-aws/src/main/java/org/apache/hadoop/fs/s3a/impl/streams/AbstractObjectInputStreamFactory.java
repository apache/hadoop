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

import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.Preconditions;

import static org.apache.hadoop.util.StringUtils.toLowerCase;

/**
 * Base implementation of {@link ObjectInputStreamFactory}.
 */
public abstract class AbstractObjectInputStreamFactory extends AbstractService
    implements ObjectInputStreamFactory {

  /**
   * Parameters passed down in
   * {@link #bind(FactoryBindingParameters)}.
   */
  private FactoryBindingParameters bindingParameters;

  /**
   * Callbacks.
   */
  private StreamFactoryCallbacks callbacks;

  /**
   * Constructor.
   * @param name service name.
   */
  protected AbstractObjectInputStreamFactory(final String name) {
    super(name);
  }

  /**
   * Bind to the callbacks.
   * <p>
   * The base class checks service state then stores
   * the callback interface.
   * @param factoryBindingParameters parameters for the factory binding
   */
  @Override
  public void bind(final FactoryBindingParameters factoryBindingParameters) {
    // must be on be invoked during service initialization
    Preconditions.checkState(isInState(STATE.INITED),
        "Input Stream factory %s is in wrong state: %s",
        this, getServiceState());
    bindingParameters = factoryBindingParameters;
    callbacks = bindingParameters.callbacks();
  }

  /**
   * Return base capabilities of all stream factories,
   * defining what the base ObjectInputStream class does.
   * This also includes the probe for stream type capability.
   * @param capability string to query the stream support for.
   * @return true if implemented
   */
  @Override
  public boolean hasCapability(final String capability) {
    switch (toLowerCase(capability)) {
    case StreamCapabilities.IOSTATISTICS:
    case StreamStatisticNames.STREAM_LEAKS:
      return true;
    default:
      // dynamic probe for the name of this stream
      return streamType().capability().equals(capability);
    }
  }

  /**
   * Get the factory callbacks.
   * @return callbacks.
   */
  protected StreamFactoryCallbacks callbacks() {
    return callbacks;
  }
}
