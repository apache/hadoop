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

import java.io.IOException;

import software.amazon.awssdk.services.s3.S3AsyncClient;

import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.service.Service;

/**
 * A Factory for {@link ObjectInputStream} streams.
 * <p>
 * This class is instantiated during initialization of
 * {@code S3AStore}, it then follows the same service
 * lifecycle.
 * <p>
 * Note for maintainers: do try and keep this mostly stable.
 * If new parameters need to be added, expand the
 * {@link ObjectReadParameters} class, rather than change the
 * interface signature.
 */
public interface ObjectInputStreamFactory
    extends Service, StreamCapabilities {

  /**
   * Set extra initialization parameters.
   * This MUST ONLY be invoked between {@code init()}
   * and {@code start()}.
   * @param factoryBindingParameters parameters for the factory binding
   * @throws IOException if IO problems.
   */
  void bind(FactoryBindingParameters factoryBindingParameters) throws IOException;

  /**
   * Create a new input stream.
   * There is no requirement to actually contact the store; this is generally done
   * lazily.
   * @param parameters parameters.
   * @return the input stream
   * @throws IOException problem creating the stream.
   */
  ObjectInputStream readObject(ObjectReadParameters parameters)
      throws IOException;

  /**
   * Get requirements from the factory which then tune behavior
   * elsewhere in the system.
   * @return the count of background threads.
   */
  StreamFactoryRequirements factoryRequirements();

  /**
   * Get the input stream type.
   * @return the specific stream type this factory produces.
   */
  InputStreamType streamType();

  /**
   * Callbacks for stream factories.
   */
  interface StreamFactoryCallbacks {

    /**
     * Get the Async S3Client, raising a failure to create as an IOException.
     * @param requireCRT is the CRT required.
     * @return the Async S3 client
     * @throws IOException failure to create the client.
     */
    S3AsyncClient getOrCreateAsyncClient(boolean requireCRT) throws IOException;
  }
}

