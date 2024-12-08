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

package org.apache.hadoop.fs.s3a.impl.model;

import java.io.IOException;

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
public interface ObjectInputStreamFactory extends Service {

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

}

