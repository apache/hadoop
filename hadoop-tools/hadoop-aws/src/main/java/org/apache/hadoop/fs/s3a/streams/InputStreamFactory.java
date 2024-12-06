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

package org.apache.hadoop.fs.s3a.streams;

import java.io.IOException;

import org.apache.hadoop.service.Service;

/**
 * A Factory for input streams.
 * <p>
 * This class is instantiated during initialization of
 * {@code S3AStore}, it then follows the same service
 * lifecycle.
 * <p>
 * Note for maintainers: do try and keep this mostly stable.
 * If new parameters need to be added, expand the
 * {@link FactoryStreamParameters} class, rather than change the
 * interface signature.
 */
public interface InputStreamFactory extends Service {

  /**
   * Create a new input stream.
   * @param parameters parameters.
   * @return the input stream
   * @throws problem creating the stream.
   */
  AbstractS3AInputStream create(FactoryStreamParameters parameters)
      throws IOException;

}

