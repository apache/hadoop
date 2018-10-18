/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.container.replication;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Contract to prepare provide the container in binary form..
 * <p>
 * Prepare will be called when container is closed. An implementation could
 * precache any binary representation of a container and store the pre packede
 * images.
 */
public interface ContainerReplicationSource {

  /**
   * Prepare for the replication.
   *
   * @param containerId The name of the container the package.
   */
  void prepare(long containerId);

  /**
   * Copy the container data to an output stream.
   *
   * @param containerId Container to replicate
   * @param destination   The destination stream to copy all the container data.
   * @throws IOException
   */
  void copyData(long containerId, OutputStream destination)
      throws IOException;

}
