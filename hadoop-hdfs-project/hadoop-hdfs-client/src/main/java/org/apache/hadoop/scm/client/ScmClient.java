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
package org.apache.hadoop.scm.client;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;

import java.io.IOException;

/**
 * The interface to call into underlying container layer.
 *
 * Written as interface to allow easy testing: implement a mock container layer
 * for standalone testing of CBlock API without actually calling into remote
 * containers. Actual container layer can simply re-implement this.
 *
 * NOTE this is temporarily needed class. When SCM containers are full-fledged,
 * this interface will likely be removed.
 */
@InterfaceStability.Unstable
public interface ScmClient {
  /**
   * Creates a Container on SCM and returns the pipeline.
   * @param containerId - String container ID
   * @return Pipeline
   * @throws IOException
   */
  Pipeline createContainer(String containerId) throws IOException;

  /**
   * Gets a container by Name -- Throws if the container does not exist.
   * @param containerId - String Container ID
   * @return Pipeline
   * @throws IOException
   */
  Pipeline getContainer(String containerId) throws IOException;

  /**
   * Delets an existing container.
   * @param pipeline - Pipeline that represents the container.
   * @throws IOException
   */
  void deleteContainer(Pipeline pipeline) throws IOException;

  /**
   * Gets the container size -- Computed by SCM from Container Reports.
   * @param pipeline - Pipeline
   * @return number of bytes used by this container.
   * @throws IOException
   */
  long getContainerSize(Pipeline pipeline) throws IOException;
}
