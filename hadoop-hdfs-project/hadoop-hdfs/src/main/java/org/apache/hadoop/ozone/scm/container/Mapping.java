/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.scm.container;


import org.apache.hadoop.scm.container.common.helpers.Pipeline;

import java.io.Closeable;
import java.io.IOException;

/**
 * Mapping class contains the mapping from a name to a pipeline mapping. This is
 * used by SCM when allocating new locations and when looking up a key.
 */
public interface Mapping extends Closeable {
  /**
   * Returns the Pipeline from the container name.
   *
   * @param containerName - Name
   * @return - Pipeline that makes up this container.
   * @throws IOException
   */
  Pipeline getContainer(String containerName) throws IOException;

  /**
   * Allocates a new container for a given keyName.
   *
   * @param containerName - Name
   * @return - Pipeline that makes up this container.
   * @throws IOException
   */
  Pipeline allocateContainer(String containerName) throws IOException;
}
