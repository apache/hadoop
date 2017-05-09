/**
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

package org.apache.hadoop.scm.protocol;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.scm.client.ScmClient;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;

/**
 * ContainerLocationProtocol is used by an HDFS node to find the set of nodes
 * that currently host a container.
 */
public interface StorageContainerLocationProtocol {

  /**
   * Find the set of nodes that currently host the container of an object, as
   * identified by the object key hash.  This method supports batch lookup by
   * passing multiple key hashes.
   *
   * @param keys batch of object keys to find
   * @return located containers for each object key
   * @throws IOException if there is any failure
   */
  Set<LocatedContainer> getStorageContainerLocations(Set<String> keys)
      throws IOException;

  /**
   * Asks SCM where a container should be allocated. SCM responds with the
   * set of datanodes that should be used creating this container.
   * @param containerName - Name of the container.
   * @return Pipeline.
   * @throws IOException
   */
  Pipeline allocateContainer(String containerName) throws IOException;

  /**
   * Asks SCM where a container should be allocated. SCM responds with the
   * set of datanodes that should be used creating this container.
   * @param containerName - Name of the container.
   * @param replicationFactor - replication factor.
   * @return Pipeline.
   * @throws IOException
   */
  Pipeline allocateContainer(String containerName,
      ScmClient.ReplicationFactor replicationFactor) throws IOException;

  /**
   * Ask SCM the location of the container. SCM responds with a group of
   * nodes where this container and its replicas are located.
   *
   * @param containerName - Name of the container.
   * @return Pipeline - the pipeline where container locates.
   * @throws IOException
   */
  Pipeline getContainer(String containerName) throws IOException;

  /**
   * Deletes a container in SCM.
   *
   * @param containerName
   * @throws IOException
   *   if failed to delete the container mapping from db store
   *   or container doesn't exist.
   */
  void deleteContainer(String containerName) throws IOException;
}
