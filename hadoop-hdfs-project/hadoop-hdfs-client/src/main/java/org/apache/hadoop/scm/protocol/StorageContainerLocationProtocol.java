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
import java.util.List;

import org.apache.hadoop.scm.client.ScmClient;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;

/**
 * ContainerLocationProtocol is used by an HDFS node to find the set of nodes
 * that currently host a container.
 */
public interface StorageContainerLocationProtocol {

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
   * Ask SCM a list of pipelines with a range of container names
   * and the limit of count.
   * Search container names between start name(exclusive), and
   * use prefix name to filter the result. the max size of the
   * searching range cannot exceed the value of count.
   *
   * @param startName start name, if null, start searching at the head.
   * @param prefixName prefix name, if null, then filter is disabled.
   * @param count count, if count < 0, the max size is unlimited.(
   *              Usually the count will be replace with a very big
   *              value instead of being unlimited in case the db is very big)
   *
   * @return a list of pipeline.
   * @throws IOException
   */
  List<Pipeline> listContainer(String startName, String prefixName, int count)
      throws IOException;

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
