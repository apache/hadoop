/**
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

import java.io.Closeable;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;

/**
 * Service to download container data from other datanodes.
 * <p>
 * The implementation of this interface should copy the raw container data in
 * compressed form to working directory.
 * <p>
 * A smart implementation would use multiple sources to do parallel download.
 */
public interface ContainerDownloader extends Closeable {

  CompletableFuture<Path> getContainerDataFromReplicas(long containerId,
      List<DatanodeDetails> sources);

}
