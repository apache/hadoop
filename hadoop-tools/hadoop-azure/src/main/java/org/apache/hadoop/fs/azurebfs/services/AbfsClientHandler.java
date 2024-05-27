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
package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.util.Preconditions;

/**
 * AbfsClientHandler is a class that provides a way to get the AbfsClient
 * based on the service type.
 */
public class AbfsClientHandler {
  private AbfsServiceType defaultServiceType;
  private AbfsClient dfsAbfsClient;
  private AbfsClient blobAbfsClient;

  public AbfsClientHandler(AbfsServiceType defaultServiceType,
      AbfsClient dfsAbfsClient, AbfsClient blobAbfsClient) {
    this.blobAbfsClient = blobAbfsClient;
    this.dfsAbfsClient = dfsAbfsClient;
    this.defaultServiceType = defaultServiceType;
  }

  public AbfsClient getClient() {
    if (defaultServiceType == AbfsServiceType.DFS) {
      Preconditions.checkNotNull(dfsAbfsClient, "DFS client is not initialized");
      return dfsAbfsClient;
    } else {
      Preconditions.checkNotNull(dfsAbfsClient, "Blob client is not initialized");
      return blobAbfsClient;
    }
  }

  public AbfsClient getClient(AbfsServiceType serviceType) {
    if (serviceType == AbfsServiceType.DFS) {
      Preconditions.checkNotNull(dfsAbfsClient, "DFS client is not initialized");
      return dfsAbfsClient;
    } else {
      Preconditions.checkNotNull(dfsAbfsClient, "Blob client is not initialized");
      return blobAbfsClient;
    }
  }

  public AbfsDfsClient getDfsClient() {
    Preconditions.checkNotNull(dfsAbfsClient, "DFS client is not initialized");
    return (AbfsDfsClient) dfsAbfsClient;
  }

  public AbfsBlobClient getBlobClient() {
    Preconditions.checkNotNull(blobAbfsClient, "Blob client is not initialized");
    return (AbfsBlobClient) blobAbfsClient;
  }
}
