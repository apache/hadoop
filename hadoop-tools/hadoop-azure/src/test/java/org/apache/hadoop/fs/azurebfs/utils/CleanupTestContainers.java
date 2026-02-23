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

package org.apache.hadoop.fs.azurebfs.utils;

import java.net.HttpURLConnection;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.contracts.services.ContainerListEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.ContainerListResponseData;
import org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This looks like a test, but it is really a command to invoke to
 * clean up containers created in other test runs.
 *
 */
public class CleanupTestContainers extends AbstractAbfsIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(CleanupTestContainers.class);
  private static final String CONTAINER_PREFIX = "abfs-testcontainer-";

  public CleanupTestContainers() throws Exception {
    super();
  }

  /**
   * Deletes a container using the Blob service endpoint.
   *
   * <p>
   * This method treats container deletion as idempotent:
   * <ul>
   *   <li>HTTP 202 (Accepted) indicates the delete request was accepted</li>
   *   <li>HTTP 404 (Not Found) indicates the container does not exist and is
   *       treated as a successful cleanup outcome</li>
   * </ul>
   * </p>
   *
   * @param blobClient ABFS Blob client used to issue the delete request
   * @param container name of the container to delete
   * @param tracingContext tracing context for the REST call
   * @return {@code true} if the container was deleted or did not exist,
   *         {@code false} otherwise
   * @throws Exception if the delete operation fails with a non-idempotent error
   */
  private boolean deleteContainer(
      AbfsBlobClient blobClient,
      String container,
      TracingContext tracingContext) throws Exception {
    AbfsRestOperation op =
        blobClient.deleteContainer(container, tracingContext);
    int status = op.getResult().getStatusCode();
    // Azure Blob semantics:
    // 202 = delete accepted
    // 404 = already deleted (idempotent success)
    return status == HttpURLConnection.HTTP_ACCEPTED
        || status == HttpURLConnection.HTTP_NOT_FOUND;
  }

  @Test
  public void testDeleteContainers() throws Throwable {
    final AbfsBlobClient blobClient =
        getAbfsStore(getFileSystem()).getClientHandler().getBlobClient();
    final TracingContext tracingContext =
        getTestTracingContext(getFileSystem(), true);
    String continuation = null;
    int deleted = 0;
    int examined = 0;
    do {
      final ContainerListResponseData response =
          blobClient.listContainers(CONTAINER_PREFIX, continuation, tracingContext);
      continuation = response.getContinuationToken();
      for (ContainerListEntrySchema entry : response.getContainers()) {
        final String containerName = entry.getName();
        examined++;
        LOG.info("Deleting test container {}", containerName);
        if (deleteContainer(blobClient, containerName, tracingContext)) {
          deleted++;
        }
      }
    } while (continuation != null && !continuation.isEmpty());
    LOG.info("Summary: Examined {} containers, deleted {} test containers",
        examined, deleted);
    // Cleanup tests should not fail if nothing exists
    assertThat(deleted)
        .as("Cleanup completed without errors")
        .isGreaterThanOrEqualTo(0);
  }
}
