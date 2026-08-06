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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsDriverException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPLICATION_APACHE_ARROW_STREAM;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPLICATION_XML;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.ACCEPT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_TYPE;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_ARROW_LIST_PARSING;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.withSettings;

/**
 * Unit tests for the Photon (Apache Arrow based ListBlob) request header
 * behavior of {@link AbfsBlobClient#applyPhotonRequestHeadersIfEnabled(List)}.
 */
public class TestAbfsBlobClientPhotonHeaders {

  /** Row count large enough to exhaust the tiny allocator limit. */
  private static final int OVER_LIMIT_ROW_COUNT = 2000;

  private AbfsBlobClient clientWithPhoton(final boolean photonEnabled) {
    return clientWithPhoton(photonEnabled, false);
  }

  private AbfsBlobClient clientWithPhoton(final boolean photonEnabled,
      final boolean namespaceEnabled) {
    AbfsConfiguration configuration = mock(AbfsConfiguration.class);
    doReturn(photonEnabled).when(configuration).isPhotonEnabled();
    AbfsBlobClient client = mock(AbfsBlobClient.class,
        withSettings().defaultAnswer(CALLS_REAL_METHODS));
    doReturn(configuration).when(client).getAbfsConfiguration();
    try {
      doReturn(namespaceEnabled).when(client).getIsNamespaceEnabled();
    } catch (AzureBlobFileSystemException e) {
      throw new RuntimeException(e);
    }
    return client;
  }

  private List<AbfsHttpHeader> defaultHeaders() {
    List<AbfsHttpHeader> headers = new ArrayList<>();
    headers.add(new AbfsHttpHeader(ACCEPT, "application/json, application/xml"));
    return headers;
  }

  private String acceptValue(final List<AbfsHttpHeader> headers) {
    return headers.stream()
        .filter(header -> ACCEPT.equals(header.getName()))
        .map(AbfsHttpHeader::getValue)
        .findFirst()
        .orElse(null);
  }

  /**
   * Verify Arrow request headers are added when Photon is enabled.
   */
  @Test
  public void testAcceptHeaderOverriddenWhenPhotonEnabled() throws Exception {
    AbfsBlobClient client = clientWithPhoton(true);
    List<AbfsHttpHeader> headers = defaultHeaders();

    boolean photonRequested = client.applyPhotonRequestHeadersIfEnabled(headers);

    assertThat(photonRequested)
        .as("Arrow should be requested when Photon is enabled")
        .isTrue();
    long acceptCount = headers.stream()
        .filter(header -> ACCEPT.equals(header.getName()))
        .count();
    assertThat(acceptCount).isEqualTo(1);
    assertThat(acceptValue(headers))
        .isEqualTo(APPLICATION_APACHE_ARROW_STREAM + ", " + APPLICATION_XML);
  }

  /**
   * Verify Arrow is not requested on a hierarchical-namespace (HNS) account
   * even when Photon is enabled, since the Blob endpoint rejects an Arrow
   * ListBlobs request on HNS with a 409 that the XML fallback cannot recover.
   */
  @Test
  public void testAcceptHeaderUnchangedOnHnsAccount() throws Exception {
    AbfsBlobClient client = clientWithPhoton(true, true);
    List<AbfsHttpHeader> headers = defaultHeaders();

    boolean photonRequested = client.applyPhotonRequestHeadersIfEnabled(headers);

    assertThat(photonRequested)
        .as("Arrow should not be requested on an HNS account")
        .isFalse();
    assertThat(acceptValue(headers))
        .isEqualTo("application/json, application/xml");
  }

  /**
   * Verify the existing Accept header is left unchanged when Photon is disabled.
   */
  @Test
  public void testAcceptHeaderUnchangedWhenPhotonDisabled() throws Exception {
    AbfsBlobClient client = clientWithPhoton(false);
    List<AbfsHttpHeader> headers = defaultHeaders();

    boolean photonRequested = client.applyPhotonRequestHeadersIfEnabled(headers);

    assertThat(photonRequested)
        .as("Arrow should not be requested when Photon is disabled")
        .isFalse();
    assertThat(acceptValue(headers))
        .isEqualTo("application/json, application/xml");
  }

  /**
   * Verify that an Arrow ListBlobs response whose parsing exceeds the configured
   * allocator memory limit surfaces an {@link AbfsDriverException} carrying the
   * Arrow parsing error message, exercising the Arrow branch of
   * {@link AbfsBlobClient#parseListPathResults} end to end.
   */
  @Test
  public void testArrowOverAllocatorLimitSurfacesDriverException()
      throws Exception {
    byte[] arrowStream = buildArrowNameStream(OVER_LIMIT_ROW_COUNT);

    AbfsConfiguration configuration = mock(AbfsConfiguration.class);
    doReturn(1024L).when(configuration).getPhotonArrowMemoryLimit();

    AbfsBlobClient client = mock(AbfsBlobClient.class,
        withSettings().defaultAnswer(CALLS_REAL_METHODS));
    doReturn(configuration).when(client).getAbfsConfiguration();
    doReturn(new URL("https://account.blob.core.windows.net/container"))
        .when(client).getBaseUrl();

    AbfsHttpOperation result = mock(AbfsHttpOperation.class);
    doReturn(APPLICATION_APACHE_ARROW_STREAM)
        .when(result).getResponseHeaderIgnoreCase(CONTENT_TYPE);
    doReturn(new ByteArrayInputStream(arrowStream))
        .when(result).getListResultStream();

    AbfsDriverException ex = intercept(AbfsDriverException.class,
        () -> client.parseListPathResults(result, null));
    assertThat(ex.getErrorMessage()).contains(ERR_ARROW_LIST_PARSING);
  }

  /**
   * Build a valid single-column ("Name") Arrow IPC stream carrying the given
   * number of rows, used to drive the allocator limit test.
   */
  private static byte[] buildArrowNameStream(final int rows) throws IOException {
    Field nameField = new Field("Name",
        FieldType.nullable(new ArrowType.Utf8()), null);
    Schema schema = new Schema(Collections.singletonList(nameField));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
      VarCharVector nameVector = (VarCharVector) root.getVector("Name");
      nameVector.allocateNew(rows);
      for (int i = 0; i < rows; i++) {
        nameVector.setSafe(i, ("some-reasonably-long-blob-name-" + i)
            .getBytes(StandardCharsets.UTF_8));
      }
      root.setRowCount(rows);
      writer.start();
      writer.writeBatch();
      writer.end();
    }
    // Read the bytes only after the writer has been closed so the Arrow stream
    // is guaranteed to be fully flushed and terminated.
    return out.toByteArray();
  }
}
