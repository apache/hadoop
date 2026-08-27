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
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsDriverException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contract.ArrowListBlobTestStreams;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPLICATION_APACHE_ARROW_STREAM;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPLICATION_XML;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.ACCEPT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_TYPE;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_ARROW_LIST_PARSING;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_BLOB_LIST_PARSING;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.withSettings;

/**
 * Unit tests for the Photon (Apache Arrow based ListBlob) request header
 * behavior of {@link AbfsBlobClient#applyPhotonRequestHeadersIfEnabled(List)}.
 */
public class TestAbfsBlobClientPhotonHeaders {

  private static final long ARROW_MEMORY_LIMIT = 256L * 1024 * 1024;

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
        .filter(header -> ACCEPT.equalsIgnoreCase(header.getName()))
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
    // The condition is !isPhotonEnabled() || getIsNamespaceEnabled(): when
    // Photon is off the namespace probe (a potential live network call) must be
    // short-circuited, otherwise every listPath on an existing customer would
    // trigger it.
    verify(client, never()).getIsNamespaceEnabled();
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
    byte[] arrowStream = ArrowListBlobTestStreams.overAllocatorLimitNameStream();

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
   * Mirror of {@link #testArrowOverAllocatorLimitSurfacesDriverException()} for
   * the XML path: an XML Content-Type with a malformed body must surface the XML
   * parsing error, not the Arrow one. Without this a regression that always
   * selected the Arrow parser would pass every other test in this file.
   */
  @Test
  public void testXmlMalformedBodySurfacesXmlDriverException() throws Exception {
    AbfsConfiguration configuration = mock(AbfsConfiguration.class);
    doReturn(ARROW_MEMORY_LIMIT).when(configuration).getPhotonArrowMemoryLimit();

    AbfsBlobClient client = mock(AbfsBlobClient.class,
        withSettings().defaultAnswer(CALLS_REAL_METHODS));
    doReturn(configuration).when(client).getAbfsConfiguration();
    doReturn(new URL("https://account.blob.core.windows.net/container"))
        .when(client).getBaseUrl();
    injectSaxParser(client);

    AbfsHttpOperation result = mock(AbfsHttpOperation.class);
    doReturn(APPLICATION_XML)
        .when(result).getResponseHeaderIgnoreCase(CONTENT_TYPE);
    doReturn(new ByteArrayInputStream(
        "<EnumerationResults><Blobs><Blob>".getBytes(StandardCharsets.UTF_8)))
        .when(result).getListResultStream();

    AbfsDriverException ex = intercept(AbfsDriverException.class,
        () -> client.parseListPathResults(result, null));
    assertThat(ex.getErrorMessage())
        .as("a malformed XML body must surface the XML parsing error")
        .contains(ERR_BLOB_LIST_PARSING)
        .doesNotContain(ERR_ARROW_LIST_PARSING);
  }

  /**
   * Verify a pre-existing Accept header written in a different casing
   * ({@code accept}) is still removed, since production removes with
   * {@code equalsIgnoreCase}. Proves the removal is genuinely case-insensitive.
   */
  @Test
  public void testAcceptHeaderRemovalIsCaseInsensitive() throws Exception {
    AbfsBlobClient client = clientWithPhoton(true);
    List<AbfsHttpHeader> headers = new ArrayList<>();
    headers.add(new AbfsHttpHeader("accept", "application/json, application/xml"));

    boolean photonRequested = client.applyPhotonRequestHeadersIfEnabled(headers);

    assertThat(photonRequested).isTrue();
    long acceptCount = headers.stream()
        .filter(header -> ACCEPT.equalsIgnoreCase(header.getName()))
        .count();
    assertThat(acceptCount)
        .as("the pre-existing lower-case accept header must be removed")
        .isEqualTo(1);
    assertThat(acceptValue(headers))
        .isEqualTo(APPLICATION_APACHE_ARROW_STREAM + ", " + APPLICATION_XML);
  }

  /**
   * Verify the Arrow Accept header is added even when the request carried no
   * Accept header to remove (e.g. if {@code createDefaultHeaders()} stops
   * setting one).
   */
  @Test
  public void testArrowAcceptAddedWhenNoExistingHeader() throws Exception {
    AbfsBlobClient client = clientWithPhoton(true);
    List<AbfsHttpHeader> headers = new ArrayList<>();

    boolean photonRequested = client.applyPhotonRequestHeadersIfEnabled(headers);

    assertThat(photonRequested).isTrue();
    assertThat(acceptValue(headers))
        .as("Arrow Accept must be added even when there was none to remove")
        .isEqualTo(APPLICATION_APACHE_ARROW_STREAM + ", " + APPLICATION_XML);
  }

  /**
   * Verify a namespace-detection failure while Photon is enabled propagates out
   * of {@code applyPhotonRequestHeadersIfEnabled} (the method declares
   * {@link AzureBlobFileSystemException}) rather than being swallowed - pinning
   * the intended behaviour for a failing namespace probe.
   */
  @Test
  public void testNamespaceProbeFailurePropagatesWhenPhotonEnabled()
      throws Exception {
    AbfsConfiguration configuration = mock(AbfsConfiguration.class);
    doReturn(true).when(configuration).isPhotonEnabled();
    AbfsBlobClient client = mock(AbfsBlobClient.class,
        withSettings().defaultAnswer(CALLS_REAL_METHODS));
    doReturn(configuration).when(client).getAbfsConfiguration();
    AzureBlobFileSystemException failure =
        new AbfsDriverException("namespace probe failed", new IOException());
    doThrow(failure).when(client).getIsNamespaceEnabled();

    AzureBlobFileSystemException thrown = intercept(
        AzureBlobFileSystemException.class,
        () -> client.applyPhotonRequestHeadersIfEnabled(defaultHeaders()));
    assertThat(thrown).isSameAs(failure);
  }

  /**
   * Inject a working {@link SAXParser} {@link ThreadLocal} into a Mockito mock
   * of {@link AbfsBlobClient}. Mocks skip field initializers, so the real
   * {@code saxParserThreadLocal} field is {@code null}; the XML parse path needs
   * it populated.
   */
  private static void injectSaxParser(final AbfsBlobClient client)
      throws Exception {
    Field field = AbfsBlobClient.class.getDeclaredField("saxParserThreadLocal");
    field.setAccessible(true);
    ThreadLocal<SAXParser> threadLocal = ThreadLocal.withInitial(() -> {
      try {
        return SAXParserFactory.newInstance().newSAXParser();
      } catch (ParserConfigurationException | SAXException e) {
        throw new RuntimeException(e);
      }
    });
    field.set(client, threadLocal);
  }
}
