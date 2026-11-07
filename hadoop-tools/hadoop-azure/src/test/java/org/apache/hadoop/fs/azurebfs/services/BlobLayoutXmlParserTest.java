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
import java.nio.charset.StandardCharsets;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutResponse;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutXmlParser;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link BlobLayoutXmlParser}, covering well-formed layout XML as
 * well as adverse responses: corrupted attributes (non-numeric / missing),
 * partial payloads (ranges without endpoints and vice-versa), empty / no-layout
 * responses and pagination markers. These document the exact behaviour the
 * read-path relies on when the service returns imperfect data.
 */
public class BlobLayoutXmlParserTest {

  /**
   * Parse the supplied XML into a {@link BlobLayoutResponse}.
   * @param xml the XML document to parse
   * @return the populated response
   * @throws Exception if the SAX parser fails
   */
  private BlobLayoutResponse parse(String xml) throws Exception {
    SAXParserFactory factory = SAXParserFactory.newInstance();
    SAXParser parser = factory.newSAXParser();
    BlobLayoutXmlParser handler = new BlobLayoutXmlParser();
    parser.parse(
        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)), handler);
    return handler.getResponse();
  }

  // ---------------------------------------------------------------------------
  // Well-formed responses.
  // ---------------------------------------------------------------------------

  /**
   * A complete, well-formed response must surface every range, every endpoint and
   * both pagination markers.
   */
  @Test
  public void testValidLayoutParsesRangesEndpointsAndMarkers() throws Exception {
    String xml = "<BlobLayout>"
        + "<Ranges>"
        + "<Range Start=\"0\" End=\"9\" EndpointIndex=\"0\" />"
        + "<Range Start=\"10\" End=\"19\" EndpointIndex=\"1\" />"
        + "</Ranges>"
        + "<Endpoints>"
        + "<Endpoint Index=\"0\" Value=\"host-a\" />"
        + "<Endpoint Index=\"1\" Value=\"host-b\" />"
        + "</Endpoints>"
        + "<NextMarker>page-2</NextMarker>"
        + "<MaxResults>5000</MaxResults>"
        + "</BlobLayout>";

    BlobLayoutResponse response = parse(xml);

    Assertions.assertThat(response.getRanges())
        .describedAs("All ranges must be parsed")
        .hasSize(2);
    Assertions.assertThat(response.getRanges().get(0))
        .isEqualTo(new BlobLayoutResponse.Range(0, 9, 0));
    Assertions.assertThat(response.getRanges().get(1))
        .isEqualTo(new BlobLayoutResponse.Range(10, 19, 1));
    Assertions.assertThat(response.getEndpoints())
        .describedAs("All endpoints must be parsed")
        .containsExactlyInAnyOrder(
            new BlobLayoutResponse.Endpoint(0, "host-a"),
            new BlobLayoutResponse.Endpoint(1, "host-b"));
    Assertions.assertThat(response.getNextMarker())
        .describedAs("NextMarker must be captured for pagination")
        .isEqualTo("page-2");
    Assertions.assertThat(response.getMaxResults()).isEqualTo("5000");
  }

  /**
   * A self-closed {@code <NextMarker />} yields an empty (not null) marker, which
   * the fetch loop treats as "no further pages".
   */
  @Test
  public void testEmptyNextMarkerIsBlankNotNull() throws Exception {
    String xml = "<BlobLayout>"
        + "<Ranges>"
        + "<Range Start=\"0\" End=\"9\" EndpointIndex=\"0\" />"
        + "</Ranges>"
        + "<Endpoints>"
        + "<Endpoint Index=\"0\" Value=\"host-a\" />"
        + "</Endpoints>"
        + "<NextMarker />"
        + "</BlobLayout>";

    BlobLayoutResponse response = parse(xml);

    Assertions.assertThat(response.getNextMarker())
        .describedAs("Self-closed NextMarker must parse to empty string")
        .isEqualTo("");
  }

  // ---------------------------------------------------------------------------
  // Empty / no-layout responses (data not distributed across layouts).
  // ---------------------------------------------------------------------------

  /**
   * When the service reports no distributed layout, the response has neither
   * ranges nor endpoints. This is the signal the cache uses to mark an entry
   * "unavailable" and fall back to host-URL fetches.
   */
  @Test
  public void testEmptyLayoutYieldsNoRangesOrEndpoints() throws Exception {
    String xml = "<BlobLayout><Ranges /><Endpoints /><NextMarker /></BlobLayout>";

    BlobLayoutResponse response = parse(xml);

    Assertions.assertThat(response.getRanges())
        .describedAs("No ranges expected for an empty layout")
        .isEmpty();
    Assertions.assertThat(response.getEndpoints())
        .describedAs("No endpoints expected for an empty layout")
        .isEmpty();
  }

  // ---------------------------------------------------------------------------
  // Partial responses.
  // ---------------------------------------------------------------------------

  /**
   * Ranges present without any endpoints is a partial response. The parser still
   * records the ranges; the cache layer later drops any range whose endpoint has
   * no host mapping, leaving the corresponding bytes as a gap to retry.
   */
  @Test
  public void testRangesWithoutEndpointsAreStillParsed() throws Exception {
    String xml = "<BlobLayout>"
        + "<Ranges>"
        + "<Range Start=\"0\" End=\"9\" EndpointIndex=\"0\" />"
        + "<Range Start=\"10\" End=\"19\" EndpointIndex=\"1\" />"
        + "</Ranges>"
        + "<Endpoints />"
        + "</BlobLayout>";

    BlobLayoutResponse response = parse(xml);

    Assertions.assertThat(response.getRanges())
        .describedAs("Ranges must be parsed even when endpoints are absent")
        .hasSize(2);
    Assertions.assertThat(response.getEndpoints()).isEmpty();
  }

  /**
   * Endpoints present without ranges is also partial; the endpoints are parsed but
   * there is nothing to cache.
   */
  @Test
  public void testEndpointsWithoutRangesAreStillParsed() throws Exception {
    String xml = "<BlobLayout>"
        + "<Ranges />"
        + "<Endpoints>"
        + "<Endpoint Index=\"0\" Value=\"host-a\" />"
        + "</Endpoints>"
        + "</BlobLayout>";

    BlobLayoutResponse response = parse(xml);

    Assertions.assertThat(response.getRanges()).isEmpty();
    Assertions.assertThat(response.getEndpoints())
        .containsExactly(new BlobLayoutResponse.Endpoint(0, "host-a"));
  }

  // ---------------------------------------------------------------------------
  // Corrupted responses.
  // ---------------------------------------------------------------------------

  /**
   * A non-numeric {@code Start} attribute is corrupt data. Parsing must fail loudly
   * (NumberFormatException) rather than silently producing a bogus range. The
   * read-path wraps this failure and falls back to a normal fetch.
   */
  @Test
  public void testNonNumericRangeAttributeThrows() {
    String xml = "<BlobLayout>"
        + "<Ranges>"
        + "<Range Start=\"not-a-number\" End=\"9\" EndpointIndex=\"0\" />"
        + "</Ranges>"
        + "</BlobLayout>";

    assertThatThrownBy(() -> parse(xml))
        .describedAs("Corrupt numeric range attribute must fail parsing")
        .isInstanceOf(NumberFormatException.class);
  }

  /**
   * A missing required {@code End} attribute makes {@code getValue} return null,
   * which {@code Long.parseLong} rejects. Treated the same as corrupt data.
   */
  @Test
  public void testMissingRangeAttributeThrows() {
    String xml = "<BlobLayout>"
        + "<Ranges>"
        + "<Range Start=\"0\" EndpointIndex=\"0\" />"
        + "</Ranges>"
        + "</BlobLayout>";

    assertThatThrownBy(() -> parse(xml))
        .describedAs("Missing numeric range attribute must fail parsing")
        .isInstanceOf(NumberFormatException.class);
  }

  /**
   * A non-numeric {@code Index} on an endpoint is corrupt and must fail parsing.
   */
  @Test
  public void testNonNumericEndpointIndexThrows() {
    String xml = "<BlobLayout>"
        + "<Endpoints>"
        + "<Endpoint Index=\"xyz\" Value=\"host-a\" />"
        + "</Endpoints>"
        + "</BlobLayout>";

    assertThatThrownBy(() -> parse(xml))
        .describedAs("Corrupt endpoint index must fail parsing")
        .isInstanceOf(NumberFormatException.class);
  }

  /**
   * Structurally malformed XML (an unclosed tag) must fail parsing rather than
   * return a half-built response.
   */
  @Test
  public void testMalformedXmlThrows() {
    String xml = "<BlobLayout><Ranges>"
        + "<Range Start=\"0\" End=\"9\" EndpointIndex=\"0\" />";

    assertThatThrownBy(() -> parse(xml))
        .describedAs("Truncated / malformed XML must fail parsing")
        .isInstanceOf(Exception.class);
  }
}
