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

package org.apache.hadoop.fs.azurebfs.contract;

import java.util.function.Supplier;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;

import org.apache.hadoop.fs.azurebfs.contracts.services.ArrowListBlobParser;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListBlobResponseParser;
import org.apache.hadoop.fs.azurebfs.contracts.services.ResponseParserFactory;
import org.apache.hadoop.fs.azurebfs.contracts.services.XmlListBlobResponseParser;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPLICATION_APACHE_ARROW_STREAM;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPLICATION_XML;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ResponseParserFactory} parser selection based on the
 * response Content-Type.
 */
public class TestResponseParserFactory {

  private static final String BASE_URL =
      "https://account.blob.core.windows.net/container";

  private static final long ARROW_MEMORY_LIMIT = 256L * 1024 * 1024;

  private static final Supplier<SAXParser> SAX_PARSER_SUPPLIER = () -> {
    try {
      return SAXParserFactory.newInstance().newSAXParser();
    } catch (ParserConfigurationException | SAXException e) {
      throw new RuntimeException(e);
    }
  };

  @Test
  public void testArrowContentTypeSelectsArrow() {
    assertThat(ResponseParserFactory.isArrowResponse(
        APPLICATION_APACHE_ARROW_STREAM)).isTrue();
  }

  @Test
  public void testArrowContentTypeWithCharsetSelectsArrow() {
    assertThat(ResponseParserFactory.isArrowResponse(
        "application/vnd.apache.arrow.stream; charset=utf-8")).isTrue();
  }

  @Test
  public void testArrowContentTypeWithSpaceBeforeSemicolonSelectsArrow() {
    // RFC 9110 permits optional whitespace around the ';' separating the media
    // type from its parameters. Such a header must still route to Arrow rather
    // than fall through to the XML parser (which would then try to SAX-parse an
    // Arrow body).
    assertThat(ResponseParserFactory.isArrowResponse(
        "application/vnd.apache.arrow.stream ; charset=utf-8")).isTrue();
  }

  @Test
  public void testArrowContentTypeIsCaseInsensitive() {
    assertThat(ResponseParserFactory.isArrowResponse(
        "APPLICATION/VND.APACHE.ARROW.STREAM")).isTrue();
  }

  @Test
  public void testArrowContentTypeCaseInsensitiveWithParametersSelectsArrow() {
    // The combination a real service response is most likely to send: mixed
    // case media type carrying parameters.
    assertThat(ResponseParserFactory.isArrowResponse(
        "Application/VND.Apache.Arrow.Stream; charset=UTF-8")).isTrue();
  }

  @Test
  public void testArrowStructuredSuffixDoesNotSelectArrow() {
    // Only the exact negotiated media type qualifies. A structured suffix such
    // as "+ipc" is a different media type and must fall back to the XML parser,
    // matching the javadoc which no longer claims structured suffixes are
    // tolerated.
    assertThat(ResponseParserFactory.isArrowResponse(
        "application/vnd.apache.arrow.stream+ipc")).isFalse();
  }

  @Test
  public void testXmlContentTypeDoesNotSelectArrow() {
    assertThat(ResponseParserFactory.isArrowResponse(APPLICATION_XML)).isFalse();
  }

  @Test
  public void testUnrelatedContentTypeContainingArrowDoesNotSelectArrow() {
    // Content types that merely contain the word "arrow" (e.g. a different
    // vendor media type or a parameter value) must not be routed to the Arrow
    // parser; only the negotiated Arrow IPC stream media type qualifies.
    assertThat(ResponseParserFactory.isArrowResponse(
        "application/x-arrowhead")).isFalse();
    assertThat(ResponseParserFactory.isArrowResponse(
        "application/json; profile=arrow")).isFalse();
  }

  @Test
  public void testNullContentTypeFallsBackToXml() {
    assertThat(ResponseParserFactory.isArrowResponse(null)).isFalse();
  }

  @Test
  public void testEmptyContentTypeFallsBackToXml() {
    assertThat(ResponseParserFactory.isArrowResponse("")).isFalse();
  }

  @Test
  public void testGetParserReturnsArrowParserForArrowContentType() {
    ListBlobResponseParser parser = ResponseParserFactory.getParser(
        APPLICATION_APACHE_ARROW_STREAM, BASE_URL, SAX_PARSER_SUPPLIER,
        ARROW_MEMORY_LIMIT);
    assertThat(parser).isInstanceOf(ArrowListBlobParser.class);
  }

  @Test
  public void testGetParserReturnsXmlParserForXmlContentType() {
    ListBlobResponseParser parser = ResponseParserFactory.getParser(
        APPLICATION_XML, BASE_URL, SAX_PARSER_SUPPLIER, ARROW_MEMORY_LIMIT);
    assertThat(parser).isInstanceOf(XmlListBlobResponseParser.class);
  }

  @Test
  public void testGetParserReturnsXmlParserForNullContentType() {
    ListBlobResponseParser parser = ResponseParserFactory.getParser(
        null, BASE_URL, SAX_PARSER_SUPPLIER, ARROW_MEMORY_LIMIT);
    assertThat(parser).isInstanceOf(XmlListBlobResponseParser.class);
  }

  @Test
  public void testSaxSupplierNotInvokedForArrowContentType() {
    // The SAX parser supplier creates a SAXParser on every invocation, so the
    // Arrow path must never call it. A counting supplier locks in that lazy
    // contract so a future change that eagerly resolves the XML parser cannot
    // regress it silently.
    AtomicInteger invocations = new AtomicInteger();
    Supplier<SAXParser> countingSupplier = () -> {
      invocations.incrementAndGet();
      return SAX_PARSER_SUPPLIER.get();
    };

    ListBlobResponseParser parser = ResponseParserFactory.getParser(
        APPLICATION_APACHE_ARROW_STREAM, BASE_URL, countingSupplier,
        ARROW_MEMORY_LIMIT);

    assertThat(parser).isInstanceOf(ArrowListBlobParser.class);
    assertThat(invocations.get())
        .as("SAX parser supplier must not be invoked on the Arrow path")
        .isZero();
  }
}
