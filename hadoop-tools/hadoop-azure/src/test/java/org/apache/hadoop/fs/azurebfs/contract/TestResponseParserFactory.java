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

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.junit.jupiter.api.Test;

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
    } catch (Exception e) {
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
  public void testArrowContentTypeIsCaseInsensitive() {
    assertThat(ResponseParserFactory.isArrowResponse(
        "APPLICATION/VND.APACHE.ARROW.STREAM")).isTrue();
  }

  @Test
  public void testXmlContentTypeDoesNotSelectArrow() {
    assertThat(ResponseParserFactory.isArrowResponse(APPLICATION_XML)).isFalse();
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
}
