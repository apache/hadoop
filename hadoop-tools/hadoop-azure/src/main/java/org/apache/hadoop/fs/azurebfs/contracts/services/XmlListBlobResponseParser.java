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

package org.apache.hadoop.fs.azurebfs.contracts.services;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

import javax.xml.parsers.SAXParser;

import org.xml.sax.SAXException;

import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_BLOB_LIST_PARSING;

/**
 * ListBlobs response parser implementation for the XML response returned by the
 * Blob Endpoint. The parser drives a SAX parse of the response stream using
 * {@link BlobListXmlParser} and produces a {@link BlobListResultSchema}.
 * <p>
 * SAX specific handling is confined to this class; the output contract is
 * {@link BlobListResultSchema} so existing listing behavior is preserved. The
 * {@link SAXParser} is provided lazily via a {@link Supplier} so that a pooled
 * (for example {@link ThreadLocal}) instance can be reused across calls without
 * this parser owning its lifecycle.
 */
public class XmlListBlobResponseParser implements ListBlobResponseParser {

  /**
   * Base URL for which the ListBlobs API is called, used to build the absolute
   * paths of the listed entries.
   */
  private final String baseUrl;

  /**
   * Supplier of a reusable {@link SAXParser}. Invoked once per parse so that a
   * pooled instance can be shared without transferring ownership to this class.
   */
  private final Supplier<SAXParser> saxParserSupplier;

  public XmlListBlobResponseParser(final String baseUrl,
      final Supplier<SAXParser> saxParserSupplier) {
    this.baseUrl = baseUrl;
    this.saxParserSupplier = saxParserSupplier;
  }

  @Override
  public BlobListResultSchema parse(final InputStream responseStream)
      throws IOException {
    final SAXParser saxParser = saxParserSupplier.get();
    saxParser.reset();
    final BlobListResultSchema listResultSchema = new BlobListResultSchema();
    try {
      saxParser.parse(responseStream,
          new BlobListXmlParser(listResultSchema, baseUrl));
    } catch (SAXException ex) {
      // Normalize the format-specific parse failure to the parser contract.
      throw new IOException(ex);
    }
    return listResultSchema;
  }

  @Override
  public String getParsingErrorMessage() {
    return ERR_BLOB_LIST_PARSING;
  }
}
