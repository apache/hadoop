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

import java.util.Locale;
import java.util.function.Supplier;

import javax.xml.parsers.SAXParser;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CONTENT_TYPE_ARROW_TOKEN;

/**
 * Factory that selects the appropriate {@link ListBlobResponseParser} for a
 * ListBlobs response based on the response {@code Content-Type}.
 * <p>
 * ABFS requests Apache Arrow (Photon) responses but does not assume that the
 * service will honor the request. The actual response format is determined from
 * the returned Content-Type: an Arrow Content-Type selects the Photon
 * {@link ArrowListBlobParser}, while any other (or missing) Content-Type selects
 * the {@link XmlListBlobResponseParser}. Callers receive a
 * {@link ListBlobResponseParser} and drive parsing polymorphically, so format
 * selection is kept local to this factory and downstream code always receives a
 * {@link BlobListResultSchema}.
 */
public final class ResponseParserFactory {

  private ResponseParserFactory() {
  }

  /**
   * Select the parser to use for a ListBlobs response.
   *
   * @param contentType       the value of the response Content-Type header (may
   *                          be {@code null}).
   * @param baseUrl           base URL for which the ListBlobs API is called,
   *                          used to build absolute paths for listed entries.
   * @param saxParserSupplier supplier of a reusable {@link SAXParser}, used only
   *                          by the XML parser.
   * @param arrowMemoryLimit  maximum off-heap memory in bytes the Arrow
   *                          allocator may use, used only by the Arrow parser.
   * @return the {@link ListBlobResponseParser} matching the response format.
   */
  public static ListBlobResponseParser getParser(final String contentType,
      final String baseUrl, final Supplier<SAXParser> saxParserSupplier,
      final long arrowMemoryLimit) {
    if (isArrowResponse(contentType)) {
      return new ArrowListBlobParser(baseUrl, arrowMemoryLimit);
    }
    return new XmlListBlobResponseParser(baseUrl, saxParserSupplier);
  }

  /**
   * Determine whether the given response Content-Type indicates an Apache Arrow
   * (Photon) response.
   *
   * @param contentType the value of the response Content-Type header (may be
   *                    {@code null}).
   * @return {@code true} if the response should be parsed as Arrow.
   */
  public static boolean isArrowResponse(final String contentType) {
    return contentType != null
        && contentType.toLowerCase(Locale.ROOT).contains(CONTENT_TYPE_ARROW_TOKEN);
  }
}
