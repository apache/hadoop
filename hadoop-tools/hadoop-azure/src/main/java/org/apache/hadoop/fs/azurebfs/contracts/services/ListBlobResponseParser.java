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

/**
 * Parser abstraction for ListBlobs responses returned by the Blob Endpoint.
 * <p>
 * A parser consumes the raw response stream returned by the ListBlobs API and
 * produces a {@link BlobListResultSchema}. Keeping the output contract as
 * {@link BlobListResultSchema} allows the downstream FileStatus conversion path
 * to remain unchanged regardless of the on-the-wire response format. Concrete
 * implementations exist for the XML response and for the Apache Arrow (Photon)
 * response; the appropriate implementation is selected by
 * {@link ResponseParserFactory} based on the response Content-Type.
 * <p>
 * Implementations must not leak format-specific (e.g. Arrow, SAX) objects
 * outside the parsing layer.
 */
public interface ListBlobResponseParser {

  /**
   * Parse the given response stream into a {@link BlobListResultSchema}.
   *
   * @param responseStream the raw response stream returned by the service.
   * @return the parsed listing result.
   * @throws IOException if the stream cannot be read or parsed.
   */
  BlobListResultSchema parse(InputStream responseStream) throws IOException;

  /**
   * Error message describing a parsing failure for this response format. It is
   * used to build the driver exception when {@link #parse(InputStream)} fails,
   * so that the surfaced error reflects the format that was actually parsed.
   *
   * @return the format-specific parsing error message.
   */
  String getParsingErrorMessage();
}
