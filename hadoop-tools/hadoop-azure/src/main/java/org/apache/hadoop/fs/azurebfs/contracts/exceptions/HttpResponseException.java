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

package org.apache.hadoop.fs.azurebfs.contracts.exceptions;

import java.io.IOException;
import java.util.Objects;

import org.apache.http.HttpResponse;

/**
 * Encapsulates an exception thrown from ApacheHttpClient response parsing.
 */
public class HttpResponseException extends IOException {
  private final HttpResponse httpResponse;
  public HttpResponseException(final String s, final HttpResponse httpResponse) {
    super(s);
    Objects.requireNonNull(httpResponse, "httpResponse should be non-null");
    this.httpResponse = httpResponse;
  }

  public HttpResponse getHttpResponse() {
    return httpResponse;
  }
}
