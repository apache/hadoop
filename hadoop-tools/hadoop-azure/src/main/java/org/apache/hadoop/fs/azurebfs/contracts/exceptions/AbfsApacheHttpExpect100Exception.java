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

import org.apache.http.HttpResponse;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EXPECT_100_JDK_ERROR;

/**
 * Exception that marks expect100 handshake error. This exception is thrown when
 * the expect100 handshake fails with ADLS server sending 4xx or 5xx status code.
 */
public class AbfsApacheHttpExpect100Exception extends HttpResponseException {

  public AbfsApacheHttpExpect100Exception(final HttpResponse httpResponse) {
    super(EXPECT_100_JDK_ERROR, httpResponse);
  }
}
