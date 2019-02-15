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

package org.apache.hadoop.ozone.s3.header;

import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.RequestScoped;

/**
 * Authentication Header parser to parse HttpHeader Authentication.
 */
@RequestScoped
public class AuthenticationHeaderParser {

  private final static Logger LOG = LoggerFactory.getLogger(
      AuthenticationHeaderParser.class);

  private String authHeader;
  private String accessKeyID;

  public void parse() throws OS3Exception {
    if (authHeader.startsWith("AWS4")) {
      LOG.debug("V4 Header {}", authHeader);
      AuthorizationHeaderV4 authorizationHeader = new AuthorizationHeaderV4(
          authHeader);
      accessKeyID = authorizationHeader.getAccessKeyID().toLowerCase();
    } else {
      LOG.debug("V2 Header {}", authHeader);
      AuthorizationHeaderV2 authorizationHeader = new AuthorizationHeaderV2(
          authHeader);
      accessKeyID = authorizationHeader.getAccessKeyID().toLowerCase();
    }
  }

  public boolean doesAuthenticationInfoExists() {
    return authHeader != null;
  }

  public String getAccessKeyID() throws OS3Exception {
    parse();
    return accessKeyID;
  }

  public void setAuthHeader(String header) {
    this.authHeader = header;
  }
}
