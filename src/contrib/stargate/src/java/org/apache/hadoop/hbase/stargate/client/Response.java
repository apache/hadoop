/*
 * Copyright 2009 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.stargate.client;

import org.apache.commons.httpclient.Header;

public class Response {
  private int code;
  private Header[] headers;
  private byte[] body;

  /**
   * Constructor
   * @param code the HTTP response code
   */
  public Response(int code) {
    this(code, null, null);
  }

  /**
   * Constructor
   * @param code the HTTP response code
   * @param headers the HTTP response headers
   */
  public Response(int code, Header[] headers) {
    this(code, headers, null);
  }

  /**
   * Constructor
   * @param code the HTTP response code
   * @param headers the HTTP response headers
   * @param body the response body, can be null
   */
  public Response(int code, Header[] headers, byte[] body) {
    this.code = code;
    this.headers = headers;
    this.body = body;
  }

  /**
   * @return the HTTP response code
   */
  public int getCode() {
    return code;
  }

  /**
   * @return the HTTP response headers
   */
  public Header[] getHeaders() {
    return headers;
  }

  /**
   * @return the value of the Location header
   */
  public String getLocation() {
    for (Header header: headers) {
      if (header.getName().equals("Location")) {
        return header.getValue();
      }
    }
    return null;
  }

  /**
   * @return true if a response body was sent
   */
  public boolean hasBody() {
    return body != null;
  }

  /**
   * @return the HTTP response body
   */
  public byte[] getBody() {
    return body;
  }

  /**
   * @param code the HTTP response code
   */
  public void setCode(int code) {
    this.code = code;
  }

  /**
   * @param headers the HTTP response headers
   */
  public void setHeaders(Header[] headers) {
    this.headers = headers;
  }

  /**
   * @param body the response body
   */
  public void setBody(byte[] body) {
    this.body = body;
  }
}
