/*
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
 *
 */

package org.apache.hadoop.fs.common;


import com.squareup.okhttp.mockwebserver.MockResponse;

import java.util.ArrayList;

/**
 * Supporting class to hold expected MockResponse object along with parameters
 * for validation in test methods.
 */
public class ExpectedResponse {
  private MockResponse response;
  private ArrayList<String> expectedQueryParameters = new ArrayList<String>();
  private int expectedBodySize;
  private String httpRequestType;

  public int getExpectedBodySize() {
    return expectedBodySize;
  }

  public String getHttpRequestType() {
    return httpRequestType;
  }

  public ArrayList<String> getExpectedQueryParameters() {
    return expectedQueryParameters;
  }

  public MockResponse getResponse() {
    return response;
  }

  ExpectedResponse set(MockResponse mockResponse) {
    this.response = mockResponse;
    return this;
  }

  ExpectedResponse addExpectedQueryParam(String param) {
    expectedQueryParameters.add(param);
    return this;
  }

  ExpectedResponse addExpectedBodySize(int bodySize) {
    this.expectedBodySize = bodySize;
    return this;
  }

  ExpectedResponse addExpectedHttpRequestType(String expectedHttpRequestType) {
    this.httpRequestType = expectedHttpRequestType;
    return this;
  }
}
