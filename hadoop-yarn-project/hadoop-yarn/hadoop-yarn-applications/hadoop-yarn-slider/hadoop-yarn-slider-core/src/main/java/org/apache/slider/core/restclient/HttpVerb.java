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
 */

package org.apache.slider.core.restclient;

/**
 * Http verbs with details on what they support in terms of submit and
 * response bodies.
 * <p>
 * Those verbs which do support bodies in the response MAY NOT return it;
 * if the response code is 204 then the answer is "no body", but the operation
 * is considered a success.
 */
public enum HttpVerb {
  GET("GET", false, true),
  POST("POST", true, true),
  PUT("PUT", true, true),
  DELETE("DELETE", false, true),
  HEAD("HEAD", false, false);
  
  private final String verb;
  private final boolean hasUploadBody;
  private final boolean hasResponseBody;

  HttpVerb(String verb, boolean hasUploadBody, boolean hasResponseBody) {
    this.verb = verb;
    this.hasUploadBody = hasUploadBody;
    this.hasResponseBody = hasResponseBody;
  }

  public String getVerb() {
    return verb;
  }

  public boolean hasUploadBody() {
    return hasUploadBody;
  }

  public boolean hasResponseBody() {
    return hasResponseBody;
  }
}
