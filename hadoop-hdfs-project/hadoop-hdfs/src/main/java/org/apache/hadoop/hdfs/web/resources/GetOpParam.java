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
package org.apache.hadoop.hdfs.web.resources;

import java.net.HttpURLConnection;

/** Http GET operation parameter. */
public class GetOpParam extends HttpOpParam<GetOpParam.Op> {
  /** Get operations. */
  public static enum Op implements HttpOpParam.Op {
    OPEN(HttpURLConnection.HTTP_OK),

    GETFILESTATUS(HttpURLConnection.HTTP_OK),
    LISTSTATUS(HttpURLConnection.HTTP_OK),
    GETCONTENTSUMMARY(HttpURLConnection.HTTP_OK),
    GETFILECHECKSUM(HttpURLConnection.HTTP_OK),

    GETHOMEDIRECTORY(HttpURLConnection.HTTP_OK),
    GETDELEGATIONTOKEN(HttpURLConnection.HTTP_OK, true),

    /** GET_BLOCK_LOCATIONS is a private unstable op. */
    GET_BLOCK_LOCATIONS(HttpURLConnection.HTTP_OK),

    NULL(HttpURLConnection.HTTP_NOT_IMPLEMENTED);

    final int expectedHttpResponseCode;
    final boolean requireAuth;

    Op(final int expectedHttpResponseCode) {
      this(expectedHttpResponseCode, false);
    }

    Op(final int expectedHttpResponseCode, boolean requireAuth) {
      this.expectedHttpResponseCode = expectedHttpResponseCode;
      this.requireAuth = requireAuth;
    }

    @Override
    public HttpOpParam.Type getType() {
      return HttpOpParam.Type.GET;
    }
    
    @Override
    public boolean getRequireAuth() {
      return requireAuth;
    }

    @Override
    public boolean getDoOutput() {
      return false;
    }

    @Override
    public int getExpectedHttpResponseCode() {
      return expectedHttpResponseCode;
    }

    @Override
    public String toQueryString() {
      return NAME + "=" + this;
    }
  }

  private static final Domain<Op> DOMAIN = new Domain<Op>(NAME, Op.class);

  /**
   * Constructor.
   * @param str a string representation of the parameter value.
   */
  public GetOpParam(final String str) {
    super(DOMAIN, DOMAIN.parse(str));
  }

  @Override
  public String getName() {
    return NAME;
  }
}
