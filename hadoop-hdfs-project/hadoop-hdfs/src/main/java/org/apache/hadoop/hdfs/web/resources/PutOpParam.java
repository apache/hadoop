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

/** Http POST operation parameter. */
public class PutOpParam extends HttpOpParam<PutOpParam.Op> {
  /** Put operations. */
  public static enum Op implements HttpOpParam.Op {
    CREATE(true, HttpURLConnection.HTTP_CREATED),

    MKDIRS(false, HttpURLConnection.HTTP_OK),
    CREATESYMLINK(false, HttpURLConnection.HTTP_OK),
    RENAME(false, HttpURLConnection.HTTP_OK),
    SETREPLICATION(false, HttpURLConnection.HTTP_OK),

    SETOWNER(false, HttpURLConnection.HTTP_OK),
    SETPERMISSION(false, HttpURLConnection.HTTP_OK),
    SETTIMES(false, HttpURLConnection.HTTP_OK),
    
    RENEWDELEGATIONTOKEN(false, HttpURLConnection.HTTP_OK, true),
    CANCELDELEGATIONTOKEN(false, HttpURLConnection.HTTP_OK, true),
    
    NULL(false, HttpURLConnection.HTTP_NOT_IMPLEMENTED);

    final boolean doOutput;
    final int expectedHttpResponseCode;
    final boolean requireAuth;

    Op(final boolean doOutput, final int expectedHttpResponseCode) {
      this(doOutput, expectedHttpResponseCode, false);
    }

    Op(final boolean doOutput, final int expectedHttpResponseCode,
       final boolean requireAuth) {
      this.doOutput = doOutput;
      this.expectedHttpResponseCode = expectedHttpResponseCode;
      this.requireAuth = requireAuth;
    }

    @Override
    public HttpOpParam.Type getType() {
      return HttpOpParam.Type.PUT;
    }
    
    @Override
    public boolean getRequireAuth() {
      return requireAuth;
    }

    @Override
    public boolean getDoOutput() {
      return doOutput;
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
  public PutOpParam(final String str) {
    super(DOMAIN, DOMAIN.parse(str));
  }

  @Override
  public String getName() {
    return NAME;
  }
}
