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
  public enum Op implements HttpOpParam.Op {
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

    MODIFYACLENTRIES(false, HttpURLConnection.HTTP_OK),
    REMOVEACLENTRIES(false, HttpURLConnection.HTTP_OK),
    REMOVEDEFAULTACL(false, HttpURLConnection.HTTP_OK),
    REMOVEACL(false, HttpURLConnection.HTTP_OK),
    SATISFYSTORAGEPOLICY(false, HttpURLConnection.HTTP_OK),
    SETACL(false, HttpURLConnection.HTTP_OK),

    SETXATTR(false, HttpURLConnection.HTTP_OK),
    REMOVEXATTR(false, HttpURLConnection.HTTP_OK),

    ENABLEECPOLICY(false, HttpURLConnection.HTTP_OK),
    DISABLEECPOLICY(false, HttpURLConnection.HTTP_OK),
    SETECPOLICY(false, HttpURLConnection.HTTP_OK),

    ALLOWSNAPSHOT(false, HttpURLConnection.HTTP_OK),
    DISALLOWSNAPSHOT(false, HttpURLConnection.HTTP_OK),
    CREATESNAPSHOT(false, HttpURLConnection.HTTP_OK),
    RENAMESNAPSHOT(false, HttpURLConnection.HTTP_OK),
    SETSTORAGEPOLICY(false, HttpURLConnection.HTTP_OK),

    NULL(false, HttpURLConnection.HTTP_NOT_IMPLEMENTED);

    final boolean doOutputAndRedirect;
    final int expectedHttpResponseCode;
    final boolean requireAuth;

    Op(final boolean doOutputAndRedirect, final int expectedHttpResponseCode) {
      this(doOutputAndRedirect, expectedHttpResponseCode, false);
    }

    Op(final boolean doOutputAndRedirect, final int expectedHttpResponseCode,
        final boolean requireAuth) {
      this.doOutputAndRedirect = doOutputAndRedirect;
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
      return doOutputAndRedirect;
    }

    @Override
    public boolean getRedirect() {
      return doOutputAndRedirect;
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

  private static final Domain<Op> DOMAIN = new Domain<>(NAME, Op.class);

  /**
   * Constructor.
   * @param str a string representation of the parameter value.
   */
  public PutOpParam(final String str) {
    super(DOMAIN, getOp(str));
  }

  private static Op getOp(String str) {
    try {
      return DOMAIN.parse(str);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(str + " is not a valid " + Type.PUT
          + " operation.");
    }
  }

  @Override
  public String getName() {
    return NAME;
  }
}
