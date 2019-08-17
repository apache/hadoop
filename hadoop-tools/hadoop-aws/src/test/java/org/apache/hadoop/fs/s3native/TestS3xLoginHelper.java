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

package org.apache.hadoop.fs.s3native;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Assert;
import org.junit.Test;


import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test how URIs and login details are extracted from URIs.
 */
public class TestS3xLoginHelper extends Assert {
  public static final String BUCKET = "s3a://bucket";
  private static final URI ENDPOINT = uri(BUCKET);
  public static final String S = "%2f";
  public static final String P = "%2b";
  public static final String P_RAW = "+";
  public static final String USER = "user";
  public static final String PASLASHSLASH = "pa" + S + S;
  public static final String PAPLUS = "pa" + P;
  public static final String PAPLUS_RAW = "pa" + P_RAW;

  public static final URI WITH_USER_AND_PASS = uri("s3a://user:pass@bucket");

  public static final URI WITH_SLASH_IN_PASS = uri(
      "s3a://user:" + PASLASHSLASH + "@bucket");
  public static final URI WITH_PLUS_IN_PASS = uri(
      "s3a://user:" + PAPLUS + "@bucket");
  public static final URI WITH_PLUS_RAW_IN_PASS = uri(
      "s3a://user:" + PAPLUS_RAW + "@bucket");
  public static final URI USER_NO_PASS = uri("s3a://user@bucket");
  public static final URI WITH_USER_AND_COLON = uri("s3a://user:@bucket");
  public static final URI NO_USER = uri("s3a://:pass@bucket");
  public static final URI NO_USER_NO_PASS = uri("s3a://:@bucket");
  public static final URI NO_USER_NO_PASS_TWO_COLON = uri("s3a://::@bucket");

  /**
   * Construct a URI; raises an RTE if it won't parse.
   * This allows it to be used in static constructors.
   * @param s URI string
   * @return the URI
   * @throws RuntimeException on a URI syntax problem
   */
  private static URI uri(String s) {
    try {
      return new URI(s);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e.toString(), e);
    }
  }

  /**
   * Assert that a built up FS URI matches the endpoint.
   * @param uri URI to build the FS URI from
   */
  private void assertMatchesEndpoint(URI uri) {
    assertEquals("Source " + uri,
        ENDPOINT, S3xLoginHelper.buildFSURI(uri));
  }

  /**
   * Assert that the supplied FS URI is invalid as it contains
   * username:password secrets.
   * @param uri URI to build the FS URI from
   */
  private void assertInvalid(URI uri) throws Exception {
    intercept(IllegalArgumentException.class,
    S3xLoginHelper.LOGIN_WARNING,
    () -> S3xLoginHelper.buildFSURI(uri));
  }

  /**
   * Assert that the login/pass details from a URI match that expected.
   * @param user username
   * @param pass password
   * @param uri URI to build login details from
   * @return the login tuple
   */
  private S3xLoginHelper.Login assertMatchesLogin(String user,
      String pass, URI uri) {
    S3xLoginHelper.Login expected = new S3xLoginHelper.Login(user, pass);
    S3xLoginHelper.Login actual = S3xLoginHelper.extractLoginDetails(uri);
    if (!expected.equals(actual)) {
      Assert.fail("Source " + uri
          + " login expected=:" + toString(expected)
          + " actual=" + toString(actual));
    }
    return actual;
  }

  @Test
  public void testSimpleFSURI() throws Throwable {
    assertMatchesEndpoint(ENDPOINT);
  }

  @Test
  public void testLoginSimple() throws Throwable {
    S3xLoginHelper.Login login = assertMatchesLogin("", "", ENDPOINT);
    assertFalse("Login of " + login, login.hasLogin());
  }

  @Test
  public void testLoginWithUser() throws Throwable {
    assertMatchesLogin(USER, "", USER_NO_PASS);
  }

  @Test
  public void testLoginWithUserAndColon() throws Throwable {
    assertMatchesLogin(USER, "", WITH_USER_AND_COLON);
  }

  @Test
  public void testLoginNoUser() throws Throwable {
    assertMatchesLogin("", "", NO_USER);
  }

  @Test
  public void testLoginNoUserNoPass() throws Throwable {
    assertMatchesLogin("", "", NO_USER_NO_PASS);
  }

  @Test
  public void testLoginNoUserNoPassTwoColon() throws Throwable {
    assertMatchesLogin("", "", NO_USER_NO_PASS_TWO_COLON);
  }

  @Test
  public void testFsUriWithUserAndPass() throws Throwable {
    assertInvalid(WITH_USER_AND_PASS);
  }

  @Test
  public void testFsUriWithSlashInPass() throws Throwable {
    assertInvalid(WITH_SLASH_IN_PASS);
  }

  @Test
  public void testFsUriWithPlusInPass() throws Throwable {
    assertInvalid(WITH_PLUS_IN_PASS);
  }

  @Test
  public void testFsUriWithPlusRawInPass() throws Throwable {
    assertInvalid(WITH_PLUS_RAW_IN_PASS);
  }

  @Test
  public void testFsUriWithUser() throws Throwable {
    assertInvalid(USER_NO_PASS);
  }

  @Test
  public void testFsUriWithUserAndColon() throws Throwable {
    assertInvalid(WITH_USER_AND_COLON);
  }

  @Test
  public void testFsiNoUser() throws Throwable {
    assertMatchesEndpoint(NO_USER);
  }

  @Test
  public void testFsUriNoUserNoPass() throws Throwable {
    assertMatchesEndpoint(NO_USER_NO_PASS);
  }

  @Test
  public void testFsUriNoUserNoPassTwoColon() throws Throwable {
    assertMatchesEndpoint(NO_USER_NO_PASS_TWO_COLON);
  }

  /**
   * Stringifier. Kept in the code to avoid accidental logging in production
   * code.
   * @return login details for assertions.
   */
  public String toString(S3xLoginHelper.Login login) {
    final StringBuilder sb = new StringBuilder("LoginTuple{");
    sb.append("<'").append(login.getUser()).append('\'');
    sb.append(", '").append(login.getPassword()).append('\'');
    sb.append('>');
    return sb.toString();
  }
}
