/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.security.authentication.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestAuthenticatedURL {

  @Test
  public void testToken() throws Exception {
    AuthenticatedURL.Token token = new AuthenticatedURL.Token();
    assertFalse(token.isSet());
    token = new AuthenticatedURL.Token("foo");
    assertTrue(token.isSet());
    assertEquals("foo", token.toString());
  }

  @Test
  public void testInjectToken() throws Exception {
    HttpURLConnection conn = mock(HttpURLConnection.class);
    AuthenticatedURL.Token token = new AuthenticatedURL.Token();
    token.set("foo");
    AuthenticatedURL.injectToken(conn, token);
    verify(conn).addRequestProperty(eq("Cookie"), anyString());
  }

  @Test
  public void testExtractTokenOK() throws Exception {
    HttpURLConnection conn = mock(HttpURLConnection.class);

    when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);

    String tokenStr = "foo";
    Map<String, List<String>> headers = new HashMap<String, List<String>>();
    List<String> cookies = new ArrayList<String>();
    cookies.add(AuthenticatedURL.AUTH_COOKIE + "=" + tokenStr);
    headers.put("Set-Cookie", cookies);
    when(conn.getHeaderFields()).thenReturn(headers);

    AuthenticatedURL.Token token = new AuthenticatedURL.Token();
    AuthenticatedURL.extractToken(conn, token);

    assertEquals(tokenStr, token.toString());
  }

  @Test
  public void testExtractTokenFail() throws Exception {
    HttpURLConnection conn = mock(HttpURLConnection.class);

    when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_UNAUTHORIZED);

    String tokenStr = "foo";
    Map<String, List<String>> headers = new HashMap<String, List<String>>();
    List<String> cookies = new ArrayList<String>();
    cookies.add(AuthenticatedURL.AUTH_COOKIE + "=" + tokenStr);
    headers.put("Set-Cookie", cookies);
    when(conn.getHeaderFields()).thenReturn(headers);

    AuthenticatedURL.Token token = new AuthenticatedURL.Token();
    token.set("bar");
    try {
      AuthenticatedURL.extractToken(conn, token);
      fail();
    } catch (AuthenticationException ex) {
      // Expected
      assertFalse(token.isSet());
    } catch (Exception ex) {
      fail();
    }
  }

  @Test
  public void testExtractTokenCookieHeader() throws Exception {
    HttpURLConnection conn = mock(HttpURLConnection.class);

    when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);

    String tokenStr = "foo";
    Map<String, List<String>> headers = new HashMap<>();
    List<String> cookies = new ArrayList<>();
    cookies.add(AuthenticatedURL.AUTH_COOKIE + "=" + tokenStr);
    headers.put("Set-Cookie", cookies);
    when(conn.getHeaderFields()).thenReturn(headers);

    AuthenticatedURL.Token token = new AuthenticatedURL.Token();
    AuthenticatedURL.extractToken(conn, token);

    assertTrue(token.isSet());
  }

  @Test
  public void testExtractTokenLowerCaseCookieHeader() throws Exception {
    HttpURLConnection conn = mock(HttpURLConnection.class);

    when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);

    String tokenStr = "foo";
    Map<String, List<String>> headers = new HashMap<>();
    List<String> cookies = new ArrayList<>();
    cookies.add(AuthenticatedURL.AUTH_COOKIE + "=" + tokenStr);
    headers.put("set-cookie", cookies);
    when(conn.getHeaderFields()).thenReturn(headers);

    AuthenticatedURL.Token token = new AuthenticatedURL.Token();
    AuthenticatedURL.extractToken(conn, token);

    assertTrue(token.isSet());
  }

  @Test
  public void testConnectionConfigurator() throws Exception {
    HttpURLConnection conn = mock(HttpURLConnection.class);
    when(conn.getResponseCode()).
        thenReturn(HttpURLConnection.HTTP_UNAUTHORIZED);

    ConnectionConfigurator connConf =
        mock(ConnectionConfigurator.class);
    Mockito.when(connConf.configure(Mockito.<HttpURLConnection>any())).
        thenReturn(conn);

    Authenticator authenticator = Mockito.mock(Authenticator.class);

    AuthenticatedURL aURL = new AuthenticatedURL(authenticator, connConf);
    aURL.openConnection(new URL("http://foo"), new AuthenticatedURL.Token());
    verify(connConf).configure(Mockito.<HttpURLConnection>any());
  }

  @Test
  public void testGetAuthenticator() throws Exception {
    Authenticator authenticator = mock(Authenticator.class);

    AuthenticatedURL aURL = new AuthenticatedURL(authenticator);
    assertEquals(authenticator, aURL.getAuthenticator());
  }

}
