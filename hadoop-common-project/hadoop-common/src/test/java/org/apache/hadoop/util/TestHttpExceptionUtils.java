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
package org.apache.hadoop.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestHttpExceptionUtils {

  @Test
  public void testCreateServletException() throws IOException {
    StringWriter writer = new StringWriter();
    PrintWriter printWriter = new PrintWriter(writer);
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.getWriter()).thenReturn(printWriter);
    int status = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
    Exception ex = new IOException("Hello IOEX");
    HttpExceptionUtils.createServletExceptionResponse(response, status, ex);
    verify(response).setStatus(status);
    verify(response).setContentType(eq("application/json"));
    ObjectMapper mapper = new ObjectMapper();
    Map json = mapper.readValue(writer.toString(), Map.class);
    json = (Map) json.get(HttpExceptionUtils.ERROR_JSON);
    assertEquals(IOException.class.getName(),
        json.get(HttpExceptionUtils.ERROR_CLASSNAME_JSON));
    assertEquals(IOException.class.getSimpleName(),
        json.get(HttpExceptionUtils.ERROR_EXCEPTION_JSON));
    assertEquals("Hello IOEX",
        json.get(HttpExceptionUtils.ERROR_MESSAGE_JSON));
  }

  @Test
  public void testCreateJerseyException() throws IOException {
    Exception ex = new IOException("Hello IOEX");
    Response response = HttpExceptionUtils.createJerseyExceptionResponse(
        Response.Status.INTERNAL_SERVER_ERROR, ex);
    assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
        response.getStatus());
    assertArrayEquals(
        Arrays.asList(MediaType.APPLICATION_JSON_TYPE).toArray(),
        response.getMetadata().get("Content-Type").toArray());
    Map entity = (Map) response.getEntity();
    entity = (Map) entity.get(HttpExceptionUtils.ERROR_JSON);
    assertEquals(IOException.class.getName(),
        entity.get(HttpExceptionUtils.ERROR_CLASSNAME_JSON));
    assertEquals(IOException.class.getSimpleName(),
        entity.get(HttpExceptionUtils.ERROR_EXCEPTION_JSON));
    assertEquals("Hello IOEX",
        entity.get(HttpExceptionUtils.ERROR_MESSAGE_JSON));
  }

  @Test
  public void testValidateResponseOK() throws IOException {
    HttpURLConnection conn = mock(HttpURLConnection.class);
    when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_CREATED);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_CREATED);
  }

  @Test
  public void testValidateResponseFailNoErrorMessage() throws Exception {
    HttpURLConnection conn = mock(HttpURLConnection.class);
    when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_BAD_REQUEST);
    LambdaTestUtils.intercept(IOException.class,
        () -> HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_CREATED));
  }

  @Test
  public void testValidateResponseNonJsonErrorMessage() throws Exception {
    String msg = "stream";
    InputStream is = new ByteArrayInputStream(msg.getBytes(StandardCharsets.UTF_8));
    HttpURLConnection conn = mock(HttpURLConnection.class);
    when(conn.getErrorStream()).thenReturn(is);
    when(conn.getResponseMessage()).thenReturn("msg");
    when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_BAD_REQUEST);
    // The body wins over the reason phrase: a servlet's reason travels in the
    // body now, so "stream" is the detail and "msg" is only the canonical
    // text for the status code.
    LambdaTestUtils.interceptAndValidateMessageContains(IOException.class,
        Arrays.asList(Integer.toString(HttpURLConnection.HTTP_BAD_REQUEST), "stream",
        "com.fasterxml.jackson.core.JsonParseException"),
        () -> HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_CREATED));
  }

  @Test
  public void testValidateResponseHtmlErrorPageReportsTheReason()
      throws Exception {
    // What AuthenticationFilter's sendError looks like on the wire.
    String page = "<html><head><title>Error 403 Invalid signature</title>"
        + "<style>h1 {color: red}</style></head><body>"
        + "<h1>HTTP ERROR 403</h1><p>Reason: Invalid signature</p>"
        + "</body></html>";
    HttpURLConnection conn = connectionReturning(page, "Forbidden", "text/html");
    when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_FORBIDDEN);
    LambdaTestUtils.interceptAndValidateMessageContains(IOException.class,
        Arrays.asList("Invalid signature"),
        () -> HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK));
  }

  @Test
  public void testValidateResponseFallsBackToThePhraseWithNoBody()
      throws Exception {
    HttpURLConnection conn = connectionReturning(null, "Forbidden", "text/html");
    when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_FORBIDDEN);
    LambdaTestUtils.interceptAndValidateMessageContains(IOException.class,
        Arrays.asList("Forbidden"),
        () -> HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK));
  }

  @Test
  public void testValidateResponseStillRebuildsTheEnvelopeException()
      throws Exception {
    // The rewind must not disturb the envelope path: a JSON body still
    // reconstructs its exception rather than being quoted back as text.
    Map<String, Object> json = new HashMap<String, Object>();
    json.put(HttpExceptionUtils.ERROR_EXCEPTION_JSON,
        IllegalStateException.class.getSimpleName());
    json.put(HttpExceptionUtils.ERROR_CLASSNAME_JSON,
        IllegalStateException.class.getName());
    json.put(HttpExceptionUtils.ERROR_MESSAGE_JSON, "EX");
    Map<String, Object> response = new HashMap<String, Object>();
    response.put(HttpExceptionUtils.ERROR_JSON, json);
    String body = new ObjectMapper().writeValueAsString(response);
    HttpURLConnection conn =
        connectionReturning(body, "Forbidden", "application/json");
    when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_FORBIDDEN);
    LambdaTestUtils.intercept(IllegalStateException.class, "EX",
        () -> HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK));
  }

  @Test
  public void testValidateResponseParsesAnEnvelopeTooLargeToRewind()
      throws Exception {
    // Larger than the rewind buffer: the parser reads straight through, so the
    // exception is still rebuilt - only the text fallback is given up.
    Map<String, Object> json = new HashMap<String, Object>();
    json.put(HttpExceptionUtils.ERROR_EXCEPTION_JSON,
        IllegalStateException.class.getSimpleName());
    json.put(HttpExceptionUtils.ERROR_CLASSNAME_JSON,
        IllegalStateException.class.getName());
    json.put(HttpExceptionUtils.ERROR_MESSAGE_JSON,
        "x".repeat(64 * 1024));
    Map<String, Object> response = new HashMap<String, Object>();
    response.put(HttpExceptionUtils.ERROR_JSON, json);
    String body = new ObjectMapper().writeValueAsString(response);
    HttpURLConnection conn =
        connectionReturning(body, "Forbidden", "application/json");
    when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_FORBIDDEN);
    LambdaTestUtils.intercept(IllegalStateException.class,
        () -> HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK));
  }

  @Test
  public void testValidateResponseJsonErrorKnownException() throws Exception {
    Map<String, Object> json = new HashMap<String, Object>();
    json.put(HttpExceptionUtils.ERROR_EXCEPTION_JSON, IllegalStateException.class.getSimpleName());
    json.put(HttpExceptionUtils.ERROR_CLASSNAME_JSON, IllegalStateException.class.getName());
    json.put(HttpExceptionUtils.ERROR_MESSAGE_JSON, "EX");
    Map<String, Object> response = new HashMap<String, Object>();
    response.put(HttpExceptionUtils.ERROR_JSON, json);
    ObjectMapper jsonMapper = new ObjectMapper();
    String msg = jsonMapper.writeValueAsString(response);
    InputStream is = new ByteArrayInputStream(msg.getBytes(StandardCharsets.UTF_8));
    HttpURLConnection conn = mock(HttpURLConnection.class);
    when(conn.getErrorStream()).thenReturn(is);
    when(conn.getResponseMessage()).thenReturn("msg");
    when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_BAD_REQUEST);
    LambdaTestUtils.intercept(IllegalStateException.class,
        "EX",
        () -> HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_CREATED));
  }

  @Test
  public void testValidateResponseJsonErrorUnknownException()
      throws Exception {
    Map<String, Object> json = new HashMap<String, Object>();
    json.put(HttpExceptionUtils.ERROR_EXCEPTION_JSON, "FooException");
    json.put(HttpExceptionUtils.ERROR_CLASSNAME_JSON, "foo.FooException");
    json.put(HttpExceptionUtils.ERROR_MESSAGE_JSON, "EX");
    Map<String, Object> response = new HashMap<String, Object>();
    response.put(HttpExceptionUtils.ERROR_JSON, json);
    ObjectMapper jsonMapper = new ObjectMapper();
    String msg = jsonMapper.writeValueAsString(response);
    InputStream is = new ByteArrayInputStream(msg.getBytes(StandardCharsets.UTF_8));
    HttpURLConnection conn = mock(HttpURLConnection.class);
    when(conn.getErrorStream()).thenReturn(is);
    when(conn.getResponseMessage()).thenReturn("msg");
    when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_BAD_REQUEST);
    LambdaTestUtils.interceptAndValidateMessageContains(IOException.class,
        Arrays.asList(Integer.toString(HttpURLConnection.HTTP_BAD_REQUEST),
        "foo.FooException", "EX"),
        () -> HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_CREATED));
  }

  @Test
  public void testValidateResponseJsonErrorNonException() throws Exception {
    Map<String, Object> json = new HashMap<String, Object>();
    json.put(HttpExceptionUtils.ERROR_EXCEPTION_JSON, "invalid");
    // test case where the exception classname is not a valid exception class
    json.put(HttpExceptionUtils.ERROR_CLASSNAME_JSON, String.class.getName());
    json.put(HttpExceptionUtils.ERROR_MESSAGE_JSON, "EX");
    Map<String, Object> response = new HashMap<String, Object>();
    response.put(HttpExceptionUtils.ERROR_JSON, json);
    ObjectMapper jsonMapper = new ObjectMapper();
    String msg = jsonMapper.writeValueAsString(response);
    InputStream is = new ByteArrayInputStream(msg.getBytes(StandardCharsets.UTF_8));
    HttpURLConnection conn = mock(HttpURLConnection.class);
    when(conn.getErrorStream()).thenReturn(is);
    when(conn.getResponseMessage()).thenReturn("msg");
    when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_BAD_REQUEST);
    LambdaTestUtils.interceptAndValidateMessageContains(IOException.class,
        Arrays.asList(Integer.toString(HttpURLConnection.HTTP_BAD_REQUEST),
        "java.lang.String", "EX"),
        () -> HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_CREATED));
  }

  private static HttpURLConnection connectionReturning(String body,
      String phrase) throws IOException {
    return connectionReturning(body, phrase, null);
  }

  private static HttpURLConnection connectionReturning(String body,
      String phrase, String contentType) throws IOException {
    HttpURLConnection conn = mock(HttpURLConnection.class);
    when(conn.getErrorStream()).thenReturn(body == null ? null
        : new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)));
    when(conn.getResponseMessage()).thenReturn(phrase);
    when(conn.getContentType()).thenReturn(contentType);
    return conn;
  }

  @Test
  public void testResponseDetailPrefersTheBody() throws Exception {
    assertEquals("the real reason", HttpExceptionUtils.getResponseDetail(
        connectionReturning("the real reason", "Forbidden")));
  }

  @Test
  public void testResponseDetailStripsAnErrorPage() throws Exception {
    // what a container renders for sendError(403, "the real reason")
    String page = "<html>\n<head>\n<title>Error 403 the real reason</title>\n"
        + "</head>\n<body><h2>HTTP ERROR 403</h2>\n"
        + "<table><tr><th>MESSAGE:</th><td>the real reason</td></tr></table>\n"
        + "</body>\n</html>\n";
    String detail = HttpExceptionUtils.getResponseDetail(
        connectionReturning(page, "Forbidden"));
    assertTrue(detail.contains("the real reason"), detail);
    assertFalse(detail.contains("<"), "markup survived: " + detail);
  }

  @Test
  public void testResponseDetailFallsBackToThePhrase() throws Exception {
    assertEquals("Forbidden", HttpExceptionUtils.getResponseDetail(
        connectionReturning(null, "Forbidden")));
    assertEquals("Forbidden", HttpExceptionUtils.getResponseDetail(
        connectionReturning("   \n  ", "Forbidden")));
  }

  @Test
  public void testResponseDetailIsNeverNull() throws Exception {
    assertEquals("", HttpExceptionUtils.getResponseDetail(
        connectionReturning(null, null)));
  }

  /**
   * A refusal carrying the JSON envelope is reported by its reason phrase, not
   * by the envelope. Such a response is sent with setStatus rather than
   * sendError - see {@link HttpExceptionUtils#createServletExceptionResponse}
   * - so it never had a reason of its own in the phrase, and quoting the JSON
   * back would replace a readable "Forbidden" with a line of markup. Callers
   * that want what is inside it use validateResponse.
   */
  @Test
  public void testResponseDetailLeavesTheJsonEnvelopeAlone() throws Exception {
    String envelope = "{\"RemoteException\":{\"message\":\"User: client is not"
        + " allowed to impersonate foo1\",\"exception\":\"AuthorizationException\","
        + "\"javaClassName\":\"org.apache.hadoop.security.authorize."
        + "AuthorizationException\"}}";

    assertEquals("Forbidden", HttpExceptionUtils.getResponseDetail(
        connectionReturning(envelope, "Forbidden", "application/json")));
    // the header may carry parameters
    assertEquals("Forbidden", HttpExceptionUtils.getResponseDetail(
        connectionReturning(envelope, "Forbidden",
            "application/json; charset=utf-8")));
  }

  /**
   * A body that is not JSON is still preferred, which is the case the reader
   * exists for: Jetty 12 puts what sendError was given in the body and leaves
   * the phrase canonical.
   */
  @Test
  public void testResponseDetailStillPrefersANonJsonBody() throws Exception {
    assertEquals("the real reason", HttpExceptionUtils.getResponseDetail(
        connectionReturning("the real reason", "Forbidden", "text/plain")));
  }
}
