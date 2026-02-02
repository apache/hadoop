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
    LambdaTestUtils.interceptAndValidateMessageContains(IOException.class,
        Arrays.asList(Integer.toString(HttpURLConnection.HTTP_BAD_REQUEST), "msg",
        "com.fasterxml.jackson.core.JsonParseException"),
        () -> HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_CREATED));
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
}
