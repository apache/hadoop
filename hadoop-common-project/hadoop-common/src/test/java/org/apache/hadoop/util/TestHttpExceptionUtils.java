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

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TestHttpExceptionUtils {

  @Test
  public void testCreateServletException() throws IOException {
    StringWriter writer = new StringWriter();
    PrintWriter printWriter = new PrintWriter(writer);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(response.getWriter()).thenReturn(printWriter);
    int status = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
    Exception ex = new IOException("Hello IOEX");
    HttpExceptionUtils.createServletExceptionResponse(response, status, ex);
    Mockito.verify(response).setStatus(status);
    Mockito.verify(response).setContentType(Mockito.eq("application/json"));
    ObjectMapper mapper = new ObjectMapper();
    Map json = mapper.readValue(writer.toString(), Map.class);
    json = (Map) json.get(HttpExceptionUtils.ERROR_JSON);
    Assert.assertEquals(IOException.class.getName(),
        json.get(HttpExceptionUtils.ERROR_CLASSNAME_JSON));
    Assert.assertEquals(IOException.class.getSimpleName(),
        json.get(HttpExceptionUtils.ERROR_EXCEPTION_JSON));
    Assert.assertEquals("Hello IOEX",
        json.get(HttpExceptionUtils.ERROR_MESSAGE_JSON));
  }

  @Test
  public void testCreateJerseyException() throws IOException {
    Exception ex = new IOException("Hello IOEX");
    Response response = HttpExceptionUtils.createJerseyExceptionResponse(
        Response.Status.INTERNAL_SERVER_ERROR, ex);
    Assert.assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
        response.getStatus());
    Assert.assertArrayEquals(
        Arrays.asList(MediaType.APPLICATION_JSON_TYPE).toArray(),
        response.getMetadata().get("Content-Type").toArray());
    Map entity = (Map) response.getEntity();
    entity = (Map) entity.get(HttpExceptionUtils.ERROR_JSON);
    Assert.assertEquals(IOException.class.getName(),
        entity.get(HttpExceptionUtils.ERROR_CLASSNAME_JSON));
    Assert.assertEquals(IOException.class.getSimpleName(),
        entity.get(HttpExceptionUtils.ERROR_EXCEPTION_JSON));
    Assert.assertEquals("Hello IOEX",
        entity.get(HttpExceptionUtils.ERROR_MESSAGE_JSON));
  }

  @Test
  public void testValidateResponseOK() throws IOException {
    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
    Mockito.when(conn.getResponseCode()).thenReturn(
        HttpURLConnection.HTTP_CREATED);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_CREATED);
  }

  @Test(expected = IOException.class)
  public void testValidateResponseFailNoErrorMessage() throws IOException {
    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
    Mockito.when(conn.getResponseCode()).thenReturn(
        HttpURLConnection.HTTP_BAD_REQUEST);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_CREATED);
  }

  @Test
  public void testValidateResponseNonJsonErrorMessage() throws IOException {
    String msg = "stream";
    InputStream is = new ByteArrayInputStream(msg.getBytes());
    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
    Mockito.when(conn.getErrorStream()).thenReturn(is);
    Mockito.when(conn.getResponseMessage()).thenReturn("msg");
    Mockito.when(conn.getResponseCode()).thenReturn(
        HttpURLConnection.HTTP_BAD_REQUEST);
    try {
      HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_CREATED);
      Assert.fail();
    } catch (IOException ex) {
      Assert.assertTrue(ex.getMessage().contains("msg"));
      Assert.assertTrue(ex.getMessage().contains("" +
          HttpURLConnection.HTTP_BAD_REQUEST));
    }
  }

  @Test
  public void testValidateResponseJsonErrorKnownException() throws IOException {
    Map<String, Object> json = new HashMap<String, Object>();
    json.put(HttpExceptionUtils.ERROR_EXCEPTION_JSON, IllegalStateException.class.getSimpleName());
    json.put(HttpExceptionUtils.ERROR_CLASSNAME_JSON, IllegalStateException.class.getName());
    json.put(HttpExceptionUtils.ERROR_MESSAGE_JSON, "EX");
    Map<String, Object> response = new HashMap<String, Object>();
    response.put(HttpExceptionUtils.ERROR_JSON, json);
    ObjectMapper jsonMapper = new ObjectMapper();
    String msg = jsonMapper.writeValueAsString(response);
    InputStream is = new ByteArrayInputStream(msg.getBytes());
    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
    Mockito.when(conn.getErrorStream()).thenReturn(is);
    Mockito.when(conn.getResponseMessage()).thenReturn("msg");
    Mockito.when(conn.getResponseCode()).thenReturn(
        HttpURLConnection.HTTP_BAD_REQUEST);
    try {
      HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_CREATED);
      Assert.fail();
    } catch (IllegalStateException ex) {
      Assert.assertEquals("EX", ex.getMessage());
    }
  }

  @Test
  public void testValidateResponseJsonErrorUnknownException()
      throws IOException {
    Map<String, Object> json = new HashMap<String, Object>();
    json.put(HttpExceptionUtils.ERROR_EXCEPTION_JSON, "FooException");
    json.put(HttpExceptionUtils.ERROR_CLASSNAME_JSON, "foo.FooException");
    json.put(HttpExceptionUtils.ERROR_MESSAGE_JSON, "EX");
    Map<String, Object> response = new HashMap<String, Object>();
    response.put(HttpExceptionUtils.ERROR_JSON, json);
    ObjectMapper jsonMapper = new ObjectMapper();
    String msg = jsonMapper.writeValueAsString(response);
    InputStream is = new ByteArrayInputStream(msg.getBytes());
    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
    Mockito.when(conn.getErrorStream()).thenReturn(is);
    Mockito.when(conn.getResponseMessage()).thenReturn("msg");
    Mockito.when(conn.getResponseCode()).thenReturn(
        HttpURLConnection.HTTP_BAD_REQUEST);
    try {
      HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_CREATED);
      Assert.fail();
    } catch (IOException ex) {
      Assert.assertTrue(ex.getMessage().contains("EX"));
      Assert.assertTrue(ex.getMessage().contains("foo.FooException"));
    }
  }

}
