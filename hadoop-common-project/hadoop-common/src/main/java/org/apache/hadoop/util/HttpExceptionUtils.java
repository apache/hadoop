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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.codehaus.jackson.map.ObjectMapper;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.net.HttpURLConnection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * HTTP utility class to help propagate server side exception to the client
 * over HTTP as a JSON payload.
 * <p/>
 * It creates HTTP Servlet and JAX-RPC error responses including details of the
 * exception that allows a client to recreate the remote exception.
 * <p/>
 * It parses HTTP client connections and recreates the exception.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class HttpExceptionUtils {

  public static final String ERROR_JSON = "RemoteException";
  public static final String ERROR_EXCEPTION_JSON = "exception";
  public static final String ERROR_CLASSNAME_JSON = "javaClassName";
  public static final String ERROR_MESSAGE_JSON = "message";

  private static final String APPLICATION_JSON_MIME = "application/json";

  private static final String ENTER = System.getProperty("line.separator");

  /**
   * Creates a HTTP servlet response serializing the exception in it as JSON.
   *
   * @param response the servlet response
   * @param status the error code to set in the response
   * @param ex the exception to serialize in the response
   * @throws IOException thrown if there was an error while creating the
   * response
   */
  public static void createServletExceptionResponse(
      HttpServletResponse response, int status, Throwable ex)
      throws IOException {
    response.setStatus(status);
    response.setContentType(APPLICATION_JSON_MIME);
    Map<String, Object> json = new LinkedHashMap<String, Object>();
    json.put(ERROR_MESSAGE_JSON, getOneLineMessage(ex));
    json.put(ERROR_EXCEPTION_JSON, ex.getClass().getSimpleName());
    json.put(ERROR_CLASSNAME_JSON, ex.getClass().getName());
    Map<String, Object> jsonResponse = new LinkedHashMap<String, Object>();
    jsonResponse.put(ERROR_JSON, json);
    ObjectMapper jsonMapper = new ObjectMapper();
    Writer writer = response.getWriter();
    jsonMapper.writerWithDefaultPrettyPrinter().writeValue(writer, jsonResponse);
    writer.flush();
  }

  /**
   * Creates a HTTP JAX-RPC response serializing the exception in it as JSON.
   *
   * @param status the error code to set in the response
   * @param ex the exception to serialize in the response
   * @return the JAX-RPC response with the set error and JSON encoded exception
   */
  public static Response createJerseyExceptionResponse(Response.Status status,
      Throwable ex) {
    Map<String, Object> json = new LinkedHashMap<String, Object>();
    json.put(ERROR_MESSAGE_JSON, getOneLineMessage(ex));
    json.put(ERROR_EXCEPTION_JSON, ex.getClass().getSimpleName());
    json.put(ERROR_CLASSNAME_JSON, ex.getClass().getName());
    Map<String, Object> response = new LinkedHashMap<String, Object>();
    response.put(ERROR_JSON, json);
    return Response.status(status).type(MediaType.APPLICATION_JSON).
        entity(response).build();
  }

  private static String getOneLineMessage(Throwable exception) {
    String message = exception.getMessage();
    if (message != null) {
      int i = message.indexOf(ENTER);
      if (i > -1) {
        message = message.substring(0, i);
      }
    }
    return message;
  }

  // trick, riding on generics to throw an undeclared exception

  private static void throwEx(Throwable ex) {
    HttpExceptionUtils.<RuntimeException>throwException(ex);
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void throwException(Throwable ex)
      throws E {
    throw (E) ex;
  }

  /**
   * Validates the status of an <code>HttpURLConnection</code> against an
   * expected HTTP status code. If the current status code is not the expected
   * one it throws an exception with a detail message using Server side error
   * messages if available.
   * <p/>
   * <b>NOTE:</b> this method will throw the deserialized exception even if not
   * declared in the <code>throws</code> of the method signature.
   *
   * @param conn the <code>HttpURLConnection</code>.
   * @param expectedStatus the expected HTTP status code.
   * @throws IOException thrown if the current status code does not match the
   * expected one.
   */
  @SuppressWarnings("unchecked")
  public static void validateResponse(HttpURLConnection conn,
      int expectedStatus) throws IOException {
    if (conn.getResponseCode() != expectedStatus) {
      Exception toThrow;
      InputStream es = null;
      try {
        es = conn.getErrorStream();
        ObjectMapper mapper = new ObjectMapper();
        Map json = mapper.readValue(es, Map.class);
        json = (Map) json.get(ERROR_JSON);
        String exClass = (String) json.get(ERROR_CLASSNAME_JSON);
        String exMsg = (String) json.get(ERROR_MESSAGE_JSON);
        if (exClass != null) {
          try {
            ClassLoader cl = HttpExceptionUtils.class.getClassLoader();
            Class klass = cl.loadClass(exClass);
            Constructor constr = klass.getConstructor(String.class);
            toThrow = (Exception) constr.newInstance(exMsg);
          } catch (Exception ex) {
            toThrow = new IOException(String.format(
                "HTTP status [%d], exception [%s], message [%s] ",
                conn.getResponseCode(), exClass, exMsg));
          }
        } else {
          String msg = (exMsg != null) ? exMsg : conn.getResponseMessage();
          toThrow = new IOException(String.format(
              "HTTP status [%d], message [%s]", conn.getResponseCode(), msg));
        }
      } catch (Exception ex) {
        toThrow = new IOException(String.format(
            "HTTP status [%d], message [%s]", conn.getResponseCode(),
            conn.getResponseMessage()));
      } finally {
        if (es != null) {
          try {
            es.close();
          } catch (IOException ex) {
            //ignore
          }
        }
      }
      throwEx(toThrow);
    }
  }

}
