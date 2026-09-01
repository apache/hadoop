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

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.BufferedInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * HTTP utility class to help propagate server side exception to the client
 * over HTTP as a JSON payload.
 * <p>
 * It creates HTTP Servlet and JAX-RPC error responses including details of the
 * exception that allows a client to recreate the remote exception.
 * <p>
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

  private static final MethodHandles.Lookup PUBLIC_LOOKUP = MethodHandles.publicLookup();
  private static final MethodType EXCEPTION_CONSTRUCTOR_TYPE =
          MethodType.methodType(void.class, String.class);

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
    Map<String, Object> jsonResponse =
        Collections.singletonMap(ERROR_JSON, json);
    Writer writer = response.getWriter();
    JsonSerialization.writer().writeValue(writer, jsonResponse);
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
    Map<String, Object> response = Collections.singletonMap(ERROR_JSON, json);
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
   * <p>
   * <b>NOTE:</b> this method will throw the deserialized exception even if not
   * declared in the <code>throws</code> of the method signature.
   * <p>
   * When the response does not carry the JSON envelope - a container error
   * page, say - the detail is taken from the body via
   * {@link #getResponseDetail}, because that is where a servlet's reason now
   * is: Jetty 12 no longer puts one in the HTTP reason phrase.
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
        InputStream raw = conn.getErrorStream();
        if (raw != null) {
          es = new BufferedInputStream(raw);
          es.mark(ERROR_BODY_REWIND_LIMIT);
        }
        Map json = JsonSerialization.mapReader().readValue(shielded(es));
        json = (Map) json.get(ERROR_JSON);
        String exClass = (String) json.get(ERROR_CLASSNAME_JSON);
        String exMsg = (String) json.get(ERROR_MESSAGE_JSON);
        if (exClass != null) {
          try {
            ClassLoader cl = HttpExceptionUtils.class.getClassLoader();
            Class klass = cl.loadClass(exClass);
            Preconditions.checkState(Exception.class.isAssignableFrom(klass),
                "Class [%s] is not a subclass of Exception", klass);
            MethodHandle methodHandle = PUBLIC_LOOKUP.findConstructor(
                    klass, EXCEPTION_CONSTRUCTOR_TYPE);
            toThrow = (Exception) methodHandle.invoke(exMsg);
          } catch (Throwable t) {
            toThrow = new IOException(String.format(
                "HTTP status [%d], exception [%s], message [%s], URL [%s]",
                conn.getResponseCode(), exClass, exMsg, conn.getURL()));
          }
        } else {
          String msg = (exMsg != null) ? exMsg : conn.getResponseMessage();
          toThrow = new IOException(String.format(
              "HTTP status [%d], message [%s], URL [%s]",
              conn.getResponseCode(), msg, conn.getURL()));
        }
      } catch (Exception ex) {
        toThrow = new IOException(String.format(
            "HTTP status [%d], message [%s], URL [%s], exception [%s]",
            conn.getResponseCode(), rewoundDetail(es, conn), conn.getURL(),
            ex.toString()), ex);
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

  /** How much of a failed response body is worth quoting back. */
  private static final int MAX_RESPONSE_DETAIL_CHARS = 4096;

  /**
   * How much of an error body {@link #validateResponse} keeps buffered so it
   * can be rewound and read as text once the JSON parse has failed. Sized to
   * hold {@link #MAX_RESPONSE_DETAIL_CHARS} characters of any UTF-8 body. A
   * body longer than this still parses - the reader runs straight through it -
   * it just cannot be rewound, which leaves the reason phrase as the fallback,
   * as it was before.
   */
  private static final int ERROR_BODY_REWIND_LIMIT =
      4 * MAX_RESPONSE_DETAIL_CHARS;

  /**
   * Describes why a request failed, preferring the response body over the HTTP
   * reason phrase.
   * <p>
   * A servlet reports its reason through
   * {@link HttpServletResponse#sendError}, and that detail used to reach the
   * caller in the reason phrase, which
   * {@link HttpURLConnection#getResponseMessage()} returns. Jetty 12 never
   * puts a reason phrase on the wire: the phrase is now always the canonical
   * text for the status code - "Forbidden", "Gone" - and the detail is in the
   * body instead. Read the body, and fall back to the phrase when there is
   * none.
   * <p>
   * For a response that carries the JSON envelope this class writes, prefer
   * {@link #validateResponse}, which rebuilds the original exception. This is
   * for everything else: a container's error page, or a plain-text reason.
   * A JSON body is therefore left alone here and the phrase reported instead:
   * the envelope is sent with {@code setStatus} rather than sendError, so its
   * phrase was the canonical text for the status code before Jetty 12 and
   * still is, and quoting the envelope back as free text would replace a
   * readable "Forbidden" with a line of JSON.
   *
   * @param conn a connection whose response status has been read
   * @return a description of the failure, never null
   */
  public static String getResponseDetail(HttpURLConnection conn) {
    String body = "";
    if (!isJson(conn.getContentType())) {
      try (InputStream es = conn.getErrorStream()) {
        if (es != null) {
          body = toPlainText(readCapped(es));
        }
      } catch (IOException ex) {
        // nothing to add: fall through to the reason phrase
      }
    }
    if (!body.isEmpty()) {
      return body;
    }
    return responsePhrase(conn);
  }

  /**
   * Describes a failure whose body has already been read - and failed - as the
   * JSON envelope. The body is the only place a servlet's reason can be now,
   * so rewind and read it as text. The envelope guard {@link
   * #getResponseDetail} applies does not belong here: nothing that parsed as
   * the envelope reaches this point, so there is no envelope to protect.
   *
   * @param es the buffered error stream, marked at its start, or null
   * @param conn the connection it came from
   * @return a description of the failure, never null
   */
  private static String rewoundDetail(InputStream es, HttpURLConnection conn) {
    if (es != null) {
      try {
        es.reset();
        String body = toPlainText(readCapped(es));
        if (!body.isEmpty()) {
          return body;
        }
      } catch (IOException ex) {
        // read too far to rewind: fall through to the reason phrase
      }
    }
    return responsePhrase(conn);
  }

  /**
   * The HTTP reason phrase, or "" when there is none. Since Jetty 12 this is
   * always the canonical text for the status code.
   */
  private static String responsePhrase(HttpURLConnection conn) {
    try {
      String phrase = conn.getResponseMessage();
      return phrase == null ? "" : phrase;
    } catch (IOException ex) {
      return "";
    }
  }

  /**
   * Hides {@link InputStream#close()} from a reader that would otherwise close
   * the stream on its way out. The JSON reader closes its source even when the
   * parse failed, and a closed stream can no longer be rewound and read as
   * text. The caller keeps ownership and closes the real stream itself.
   */
  private static InputStream shielded(InputStream in) {
    if (in == null) {
      return null;
    }
    return new FilterInputStream(in) {
      @Override
      public void close() {
        // the caller owns the stream
      }
    };
  }

  /**
   * Whether the content type names the JSON error envelope. The header can
   * carry parameters - "application/json; charset=utf-8" - so this matches a
   * prefix rather than the whole value.
   */
  private static boolean isJson(String contentType) {
    return contentType != null
        && contentType.trim().toLowerCase().startsWith(APPLICATION_JSON_MIME);
  }

  private static String readCapped(InputStream in) throws IOException {
    InputStreamReader reader =
        new InputStreamReader(in, StandardCharsets.UTF_8);
    StringBuilder sb = new StringBuilder();
    char[] buf = new char[1024];
    int n;
    while (sb.length() < MAX_RESPONSE_DETAIL_CHARS
        && (n = reader.read(buf)) != -1) {
      sb.append(buf, 0, Math.min(n, MAX_RESPONSE_DETAIL_CHARS - sb.length()));
    }
    return sb.toString();
  }

  /**
   * Reduces a response body to something readable in a log line. A container
   * that renders sendError as an HTML page buries the message in markup; strip
   * it out rather than quoting the page.
   */
  private static String toPlainText(String body) {
    String text = body;
    if (text.indexOf('<') >= 0) {
      text = text.replaceAll("(?s)<(script|style)\\b.*?</\\1>", " ")
          .replaceAll("(?s)<[^>]*>", " ");
    }
    text = text.replace("&lt;", "<").replace("&gt;", ">")
        .replace("&quot;", "\"").replace("&#39;", "'")
        .replace("&amp;", "&");
    return text.replaceAll("\\s+", " ").trim();
  }

}
