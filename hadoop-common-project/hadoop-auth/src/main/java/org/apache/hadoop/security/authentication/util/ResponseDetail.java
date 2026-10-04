/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.security.authentication.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Describes why a request failed, preferring the response body over the HTTP
 * reason phrase.
 * <p>
 * A servlet reports its reason through
 * {@link javax.servlet.http.HttpServletResponse#sendError}, and that detail
 * used to reach the caller in the reason phrase, which
 * {@link HttpURLConnection#getResponseMessage()} returns. Jetty 12 never puts a
 * reason phrase on the wire: the phrase is now always the canonical text for
 * the status code - "Forbidden", "Gone" - and the detail is in the body
 * instead.
 */
@InterfaceAudience.Private
public final class ResponseDetail {

  /** How much of a failed response body is worth quoting back. */
  public static final int MAX_BYTES = 4096;

  private static final String APPLICATION_JSON_MIME = "application/json";

  /**
   * The MESSAGE row of the error page Jetty renders for sendError - the same
   * on 9.4 and on 12 - which holds the reason and nothing else.
   */
  private static final Pattern ERROR_PAGE_MESSAGE = Pattern.compile(
      "<th>\\s*MESSAGE:\\s*</th>\\s*<td>(.*?)</td>",
      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private ResponseDetail() {
  }

  /**
   * Reads the body, and falls back to the phrase when there is none.
   * <p>
   * A JSON body is left alone and the phrase reported instead. That is the
   * envelope HttpExceptionUtils#createServletExceptionResponse writes, sent
   * with setStatus rather than sendError, so its phrase was the canonical text
   * for the status code before Jetty 12 and still is. Quoting the envelope back
   * as free text would replace a readable "Forbidden" with a line of JSON, and
   * callers that want what is inside it parse it with
   * HttpExceptionUtils#validateResponse instead.
   *
   * @param conn a connection whose response status has been read
   * @return a description of the failure, never null
   */
  public static String of(HttpURLConnection conn) {
    if (!isJson(conn.getContentType())) {
      try (InputStream es = conn.getErrorStream()) {
        if (es != null) {
          String body = toPlainText(
              new String(es.readNBytes(MAX_BYTES), StandardCharsets.UTF_8));
          if (!body.isEmpty()) {
            return body;
          }
        }
      } catch (IOException ex) {
        // nothing to add: fall through to the reason phrase
      }
    }
    return phrase(conn);
  }

  /**
   * The HTTP reason phrase, or "" when there is none. Since Jetty 12 this is
   * always the canonical text for the status code.
   *
   * @param conn a connection whose response status has been read
   * @return the reason phrase, never null
   */
  public static String phrase(HttpURLConnection conn) {
    try {
      String phrase = conn.getResponseMessage();
      return phrase == null ? "" : phrase;
    } catch (IOException ex) {
      return "";
    }
  }

  /**
   * Reduces a response body to something readable in a one-line message. A
   * container renders sendError as an HTML page, so the reason arrives buried
   * in markup. From Jetty's error page take the message row alone, which is
   * the text the reason phrase used to carry; from any other page strip the
   * markup.
   *
   * @param body the response body
   * @return the body as one line of plain text
   */
  public static String toPlainText(String body) {
    Matcher message = ERROR_PAGE_MESSAGE.matcher(body);
    String text = message.find() && !message.group(1).trim().isEmpty()
        ? message.group(1) : body;
    if (text.indexOf('<') >= 0) {
      text = text.replaceAll("(?s)<(script|style)\\b.*?</\\1>", " ")
          .replaceAll("(?s)<[^>]*>", " ");
    }
    text = text.replace("&lt;", "<").replace("&gt;", ">")
        .replace("&quot;", "\"").replace("&#39;", "'")
        .replace("&amp;", "&");
    return text.replaceAll("\\s+", " ").trim();
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
}
