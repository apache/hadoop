/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.utils.Base64;

/**
 * Represents the shared key credentials used to access an Azure Storage
 * account.
 */
public class SharedKeyCredentials {
  private static final int EXPECTED_BLOB_QUEUE_CANONICALIZED_STRING_LENGTH = 300;
  private static final Pattern CRLF = Pattern.compile("\r\n", Pattern.LITERAL);
  private static final String HMAC_SHA256 = "HmacSHA256";
  /**
   * Stores a reference to the RFC1123 date/time pattern.
   */
  private static final String RFC1123_PATTERN = "EEE, dd MMM yyyy HH:mm:ss z";


  private String accountName;
  private byte[] accountKey;
  private Mac hmacSha256;

  public SharedKeyCredentials(final String accountName,
                              final String accountKey) {
    if (accountName == null || accountName.isEmpty()) {
      throw new IllegalArgumentException("Invalid account name.");
    }
    if (accountKey == null || accountKey.isEmpty()) {
      throw new IllegalArgumentException("Invalid account key.");
    }
    this.accountName = accountName;
    this.accountKey = Base64.decode(accountKey);
    initializeMac();
  }

  public void signRequest(HttpURLConnection connection, final long contentLength) throws UnsupportedEncodingException {

    connection.setRequestProperty(HttpHeaderConfigurations.X_MS_DATE, getGMTTime());

    final String stringToSign = canonicalize(connection, accountName, contentLength);

    final String computedBase64Signature = computeHmac256(stringToSign);

    connection.setRequestProperty(HttpHeaderConfigurations.AUTHORIZATION,
        String.format("%s %s:%s", "SharedKey", accountName, computedBase64Signature));
  }

  private String computeHmac256(final String stringToSign) {
    byte[] utf8Bytes;
    try {
      utf8Bytes = stringToSign.getBytes(AbfsHttpConstants.UTF_8);
    } catch (final UnsupportedEncodingException e) {
      throw new IllegalArgumentException(e);
    }
    byte[] hmac;
    synchronized (this) {
      hmac = hmacSha256.doFinal(utf8Bytes);
    }
    return Base64.encode(hmac);
  }

  /**
   * Add x-ms- prefixed headers in a fixed order.
   *
   * @param conn                the HttpURLConnection for the operation
   * @param canonicalizedString the canonicalized string to add the canonicalized headerst to.
   */
  private static void addCanonicalizedHeaders(final HttpURLConnection conn, final StringBuilder canonicalizedString) {
    // Look for header names that start with
    // HeaderNames.PrefixForStorageHeader
    // Then sort them in case-insensitive manner.

    final Map<String, List<String>> headers = conn.getRequestProperties();
    final ArrayList<String> httpStorageHeaderNameArray = new ArrayList<String>();

    for (final String key : headers.keySet()) {
      if (key.toLowerCase(Locale.ROOT).startsWith(AbfsHttpConstants.HTTP_HEADER_PREFIX)) {
        httpStorageHeaderNameArray.add(key.toLowerCase(Locale.ROOT));
      }
    }

    Collections.sort(httpStorageHeaderNameArray);

    // Now go through each header's values in the sorted order and append
    // them to the canonicalized string.
    for (final String key : httpStorageHeaderNameArray) {
      final StringBuilder canonicalizedElement = new StringBuilder(key);
      String delimiter = ":";
      final ArrayList<String> values = getHeaderValues(headers, key);

      boolean appendCanonicalizedElement = false;
      // Go through values, unfold them, and then append them to the
      // canonicalized element string.
      for (final String value : values) {
        if (value != null) {
          appendCanonicalizedElement = true;
        }

        // Unfolding is simply removal of CRLF.
        final String unfoldedValue = CRLF.matcher(value)
            .replaceAll(Matcher.quoteReplacement(""));

        // Append it to the canonicalized element string.
        canonicalizedElement.append(delimiter);
        canonicalizedElement.append(unfoldedValue);
        delimiter = ",";
      }

      // Now, add this canonicalized element to the canonicalized header
      // string.
      if (appendCanonicalizedElement) {
        appendCanonicalizedElement(canonicalizedString, canonicalizedElement.toString());
      }
    }
  }

  /**
   * Initialize the HmacSha256 associated with the account key.
   */
  private void initializeMac() {
    // Initializes the HMAC-SHA256 Mac and SecretKey.
    try {
      hmacSha256 = Mac.getInstance(HMAC_SHA256);
      hmacSha256.init(new SecretKeySpec(accountKey, HMAC_SHA256));
    } catch (final Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Append a string to a string builder with a newline constant.
   *
   * @param builder the StringBuilder object
   * @param element the string to append.
   */
  private static void appendCanonicalizedElement(final StringBuilder builder, final String element) {
    builder.append("\n");
    builder.append(element);
  }

  /**
   * Constructs a canonicalized string from the request's headers that will be used to construct the signature string
   * for signing a Blob or Queue service request under the Shared Key Full authentication scheme.
   *
   * @param address       the request URI
   * @param accountName   the account name associated with the request
   * @param method        the verb to be used for the HTTP request.
   * @param contentType   the content type of the HTTP request.
   * @param contentLength the length of the content written to the outputstream in bytes, -1 if unknown
   * @param date          the date/time specification for the HTTP request
   * @param conn          the HttpURLConnection for the operation.
   * @return A canonicalized string.
   */
  private static String canonicalizeHttpRequest(final URL address,
      final String accountName, final String method, final String contentType,
      final long contentLength, final String date, final HttpURLConnection conn)
      throws UnsupportedEncodingException {

    // The first element should be the Method of the request.
    // I.e. GET, POST, PUT, or HEAD.
    final StringBuilder canonicalizedString = new StringBuilder(EXPECTED_BLOB_QUEUE_CANONICALIZED_STRING_LENGTH);
    canonicalizedString.append(conn.getRequestMethod());

    // The next elements are
    // If any element is missing it may be empty.
    appendCanonicalizedElement(canonicalizedString,
        getHeaderValue(conn, HttpHeaderConfigurations.CONTENT_ENCODING, AbfsHttpConstants.EMPTY_STRING));
    appendCanonicalizedElement(canonicalizedString,
        getHeaderValue(conn, HttpHeaderConfigurations.CONTENT_LANGUAGE, AbfsHttpConstants.EMPTY_STRING));
    appendCanonicalizedElement(canonicalizedString,
        contentLength <= 0 ? "" : String.valueOf(contentLength));
    appendCanonicalizedElement(canonicalizedString,
        getHeaderValue(conn, HttpHeaderConfigurations.CONTENT_MD5, AbfsHttpConstants.EMPTY_STRING));
    appendCanonicalizedElement(canonicalizedString, contentType != null ? contentType : AbfsHttpConstants.EMPTY_STRING);

    final String dateString = getHeaderValue(conn, HttpHeaderConfigurations.X_MS_DATE, AbfsHttpConstants.EMPTY_STRING);
    // If x-ms-date header exists, Date should be empty string
    appendCanonicalizedElement(canonicalizedString, dateString.equals(AbfsHttpConstants.EMPTY_STRING) ? date
        : "");

    appendCanonicalizedElement(canonicalizedString,
        getHeaderValue(conn, HttpHeaderConfigurations.IF_MODIFIED_SINCE, AbfsHttpConstants.EMPTY_STRING));
    appendCanonicalizedElement(canonicalizedString,
        getHeaderValue(conn, HttpHeaderConfigurations.IF_MATCH, AbfsHttpConstants.EMPTY_STRING));
    appendCanonicalizedElement(canonicalizedString,
        getHeaderValue(conn, HttpHeaderConfigurations.IF_NONE_MATCH, AbfsHttpConstants.EMPTY_STRING));
    appendCanonicalizedElement(canonicalizedString,
        getHeaderValue(conn, HttpHeaderConfigurations.IF_UNMODIFIED_SINCE, AbfsHttpConstants.EMPTY_STRING));
    appendCanonicalizedElement(canonicalizedString,
        getHeaderValue(conn, HttpHeaderConfigurations.RANGE, AbfsHttpConstants.EMPTY_STRING));

    addCanonicalizedHeaders(conn, canonicalizedString);

    appendCanonicalizedElement(canonicalizedString, getCanonicalizedResource(address, accountName));

    return canonicalizedString.toString();
  }

  /**
   * Gets the canonicalized resource string for a Blob or Queue service request under the Shared Key Lite
   * authentication scheme.
   *
   * @param address     the resource URI.
   * @param accountName the account name for the request.
   * @return the canonicalized resource string.
   */
  private static String getCanonicalizedResource(final URL address,
      final String accountName) throws UnsupportedEncodingException {
    // Resource path
    final StringBuilder resourcepath = new StringBuilder(AbfsHttpConstants.FORWARD_SLASH);
    resourcepath.append(accountName);

    // Note that AbsolutePath starts with a '/'.
    resourcepath.append(address.getPath());
    final StringBuilder canonicalizedResource = new StringBuilder(resourcepath.toString());

    // query parameters
    if (address.getQuery() == null || !address.getQuery().contains(AbfsHttpConstants.EQUAL)) {
      //no query params.
      return canonicalizedResource.toString();
    }

    final Map<String, String[]> queryVariables = parseQueryString(address.getQuery());

    final Map<String, String> lowercasedKeyNameValue = new HashMap<>();

    for (final Entry<String, String[]> entry : queryVariables.entrySet()) {
      // sort the value and organize it as comma separated values
      final List<String> sortedValues = Arrays.asList(entry.getValue());
      Collections.sort(sortedValues);

      final StringBuilder stringValue = new StringBuilder();

      for (final String value : sortedValues) {
        if (stringValue.length() > 0) {
          stringValue.append(AbfsHttpConstants.COMMA);
        }

        stringValue.append(value);
      }

      // key turns out to be null for ?a&b&c&d
      lowercasedKeyNameValue.put((entry.getKey()) == null ? null
          : entry.getKey().toLowerCase(Locale.ROOT), stringValue.toString());
    }

    final ArrayList<String> sortedKeys = new ArrayList<String>(lowercasedKeyNameValue.keySet());

    Collections.sort(sortedKeys);

    for (final String key : sortedKeys) {
      final StringBuilder queryParamString = new StringBuilder();

      queryParamString.append(key);
      queryParamString.append(":");
      queryParamString.append(lowercasedKeyNameValue.get(key));

      appendCanonicalizedElement(canonicalizedResource, queryParamString.toString());
    }

    return canonicalizedResource.toString();
  }

  /**
   * Gets all the values for the given header in the one to many map,
   * performs a trimStart() on each return value.
   *
   * @param headers    a one to many map of key / values representing the header values for the connection.
   * @param headerName the name of the header to lookup
   * @return an ArrayList<String> of all trimmed values corresponding to the requested headerName. This may be empty
   * if the header is not found.
   */
  private static ArrayList<String> getHeaderValues(
      final Map<String, List<String>> headers,
      final String headerName) {

    final ArrayList<String> arrayOfValues = new ArrayList<String>();
    List<String> values = null;

    for (final Entry<String, List<String>> entry : headers.entrySet()) {
      if (entry.getKey().toLowerCase(Locale.ROOT).equals(headerName)) {
        values = entry.getValue();
        break;
      }
    }
    if (values != null) {
      for (final String value : values) {
        // canonicalization formula requires the string to be left
        // trimmed.
        arrayOfValues.add(trimStart(value));
      }
    }
    return arrayOfValues;
  }

  /**
   * Parses a query string into a one to many hashmap.
   *
   * @param parseString the string to parse
   * @return a HashMap<String, String[]> of the key values.
   */
  private static HashMap<String, String[]> parseQueryString(String parseString) throws UnsupportedEncodingException {
    final HashMap<String, String[]> retVals = new HashMap<>();
    if (parseString == null || parseString.isEmpty()) {
      return retVals;
    }

    // 1. Remove ? if present
    final int queryDex = parseString.indexOf(AbfsHttpConstants.QUESTION_MARK);
    if (queryDex >= 0 && parseString.length() > 0) {
      parseString = parseString.substring(queryDex + 1);
    }

    // 2. split name value pairs by splitting on the 'c&' character
    final String[] valuePairs = parseString.contains(AbfsHttpConstants.AND_MARK)
            ? parseString.split(AbfsHttpConstants.AND_MARK)
            : parseString.split(AbfsHttpConstants.SEMICOLON);

    // 3. for each field value pair parse into appropriate map entries
    for (int m = 0; m < valuePairs.length; m++) {
      final int equalDex = valuePairs[m].indexOf(AbfsHttpConstants.EQUAL);

      if (equalDex < 0 || equalDex == valuePairs[m].length() - 1) {
        continue;
      }

      String key = valuePairs[m].substring(0, equalDex);
      String value = valuePairs[m].substring(equalDex + 1);

      key = safeDecode(key);
      value = safeDecode(value);

      // 3.1 add to map
      String[] values = retVals.get(key);

      if (values == null) {
        values = new String[]{value};
        if (!value.equals("")) {
          retVals.put(key, values);
        }
      }
    }

    return retVals;
  }

  /**
   * Performs safe decoding of the specified string, taking care to preserve each <code>+</code> character, rather
   * than replacing it with a space character.
   *
   * @param stringToDecode A <code>String</code> that represents the string to decode.
   * @return A <code>String</code> that represents the decoded string.
   * <p>
   * If a storage service error occurred.
   */
  private static String safeDecode(final String stringToDecode) throws UnsupportedEncodingException {
    if (stringToDecode == null) {
      return null;
    }

    if (stringToDecode.length() == 0) {
      return "";
    }

    if (stringToDecode.contains(AbfsHttpConstants.PLUS)) {
      final StringBuilder outBuilder = new StringBuilder();

      int startDex = 0;
      for (int m = 0; m < stringToDecode.length(); m++) {
        if (stringToDecode.charAt(m) == '+') {
          if (m > startDex) {
            outBuilder.append(URLDecoder.decode(stringToDecode.substring(startDex, m),
                    AbfsHttpConstants.UTF_8));
          }

          outBuilder.append(AbfsHttpConstants.PLUS);
          startDex = m + 1;
        }
      }

      if (startDex != stringToDecode.length()) {
        outBuilder.append(URLDecoder.decode(stringToDecode.substring(startDex, stringToDecode.length()),
                AbfsHttpConstants.UTF_8));
      }

      return outBuilder.toString();
    } else {
      return URLDecoder.decode(stringToDecode, AbfsHttpConstants.UTF_8);
    }
  }

  private static String trimStart(final String value) {
    int spaceDex = 0;
    while (spaceDex < value.length() && value.charAt(spaceDex) == ' ') {
      spaceDex++;
    }

    return value.substring(spaceDex);
  }

  private static String getHeaderValue(final HttpURLConnection conn, final String headerName, final String defaultValue) {
    final String headerValue = conn.getRequestProperty(headerName);
    return headerValue == null ? defaultValue : headerValue;
  }


  /**
   * Constructs a canonicalized string for signing a request.
   *
   * @param conn          the HttpURLConnection to canonicalize
   * @param accountName   the account name associated with the request
   * @param contentLength the length of the content written to the outputstream in bytes,
   *                      -1 if unknown
   * @return a canonicalized string.
   */
  private String canonicalize(final HttpURLConnection conn,
                              final String accountName,
                              final Long contentLength) throws UnsupportedEncodingException {

    if (contentLength < -1) {
      throw new IllegalArgumentException(
          "The Content-Length header must be greater than or equal to -1.");
    }

    String contentType = getHeaderValue(conn, HttpHeaderConfigurations.CONTENT_TYPE, "");

    return canonicalizeHttpRequest(conn.getURL(), accountName,
        conn.getRequestMethod(), contentType, contentLength, null, conn);
  }

  /**
   * Thread local for storing GMT date format.
   */
  private static ThreadLocal<DateFormat> rfc1123GmtDateTimeFormatter
      = new ThreadLocal<DateFormat>() {
    @Override
    protected DateFormat initialValue() {
      final DateFormat formatter = new SimpleDateFormat(RFC1123_PATTERN, Locale.ROOT);
      formatter.setTimeZone(GMT_ZONE);
      return formatter;
    }
  };

  public static final TimeZone GMT_ZONE = TimeZone.getTimeZone(AbfsHttpConstants.GMT_TIMEZONE);


  /**
   * Returns the current GMT date/time String using the RFC1123 pattern.
   *
   * @return A <code>String</code> that represents the current GMT date/time using the RFC1123 pattern.
   */
  static String getGMTTime() {
    return getGMTTime(new Date());
  }

  /**
   * Returns the GTM date/time String for the specified value using the RFC1123 pattern.
   *
   * @param date
   *            A <code>Date</code> object that represents the date to convert to GMT date/time in the RFC1123
   *            pattern.
   *
   * @return A <code>String</code> that represents the GMT date/time for the specified value using the RFC1123
   *         pattern.
   */
  static String getGMTTime(final Date date) {
    return rfc1123GmtDateTimeFormatter.get().format(date);
  }
}
