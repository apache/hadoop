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

package org.apache.hadoop.fs.s3a.audit;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.s3a.impl.LogExactlyOnce;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.audit.AuditConstants.*;

/**
 * Contains all the logic for generating an HTTP "Referer"
 * entry; includes escaping query params.
 */
public final class HttpReferrerAuditEntry {

  private static final Logger LOG =
      LoggerFactory.getLogger(HttpReferrerAuditEntry.class);

  public static final String AUTHORITY = "hadoop.apache.org";

  private final LogExactlyOnce warnOfUrlCreation = new LogExactlyOnce(
      LOG);

  private final String context;

  private final String operationName;

  private final String operationId;

  private final String path1;

  private final String path2;

  private final String header;

  private final Map<String, String> parameters;

  /**
   * Instantiate.
   *
   * Context and operationId are expected to be well formed
   * numeric/hex strings, at least adequate to be
   * used as individual path elements in a URL.
   * @param context context as string
   * @param operationId operation ID as a string
   * @param operationName operation name
   * @param path1 optional first path
   * @param path2 optional second path
   * @param attributes map of attributes to add as query parameters.
   * @param attributes2 second map of attributes to add as query parameters.
   */
  private HttpReferrerAuditEntry(
      final Builder builder) {
    this.context = requireNonNull(builder.context);
    this.operationName = requireNonNull(builder.operationName);
    this.operationId = requireNonNull(builder.operationId);
    this.path1 = builder.path1;
    this.path2 = builder.path2;
    // clone params
    parameters = new HashMap<>();
    add(parameters, builder.attributes);
    add(parameters, builder.attributes2);
    addParameter(parameters, OP, operationName);
    addParameter(parameters, PATH, path1);
    addParameter(parameters, PATH2, path2);
    // build the referrer up. so as to find/report problems early
    header = buildHttpReferrerString();
  }

  /**
   * Build the referrer string.
   * If there is an error creating the string it will be logged once
   * per entry, and "" returned.
   * @return a referrer string or ""
   */
  private String buildHttpReferrerString() {
    final String queries;
    // queries as ? params.
    queries = parameters.entrySet().stream()
        .map(e -> e.getKey() + "=" + e.getValue())
        .collect(Collectors.joining("&"));
    String h;
    try {
      final URI uri = new URI("https", AUTHORITY,
          String.format(Locale.ENGLISH, PATH_FORMAT,
              context, operationId),
          queries,
          null);
      h = uri.toASCIIString();
    } catch (URISyntaxException e) {
      warnOfUrlCreation.warn("Failed to build URI for {}/{}", e);
      h = "";
    }
    return h;
  }

  /**
   * Add the map of attributes to the map of request parameters
   * @param requestParams HTTP request parameters
   * @param attributes attributes
   */
  private void add(final Map<String, String> requestParams,
      final Map<String, String> attributes) {
    if (attributes != null) {
      attributes.entrySet()
          .forEach(e -> addParameter(requestParams, e.getKey(), e.getValue()));
    }
  }

  /**
   * Add a query parameter if not null/empty
   * There's no need to escape here as it is done in the URI
   * constructor.
   * @param requestParams query map
   * @param key query key
   * @param value query value
   */
  private void addParameter(Map<String, String> requestParams,
      String key,
      String value) {
    if (StringUtils.isNotEmpty(value)) {
      requestParams.put(key, value);
    }
  }

  public String getContext() {
    return context;
  }

  public String getOperationName() {
    return operationName;
  }

  public String getOperationId() {
    return operationId;
  }

  public String getPath1() {
    return path1;
  }

  public String getPath2() {
    return path2;
  }

  /**
   * Get the header or "" if there were problems creating it.
   * @return the referrer header
   */
  public String getReferrerHeader() {
    return buildHttpReferrerString();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ",
        HttpReferrerAuditEntry.class.getSimpleName() + "[", "]")
        .add(header)
        .toString();
  }

  /**
   * Perform any escaping to valid path elements in advance of
   * new URI() doing this itself. Only path separators need to
   * be escaped/converted at this point.
   * @param source source string
   * @return an escaped path element.
   */
  public static String escapeToPathElement(CharSequence source) {
    int len = source.length();
    StringBuilder r = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      char c = source.charAt(i);
      String s = Character.toString(c);
      switch (c) {
      case '/':
      case '@':
        s = "+";
        break;
      default:
        break;
      }
      r.append(s);
    }
    return r.toString();

  }

  /**
   * Get a builder.
   * @return a new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder.
   *
   * Context and operationId are expected to be well formed
   * numeric/hex strings, at least adequate to be
   * used as individual path elements in a URL.
   * @param context
   * @param operationId operation ID as a string
   * @param operationName operation name
   * @param path1 optional first path
   * @param path2 optional second path
   * @param attributes map of attributes to add as query parameters.
   * @param attributes2 second map of attributes to add as query parameters.
   */
  public static final class Builder {

    private  String context;

    private  String operationName;

    private  String operationId;

    private  String path1;

    private  String path2;

    private Map<String, String> attributes;
    private Map<String, String> attributes2;

    private int i;
    private Builder() {
    }

    /**
     * Build.
     * @return
     */
    public HttpReferrerAuditEntry build() {
      return new HttpReferrerAuditEntry(this);
    }

    /**
     * Set context as string
     * @param value context
     * @return the builder
     */
    public Builder withContext(final String value) {
      context = value;
      return this;
    }

    /**
     * Set Operation name.
     * @param value new value
     * @return the builder
     */
    public Builder withOperationName(final String value) {
      operationName = value;
      return this;
    }

    /**
     * Set ID.
     * @param value new value
     * @return the builder
     */
    public Builder withOperationId(final String value) {
      operationId = value;
      return this;
    }

    /**
     * Set Path1 of operation.
     * @param value new value
     * @return the builder
     */
    public Builder withPath1(final String value) {
      path1 = value;
      return this;
    }

    /**
     * Set Path2 of operation.
     * @param value new value
     * @return the builder
     */
    public Builder withPath2(final String _path2) {
      path2 = _path2;
      return this;
    }

    /**
     * Set map 1 of attributes (common context) 
     * @param value new value
     * @return the builder
     */
    public Builder withAttributes(final Map<String, String> value) {
      attributes = value;
      return this;
    }

    /**
     * Set map 2 of attributes (span attributes)
     * @param value new value
     * @return the builder
     */
    public Builder withAttributes2(final Map<String, String> value) {
      attributes2 = value;
      return this;
    }
  }
}
