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

package org.apache.hadoop.fs.store.audit;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.audit.CommonAuditContext;
import org.apache.hadoop.fs.store.LogExactlyOnce;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_ID;
import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_OP;
import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_PATH;
import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_PATH2;
import static org.apache.hadoop.fs.audit.AuditConstants.REFERRER_ORIGIN_HOST;

/**
 * Contains all the logic for generating an HTTP "Referer"
 * entry; includes escaping query params.
 * Tests for this are in
 * {@code org.apache.hadoop.fs.s3a.audit.TestHttpReferrerAuditHeader}
 * so as to verify that header generation in the S3A auditors, and
 * S3 log parsing, all work.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class HttpReferrerAuditHeader {

  /**
   * Format of path to build: {@value}.
   * the params passed in are (context ID, span ID, op).
   * Update
   * {@code TestHttpReferrerAuditHeader.SAMPLE_LOG_ENTRY} on changes
   */
  public static final String REFERRER_PATH_FORMAT = "/hadoop/1/%3$s/%2$s/";

  private static final Logger LOG =
      LoggerFactory.getLogger(HttpReferrerAuditHeader.class);

  /**
   * Log for warning of problems creating headers will only log of
   * a problem once per process instance.
   * This is to avoid logs being flooded with errors.
   */
  private static final LogExactlyOnce WARN_OF_URL_CREATION =
      new LogExactlyOnce(LOG);

  /** Context ID. */
  private final String contextId;

  /** operation name. */
  private final String operationName;

  /** Span ID. */
  private final String spanId;

  /** optional first path. */
  private final String path1;

  /** optional second path. */
  private final String path2;

  /**
   * The header as created in the constructor; used in toString().
   * A new header is built on demand in {@link #buildHttpReferrer()}
   * so that evaluated attributes are dynamically evaluated
   * in the correct thread/place.
   */
  private final String initialHeader;

  /**
   * Map of simple attributes.
   */
  private final Map<String, String> attributes;

  /**
   * Parameters dynamically evaluated on the thread just before
   * the request is made.
   */
  private final Map<String, Supplier<String>> evaluated;

  /**
   * Elements to filter from the final header.
   */
  private final Set<String> filter;

  /**
   * Instantiate.
   *
   * Context and operationId are expected to be well formed
   * numeric/hex strings, at least adequate to be
   * used as individual path elements in a URL.
   */
  private HttpReferrerAuditHeader(
      final Builder builder) {
    this.contextId = requireNonNull(builder.contextId);
    this.evaluated = builder.evaluated;
    this.filter = builder.filter;
    this.operationName = requireNonNull(builder.operationName);
    this.path1 = builder.path1;
    this.path2 = builder.path2;
    this.spanId = requireNonNull(builder.spanId);

    // copy the parameters from the builder and extend
    attributes = builder.attributes;

    addAttribute(PARAM_OP, operationName);
    addAttribute(PARAM_PATH, path1);
    addAttribute(PARAM_PATH2, path2);
    addAttribute(PARAM_ID, spanId);

    // patch in global context values where not set
    Iterable<Map.Entry<String, String>> globalContextValues
        = builder.globalContextValues;
    if (globalContextValues != null) {
      for (Map.Entry<String, String> entry : globalContextValues) {
        attributes.putIfAbsent(entry.getKey(), entry.getValue());
      }
    }

    // build the referrer up. so as to find/report problems early
    initialHeader = buildHttpReferrer();
  }

  /**
   * Build the referrer string.
   * This includes dynamically evaluating all of the evaluated
   * attributes.
   * If there is an error creating the string it will be logged once
   * per entry, and "" returned.
   * @return a referrer string or ""
   */
  public String buildHttpReferrer() {

    String header;
    try {
      String queries;
      // Update any params which are dynamically evaluated
      evaluated.forEach((key, eval) ->
          addAttribute(key, eval.get()));
      // now build the query parameters from all attributes, static and
      // evaluated, stripping out any from the filter
      queries = attributes.entrySet().stream()
          .filter(e -> !filter.contains(e.getKey()))
          .map(e -> e.getKey() + "=" + e.getValue())
          .collect(Collectors.joining("&"));
      final URI uri = new URI("https", REFERRER_ORIGIN_HOST,
          String.format(Locale.ENGLISH, REFERRER_PATH_FORMAT,
              contextId, spanId, operationName),
          queries,
          null);
      header = uri.toASCIIString();
    } catch (URISyntaxException e) {
      WARN_OF_URL_CREATION.warn("Failed to build URI for auditor: " + e, e);
      header = "";
    }
    return header;
  }

  /**
   * Add a query parameter if not null/empty
   * There's no need to escape here as it is done in the URI
   * constructor.
   * @param key query key
   * @param value query value
   */
  private void addAttribute(String key,
      String value) {
    if (StringUtils.isNotEmpty(value)) {
      attributes.put(key, value);
    }
  }

  /**
   * Set an attribute. If the value is non-null/empty,
   * it will be used as a query parameter.
   *
   * @param key key to set
   * @param value value.
   */
  public void set(final String key, final String value) {
    addAttribute(requireNonNull(key), value);
  }

  public String getContextId() {
    return contextId;
  }

  public String getOperationName() {
    return operationName;
  }

  public String getSpanId() {
    return spanId;
  }

  public String getPath1() {
    return path1;
  }

  public String getPath2() {
    return path2;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ",
        HttpReferrerAuditHeader.class.getSimpleName() + "[", "]")
        .add(initialHeader)
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
   * Strip any quotes from around a header.
   * This is needed when processing log entries.
   * @param header field.
   * @return field without quotes.
   */
  public static String maybeStripWrappedQuotes(String header) {
    String h = header;
    // remove quotes if needed.
    while (h.startsWith("\"")) {
      h = h.substring(1);
    }
    while (h.endsWith("\"")) {
      h = h.substring(0, h.length() - 1);
    }
    return h;
  }

  /**
   * Split up the string. Uses httpClient: make sure it is on the classpath.
   * Any query param with a name but no value, e.g ?something is
   * returned in the map with an empty string as the value.
   * @param header URI to parse
   * @return a map of parameters.
   * @throws URISyntaxException failure to build URI from header.
   */
  public static Map<String, String> extractQueryParameters(String header)
      throws URISyntaxException {
    URI uri = new URI(maybeStripWrappedQuotes(header));
    // get the decoded query
    List<NameValuePair> params = URLEncodedUtils.parse(uri,
        StandardCharsets.UTF_8);
    Map<String, String> result = new HashMap<>(params.size());
    for (NameValuePair param : params) {
      String name = param.getName();
      String value = param.getValue();
      if (value == null) {
        value = "";
      }
      result.put(name, value);
    }
    return result;
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
   */
  public static final class Builder {

    /** Context ID. */
    private String contextId;

    /** operation name. */
    private String operationName;

    /** operation ID. */
    private String spanId;

    /** optional first path. */
    private String path1;

    /** optional second path. */
    private String path2;

    /** Map of attributes to add as query parameters. */
    private final Map<String, String> attributes = new HashMap<>();

    /**
     * Parameters dynamically evaluated on the thread just before
     * the request is made.
     */
    private final Map<String, Supplier<String>> evaluated =
        new HashMap<>();

    /**
     * Global context values; defaults to that of
     * {@link CommonAuditContext#getGlobalContextEntries()} and
     * should not need to be changed.
     */
    private Iterable<Map.Entry<String, String>> globalContextValues =
        CommonAuditContext.getGlobalContextEntries();

    /**
     * Elements to filter from the final header.
     */
    private Set<String> filter = new HashSet<>();

    private Builder() {
    }

    /**
     * Build.
     * @return an HttpReferrerAuditHeader
     */
    public HttpReferrerAuditHeader build() {
      return new HttpReferrerAuditHeader(this);
    }

    /**
     * Set context ID.
     * @param value context
     * @return the builder
     */
    public Builder withContextId(final String value) {
      contextId = value;
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
    public Builder withSpanId(final String value) {
      spanId = value;
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
    public Builder withPath2(final String value) {
      path2 = value;
      return this;
    }

    /**
     * Add all attributes to the current map.
     * @param value new value
     * @return the builder
     */
    public Builder withAttributes(final Map<String, String> value) {
      attributes.putAll(value);
      return this;
    }

    /**
     * Add an attribute to the current map.
     * Replaces any with the existing key.
     * @param key key to set/update
     * @param value new value
     * @return the builder
     */
    public Builder withAttribute(String key, String value) {
      attributes.put(key, value);
      return this;
    }

    /**
     * Add all evaluated attributes to the current map.
     * @param value new value
     * @return the builder
     */
    public Builder withEvaluated(final Map<String, Supplier<String>> value) {
      evaluated.putAll(value);
      return this;
    }

    /**
     * Add an evaluated attribute to the current map.
     * Replaces any with the existing key.
     * Set evaluated methods.
     * @param key key
     * @param value new value
     * @return the builder
     */
    public Builder withEvaluated(String key, Supplier<String> value) {
      evaluated.put(key, value);
      return this;
    }

    /**
     * Set the global context values (replaces the default binding
     * to {@link CommonAuditContext#getGlobalContextEntries()}).
     * @param value new value
     * @return the builder
     */
    public Builder withGlobalContextValues(
        final Iterable<Map.Entry<String, String>> value) {
      globalContextValues = value;
      return this;
    }

    /**
     * Declare the fields to filter.
     * @param fields iterable of field names.
     * @return the builder
     */
    public Builder withFilter(final Collection<String> fields) {
      this.filter = new HashSet<>(fields);
      return this;
    }
  }
}
