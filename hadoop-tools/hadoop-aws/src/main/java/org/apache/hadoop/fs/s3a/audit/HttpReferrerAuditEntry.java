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
import java.util.function.Supplier;
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

  /** operation ID. */
  private final String operationId;

  /** optional first path. */
  private final String path1;

  /** optional second path. */
  private final String path2;

  /**
   * The header as created in the constructor; used in toString().
   * A new header is built on demand in {@link #buildHttpReferrerString()}
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
  private Map<String, Supplier<String>> evaluated;

  /**
   * Instantiate.
   *
   * Context and operationId are expected to be well formed
   * numeric/hex strings, at least adequate to be
   * used as individual path elements in a URL.
   */
  private HttpReferrerAuditEntry(
      final Builder builder) {
    this.contextId = requireNonNull(builder.contextId);
    this.operationName = requireNonNull(builder.operationName);
    this.operationId = requireNonNull(builder.operationId);
    this.path1 = builder.path1;
    this.path2 = builder.path2;

    // copy the parameters from the builder and extend
    attributes = builder.attributes;
    addAttribute(OP, operationName);
    addAttribute(OP_ID, operationId);
    addAttribute(PATH, path1);
    addAttribute(PATH2, path2);
    evaluated = builder.evaluated;
    // build the referrer up. so as to find/report problems early
    initialHeader = buildHttpReferrerString();
  }

  /**
   * Build the referrer string.
   * This includes dynamically evaluating all of the evaluated
   * attributes.
   * If there is an error creating the string it will be logged once
   * per entry, and "" returned.
   * @return a referrer string or ""
   */
  private String buildHttpReferrerString() {

    String header;
    try {
      String queries;
      // Update any params which are dynamically evaluated
      evaluated.forEach((key, eval) ->
          addAttribute(key, eval.get()));
      // now build the query parameters from all attributes, static and
      // evaluated.
      queries = attributes.entrySet().stream()
          .map(e -> e.getKey() + "=" + e.getValue())
          .collect(Collectors.joining("&"));
      final URI uri = new URI("https", REFERRER_ORIGIN_HOST,
          String.format(Locale.ENGLISH, PATH_FORMAT,
              contextId, operationId),
          queries,
          null);
      header = uri.toASCIIString();
    } catch (URISyntaxException e) {
      WARN_OF_URL_CREATION.warn("Failed to build URI for {}/{}", e);
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

  public String getContextId() {
    return contextId;
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
    private String operationId;

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

    private Builder() {
    }

    /**
     * Build.
     * @return an HttpReferrerAuditEntry
     */
    public HttpReferrerAuditEntry build() {
      return new HttpReferrerAuditEntry(this);
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
n     * @param value new value
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
  }
}
