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
package org.apache.hadoop.hdfs.server.common;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.MachineList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An HTTP filter that can filter requests based on Hosts.
 */
public class HostRestrictingAuthorizationFilter implements Filter {
  public static final String HDFS_CONFIG_PREFIX = "dfs.web.authentication.";
  public static final String RESTRICTION_CONFIG = "host.allow.rules";
  // A Java Predicate for query string parameters on which to filter requests
  public static final Predicate<String> RESTRICTED_OPERATIONS =
      qStr -> (qStr.trim().equalsIgnoreCase("op=OPEN") ||
      qStr.trim().equalsIgnoreCase("op=GETDELEGATIONTOKEN"));
  private final Map<String, CopyOnWriteArrayList<Rule>> rulemap =
      new ConcurrentHashMap<>();
  private static final Logger LOG =
      LoggerFactory.getLogger(HostRestrictingAuthorizationFilter.class);

  /*
   * Constructs a mapping of configuration properties to be used for filter
   * initialization.  The mapping includes all properties that start with the
   * specified configuration prefix.  Property names in the mapping are trimmed
   * to remove the configuration prefix.
   *
   * @param conf configuration to read
   * @param confPrefix configuration prefix
   * @return mapping of configuration properties to be used for filter
   *     initialization
   */
  public static Map<String, String> getFilterParams(Configuration conf,
      String confPrefix) {
    return conf.getPropsWithPrefix(confPrefix);
  }

  /*
   * Check all rules for this user to see if one matches for this host/path pair
   *
   * @param: user - user to check rules for
   * @param: host - IP address (e.g. "192.168.0.1")
   * @param: path - file path with no scheme (e.g. /path/foo)
   * @returns: true if a rule matches this user, host, path tuple false if an
   * error occurs or no match
   */
  private boolean matchRule(String user, String remoteIp, String path) {
    // allow lookups for blank in the rules for user and path
    user = (user != null ? user : "");
    path = (path != null ? path : "");

    LOG.trace("Got user: {}, remoteIp: {}, path: {}", user, remoteIp, path);

    // isInRange fails for null/blank IPs, require an IP to approve
    if (remoteIp == null) {
      LOG.trace("Returned false due to null rempteIp");
      return false;
    }

    List<Rule> userRules = ((userRules = rulemap.get(user)) != null) ?
        userRules : new ArrayList<Rule>();
    List<Rule> anyRules = ((anyRules = rulemap.get("*")) != null) ?
        anyRules : new ArrayList<Rule>();

    List<Rule> rules = Stream.of(userRules, anyRules)
        .flatMap(l -> l.stream()).collect(Collectors.toList());

    for (Rule rule : rules) {
      String rulePath = rule.getPath();
      LOG.trace("Evaluating rule, subnet: {}, path: {}", rule.getNetwork(), rulePath);
      if (rule.matchesNetwork(remoteIp)
          && FilenameUtils.directoryContains(rulePath, path)) {
        LOG.debug("Found matching rule, subnet: {}, path: {}; returned true",
            rule.getNetwork(), rulePath);
        return true;
      }
    }

    LOG.trace("Found no rules for user");
    return false;
  }

  @Override
  public void destroy() {
  }

  @Override
  public void init(FilterConfig config) throws ServletException {
    // Process dropbox rules
    String dropboxRules = config.getInitParameter(RESTRICTION_CONFIG);
    loadRuleMap(dropboxRules);
  }

  /*
   * Initializes the rule map state for the filter
   *
   * @param ruleString - a string of newline delineated, comma separated
   * three field records
   * @throws IllegalArgumentException - when a rule can not be properly parsed
   * Postconditions:
   * <ul>
   * <li>The {@rulemap} hash will be populated with all parsed rules.</li>
   * </ul>
   */
  private void loadRuleMap(String ruleString) throws IllegalArgumentException {
    if (ruleString == null || ruleString.equals("")) {
      LOG.debug("Got no rules - will disallow anyone access");
    } else {
      // value: user1,network/bits1,path_glob1|user2,network/bits2,path_glob2...
      Pattern comma_split = Pattern.compile(",");
      Pattern rule_split = Pattern.compile("\\||\n");
      // split all rule lines
      Map<Integer, List<String[]>> splits = rule_split.splitAsStream(ruleString)
          .map(x -> comma_split.split(x, 3))
          .collect(Collectors.groupingBy(x -> x.length));
      // verify all rules have three parts
      if (!splits.keySet().equals(Collections.singleton(3))) {
        // instead of re-joining parts, re-materialize lines which do not split
        // correctly for the exception
        String bad_lines = rule_split.splitAsStream(ruleString)
            .filter(x -> comma_split.split(x, 3).length != 3)
            .collect(Collectors.joining("\n"));
        throw new IllegalArgumentException("Bad rule definition: " + bad_lines);
      }
      // create a list of Rules
      int user = 0;
      int cidr = 1;
      int path = 2;
      BiFunction<CopyOnWriteArrayList<Rule>, CopyOnWriteArrayList<Rule>,
          CopyOnWriteArrayList<Rule>> arrayListMerge = (v1, v2) -> {
        v1.addAll(v2);
        return v1;
      };
      for (String[] split : splits.get(3)) {
        LOG.debug("Loaded rule: user: {}, network/bits: {} path: {}",
            split[user], split[cidr], split[path]);
        Rule rule = new Rule(split[cidr], split[path]);
        // Rule map is {"user": [rule1, rule2, ...]}, update the user's array
        CopyOnWriteArrayList<Rule> arrayListRule =
            new CopyOnWriteArrayList<Rule>() {
          {
            add(rule);
          }
        };
        rulemap.merge(split[user], arrayListRule, arrayListMerge);
      }
    }
  }

  /*
   * doFilter() is a shim to create an HttpInteraction object and pass that to
   * the actual processing logic
   */
  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain filterChain)
      throws IOException, ServletException {
    final HttpServletRequest httpRequest = (HttpServletRequest) request;
    HttpServletResponse httpResponse = (HttpServletResponse) response;

    handleInteraction(new ServletFilterHttpInteraction(httpRequest,
        httpResponse, filterChain));
  }

  /*
   * The actual processing logic of the Filter
   * Uses our {@HttpInteraction} shim which can be called from a variety of
   * incoming request sources
   * @param interaction - An HttpInteraction object from any of our callers
   */
  public void handleInteraction(HttpInteraction interaction)
      throws IOException, ServletException {
    final String address = interaction.getRemoteAddr();
    final String query = interaction.getQueryString();
    final String uri = interaction.getRequestURI();
    if (!uri.startsWith(WebHdfsFileSystem.PATH_PREFIX)) {
      LOG.trace("Proceeding with interaction since the request doesn't access WebHDFS API");
      interaction.proceed();
      return;
    }
    final String path = uri.substring(WebHdfsFileSystem.PATH_PREFIX.length());
    String user = interaction.getRemoteUser();

    LOG.trace("Got request user: {}, remoteIp: {}, query: {}, path: {}",
        user, address, query, path);
    boolean authenticatedQuery =
        Arrays.stream(Optional.ofNullable(query).orElse("")
            .trim()
            .split("&"))
            .anyMatch(RESTRICTED_OPERATIONS);
    if (!interaction.isCommitted() && authenticatedQuery) {
      // loop over all query parts
      String[] queryParts = query.split("&");

      if (user == null) {
        LOG.trace("Looking for delegation token to identify user");
        for (String part : queryParts) {
          if (part.trim().startsWith("delegation=")) {
            Token t = new Token();
            t.decodeFromUrlString(part.split("=", 2)[1]);
            ByteArrayInputStream buf =
                new ByteArrayInputStream(t.getIdentifier());
            DelegationTokenIdentifier identifier =
                new DelegationTokenIdentifier();
            identifier.readFields(new DataInputStream(buf));
            user = identifier.getUser().getUserName();
            LOG.trace("Updated request user: {}, remoteIp: {}, query: {}, " +
                "path: {}", user, address, query, path);
          }
        }
      }

      if (authenticatedQuery && !(matchRule("*", address,
          path) || matchRule(user, address, path))) {
        LOG.trace("Rejecting interaction; no rule found");
        interaction.sendError(HttpServletResponse.SC_FORBIDDEN,
            "WebHDFS is configured write-only for " + user + "@" + address +
                " for file: " + path);
        return;
      }
    }

    LOG.trace("Proceeding with interaction");
    interaction.proceed();
  }

  /*
   * Defines the minimal API requirements for the filter to execute its
   * filtering logic.  This interface exists to facilitate integration in
   * components that do not run within a servlet container and therefore cannot
   * rely on a servlet container to dispatch to the {@link #doFilter} method.
   * Applications that do run inside a servlet container will not need to write
   * code that uses this interface.  Instead, they can use typical servlet
   * container configuration mechanisms to insert the filter.
   */
  public interface HttpInteraction {

    /*
     * Returns if the request has been committed.
     *
     * @return boolean
     */
    boolean isCommitted();

    /*
     * Returns the value of the requesting client address.
     *
     * @return the remote address
     */
    String getRemoteAddr();

    /*
     * Returns the user ID making the request.
     *
     * @return the user
     */
    String getRemoteUser();

    /*
     * Returns the value of the request URI.
     *
     * @return the request URI
     */
    String getRequestURI();

    /*
     * Returns the value of the query string.
     *
     * @return an optional contianing the URL query string
     */
    String getQueryString();

    /*
     * Returns the method.
     *
     * @return method
     */
    String getMethod();

    /*
     * Called by the filter after it decides that the request may proceed.
     *
     * @throws IOException if there is an I/O error
     * @throws ServletException if the implementation relies on the servlet API
     *     and a servlet API call has failed
     */
    void proceed() throws IOException, ServletException;

    /*
     * Called by the filter after it decides that the request is an
     * unauthorized request and therefore must be rejected.
     *
     * @param code status code to send
     * @param message response message
     * @throws IOException if there is an I/O error
     */
    void sendError(int code, String message) throws IOException;
  }

  private static class Rule {
    private final String network;
    private final Predicate<String> networkMatcher;
    private final String path;

    /*
     * A class for holding dropbox filter rules
     *
     * @param network - the IPv4 or IPv6 network/bits for which this rule is
     * valid (pass "*" for any network location)
     * @param path - the HDFS path for which this rule is valid
     * @throws IllegalArgumentException - when the network can not be parsed
     */
    Rule(String network, String path) {
      this.network = network;
      this.path = path;
      if (network.trim().equals("*")) {
        networkMatcher = remoteIp -> true;
      } else if (network.indexOf(':') < 0) {
        SubnetUtils.SubnetInfo subnet = new SubnetUtils(network).getInfo();
        // SubnetUtils cannot parse an IPv6 client, which never matches an IPv4 network.
        networkMatcher = remoteIp -> remoteIp.indexOf(':') < 0 && subnet.isInRange(remoteIp);
      } else if (network.indexOf('/') < 0) {
        // MachineList would also accept a bare address, but a rule needs network/bits.
        throw new IllegalArgumentException("Could not parse [" + network + "]");
      } else {
        MachineList subnet = new MachineList(Collections.singletonList(network));
        // An IPv4 client never matches an IPv6 network; MachineList would resolve "" as loopback.
        networkMatcher = remoteIp -> remoteIp.indexOf(':') >= 0 && subnet.includes(remoteIp);
      }
    }

    public String getNetwork() {
      return (network);
    }

    public boolean matchesNetwork(String remoteIp) {
      return networkMatcher.test(remoteIp);
    }

    public String getPath() {
      return (path);
    }
  }

  /*
   * {@link HttpInteraction} implementation for use in the servlet filter.
   */
  private static final class ServletFilterHttpInteraction
      implements HttpInteraction {

    private final FilterChain chain;
    private final HttpServletRequest httpRequest;
    private final HttpServletResponse httpResponse;

    /*
     * Creates a new ServletFilterHttpInteraction.
     *
     * @param httpRequest request to process
     * @param httpResponse response to process
     * @param chain filter chain to forward to if HTTP interaction is allowed
     */
    public ServletFilterHttpInteraction(HttpServletRequest httpRequest,
        HttpServletResponse httpResponse, FilterChain chain) {
      this.httpRequest = httpRequest;
      this.httpResponse = httpResponse;
      this.chain = chain;
    }

    @Override
    public boolean isCommitted() {
      return (httpResponse.isCommitted());
    }

    @Override
    public String getRemoteAddr() {
      return (httpRequest.getRemoteAddr());
    }

    @Override
    public String getRemoteUser() {
      return (httpRequest.getRemoteUser());
    }

    @Override
    public String getRequestURI() {
      return (httpRequest.getRequestURI());
    }

    @Override
    public String getQueryString() {
      return (httpRequest.getQueryString());
    }

    @Override
    public String getMethod() {
      return httpRequest.getMethod();
    }

    @Override
    public void proceed() throws IOException, ServletException {
      chain.doFilter(httpRequest, httpResponse);
    }

    @Override
    public void sendError(int code, String message) throws IOException {
      httpResponse.sendError(code, message);
    }

  }
}
