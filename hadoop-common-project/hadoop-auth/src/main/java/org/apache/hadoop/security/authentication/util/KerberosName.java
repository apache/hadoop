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

package org.apache.hadoop.security.authentication.util;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class implements parsing and handling of Kerberos principal names. In
 * particular, it splits them apart and translates them down into local
 * operating system names.
 */
@SuppressWarnings("all")
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class KerberosName {
  private static final Logger LOG = LoggerFactory.getLogger(KerberosName.class);

  /**
   * Constant that defines auth_to_local legacy hadoop evaluation
   */
  public static final String MECHANISM_HADOOP = "hadoop";

  /**
   * Constant that defines auth_to_local MIT evaluation
   */
  public static final String MECHANISM_MIT = "mit";

  public static final String DEFAULT_RULES = "DEFAULT";

  /** Constant that defines the default behavior of the rule mechanism */
  public static final String DEFAULT_MECHANISM = MECHANISM_HADOOP;

  /** The first component of the name */
  private final String serviceName;
  /** The second component of the name. It may be null. */
  private final String hostName;
  /** The realm of the name. */
  private final String realm;

  /**
   * A pattern that matches a Kerberos name with at most 2 components.
   */
  private static final Pattern nameParser =
      Pattern.compile("([^/@]+)(/([^/@]+))?(@([^/@]+))?");

  /**
   * A pattern that matches a string with out '$' and then a single
   * parameter with $n.
   */
  private static Pattern parameterPattern =
    Pattern.compile("([^$]*)(\\$(\\d*))?");

  /**
   * A pattern for parsing a auth_to_local rule.
   */
  private static final Pattern ruleParser =
    Pattern.compile("\\s*((DEFAULT)|(RULE:\\[(\\d*):([^\\]]*)](\\(([^)]*)\\))?"+
                    "(s/([^/]*)/([^/]*)/(g)?)?))/?(L)?");

  /**
   * A pattern that recognizes simple/non-simple names.
   */
  private static final Pattern nonSimplePattern = Pattern.compile("[/@]");

  /**
   * The list of translation rules.
   */
  private static List<Rule> rules;

  /**
   * How to evaluate auth_to_local rules
   */
  private static String ruleMechanism = null;

  private static String defaultRealm = null;

  @VisibleForTesting
  public static void resetDefaultRealm() {
    try {
      defaultRealm = KerberosUtil.getDefaultRealm();
    } catch (Exception ke) {
      LOG.debug("resetting default realm failed, "
          + "current default realm will still be used.", ke);
    }
  }

  /**
   * Create a name from the full Kerberos principal name.
   * @param name full Kerberos principal name.
   */
  public KerberosName(String name) {
    Matcher match = nameParser.matcher(name);
    if (!match.matches()) {
      if (name.contains("@")) {
        throw new IllegalArgumentException("Malformed Kerberos name: " + name);
      } else {
        serviceName = name;
        hostName = null;
        realm = null;
      }
    } else {
      serviceName = match.group(1);
      hostName = match.group(3);
      realm = match.group(5);
    }
  }

  /**
   * Get the configured default realm.
   * Used syncronized method here, because double-check locking is overhead.
   * @return the default realm from the krb5.conf
   */
  public static synchronized String getDefaultRealm() {
    if (defaultRealm == null) {
      try {
        defaultRealm = KerberosUtil.getDefaultRealm();
      } catch (Exception ke) {
        LOG.debug("Kerberos krb5 configuration not found, setting default realm to empty");
        defaultRealm = "";
      }
    }
    return defaultRealm;
  }

  /**
   * Put the name back together from the parts.
   */
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(serviceName);
    if (hostName != null) {
      result.append('/');
      result.append(hostName);
    }
    if (realm != null) {
      result.append('@');
      result.append(realm);
    }
    return result.toString();
  }

  /**
   * Get the first component of the name.
   * @return the first section of the Kerberos principal name
   */
  public String getServiceName() {
    return serviceName;
  }

  /**
   * Get the second component of the name.
   * @return the second section of the Kerberos principal name, and may be null
   */
  public String getHostName() {
    return hostName;
  }

  /**
   * Get the realm of the name.
   * @return the realm of the name, may be null
   */
  public String getRealm() {
    return realm;
  }

  /**
   * An encoding of a rule for translating kerberos names.
   */
  private static class Rule {
    private final boolean isDefault;
    private final int numOfComponents;
    private final String format;
    private final Pattern match;
    private final Pattern fromPattern;
    private final String toPattern;
    private final boolean repeat;
    private final boolean toLowerCase;

    Rule() {
      isDefault = true;
      numOfComponents = 0;
      format = null;
      match = null;
      fromPattern = null;
      toPattern = null;
      repeat = false;
      toLowerCase = false;
    }

    Rule(int numOfComponents, String format, String match, String fromPattern,
         String toPattern, boolean repeat, boolean toLowerCase) {
      isDefault = false;
      this.numOfComponents = numOfComponents;
      this.format = format;
      this.match = match == null ? null : Pattern.compile(match);
      this.fromPattern =
        fromPattern == null ? null : Pattern.compile(fromPattern);
      this.toPattern = toPattern;
      this.repeat = repeat;
      this.toLowerCase = toLowerCase;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder();
      if (isDefault) {
        buf.append("DEFAULT");
      } else {
        buf.append("RULE:[");
        buf.append(numOfComponents);
        buf.append(':');
        buf.append(format);
        buf.append(']');
        if (match != null) {
          buf.append('(');
          buf.append(match);
          buf.append(')');
        }
        if (fromPattern != null) {
          buf.append("s/");
          buf.append(fromPattern);
          buf.append('/');
          buf.append(toPattern);
          buf.append('/');
          if (repeat) {
            buf.append('g');
          }
        }
        if (toLowerCase) {
          buf.append("/L");
        }
      }
      return buf.toString();
    }

    /**
     * Replace the numbered parameters of the form $n where n is from 1 to
     * the length of params. Normal text is copied directly and $n is replaced
     * by the corresponding parameter.
     * @param format the string to replace parameters again
     * @param params the list of parameters
     * @return the generated string with the parameter references replaced.
     * @throws BadFormatString
     */
    static String replaceParameters(String format,
                                    String[] params) throws BadFormatString {
      Matcher match = parameterPattern.matcher(format);
      int start = 0;
      StringBuilder result = new StringBuilder();
      while (start < format.length() && match.find(start)) {
        result.append(match.group(1));
        String paramNum = match.group(3);
        if (paramNum != null) {
          try {
            int num = Integer.parseInt(paramNum);
            if (num < 0 || num >= params.length) {
              throw new BadFormatString("index " + num + " from " + format +
                                        " is outside of the valid range 0 to " +
                                        (params.length - 1));
            }
            result.append(params[num]);
          } catch (NumberFormatException nfe) {
            throw new BadFormatString("bad format in username mapping in " +
                                      paramNum, nfe);
          }

        }
        start = match.end();
      }
      return result.toString();
    }

    /**
     * Replace the matches of the from pattern in the base string with the value
     * of the to string.
     * @param base the string to transform
     * @param from the pattern to look for in the base string
     * @param to the string to replace matches of the pattern with
     * @param repeat whether the substitution should be repeated
     * @return
     */
    static String replaceSubstitution(String base, Pattern from, String to,
                                      boolean repeat) {
      Matcher match = from.matcher(base);
      if (repeat) {
        return match.replaceAll(to);
      } else {
        return match.replaceFirst(to);
      }
    }

    /**
     * Try to apply this rule to the given name represented as a parameter
     * array.
     * @param params first element is the realm, second and later elements are
     *        are the components of the name "a/b@FOO" -> {"FOO", "a", "b"}
     * @param ruleMechanism defines the rule evaluation mechanism
     * @return the short name if this rule applies or null
     * @throws IOException throws if something is wrong with the rules
     */
    String apply(String[] params, String ruleMechanism) throws IOException {
      String result = null;
      if (isDefault) {
        if (getDefaultRealm().equals(params[0])) {
          result = params[1];
        }
      } else if (params.length - 1 == numOfComponents) {
        String base = replaceParameters(format, params);
        if (match == null || match.matcher(base).matches()) {
          if (fromPattern == null) {
            result = base;
          } else {
            result = replaceSubstitution(base, fromPattern, toPattern,  repeat);
          }
        }
      }
      if (result != null
              && nonSimplePattern.matcher(result).find()
              && ruleMechanism.equalsIgnoreCase(MECHANISM_HADOOP)) {
        throw new NoMatchingRule("Non-simple name " + result +
                                 " after auth_to_local rule " + this);
      }
      if (toLowerCase && result != null) {
        result = result.toLowerCase(Locale.ENGLISH);
      }
      return result;
    }
  }

  static List<Rule> parseRules(String rules) {
    List<Rule> result = new ArrayList<Rule>();
    String remaining = rules.trim();
    while (remaining.length() > 0) {
      Matcher matcher = ruleParser.matcher(remaining);
      if (!matcher.lookingAt()) {
        throw new IllegalArgumentException("Invalid rule: " + remaining);
      }
      if (matcher.group(2) != null) {
        result.add(new Rule());
      } else {
        result.add(new Rule(Integer.parseInt(matcher.group(4)),
                            matcher.group(5),
                            matcher.group(7),
                            matcher.group(9),
                            matcher.group(10),
                            "g".equals(matcher.group(11)),
                            "L".equals(matcher.group(12))));
      }
      remaining = remaining.substring(matcher.end());
    }
    return result;
  }

  @SuppressWarnings("serial")
  public static class BadFormatString extends IOException {
    BadFormatString(String msg) {
      super(msg);
    }
    BadFormatString(String msg, Throwable err) {
      super(msg, err);
    }
  }

  @SuppressWarnings("serial")
  public static class NoMatchingRule extends IOException {
    NoMatchingRule(String msg) {
      super(msg);
    }
  }

  /**
   * Get the translation of the principal name into an operating system
   * user name.
   * @return the short name
   * @throws IOException throws if something is wrong with the rules
   */
  public String getShortName() throws IOException {
    String[] params;
    if (hostName == null) {
      // if it is already simple, just return it
      if (realm == null) {
        return serviceName;
      }
      params = new String[]{realm, serviceName};
    } else {
      params = new String[]{realm, serviceName, hostName};
    }
    String ruleMechanism = this.ruleMechanism;
    List<Rule> rules = this.rules;

    if (rules == null) {
      LOG.warn("auth_to_local rules not set."
        + "Using default of " + DEFAULT_RULES);
      rules = parseRules(DEFAULT_RULES);
    }
    if (ruleMechanism == null && rules != null) {
      LOG.warn("auth_to_local rule mechanism not set."
      + "Using default of " + DEFAULT_MECHANISM);
      ruleMechanism = DEFAULT_MECHANISM;
    }

    for(Rule r: rules) {
      String result = r.apply(params, ruleMechanism);
      if (result != null) {
        return result;
      }
    }
    if (ruleMechanism.equalsIgnoreCase(MECHANISM_HADOOP)) {
      throw new NoMatchingRule("No rules applied to " + toString());
    }
    return toString();
  }

  /**
   * Get the rules.
   * @return String of configured rules, or null if not yet configured
   */
  public static String getRules() {
    String ruleString = null;
    if (rules != null) {
      StringBuilder sb = new StringBuilder();
      for (Rule rule : rules) {
        sb.append(rule.toString()).append("\n");
      }
      ruleString = sb.toString().trim();
    }
    return ruleString;
  }

  /**
   * Indicates if the name rules have been set.
   *
   * @return if the name rules have been set.
   */
  public static boolean hasRulesBeenSet() {
    return rules != null;
  }

  /**
   * Indicates of the rule mechanism has been set
   *
   * @return if the rule mechanism has been set.
   */
  public static boolean hasRuleMechanismBeenSet() {
    return ruleMechanism != null;
  }

  /**
   * Set the rules.
   * @param ruleString the rules string.
   */
  public static void setRules(String ruleString) {
    rules = (ruleString != null) ? parseRules(ruleString) : null;
  }

  /**
   *
   * @param ruleMech the evaluation type: hadoop, mit
   *                 'hadoop' indicates '@' or '/' are not allowed the result
   *                 evaluation. 'MIT' indicates that auth_to_local
   *                 rules follow MIT Kerberos evaluation.
   */
  public static void setRuleMechanism(String ruleMech) {
    if (ruleMech != null
            && (!ruleMech.equalsIgnoreCase(MECHANISM_HADOOP)
            && !ruleMech.equalsIgnoreCase(MECHANISM_MIT))) {
      throw new IllegalArgumentException("Invalid rule mechanism: " + ruleMech);
    }
    ruleMechanism = ruleMech;
  }

  /**
   * Get the rule evaluation mechanism
   * @return the rule evaluation mechanism
   */
  public static String getRuleMechanism() {
    return ruleMechanism;
  }

  static void printRules() throws IOException {
    int i = 0;
    for(Rule r: rules) {
      System.out.println(++i + " " + r);
    }
  }

}
