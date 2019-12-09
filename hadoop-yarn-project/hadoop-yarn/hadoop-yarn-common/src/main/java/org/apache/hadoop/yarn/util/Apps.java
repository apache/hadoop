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

package org.apache.hadoop.yarn.util;

import static org.apache.hadoop.yarn.util.StringHelper._split;
import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.util.StringHelper.sjoin;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

/**
 * YARN internal application-related utilities
 */
@Private
public class Apps {
  public static final String APP = "application";
  public static final String ID = "ID";
  private static final Pattern VAR_SUBBER =
      Pattern.compile(Shell.getEnvironmentVariableRegex());
  private static final Pattern VARVAL_SPLITTER = Pattern.compile(
        "(?<=^|,)"                            // preceded by ',' or line begin
      + '(' + Shell.ENV_NAME_REGEX + ')'      // var group
      + '='
      + "([^,]*)"                             // val group
      );

  public static ApplicationId toAppID(String aid) {
    Iterator<String> it = _split(aid).iterator();
    return toAppID(APP, aid, it);
  }

  public static ApplicationId toAppID(String prefix, String s, Iterator<String> it) {
    if (!it.hasNext() || !it.next().equals(prefix)) {
      throwParseException(sjoin(prefix, ID), s);
    }
    shouldHaveNext(prefix, s, it);
    ApplicationId appId = ApplicationId.newInstance(Long.parseLong(it.next()),
        Integer.parseInt(it.next()));
    return appId;
  }

  public static void shouldHaveNext(String prefix, String s, Iterator<String> it) {
    if (!it.hasNext()) {
      throwParseException(sjoin(prefix, ID), s);
    }
  }

  public static void throwParseException(String name, String s) {
    throw new YarnRuntimeException(join("Error parsing ", name, ": ", s));
  }

  private static void setEnvFromString(Map<String, String> env,
      String envVar, String varString, String classPathSeparator) {
    Matcher m = VAR_SUBBER.matcher(varString);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      String var = m.group(1);
      // do variable substitution of $var from passed in environment or from
      // system environment and default to empty string if undefined in both.
      String replace = env.get(var);
      if (replace == null) {
        replace = System.getenv(var);
      }
      if (replace == null) {
        replace = "";
      }
      m.appendReplacement(sb, Matcher.quoteReplacement(replace));
    }
    m.appendTail(sb);
    addToEnvironment(env, envVar, sb.toString(), classPathSeparator);
  }

  public static void setEnvFromInputString(Map<String, String> env,
      String envString,  String classPathSeparator) {
    if (envString != null && envString.length() > 0) {
      Matcher varValMatcher = VARVAL_SPLITTER.matcher(envString);
      while (varValMatcher.find()) {
        String envVar = varValMatcher.group(1);
        String varString = varValMatcher.group(2);
        setEnvFromString(env, envVar, varString, classPathSeparator);
      }
    }
  }

  /**
   * Set environment from string without doing any variable substitution.
   * Used internally to avoid double expansion.
   * @param env environment to set
   * @param envString comma-separated k=v pairs.
   * @param classPathSeparator Separator to use when appending to an existing
   *                           environment variable.
   */
  private static void setEnvFromInputStringNoExpand(Map<String, String> env,
      String envString,  String classPathSeparator) {
    if (envString != null && envString.length() > 0) {
      Matcher varValMatcher = VARVAL_SPLITTER.matcher(envString);
      while (varValMatcher.find()) {
        String envVar = varValMatcher.group(1);
        String varString = varValMatcher.group(2);
        addToEnvironment(env, envVar, varString, classPathSeparator);
      }
    }
  }

  /**
   * Set environment variables from map of input properties.
   * @param env environment to update
   * @param inputMap environment variable property keys and values
   * @param classPathSeparator separator to use when appending to an existing
   *                           environment variable
   */
  private static void setEnvFromInputStringMap(Map<String, String> env,
      Map<String, String> inputMap, String classPathSeparator) {
    for(Map.Entry<String, String> inputVar: inputMap.entrySet()) {
      String envVar = inputVar.getKey();
      String varString = inputVar.getValue();
      setEnvFromString(env, envVar, varString, classPathSeparator);
    }
  }

  /**
   * Set environment variables from the given environment input property.
   * For example, given the property mapreduce.map.env, this method
   * will extract environment variables from:
   *    the comma-separated string value of mapreduce.map.env, and
   *    the values of any properties of the form mapreduce.map.env.VAR_NAME
   * Variables specified via the latter syntax take precedence over those
   * specified using the former syntax.
   * @param env the environment to update
   * @param propName the name of the property
   * @param defaultPropValue the default value for propName
   * @param conf configuration containing properties
   * @param classPathSeparator Separator used when appending to an existing var
   */
  public static void setEnvFromInputProperty(Map<String, String> env,
      String propName, String defaultPropValue, Configuration conf,
      String classPathSeparator) {

    String envString = conf.get(propName, defaultPropValue);

    // Get k,v pairs from string into a tmp env. Note that we don't want
    // to expand the env var values, because we will do that below -
    // don't want to do it twice.
    Map<String, String> tmpEnv = new HashMap<String, String>();
    Apps.setEnvFromInputStringNoExpand(tmpEnv, envString, classPathSeparator);

    // Get map of props with prefix propName.
    // (e.g., map.reduce.env.ENV_VAR_NAME=value)
    Map<String, String> inputMap = conf.getPropsWithPrefix(propName + ".");

    // Entries from map should override entries from input string.
    tmpEnv.putAll(inputMap);

    // Add them to the environment
    setEnvFromInputStringMap(env, tmpEnv, classPathSeparator);
  }

  /**
   *
   * @param envString String containing env variable definitions
   * @return Set of environment variable names
   */
  private static Set<String> getEnvVarsFromInputString(String envString) {
    Set<String> envSet = new HashSet<>();
    if (envString != null && envString.length() > 0) {
      Matcher varValMatcher = VARVAL_SPLITTER.matcher(envString);
      while (varValMatcher.find()) {
        String envVar = varValMatcher.group(1);
        envSet.add(envVar);
      }
    }
    return envSet;
  }

  /**
   * Return the list of environment variable names specified in the
   * given property or default string and those specified individually
   * with the propname.VARNAME syntax (e.g., mapreduce.map.env.VARNAME=value).
   * @param propName the name of the property
   * @param defaultPropValue the default value for propName
   * @param conf configuration containing properties
   * @return Set of environment variable names
   */
  public static Set<String> getEnvVarsFromInputProperty(
      String propName, String defaultPropValue, Configuration conf) {
    String envString = conf.get(propName, defaultPropValue);
    Set<String> varSet = getEnvVarsFromInputString(envString);
    Map<String, String> propMap = conf.getPropsWithPrefix(propName + ".");
    varSet.addAll(propMap.keySet());
    return varSet;
  }

  /**
   * This older version of this method is kept around for compatibility
   * because downstream frameworks like Spark and Tez have been using it.
   * Downstream frameworks are expected to move off of it.
   */
  @Deprecated
  public static void setEnvFromInputString(Map<String, String> env,
      String envString) {
    setEnvFromInputString(env, envString, File.pathSeparator);
  }

  @Public
  @Unstable
  public static void addToEnvironment(
      Map<String, String> environment,
      String variable, String value, String classPathSeparator) {
    String val = environment.get(variable);
    if (val == null) {
      val = value;
    } else {
      val = val + classPathSeparator + value;
    }
    environment.put(StringInterner.weakIntern(variable), 
        StringInterner.weakIntern(val));
  }
  
  /**
   * This older version of this method is kept around for compatibility
   * because downstream frameworks like Spark and Tez have been using it.
   * Downstream frameworks are expected to move off of it.
   */
  @Deprecated
  public static void addToEnvironment(
      Map<String, String> environment,
      String variable, String value) {
    addToEnvironment(environment, variable, value, File.pathSeparator);
  }

  public static String crossPlatformify(String var) {
    return ApplicationConstants.PARAMETER_EXPANSION_LEFT + var
        + ApplicationConstants.PARAMETER_EXPANSION_RIGHT;
  }

  // Check if should black list the node based on container exit status
  @Private
  @Unstable
  public static boolean shouldCountTowardsNodeBlacklisting(int exitStatus) {
    switch (exitStatus) {
    case ContainerExitStatus.PREEMPTED:
    case ContainerExitStatus.KILLED_BY_RESOURCEMANAGER:
    case ContainerExitStatus.KILLED_BY_APPMASTER:
    case ContainerExitStatus.KILLED_AFTER_APP_COMPLETION:
    case ContainerExitStatus.ABORTED:
      // Neither the app's fault nor the system's fault. This happens by design,
      // so no need for skipping nodes
      return false;
    case ContainerExitStatus.DISKS_FAILED:
      // This container is marked with this exit-status means that the node is
      // already marked as unhealthy given that most of the disks failed. So, no
      // need for any explicit skipping of nodes.
      return false;
    case ContainerExitStatus.KILLED_EXCEEDED_VMEM:
    case ContainerExitStatus.KILLED_EXCEEDED_PMEM:
      // No point in skipping the node as it's not the system's fault
      return false;
    case ContainerExitStatus.SUCCESS:
      return false;
    case ContainerExitStatus.INVALID:
      // Ideally, this shouldn't be considered for skipping a node. But in
      // reality, it seems like there are cases where we are not setting
      // exit-code correctly and so it's better to be conservative. See
      // YARN-4284.
      return true;
    default:
      return true;
    }
  }
}
