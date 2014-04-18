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
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

/**
 * Yarn internal application-related utilities
 */
@Private
public class Apps {
  public static final String APP = "application";
  public static final String ID = "ID";

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

  public static void setEnvFromInputString(Map<String, String> env,
      String envString,  String classPathSeparator) {
    if (envString != null && envString.length() > 0) {
      String childEnvs[] = envString.split(",");
      Pattern p = Pattern.compile(Shell.getEnvironmentVariableRegex());
      for (String cEnv : childEnvs) {
        String[] parts = cEnv.split("="); // split on '='
        Matcher m = p.matcher(parts[1]);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
          String var = m.group(1);
          // replace $env with the child's env constructed by tt's
          String replace = env.get(var);
          // if this key is not configured by the tt for the child .. get it
          // from the tt's env
          if (replace == null)
            replace = System.getenv(var);
          // the env key is note present anywhere .. simply set it
          if (replace == null)
            replace = "";
          m.appendReplacement(sb, Matcher.quoteReplacement(replace));
        }
        m.appendTail(sb);
        addToEnvironment(env, parts[0], sb.toString(), classPathSeparator);
      }
    }
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
}
