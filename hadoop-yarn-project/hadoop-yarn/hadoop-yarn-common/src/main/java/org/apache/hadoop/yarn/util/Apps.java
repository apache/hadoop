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

import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

import static org.apache.hadoop.yarn.util.StringHelper.*;

/**
 * Yarn application related utilities
 */
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
    ApplicationId appId = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(ApplicationId.class);
    appId.setClusterTimestamp(Long.parseLong(it.next()));
    shouldHaveNext(prefix, s, it);
    appId.setId(Integer.parseInt(it.next()));
    return appId;
  }

  public static void shouldHaveNext(String prefix, String s, Iterator<String> it) {
    if (!it.hasNext()) {
      throwParseException(sjoin(prefix, ID), s);
    }
  }

  public static void throwParseException(String name, String s) {
    throw new YarnException(join("Error parsing ", name, ": ", s));
  }

  public static void setEnvFromInputString(Map<String, String> env,
      String envString) {
    if (envString != null && envString.length() > 0) {
      String childEnvs[] = envString.split(",");
      for (String cEnv : childEnvs) {
        String[] parts = cEnv.split("="); // split on '='
        String value = env.get(parts[0]);

        if (value != null) {
          // Replace $env with the child's env constructed by NM's
          // For example: LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/tmp
          value = parts[1].replace("$" + parts[0], value);
        } else {
          // example PATH=$PATH:/tmp
          value = System.getenv(parts[0]);
          if (value != null) {
            // the env key is present in the tt's env
            value = parts[1].replace("$" + parts[0], value);
          } else {
            // check for simple variable substitution
            // for e.g. ROOT=$HOME
            String envValue = System.getenv(parts[1].substring(1));
            if (envValue != null) {
              value = envValue;
            } else {
              // the env key is note present anywhere .. simply set it
              // example X=$X:/tmp or X=/tmp
              value = parts[1].replace("$" + parts[0], "");
            }
          }
        }
        addToEnvironment(env, parts[0], value);
      }
    }
  }

  private static final String SYSTEM_PATH_SEPARATOR =
      System.getProperty("path.separator");

  public static void addToEnvironment(
      Map<String, String> environment,
      String variable, String value) {
    String val = environment.get(variable);
    if (val == null) {
      val = value;
    } else {
      val = val + SYSTEM_PATH_SEPARATOR + value;
    }
    environment.put(variable, val);
  }
}
