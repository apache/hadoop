/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;

/**
 * Utilities for environment variable related operations
 * for {@link Service} objects.
 */
public final class EnvironmentUtilities {
  private EnvironmentUtilities() {
    throw new UnsupportedOperationException("This class should not be " +
        "instantiated!");
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(EnvironmentUtilities.class);

  static final String ENV_DOCKER_MOUNTS_FOR_CONTAINER_RUNTIME =
      "YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS";
  private static final String MOUNTS_DELIM = ",";
  private static final String ENV_SEPARATOR = "=";
  private static final String ETC_PASSWD_MOUNT_STRING =
      "/etc/passwd:/etc/passwd:ro";
  private static final String KERBEROS_CONF_MOUNT_STRING =
      "/etc/krb5.conf:/etc/krb5.conf:ro";
  private static final String ENV_VAR_DELIM = ":";

  /**
   * Extracts value from a string representation of an environment variable.
   * @param envVar The environment variable in 'key=value' format.
   * @return The value of the environment variable
   */
  public static String getValueOfEnvironment(String envVar) {
    if (envVar == null || !envVar.contains(ENV_SEPARATOR)) {
      return "";
    } else {
      return envVar.substring(envVar.indexOf(ENV_SEPARATOR) + 1);
    }
  }

  public static void handleServiceEnvs(Service service,
      Configuration yarnConfig, List<String> envVars) {
    if (envVars != null) {
      for (String envVarPair : envVars) {
        String key, value;
        if (envVarPair.contains(ENV_SEPARATOR)) {
          int idx = envVarPair.indexOf(ENV_SEPARATOR);
          key = envVarPair.substring(0, idx);
          value = envVarPair.substring(idx + 1);
        } else {
          LOG.warn("Found environment variable with unusual format: '{}'",
              envVarPair);
          // No "=" found so use the whole key
          key = envVarPair;
          value = "";
        }
        appendToEnv(service, key, value, ENV_VAR_DELIM);
      }
    }
    appendOtherConfigs(service, yarnConfig);
  }

  /**
   * Appends other configs like /etc/passwd, /etc/krb5.conf.
   * @param service
   * @param yarnConfig
   */
  private static void appendOtherConfigs(Service service,
      Configuration yarnConfig) {
    appendToEnv(service, ENV_DOCKER_MOUNTS_FOR_CONTAINER_RUNTIME,
        ETC_PASSWD_MOUNT_STRING, MOUNTS_DELIM);

    String authentication = yarnConfig.get(HADOOP_SECURITY_AUTHENTICATION);
    if (authentication != null && authentication.equals("kerberos")) {
      appendToEnv(service, ENV_DOCKER_MOUNTS_FOR_CONTAINER_RUNTIME,
          KERBEROS_CONF_MOUNT_STRING, MOUNTS_DELIM);
    }
  }

  static void appendToEnv(Service service, String key, String value,
      String delim) {
    Map<String, String> env = service.getConfiguration().getEnv();
    if (!env.containsKey(key)) {
      env.put(key, value);
    } else {
      if (!value.isEmpty()) {
        String existingValue = env.get(key);
        if (!existingValue.endsWith(delim)) {
          env.put(key, existingValue + delim + value);
        } else {
          env.put(key, existingValue + value);
        }
      }
    }
  }
}
