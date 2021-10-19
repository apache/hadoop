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

package org.apache.hadoop.lib.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Configuration utilities.
 */
@InterfaceAudience.Private
public abstract class ConfigurationUtils {

  /**
   * Copy configuration key/value pairs from one configuration to another if a property exists in the target, it gets
   * replaced.
   *
   * @param source source configuration.
   * @param target target configuration.
   */
  public static void copy(Configuration source, Configuration target) {
    Check.notNull(source, "source");
    Check.notNull(target, "target");
    for (Map.Entry<String, String> entry : source) {
      target.set(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Injects configuration key/value pairs from one configuration to another if the key does not exist in the target
   * configuration.
   *
   * @param source source configuration.
   * @param target target configuration.
   */
  public static void injectDefaults(Configuration source, Configuration target) {
    Check.notNull(source, "source");
    Check.notNull(target, "target");
    for (Map.Entry<String, String> entry : source) {
      if (target.get(entry.getKey()) == null) {
        target.set(entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * Returns a new ConfigurationUtils instance with all inline values resolved.
   *
   * @return a new ConfigurationUtils instance with all inline values resolved.
   */
  public static Configuration resolve(Configuration conf) {
    Configuration resolved = new Configuration(false);
    for (Map.Entry<String, String> entry : conf) {
      resolved.set(entry.getKey(), conf.get(entry.getKey()));
    }
    return resolved;
  }

  // Canibalized from FileSystemAccess <code>Configuration.loadResource()</code>.

  /**
   * Create a configuration from an InputStream.
   * <p>
   * ERROR canibalized from <code>Configuration.loadResource()</code>.
   *
   * @param is inputstream to read the configuration from.
   *
   * @throws IOException thrown if the configuration could not be read.
   */
  public static void load(Configuration conf, InputStream is) throws IOException {
    conf.addResource(is);
  }
}
