/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.yarn.submarine.common.conf;

import org.apache.hadoop.conf.Configuration;

public class SubmarineConfiguration extends Configuration {
  private static final String SUBMARINE_CONFIGURATION_FILE = "submarine.xml";

  public static final String SUBMARINE_CONFIGURATION_PREFIX = "submarine.";

  public static final String SUBMARINE_LOCALIZATION_PREFIX =
      SUBMARINE_CONFIGURATION_PREFIX + "localization.";
  /**
   * Limit the size of directory/file to be localized.
   * To avoid exhausting local disk space,
   * this limit both remote and local file to be localized
   */
  public static final String LOCALIZATION_MAX_ALLOWED_FILE_SIZE_MB =
      SUBMARINE_LOCALIZATION_PREFIX + "max-allowed-file-size-mb";

  // Default 2GB
  public static final long DEFAULT_MAX_ALLOWED_REMOTE_URI_SIZE_MB = 2048;

  public SubmarineConfiguration() {
    this(new Configuration(false), true);
  }

  public SubmarineConfiguration(Configuration configuration) {
    this(configuration, false);
  }

  public SubmarineConfiguration(Configuration configuration,
      boolean loadLocalConfig) {
    super(configuration);
    if (loadLocalConfig) {
      addResource(SUBMARINE_CONFIGURATION_FILE);
    }
  }

  /*
   * Runtime of submarine
   */

  private static final String PREFIX = "submarine.";

  public static final String RUNTIME_CLASS = PREFIX + "runtime.class";
  public static final String DEFAULT_RUNTIME_CLASS =
      "org.apache.hadoop.yarn.submarine.runtimes.yarnservice.YarnServiceRuntimeFactory";

  public void setSubmarineRuntimeClass(String runtimeClass) {
    set(RUNTIME_CLASS, runtimeClass);
  }
}
