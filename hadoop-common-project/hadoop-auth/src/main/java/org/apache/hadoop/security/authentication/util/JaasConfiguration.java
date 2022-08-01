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
package org.apache.hadoop.security.authentication.util;

import java.util.HashMap;
import java.util.Map;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;


/**
 * Creates a programmatic version of a jaas.conf file. This can be used
 * instead of writing a jaas.conf file and setting the system property,
 * "java.security.auth.login.config", to point to that file. It is meant to be
 * used for connecting to ZooKeeper.
 */
public class JaasConfiguration extends Configuration {

  private final javax.security.auth.login.Configuration baseConfig =
      javax.security.auth.login.Configuration.getConfiguration();
  private final AppConfigurationEntry[] entry;
  private final String entryName;

  /**
   * Add an entry to the jaas configuration with the passed in name,
   * principal, and keytab. The other necessary options will be set for you.
   *
   * @param entryName The name of the entry (e.g. "Client")
   * @param principal The principal of the user
   * @param keytab The location of the keytab
   */
  public JaasConfiguration(String entryName, String principal, String keytab) {
    this.entryName = entryName;
    Map<String, String> options = new HashMap<>();
    options.put("keyTab", keytab);
    options.put("principal", principal);
    options.put("useKeyTab", "true");
    options.put("storeKey", "true");
    options.put("useTicketCache", "false");
    options.put("refreshKrb5Config", "true");
    String jaasEnvVar = System.getenv("HADOOP_JAAS_DEBUG");
    if ("true".equalsIgnoreCase(jaasEnvVar)) {
      options.put("debug", "true");
    }
    entry = new AppConfigurationEntry[]{
        new AppConfigurationEntry(getKrb5LoginModuleName(),
            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
            options)};
  }

  @Override
  public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
    return (entryName.equals(name)) ? entry : ((baseConfig != null)
        ? baseConfig.getAppConfigurationEntry(name) : null);
  }

  private String getKrb5LoginModuleName() {
    String krb5LoginModuleName;
    if (System.getProperty("java.vendor").contains("IBM")) {
      krb5LoginModuleName = "com.ibm.security.auth.module.Krb5LoginModule";
    } else {
      krb5LoginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
    }
    return krb5LoginModuleName;
  }
}
