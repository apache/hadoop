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

package org.apache.hadoop.registry.secure;

import org.apache.hadoop.security.authentication.util.KerberosUtil;

import javax.security.auth.login.AppConfigurationEntry;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

class KerberosConfiguration extends javax.security.auth.login.Configuration {
  private String principal;
  private String keytab;
  private boolean isInitiator;

  KerberosConfiguration(String principal, File keytab,
      boolean client) {
    this.principal = principal;
    this.keytab = keytab.getAbsolutePath();
    this.isInitiator = client;
  }

  public static javax.security.auth.login.Configuration createClientConfig(
      String principal,
      File keytab) {
    return new KerberosConfiguration(principal, keytab, true);
  }

  public static javax.security.auth.login.Configuration createServerConfig(
      String principal,
      File keytab) {
    return new KerberosConfiguration(principal, keytab, false);
  }

  @Override
  public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
    Map<String, String> options = new HashMap<String, String>();
    options.put("keyTab", keytab);
    options.put("principal", principal);
    options.put("useKeyTab", "true");
    options.put("storeKey", "true");
    options.put("doNotPrompt", "true");
    options.put("useTicketCache", "true");
    options.put("renewTGT", "true");
    options.put("refreshKrb5Config", "true");
    options.put("isInitiator", Boolean.toString(isInitiator));
    String ticketCache = System.getenv("KRB5CCNAME");
    if (ticketCache != null) {
      options.put("ticketCache", ticketCache);
    }
    options.put("debug", "true");

    return new AppConfigurationEntry[]{
        new AppConfigurationEntry(KerberosUtil.getKrb5LoginModuleName(),
            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
            options)
    };
  }

  @Override
  public String toString() {
    return "KerberosConfiguration with principal " + principal;
  }
}
