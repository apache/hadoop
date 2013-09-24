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
package org.apache.hadoop.mapreduce.v2.app.webapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;


public class WebAppUtil {
  private static boolean isSSLEnabledInYARN;
  
  public static void setSSLEnabledInYARN(boolean isSSLEnabledInYARN) {
    WebAppUtil.isSSLEnabledInYARN = isSSLEnabledInYARN;
  }
  
  public static boolean isSSLEnabledInYARN() {
    return isSSLEnabledInYARN;
  }
  
  public static String getSchemePrefix() {
    if (isSSLEnabledInYARN) {
      return "https://";
    } else {
      return "http://";
    }
  }
  
  public static void setJHSWebAppURLWithoutScheme(Configuration conf,
      String hostAddress) {
    if (HttpConfig.isSecure()) {
      conf.set(JHAdminConfig.MR_HISTORY_WEBAPP_HTTPS_ADDRESS, hostAddress);
    } else {
      conf.set(JHAdminConfig.MR_HISTORY_WEBAPP_ADDRESS, hostAddress);
    }
  }
  
  public static String getJHSWebAppURLWithoutScheme(Configuration conf) {
    if (HttpConfig.isSecure()) {
      return conf.get(JHAdminConfig.MR_HISTORY_WEBAPP_HTTPS_ADDRESS);
    } else {
      return conf.get(JHAdminConfig.MR_HISTORY_WEBAPP_ADDRESS);
    }
  }
}