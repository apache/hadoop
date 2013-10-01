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
package org.apache.hadoop.mapreduce.v2.util;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

@Private
@Evolving
public class MRWebAppUtil {
  private static final Splitter ADDR_SPLITTER = Splitter.on(':').trimResults();
  private static final Joiner JOINER = Joiner.on("");

  private static boolean isSSLEnabledInYARN;
  private static boolean isSSLEnabledInJHS;
  private static boolean isSSLEnabledInMRAM;
  
  public static void initialize(Configuration conf) {
    setSSLEnabledInYARN(conf.getBoolean(
          CommonConfigurationKeysPublic.HADOOP_SSL_ENABLED_KEY,
          CommonConfigurationKeysPublic.HADOOP_SSL_ENABLED_DEFAULT));
    setSSLEnabledInJHS(conf.getBoolean(JHAdminConfig.MR_HS_SSL_ENABLED,
        JHAdminConfig.DEFAULT_MR_HS_SSL_ENABLED));
    setSSLEnabledInMRAM(conf.getBoolean(MRConfig.SSL_ENABLED_KEY,
        MRConfig.SSL_ENABLED_KEY_DEFAULT));
  }
  
  private static void setSSLEnabledInYARN(boolean isSSLEnabledInYARN) {
    MRWebAppUtil.isSSLEnabledInYARN = isSSLEnabledInYARN;
  }
  
  private static void setSSLEnabledInJHS(boolean isSSLEnabledInJHS) {
    MRWebAppUtil.isSSLEnabledInJHS = isSSLEnabledInJHS;
  }

  private static void setSSLEnabledInMRAM(boolean isSSLEnabledInMRAM) {
    MRWebAppUtil.isSSLEnabledInMRAM = isSSLEnabledInMRAM;
  }

  public static boolean isSSLEnabledInYARN() {
    return isSSLEnabledInYARN;
  }
  
  public static boolean isSSLEnabledInJHS() {
    return isSSLEnabledInJHS;
  }

  public static boolean isSSLEnabledInMRAM() {
    return isSSLEnabledInMRAM;
  }

  public static String getYARNWebappScheme() {
    if (isSSLEnabledInYARN) {
      return "https://";
    } else {
      return "http://";
    }
  }
  
  public static String getJHSWebappScheme() {
    if (isSSLEnabledInJHS) {
      return "https://";
    } else {
      return "http://";
    }
  }
  
  public static void setJHSWebappURLWithoutScheme(Configuration conf,
      String hostAddress) {
    if (isSSLEnabledInJHS) {
      conf.set(JHAdminConfig.MR_HISTORY_WEBAPP_HTTPS_ADDRESS, hostAddress);
    } else {
      conf.set(JHAdminConfig.MR_HISTORY_WEBAPP_ADDRESS, hostAddress);
    }
  }
  
  public static String getJHSWebappURLWithoutScheme(Configuration conf) {
    if (isSSLEnabledInJHS) {
      return conf.get(JHAdminConfig.MR_HISTORY_WEBAPP_HTTPS_ADDRESS,
          JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_HTTPS_ADDRESS);
    } else {
      return conf.get(JHAdminConfig.MR_HISTORY_WEBAPP_ADDRESS,
          JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_ADDRESS);
    }
  }
  
  public static String getJHSWebappURLWithScheme(Configuration conf) {
    return getJHSWebappScheme() + getJHSWebappURLWithoutScheme(conf);
  }
  
  public static InetSocketAddress getJHSWebBindAddress(Configuration conf) {
    if (isSSLEnabledInJHS) {
      return conf.getSocketAddr(JHAdminConfig.MR_HISTORY_WEBAPP_HTTPS_ADDRESS,
          JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_HTTPS_ADDRESS,
          JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_HTTPS_PORT);
    } else {
      return conf.getSocketAddr(JHAdminConfig.MR_HISTORY_WEBAPP_ADDRESS,
          JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_ADDRESS,
          JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_PORT);
    }
  }
  
  public static String getApplicationWebURLOnJHSWithoutScheme(Configuration conf,
      ApplicationId appId)
      throws UnknownHostException {
    //construct the history url for job
    String addr = getJHSWebappURLWithoutScheme(conf);
    Iterator<String> it = ADDR_SPLITTER.split(addr).iterator();
    it.next(); // ignore the bind host
    String port = it.next();
    // Use hs address to figure out the host for webapp
    addr = conf.get(JHAdminConfig.MR_HISTORY_ADDRESS,
        JHAdminConfig.DEFAULT_MR_HISTORY_ADDRESS);
    String host = ADDR_SPLITTER.split(addr).iterator().next();
    String hsAddress = JOINER.join(host, ":", port);
    InetSocketAddress address = NetUtils.createSocketAddr(
      hsAddress, getDefaultJHSWebappPort(),
      getDefaultJHSWebappURLWithoutScheme());
    StringBuffer sb = new StringBuffer();
    if (address.getAddress().isAnyLocalAddress() || 
        address.getAddress().isLoopbackAddress()) {
      sb.append(InetAddress.getLocalHost().getCanonicalHostName());
    } else {
      sb.append(address.getHostName());
    }
    sb.append(":").append(address.getPort());
    sb.append("/jobhistory/job/");
    JobID jobId = TypeConverter.fromYarn(appId);
    sb.append(jobId.toString());
    return sb.toString();
  }
  
  public static String getApplicationWebURLOnJHSWithScheme(Configuration conf,
      ApplicationId appId) throws UnknownHostException {
    return getJHSWebappScheme()
        + getApplicationWebURLOnJHSWithoutScheme(conf, appId);
  }

  private static int getDefaultJHSWebappPort() {
    if (isSSLEnabledInJHS) {
      return JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_HTTPS_PORT;
    } else {
      return JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_PORT;
    }
  }
  
  private static String getDefaultJHSWebappURLWithoutScheme() {
    if (isSSLEnabledInJHS) {
      return JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_HTTPS_ADDRESS;
    } else {
      return JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_ADDRESS;
    }
  }
  
  public static String getAMWebappScheme(Configuration conf) {
    if (isSSLEnabledInMRAM) {
      return "https://";
    } else {
      return "http://";
    }
  }
}