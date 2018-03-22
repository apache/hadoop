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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.NoSuchElementException;
import java.util.Iterator;

import static org.apache.hadoop.http.HttpConfig.Policy;

@Private
@Evolving
public class MRWebAppUtil {
  private static final Splitter ADDR_SPLITTER = Splitter.on(':').trimResults();
  private static final Joiner JOINER = Joiner.on("");

  private static Policy httpPolicyInYarn;
  private static Policy httpPolicyInJHS;

  public static void initialize(Configuration conf) {
    setHttpPolicyInYARN(conf.get(
            YarnConfiguration.YARN_HTTP_POLICY_KEY,
            YarnConfiguration.YARN_HTTP_POLICY_DEFAULT));
    setHttpPolicyInJHS(conf.get(JHAdminConfig.MR_HS_HTTP_POLICY,
            JHAdminConfig.DEFAULT_MR_HS_HTTP_POLICY));
  }
  
  private static void setHttpPolicyInJHS(String policy) {
    MRWebAppUtil.httpPolicyInJHS = Policy.fromString(policy);
  }
  
  private static void setHttpPolicyInYARN(String policy) {
    MRWebAppUtil.httpPolicyInYarn = Policy.fromString(policy);
  }

  public static Policy getJHSHttpPolicy() {
    return MRWebAppUtil.httpPolicyInJHS;
  }

  public static Policy getYARNHttpPolicy() {
    return MRWebAppUtil.httpPolicyInYarn;
  }

  public static String getYARNWebappScheme() {
    return httpPolicyInYarn == HttpConfig.Policy.HTTPS_ONLY ? "https://"
        : "http://";
  }

  public static String getJHSWebappScheme(Configuration conf) {
    setHttpPolicyInJHS(conf.get(JHAdminConfig.MR_HS_HTTP_POLICY,
        JHAdminConfig.DEFAULT_MR_HS_HTTP_POLICY));
    return httpPolicyInJHS == HttpConfig.Policy.HTTPS_ONLY ? "https://"
        : "http://";
  }
  
  public static void setJHSWebappURLWithoutScheme(Configuration conf,
      String hostAddress) {
    if (httpPolicyInJHS == Policy.HTTPS_ONLY) {
      conf.set(JHAdminConfig.MR_HISTORY_WEBAPP_HTTPS_ADDRESS, hostAddress);
    } else {
      conf.set(JHAdminConfig.MR_HISTORY_WEBAPP_ADDRESS, hostAddress);
    }
  }
  
  public static String getJHSWebappURLWithoutScheme(Configuration conf) {
    if (httpPolicyInJHS == Policy.HTTPS_ONLY) {
      return conf.get(JHAdminConfig.MR_HISTORY_WEBAPP_HTTPS_ADDRESS,
          JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_HTTPS_ADDRESS);
    } else {
      return conf.get(JHAdminConfig.MR_HISTORY_WEBAPP_ADDRESS,
          JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_ADDRESS);
    }
  }
  
  public static String getJHSWebappURLWithScheme(Configuration conf) {
    return getJHSWebappScheme(conf) + getJHSWebappURLWithoutScheme(conf);
  }
  
  public static InetSocketAddress getJHSWebBindAddress(Configuration conf) {
    if (httpPolicyInJHS == Policy.HTTPS_ONLY) {
      return conf.getSocketAddr(
          JHAdminConfig.MR_HISTORY_BIND_HOST,
          JHAdminConfig.MR_HISTORY_WEBAPP_HTTPS_ADDRESS,
          JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_HTTPS_ADDRESS,
          JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_HTTPS_PORT);
    } else {
      return conf.getSocketAddr(
          JHAdminConfig.MR_HISTORY_BIND_HOST,
          JHAdminConfig.MR_HISTORY_WEBAPP_ADDRESS,
          JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_ADDRESS,
          JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_PORT);
    }
  }
  
  public static String getApplicationWebURLOnJHSWithoutScheme(Configuration conf,
      ApplicationId appId)
      throws UnknownHostException {
    //construct the history url for job
    String addr = getJHSWebappURLWithoutScheme(conf);
    String port;
    try{
      Iterator<String> it = ADDR_SPLITTER.split(addr).iterator();
      it.next(); // ignore the bind host
      port = it.next();
    } catch(NoSuchElementException e) {
      throw new IllegalArgumentException("MapReduce JobHistory WebApp Address"
        + " does not contain a valid host:port authority: " + addr);
    }
    // Use hs address to figure out the host for webapp
    addr = conf.get(JHAdminConfig.MR_HISTORY_ADDRESS,
        JHAdminConfig.DEFAULT_MR_HISTORY_ADDRESS);
    String host = ADDR_SPLITTER.split(addr).iterator().next();
    String hsAddress = JOINER.join(host, ":", port);
    InetSocketAddress address = NetUtils.createSocketAddr(
      hsAddress, getDefaultJHSWebappPort(),
      getDefaultJHSWebappURLWithoutScheme());
    StringBuffer sb = new StringBuffer();
    if (address.getAddress() != null &&
        (address.getAddress().isAnyLocalAddress() ||
         address.getAddress().isLoopbackAddress())) {
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
    return getJHSWebappScheme(conf)
        + getApplicationWebURLOnJHSWithoutScheme(conf, appId);
  }

  private static int getDefaultJHSWebappPort() {
    return httpPolicyInJHS == Policy.HTTPS_ONLY ?
      JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_HTTPS_PORT:
      JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_PORT;
  }
  
  private static String getDefaultJHSWebappURLWithoutScheme() {
    return httpPolicyInJHS == Policy.HTTPS_ONLY ?
      JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_HTTPS_ADDRESS :
      JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_ADDRESS;
  }

  public static String getAMWebappScheme(Configuration conf) {
    return "http://";
  }
}
