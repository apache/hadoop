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
package org.apache.hadoop.security;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.WhitelistBasedResolver;
import org.apache.hadoop.util.TestFileBasedIPList;

public class TestWhitelistBasedResolver extends TestCase {

  public static final Map<String, String> SASL_PRIVACY_PROPS =
    WhitelistBasedResolver.getSaslProperties(new Configuration());

  public void testFixedVariableAndLocalWhiteList() throws IOException {

    String[] fixedIps = {"10.119.103.112", "10.221.102.0/23"};

    TestFileBasedIPList.createFileWithEntries ("fixedwhitelist.txt", fixedIps);

    String[] variableIps = {"10.222.0.0/16", "10.113.221.221"};

    TestFileBasedIPList.createFileWithEntries ("variablewhitelist.txt", variableIps);

    Configuration conf = new Configuration();
    conf.set(WhitelistBasedResolver.HADOOP_SECURITY_SASL_FIXEDWHITELIST_FILE ,
        "fixedwhitelist.txt");

    conf.setBoolean(WhitelistBasedResolver.HADOOP_SECURITY_SASL_VARIABLEWHITELIST_ENABLE,
        true);

    conf.setLong(WhitelistBasedResolver.HADOOP_SECURITY_SASL_VARIABLEWHITELIST_CACHE_SECS,
        1);

    conf.set(WhitelistBasedResolver.HADOOP_SECURITY_SASL_VARIABLEWHITELIST_FILE ,
        "variablewhitelist.txt");

    WhitelistBasedResolver wqr = new WhitelistBasedResolver ();
    wqr.setConf(conf);

    assertEquals (wqr.getDefaultProperties(),
        wqr.getServerProperties(InetAddress.getByName("10.119.103.112")));
    assertEquals (SASL_PRIVACY_PROPS, wqr.getServerProperties("10.119.103.113"));
    assertEquals (wqr.getDefaultProperties(), wqr.getServerProperties("10.221.103.121"));
    assertEquals (SASL_PRIVACY_PROPS, wqr.getServerProperties("10.221.104.0"));
    assertEquals (wqr.getDefaultProperties(), wqr.getServerProperties("10.222.103.121"));
    assertEquals (SASL_PRIVACY_PROPS, wqr.getServerProperties("10.223.104.0"));
    assertEquals (wqr.getDefaultProperties(), wqr.getServerProperties("10.113.221.221"));
    assertEquals (SASL_PRIVACY_PROPS, wqr.getServerProperties("10.113.221.222"));
    assertEquals (wqr.getDefaultProperties(), wqr.getServerProperties("127.0.0.1"));

    TestFileBasedIPList.removeFile("fixedwhitelist.txt");
    TestFileBasedIPList.removeFile("variablewhitelist.txt");
  }


  /**
   * Add a bunch of subnets and IPSs to the whitelist
   * Check  for inclusion in whitelist
   * Check for exclusion from whitelist
   */
  public void testFixedAndLocalWhiteList() throws IOException {

    String[] fixedIps = {"10.119.103.112", "10.221.102.0/23"};

    TestFileBasedIPList.createFileWithEntries ("fixedwhitelist.txt", fixedIps);

    String[] variableIps = {"10.222.0.0/16", "10.113.221.221"};

    TestFileBasedIPList.createFileWithEntries ("variablewhitelist.txt", variableIps);

    Configuration conf = new Configuration();

    conf.set(WhitelistBasedResolver.HADOOP_SECURITY_SASL_FIXEDWHITELIST_FILE ,
        "fixedwhitelist.txt");

    conf.setBoolean(WhitelistBasedResolver.HADOOP_SECURITY_SASL_VARIABLEWHITELIST_ENABLE,
        false);

    conf.setLong(WhitelistBasedResolver.HADOOP_SECURITY_SASL_VARIABLEWHITELIST_CACHE_SECS,
        100);

    conf.set(WhitelistBasedResolver.HADOOP_SECURITY_SASL_VARIABLEWHITELIST_FILE ,
        "variablewhitelist.txt");

    WhitelistBasedResolver wqr = new WhitelistBasedResolver();
    wqr.setConf(conf);

    assertEquals (wqr.getDefaultProperties(),
        wqr.getServerProperties(InetAddress.getByName("10.119.103.112")));

    assertEquals (SASL_PRIVACY_PROPS, wqr.getServerProperties("10.119.103.113"));

    assertEquals (wqr.getDefaultProperties(), wqr.getServerProperties("10.221.103.121"));

    assertEquals (SASL_PRIVACY_PROPS, wqr.getServerProperties("10.221.104.0"));
    assertEquals (SASL_PRIVACY_PROPS, wqr.getServerProperties("10.222.103.121"));
    assertEquals (SASL_PRIVACY_PROPS, wqr.getServerProperties("10.223.104.0"));
    assertEquals (SASL_PRIVACY_PROPS, wqr.getServerProperties("10.113.221.221"));
    assertEquals (SASL_PRIVACY_PROPS, wqr.getServerProperties("10.113.221.222"));
    assertEquals (wqr.getDefaultProperties(), wqr.getServerProperties("127.0.0.1"));;

    TestFileBasedIPList.removeFile("fixedwhitelist.txt");
    TestFileBasedIPList.removeFile("variablewhitelist.txt");
  }

  /**
   * Add a bunch of subnets and IPSs to the whitelist
   * Check  for inclusion in whitelist with a null value
   */
  public void testNullIPAddress() throws IOException {

    String[] fixedIps = {"10.119.103.112", "10.221.102.0/23"};

    TestFileBasedIPList.createFileWithEntries ("fixedwhitelist.txt", fixedIps);

    String[] variableIps = {"10.222.0.0/16", "10.113.221.221"};

    TestFileBasedIPList.createFileWithEntries ("variablewhitelist.txt", variableIps);

    Configuration conf = new Configuration();
    conf.set(WhitelistBasedResolver.HADOOP_SECURITY_SASL_FIXEDWHITELIST_FILE ,
        "fixedwhitelist.txt");

    conf.setBoolean(WhitelistBasedResolver.HADOOP_SECURITY_SASL_VARIABLEWHITELIST_ENABLE,
        true);

    conf.setLong(WhitelistBasedResolver.HADOOP_SECURITY_SASL_VARIABLEWHITELIST_CACHE_SECS,
        100);

    conf.set(WhitelistBasedResolver.HADOOP_SECURITY_SASL_VARIABLEWHITELIST_FILE ,
        "variablewhitelist.txt");

    WhitelistBasedResolver wqr = new WhitelistBasedResolver();
    wqr.setConf(conf);

    assertEquals (SASL_PRIVACY_PROPS, wqr.getServerProperties((InetAddress)null));
    assertEquals (SASL_PRIVACY_PROPS, wqr.getServerProperties((String)null));

    TestFileBasedIPList.removeFile("fixedwhitelist.txt");
    TestFileBasedIPList.removeFile("variablewhitelist.txt");
  }
}
