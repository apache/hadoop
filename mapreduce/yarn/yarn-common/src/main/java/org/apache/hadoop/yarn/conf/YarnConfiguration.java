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

package org.apache.hadoop.yarn.conf;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;

public class YarnConfiguration extends Configuration {
  private static final Splitter ADDR_SPLITTER = Splitter.on(':').trimResults();
  private static final Joiner JOINER = Joiner.on("");

  public static final String RM_PREFIX = "yarn.server.resourcemanager.";

  public static final String SCHEDULER_ADDRESS = RM_PREFIX
      + "scheduler.address";    
  
  public static final String AM_EXPIRY_INTERVAL = RM_PREFIX
  + "application.expiry.interval";
  
  public static final String DEFAULT_SCHEDULER_BIND_ADDRESS = "0.0.0.0:8030";

  public static final String APPSMANAGER_ADDRESS = RM_PREFIX
      + "appsManager.address";

  public static final String DEFAULT_APPSMANAGER_BIND_ADDRESS =
      "0.0.0.0:8040";

  private static final String YARN_DEFAULT_XML_FILE = "yarn-default.xml";
  private static final String YARN_SITE_XML_FILE = "yarn-site.xml";

  public static final String APPLICATION_MANAGER_PRINCIPAL =
      "yarn.jobmanager.user-name";

  public static final String RM_WEBAPP_BIND_ADDRESS = RM_PREFIX
      + "webapp.address";

  public static final String DEFAULT_RM_WEBAPP_BIND_ADDRESS = "0.0.0.0:8088";

  static {
    Configuration.addDefaultResource(YARN_DEFAULT_XML_FILE);
    Configuration.addDefaultResource(YARN_SITE_XML_FILE);
  }

  public static final String RM_SERVER_PRINCIPAL_KEY =
      "yarn.resourcemanager.principal";

  public static final String APPLICATION_ACL_VIEW_APP =
      "application.acl-view-job";

  public static final String APPLICATION_ACL_MODIFY_APP =
      "application.acl-modify-job";

  public YarnConfiguration() {
    super();
  }
  
  public YarnConfiguration(Configuration conf) {
    super(conf);
    if (! (conf instanceof YarnConfiguration)) {
      this.reloadConfiguration();
    }
  }

  public static String getRMWebAppURL(Configuration conf) {
    String addr = conf.get(RM_WEBAPP_BIND_ADDRESS,
                           DEFAULT_RM_WEBAPP_BIND_ADDRESS);
    Iterator<String> it = ADDR_SPLITTER.split(addr).iterator();
    it.next(); // ignore the bind host
    String port = it.next();
    // Use apps manager address to figure out the host for webapp
    addr = conf.get(APPSMANAGER_ADDRESS, DEFAULT_APPSMANAGER_BIND_ADDRESS);
    String host = ADDR_SPLITTER.split(addr).iterator().next();
    return JOINER.join("http://", host, ":", port, "/");
  }
}
