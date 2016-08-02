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

package org.apache.slider.core.zk;

import java.util.Locale;

public final class ZKPathBuilder {

  private final String username, appname, clustername;
  private final String quorum;

  private String appPath;
  private String registryPath;
  private final String appQuorum;
  
  public ZKPathBuilder(String username,
    String appname,
    String clustername,
    String quorum,
      String appQuorum) {
    this.username = username;
    this.appname = appname;
    this.clustername = clustername;
    this.quorum = quorum;
    appPath = buildAppPath();
    registryPath = buildRegistryPath();
    this.appQuorum = appQuorum;
  }

  public String buildAppPath() {
    return String.format(Locale.ENGLISH, "/yarnapps_%s_%s_%s", appname,
                         username, clustername);

  }

  public String buildRegistryPath() {
    return String.format(Locale.ENGLISH, "/services_%s_%s_%s", appname,
                         username, clustername);

  }

  public String getQuorum() {
    return quorum;
  }

  public String getAppQuorum() {
    return appQuorum;
  }

  public String getAppPath() {
    return appPath;
  }

  public void setAppPath(String appPath) {
    this.appPath = appPath;
  }

  public String getRegistryPath() {
    return registryPath;
  }

  public void setRegistryPath(String registryPath) {
    this.registryPath = registryPath;
  }

}
