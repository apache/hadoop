/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.client.cli.param.yaml;

/**
 * Class that holds values found in 'security' section of YAML configuration.
 */
public class Security {
  private String keytab;
  private String principal;
  private boolean distributeKeytab;

  public String getKeytab() {
    return keytab;
  }

  public void setKeytab(String keytab) {
    this.keytab = keytab;
  }

  public String getPrincipal() {
    return principal;
  }

  public void setPrincipal(String principal) {
    this.principal = principal;
  }

  public boolean isDistributeKeytab() {
    return distributeKeytab;
  }

  public void setDistributeKeytab(boolean distributeKeytab) {
    this.distributeKeytab = distributeKeytab;
  }
}
