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
 * Root class of YAML configuration.
 */
public class YamlConfigFile {
  private Spec spec;
  private Configs configs;
  private Roles roles;
  private Scheduling scheduling;
  private Security security;
  private TensorBoard tensorBoard;

  public Spec getSpec() {
    return spec;
  }

  public void setSpec(Spec spec) {
    this.spec = spec;
  }

  public Configs getConfigs() {
    return configs;
  }

  public void setConfigs(Configs configs) {
    this.configs = configs;
  }

  public Roles getRoles() {
    return roles;
  }

  public void setRoles(Roles roles) {
    this.roles = roles;
  }

  public Scheduling getScheduling() {
    return scheduling;
  }

  public void setScheduling(Scheduling scheduling) {
    this.scheduling = scheduling;
  }

  public Security getSecurity() {
    return security;
  }

  public void setSecurity(Security security) {
    this.security = security;
  }

  public TensorBoard getTensorBoard() {
    return tensorBoard;
  }

  public void setTensorBoard(TensorBoard tensorBoard) {
    this.tensorBoard = tensorBoard;
  }
}
