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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import java.util.HashMap;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "credentials-info")
@XmlAccessorType(XmlAccessType.FIELD)
public class CredentialsInfo {

  @XmlElementWrapper(name = "tokens")
  HashMap<String, String> tokens;

  @XmlElementWrapper(name = "secrets")
  HashMap<String, String> secrets;

  public CredentialsInfo() {
    tokens = new HashMap<String, String>();
    secrets = new HashMap<String, String>();
  }

  public HashMap<String, String> getTokens() {
    return tokens;
  }

  public HashMap<String, String> getSecrets() {
    return secrets;
  }

  public void setTokens(HashMap<String, String> tokens) {
    this.tokens = tokens;
  }

  public void setSecrets(HashMap<String, String> secrets) {
    this.secrets = secrets;
  }

}
