/*
 * Copyright 2010 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.stargate.auth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.stargate.User;

public class HBCAuthenticator extends Authenticator {

  Configuration conf;

  /**
   * Default constructor
   */
  public HBCAuthenticator() {
    this(HBaseConfiguration.create());
  }

  /**
   * Constructor
   * @param conf
   */
  public HBCAuthenticator(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public User getUserForToken(String token) {
    String name = conf.get("stargate.auth.token." + token);
    if (name == null) {
      return null;
    }
    boolean admin = conf.getBoolean("stargate.auth.user." + name + ".admin",
      false);
    boolean disabled = conf.getBoolean("stargate.auth.user." + name + ".disabled",
      false);
    return new User(name, token, admin, disabled);
  }

}
