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

package org.apache.hadoop.registry.client.impl;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.registry.client.impl.zk.RegistryBindingSource;
import org.apache.hadoop.registry.client.impl.zk.RegistryOperationsService;


/**
 * This is the client service for applications to work with the registry.
 *
 * It does not set up the root paths for the registry, is bonded
 * to a user, and can be set to use SASL, anonymous or id:pass auth.
 *
 * For SASL, the client must be operating in the context of an authed user.
 *
 * For id:pass the client must have the relevant id and password, SASL is
 * not used even if the client has credentials.
 *
 * For anonymous, nothing is used.
 *
 * Any SASL-authed client also has the ability to add one or more authentication
 * id:pass pair on all future writes, and to reset them later.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RegistryOperationsClient extends RegistryOperationsService {

  public RegistryOperationsClient(String name) {
    super(name);
  }

  public RegistryOperationsClient(String name,
      RegistryBindingSource bindingSource) {
    super(name, bindingSource);
  }
}
