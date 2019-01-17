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

package org.apache.hadoop.registry.server.services;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;

/**
 * Composite service that exports the add/remove methods.
 * <p>
 * This allows external classes to add services to these methods, after which
 * they follow the same lifecyce.
 * <p>
 * It is essential that any service added is in a state where it can be moved
 * on with that of the parent services. Specifically, do not add an uninited
 * service to a parent that is already inited —as the <code>start</code>
 * operation will then fail
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AddingCompositeService extends CompositeService {


  public AddingCompositeService(String name) {
    super(name);
  }

  @Override
  public void addService(Service service) {
    super.addService(service);
  }

  @Override
  public boolean removeService(Service service) {
    return super.removeService(service);
  }
}
