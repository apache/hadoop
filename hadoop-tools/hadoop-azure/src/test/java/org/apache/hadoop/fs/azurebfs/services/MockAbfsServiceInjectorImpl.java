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

package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.conf.Configuration;

/**
 * Mock AbfsServiceInjectorImpl.
 */
public class MockAbfsServiceInjectorImpl extends AbfsServiceInjectorImpl {
  public MockAbfsServiceInjectorImpl(Configuration configuration) {
    super(configuration);
  }

  public <T> void replaceInstance(Class<T> tInterface, Object object) {
    this.removeInstance(tInterface);
    this.removeProvider(tInterface);
    this.getInstances().put(tInterface, object);
  }

  public <T> void removeInstance(Class<T> tInterface) {
    this.getInstances().remove(tInterface);
  }

  public <T> void replaceProvider(Class<T> tInterface, Class<? extends T> tClazz) {
    this.removeInstance(tInterface);
    this.removeProvider(tInterface);
    this.getProviders().put(tInterface, tClazz);
  }

  public <T> void removeProvider(Class<T> tInterface) {
    this.getProviders().remove(tInterface);
  }
}