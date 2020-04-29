/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin;


/**
 * YarnRuntime parameter enum for {@link DevicePlugin}.
 * It's passed into {@code onDevicesAllocated}.
 * Device plugin could populate {@link DeviceRuntimeSpec}
 * based on which YARN container runtime will use.
 * */
public enum YarnRuntimeType {

  RUNTIME_DEFAULT("default"),
  RUNTIME_DOCKER("docker");

  private final String name;

  YarnRuntimeType(String n) {
    this.name = n;
  }

  public String getName() {
    return name;
  }
}
