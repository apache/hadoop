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
package org.apache.hadoop.registry.conf;

import org.apache.hadoop.conf.Configuration;

/**
 * Intermediate configuration class to import the keys from YarnConfiguration
 * in yarn-default.xml and yarn-site.xml. Once hadoop-yarn-registry is totally
 * deprecated, this should be deprecated.
 */
public class RegistryConfiguration extends Configuration {

  static {
    Configuration.addDefaultResource("yarn-default.xml");
    Configuration.addDefaultResource("yarn-site.xml");
  }

  /**
   * Default constructor which relies on the static method to import the YARN
   * settings.
   */
  public RegistryConfiguration() {
    super();
  }
}
