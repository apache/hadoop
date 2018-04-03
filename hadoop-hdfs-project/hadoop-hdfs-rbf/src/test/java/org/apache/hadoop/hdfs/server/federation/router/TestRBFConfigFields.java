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
package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.conf.TestConfigurationFieldsBase;

import java.util.HashSet;

/**
 * Unit test class to compare the following RBF configuration class:
 * <p></p>
 * {@link RBFConfigKeys}
 * <p></p>
 * against hdfs-rbf-default.xml for missing properties.
 * <p></p>
 * Refer to {@link org.apache.hadoop.conf.TestConfigurationFieldsBase}
 * for how this class works.
 */
public class TestRBFConfigFields extends TestConfigurationFieldsBase {
  @Override
  public void initializeMemberVariables() {
    xmlFilename = "hdfs-rbf-default.xml";
    configurationClasses = new Class[] {RBFConfigKeys.class};

    // Set error modes
    errorIfMissingConfigProps = true;
    errorIfMissingXmlProps = true;

    // Initialize used variables
    configurationPropsToSkipCompare = new HashSet<String>();

    // Allocate
    xmlPropsToSkipCompare = new HashSet<String>();
    xmlPrefixToSkipCompare = new HashSet<String>();
  }
}
