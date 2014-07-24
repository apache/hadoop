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
package org.apache.hadoop.mapred.nativetask.testutil;

import org.apache.hadoop.conf.Configuration;

public class ScenarioConfiguration extends Configuration {
  public ScenarioConfiguration() {
    super();
    this.addResource(TestConstants.COMMON_CONF_PATH);
  }

  public void addcombinerConf() {
    this.addResource(TestConstants.COMBINER_CONF_PATH);
  }

  public void addKVTestConf() {
    this.addResource(TestConstants.KVTEST_CONF_PATH);
  }
  
  public void addNonSortTestConf() {
    this.addResource(TestConstants.NONSORT_TEST_CONF);
  }

  public void addNativeConf() {
    this.set(TestConstants.NATIVETASK_COLLECTOR_DELEGATOR,
        TestConstants.NATIVETASK_COLLECTOR_DELEGATOR_CLASS);
  }

  public static Configuration getNormalConfiguration() {
    Configuration normalConf = new Configuration();
    normalConf.addResource("common_conf.xml");
    normalConf.addResource("normal_conf.xml");
    return normalConf;
  }

  public static Configuration getNativeConfiguration() {
    Configuration nativeConf = new Configuration();
    nativeConf.addResource("common_conf.xml");
    nativeConf.addResource("native_conf.xml");
    return nativeConf;
  }
}
