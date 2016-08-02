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

package org.apache.slider.common;

/**
 * Keys shared across tests
 */
public interface SliderXMLConfKeysForTesting {
  
  String KEY_TEST_THAW_WAIT_TIME = "slider.test.thaw.wait.seconds";

  int DEFAULT_THAW_WAIT_TIME_SECONDS = 60;


  String KEY_TEST_FREEZE_WAIT_TIME = "slider.test.freeze.wait.seconds";

  int DEFAULT_TEST_FREEZE_WAIT_TIME_SECONDS = 60;

  String KEY_TEST_TIMEOUT = "slider.test.timeout.seconds";

  int DEFAULT_TEST_TIMEOUT_SECONDS = 30 * 60;

  String KEY_ACCUMULO_LAUNCH_TIME =
    "slider.test.accumulo.launch.wait.seconds";
  int DEFAULT_ACCUMULO_LAUNCH_TIME_SECONDS = 60 * 3;

  String KEY_ACCUMULO_GO_LIVE_TIME =
      "slider.test.accumulo.live.wait.seconds";
  int DEFAULT_ACCUMULO_LIVE_TIME_SECONDS = 90;

  String KEY_TEST_AGENT_ENABLED = "slider.test.agent.enabled";
  String KEY_AGENTTESTS_QUEUE_LABELED_DEFINED = "slider.test.agent.labeled.queue.enabled";
  String KEY_AGENTTESTS_LABELS_RED_BLUE_DEFINED = "slider.test.agent.labels.defined";
  String KEY_AGENTTESTS_AM_FAILURES_ENABLED = "slider.test.agent.am.failures.enabled";

  int DEFAULT_AGENT_LAUNCH_TIME_SECONDS = 60 * 3;

  String KEY_TEST_AGENT_HOME = "slider.test.agent.home";
  String KEY_TEST_AGENT_TAR = "slider.test.agent.tar";

  String KEY_TEST_TEARDOWN_KILLALL = "slider.test.teardown.killall";
  boolean DEFAULT_TEARDOWN_KILLALL = true;


  /**
   * Key for amount of RAM to request
   */
  String KEY_TEST_YARN_RAM_REQUEST = "slider.test.yarn.ram";
  String DEFAULT_YARN_RAM_REQUEST = "192";

  /**
   * security related keys
   */
  String TEST_SECURITY_DIR = "/tmp/work/security";

  /**
   * Local path to AM keytab: {@value}
   */
  String KEY_TEST_AM_KEYTAB = "slider.test.am.keytab.local";

  /**
   * Is the test cluster windows? Default is: same as the local system.
   *  {@value}
   */
  String KEY_TEST_WINDOWS_CLUSTER = "slider.test.windows.cluster";

}
