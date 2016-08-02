/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.slider.server.servicemonitor;

/**
 * Config keys for monitoring
 */
public interface MonitorKeys {

  /**
   * Prefix of all other configuration options: {@value}
   */
  String MONITOR_KEY_PREFIX = "service.monitor.";


  /**
   * Classname of the reporter Key: {@value}
   */
  String MONITOR_REPORTER =
    MONITOR_KEY_PREFIX + "report.classname";

  /**
   * Interval in milliseconds between reporting health status to the reporter
   * Key: {@value}
   */
  String MONITOR_REPORT_INTERVAL =
    MONITOR_KEY_PREFIX + "report.interval";

  /**
   * Time in millis between the last probing cycle ending and the new one
   * beginning. Key: {@value}
   */
  String MONITOR_PROBE_INTERVAL =
    MONITOR_KEY_PREFIX + "probe.interval";

  /**
   * How long in milliseconds does the probing loop have to be blocked before
   * that is considered a liveness failure Key: {@value}
   */
  String MONITOR_PROBE_TIMEOUT =
    MONITOR_KEY_PREFIX + "probe.timeout";

  /**
   * How long in milliseconds does the probing loop have to be blocked before
   * that is considered a liveness failure Key: {@value}
   */
  String MONITOR_BOOTSTRAP_TIMEOUT =
    MONITOR_KEY_PREFIX + "bootstrap.timeout";


  /**
   * does the monitor depend on DFS being live
   */
  String MONITOR_DEPENDENCY_DFSLIVE =
    MONITOR_KEY_PREFIX + "dependency.dfslive";


  /**
   * default timeout for the entire bootstrap phase {@value}
   */

  int BOOTSTRAP_TIMEOUT_DEFAULT = 60000;


  /**
   * Default value if the key is not in the config file: {@value}
   */
  int REPORT_INTERVAL_DEFAULT = 10000;
  /**
   * Default value if the key is not in the config file: {@value}
   */
  int PROBE_INTERVAL_DEFAULT = 10000;
  /**
   * Default value if the key is not in the config file: {@value}
   */
  int PROBE_TIMEOUT_DEFAULT = 60000;

  /**
   * Port probe enabled/disabled flag Key: {@value}
   */
  String PORT_PROBE_ENABLED =
    MONITOR_KEY_PREFIX + "portprobe.enabled";


  /**
   * Port probing key : port to attempt to create a TCP connection to {@value}
   */
  String PORT_PROBE_PORT =
    MONITOR_KEY_PREFIX + "portprobe.port";

  /**
   * Port probing key : port to attempt to create a TCP connection to {@value}
   */
  String PORT_PROBE_HOST =
    MONITOR_KEY_PREFIX + "portprobe.host";


  /**
   * Port probing key : timeout of the connection attempt {@value}
   */
  String PORT_PROBE_CONNECT_TIMEOUT =
    MONITOR_KEY_PREFIX + "portprobe.connect.timeout";

  /**
   * Port probing key : bootstrap timeout -how long in milliseconds should the
   * port probing take to connect before the failure to connect is considered a
   * liveness failure. That is: how long should the IPC port take to come up?
   * {@value}
   */
  String PORT_PROBE_BOOTSTRAP_TIMEOUT =
    MONITOR_KEY_PREFIX + "portprobe.bootstrap.timeout";


  /**
   * default timeout for port probes {@value}
   */
  int PORT_PROBE_BOOTSTRAP_TIMEOUT_DEFAULT = 60000;

  /**
   * default value for port probe connection attempts {@value}
   */

  int PORT_PROBE_CONNECT_TIMEOUT_DEFAULT = 1000;


  /**
   * default port for probes {@value}
   */
  int DEFAULT_PROBE_PORT = 8020;


  /**
   * default host for probes {@value}
   */
  String DEFAULT_PROBE_HOST = "localhost";


  /**
   * Probe enabled/disabled flag Key: {@value}
   */
  String LS_PROBE_ENABLED =
    MONITOR_KEY_PREFIX + "lsprobe.enabled";

  /**
   * Probe path for LS operation Key: {@value}
   */
  String LS_PROBE_PATH =
    MONITOR_KEY_PREFIX + "lsprobe.path";

  /**
   * Default path for LS operation Key: {@value}
   */
  String LS_PROBE_DEFAULT = "/";

  /**
   * Port probing key : bootstrap timeout -how long in milliseconds should the
   * port probing take to connect before the failure to connect is considered a
   * liveness failure. That is: how long should the IPC port take to come up?
   * {@value}
   */
  String LS_PROBE_BOOTSTRAP_TIMEOUT =
    MONITOR_KEY_PREFIX + "lsprobe.bootstrap.timeout";


  /**
   * default timeout for port probes {@value}
   */

  int LS_PROBE_BOOTSTRAP_TIMEOUT_DEFAULT = PORT_PROBE_BOOTSTRAP_TIMEOUT_DEFAULT;


  /**
   * Probe enabled/disabled flag Key: {@value}
   */
  String WEB_PROBE_ENABLED =
    MONITOR_KEY_PREFIX + "webprobe.enabled";

  /**
   * Probe URL Key: {@value}
   */
  String WEB_PROBE_URL =
    MONITOR_KEY_PREFIX + "webprobe.url";

  /**
   * Default path for web probe Key: {@value}
   */
  String WEB_PROBE_DEFAULT_URL = "http://localhost:50070/";

  /**
   * min error code Key: {@value}
   */
  String WEB_PROBE_MIN =
    MONITOR_KEY_PREFIX + "webprobe.min";
  /**
   * min error code Key: {@value}
   */
  String WEB_PROBE_MAX =
    MONITOR_KEY_PREFIX + "webprobe.max";


  /**
   * Port probing key : timeout of the connection attempt {@value}
   */
  String WEB_PROBE_CONNECT_TIMEOUT =
    MONITOR_KEY_PREFIX + "webprobe.connect.timeout";

  /**
   * Default HTTP response code expected from the far end for
   * the endpoint to be considered live.
   */
  int WEB_PROBE_DEFAULT_CODE = 200;

  /**
   * Port probing key : bootstrap timeout -how long in milliseconds should the
   * port probing take to connect before the failure to connect is considered a
   * liveness failure. That is: how long should the IPC port take to come up?
   * {@value}
   */
  String WEB_PROBE_BOOTSTRAP_TIMEOUT =
    MONITOR_KEY_PREFIX + "webprobe.bootstrap.timeout";


  /**
   * default timeout for port probes {@value}
   */

  int WEB_PROBE_BOOTSTRAP_TIMEOUT_DEFAULT = PORT_PROBE_BOOTSTRAP_TIMEOUT_DEFAULT;

  /**
   * Probe enabled/disabled flag Key: {@value}
   */
  String JT_PROBE_ENABLED =
    MONITOR_KEY_PREFIX + "jtprobe.enabled";

  /**
   * Port probing key : bootstrap timeout -how long in milliseconds should the
   * port probing take to connect before the failure to connect is considered a
   * liveness failure. That is: how long should the IPC port take to come up?
   * {@value}
   */
  String JT_PROBE_BOOTSTRAP_TIMEOUT =
    MONITOR_KEY_PREFIX + "jtprobe.bootstrap.timeout";


  /**
   * default timeout for port probes {@value}
   */

  int JT_PROBE_BOOTSTRAP_TIMEOUT_DEFAULT = PORT_PROBE_BOOTSTRAP_TIMEOUT_DEFAULT;


  /**
   * Probe enabled/disabled flag Key: {@value}
   */
  String PID_PROBE_ENABLED =
    MONITOR_KEY_PREFIX + "pidprobe.enabled";

  /**
   * PID probing key : pid to attempt to create a TCP connection to {@value}
   */
  String PID_PROBE_PIDFILE =
    MONITOR_KEY_PREFIX + "pidprobe.pidfile";

}
