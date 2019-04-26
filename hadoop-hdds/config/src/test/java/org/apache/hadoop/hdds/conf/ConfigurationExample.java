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
package org.apache.hadoop.hdds.conf;

import java.util.concurrent.TimeUnit;

/**
 * Example configuration to test the configuration injection.
 */
@ConfigGroup(prefix = "ozone.scm.client")
public class ConfigurationExample {

  private String clientAddress;

  private String bindHost;

  private boolean compressionEnabled;

  private int port = 1234;

  private long waitTime = 1;

  @Config(key = "address", defaultValue = "localhost", description = "Client "
      + "addres (To test string injection).", tags = ConfigTag.MANAGEMENT)
  public void setClientAddress(String clientAddress) {
    this.clientAddress = clientAddress;
  }

  @Config(key = "bind.host", defaultValue = "0.0.0.0", description = "Bind "
      + "host(To test string injection).", tags = ConfigTag.MANAGEMENT)
  public void setBindHost(String bindHost) {
    this.bindHost = bindHost;
  }

  @Config(key = "compression.enabled", defaultValue = "true", description =
      "Compression enabled. (Just to test boolean flag)", tags =
      ConfigTag.MANAGEMENT)
  public void setCompressionEnabled(boolean compressionEnabled) {
    this.compressionEnabled = compressionEnabled;
  }

  @Config(key = "port", defaultValue = "1234", description = "Port number "
      + "config (To test in injection)", tags = ConfigTag.MANAGEMENT)
  public void setPort(int port) {
    this.port = port;
  }

  @Config(key = "wait", type = ConfigType.TIME, timeUnit =
      TimeUnit.SECONDS, defaultValue = "30m", description = "Wait time (To "
      + "test TIME config type)", tags = ConfigTag.MANAGEMENT)
  public void setWaitTime(long waitTime) {
    this.waitTime = waitTime;
  }

  public String getClientAddress() {
    return clientAddress;
  }

  public String getBindHost() {
    return bindHost;
  }

  public boolean isCompressionEnabled() {
    return compressionEnabled;
  }

  public int getPort() {
    return port;
  }

  public long getWaitTime() {
    return waitTime;
  }
}
