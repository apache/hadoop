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

package org.apache.hadoop.yarn.service.monitor.probe;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;

import java.io.IOException;
import java.util.Map;

/**
 * Base class of all probes.
 */
public abstract class Probe implements MonitorKeys {

  protected final Configuration conf;
  private String name;

  /**
   * Create a probe of a specific name
   *
   * @param name probe name
   * @param conf configuration being stored.
   */
  public Probe(String name, Configuration conf) {
    this.name = name;
    this.conf = conf;
  }


  protected void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }


  @Override
  public String toString() {
    return getName();
  }

  public static String getProperty(Map<String, String> props, String name,
      String defaultValue) throws IOException {
    String value = props.get(name);
    if (StringUtils.isEmpty(value)) {
      if (defaultValue == null) {
        throw new IOException(name + " not specified");
      }
      return defaultValue;
    }
    return value;
  }

  public static int getPropertyInt(Map<String, String> props, String name,
      Integer defaultValue) throws IOException {
    String value = props.get(name);
    if (StringUtils.isEmpty(value)) {
      if (defaultValue == null) {
        throw new IOException(name + " not specified");
      }
      return defaultValue;
    }
    return Integer.parseInt(value);
  }

  /**
   * perform any prelaunch initialization
   */
  public void init() throws IOException {

  }

  /**
   * Ping the endpoint. All exceptions must be caught and included in the
   * (failure) status.
   *
   * @param instance instance to ping
   * @return the status
   */
  public abstract ProbeStatus ping(ComponentInstance instance);
}
