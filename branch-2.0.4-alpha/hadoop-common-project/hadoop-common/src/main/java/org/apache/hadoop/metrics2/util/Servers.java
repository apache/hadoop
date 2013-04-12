/*
 * Util.java
 *
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


package org.apache.hadoop.metrics2.util;

import java.net.InetSocketAddress;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.net.NetUtils;

/**
 * Helpers to handle server addresses
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Servers {
  /**
   * This class is not intended to be instantiated
   */
  private Servers() {}

  /**
   * Parses a space and/or comma separated sequence of server specifications
   * of the form <i>hostname</i> or <i>hostname:port</i>.  If
   * the specs string is null, defaults to localhost:defaultPort.
   *
   * @param specs   server specs (see description)
   * @param defaultPort the default port if not specified
   * @return a list of InetSocketAddress objects.
   */
  public static List<InetSocketAddress> parse(String specs, int defaultPort) {
    List<InetSocketAddress> result = Lists.newArrayList();
    if (specs == null) {
      result.add(new InetSocketAddress("localhost", defaultPort));
    }
    else {
      String[] specStrings = specs.split("[ ,]+");
      for (String specString : specStrings) {
        result.add(NetUtils.createSocketAddr(specString, defaultPort));
      }
    }
    return result;
  }
}
