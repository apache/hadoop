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

import org.apache.hadoop.yarn.service.api.records.ReadinessCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Formatter;
import java.util.Locale;

/**
 * Various utils to work with the monitor
 */
public final class MonitorUtils {
  protected static final Logger LOG = LoggerFactory.getLogger(MonitorUtils
      .class);

  private MonitorUtils() {
  }

  public static String toPlural(int val) {
    return val != 1 ? "s" : "";
  }

  /**
   * Convert milliseconds to human time -the exact format is unspecified
   * @param milliseconds a time in milliseconds
   * @return a time that is converted to human intervals
   */
  public static String millisToHumanTime(long milliseconds) {
    StringBuilder sb = new StringBuilder();
    // Send all output to the Appendable object sb
    Formatter formatter = new Formatter(sb, Locale.US);

    long s = Math.abs(milliseconds / 1000);
    long m = Math.abs(milliseconds % 1000);
    if (milliseconds > 0) {
      formatter.format("%d.%03ds", s, m);
    } else if (milliseconds == 0) {
      formatter.format("0");
    } else {
      formatter.format("-%d.%03ds", s, m);
    }
    return sb.toString();
  }

  public static Probe getProbe(ReadinessCheck readinessCheck) {
    if (readinessCheck == null) {
      return null;
    }
    if (readinessCheck.getType() == null) {
      return null;
    }
    try {
      switch (readinessCheck.getType()) {
      case HTTP:
        return HttpProbe.create(readinessCheck.getProperties());
      case PORT:
        return PortProbe.create(readinessCheck.getProperties());
      default:
        return null;
      }
    } catch (Throwable t) {
      throw new IllegalArgumentException("Error creating readiness check " +
          t);
    }
  }
}
