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

package org.apache.hadoop.fs.slive;

import java.text.DecimalFormat;
import java.text.NumberFormat;

/**
 * Simple class that holds the number formatters used in the slive application
 */
class Formatter {

  private static final String NUMBER_FORMAT = "###.###";

  private static NumberFormat decFormatter = null;

  private static NumberFormat percFormatter = null;

  /**
   * No construction allowed - only simple static accessor functions
   */
  private Formatter() {

  }

  /**
   * Gets a decimal formatter that has 3 decimal point precision
   * 
   * @return NumberFormat formatter
   */
  static synchronized NumberFormat getDecimalFormatter() {
    if (decFormatter == null) {
      decFormatter = new DecimalFormat(NUMBER_FORMAT);
    }
    return decFormatter;
  }

  /**
   * Gets a percent formatter that has 3 decimal point precision
   * 
   * @return NumberFormat formatter
   */
  static synchronized NumberFormat getPercentFormatter() {
    if (percFormatter == null) {
      percFormatter = NumberFormat.getPercentInstance();
      percFormatter.setMaximumFractionDigits(3);
    }
    return percFormatter;
  }

}
