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

import org.apache.hadoop.fs.slive.Constants.Distribution;
import org.apache.hadoop.util.StringUtils;

/**
 * This class holds the data representing what an operations distribution and
 * its percentage is (between 0 and 1) and provides operations to access those
 * types and parse and unparse from and into strings
 */
class OperationData {

  private static final String SEP = ",";

  private Distribution distribution;
  private Double percent;

  OperationData(Distribution d, Double p) {
    this.distribution = d;
    this.percent = p;
  }

  /**
   * Expects a comma separated list (where the first element is the ratio
   * (between 0 and 100)) and the second element is the distribution (if
   * non-existent then uniform will be selected). If an empty list is passed in
   * then this element will just set the distribution (to uniform) and leave the
   * percent as null.
   */
  OperationData(String data) {
    String pieces[] = Helper.getTrimmedStrings(data);
    distribution = Distribution.UNIFORM;
    percent = null;
    if (pieces.length == 1) {
      percent = (Double.parseDouble(pieces[0]) / 100.0d);
    } else if (pieces.length >= 2) {
      percent = (Double.parseDouble(pieces[0]) / 100.0d);
      distribution = Distribution.valueOf(StringUtils.toUpperCase(pieces[1]));
    }
  }

  /**
   * Gets the distribution this operation represents
   * 
   * @return Distribution
   */
  Distribution getDistribution() {
    return distribution;
  }

  /**
   * Gets the 0 - 1 percent that this operations run ratio should be
   * 
   * @return Double (or null if not given)
   */
  Double getPercent() {
    return percent;
  }

  /**
   * Returns a string list representation of this object (if the percent is
   * null) then NaN will be output instead. Format is percent,distribution.
   */
  public String toString() {
    StringBuilder str = new StringBuilder();
    if (getPercent() != null) {
      str.append(getPercent() * 100.0d);
    } else {
      str.append(Double.NaN);
    }
    str.append(SEP);
    str.append(getDistribution().lowerName());
    return str.toString();
  }
}