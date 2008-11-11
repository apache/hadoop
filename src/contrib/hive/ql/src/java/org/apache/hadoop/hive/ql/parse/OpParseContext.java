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

package org.apache.hadoop.hive.ql.parse;

import java.util.List;

/**
 * Implementation of the Operator Parse Context. It maintains the parse context
 * that may be needed by an operator. Currently, it only maintains the row
 * resolver and the list of columns used by the operator
 **/

public class OpParseContext {
  private RowResolver rr;  // row resolver for the operator

  // list of internal column names used
  private List<String> colNames;

  /**
   * @param rr row resolver
   */
  public OpParseContext(RowResolver rr) {
    this.rr = rr;
  }

  /**
   * @return the row resolver
   */
  public RowResolver getRR() {
    return rr;
  }

  /**
   * @param rr the row resolver to set
   */
  public void setRR(RowResolver rr) {
    this.rr = rr;
  }

  /**
   * @return the column names desired
   */
  public List<String> getColNames() {
    return colNames;
  }

  /**
   * @param colNames the column names to set
   */
  public void setColNames(List<String> colNames) {
    this.colNames = colNames;
  }
}
