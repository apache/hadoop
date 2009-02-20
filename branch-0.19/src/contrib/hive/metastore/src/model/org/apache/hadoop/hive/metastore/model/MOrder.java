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

package org.apache.hadoop.hive.metastore.model;

public class MOrder {
  private String col;
  private int order;
  
  /**
   * @param col
   * @param order
   */
  public MOrder(String col, int order) {
    this.col = col;
    this.order = order;
  }

  /**
   * @return the col
   */
  public String getCol() {
    return col;
  }

  /**
   * @param col the col to set
   */
  public void setCol(String col) {
    this.col = col;
  }

  /**
   * @return the order
   */
  public int getOrder() {
    return order;
  }

  /**
   * @param order the order to set
   */
  public void setOrder(int order) {
    this.order = order;
  }
  
}
