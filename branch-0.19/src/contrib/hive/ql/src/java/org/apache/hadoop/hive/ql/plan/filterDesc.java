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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

@explain(displayName="Filter Operator")
public class filterDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  private org.apache.hadoop.hive.ql.plan.exprNodeDesc predicate;
  public filterDesc() { }
  public filterDesc(
    final org.apache.hadoop.hive.ql.plan.exprNodeDesc predicate) {
    this.predicate = predicate;
  }
  @explain(displayName="predicate")
  public org.apache.hadoop.hive.ql.plan.exprNodeDesc getPredicate() {
    return this.predicate;
  }
  public void setPredicate(final org.apache.hadoop.hive.ql.plan.exprNodeDesc predicate) {
    this.predicate = predicate;
  }
}
