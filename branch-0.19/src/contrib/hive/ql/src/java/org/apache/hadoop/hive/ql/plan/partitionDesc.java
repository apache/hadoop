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

@explain(displayName="Partition")
public class partitionDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  private tableDesc table;
  private java.util.LinkedHashMap<String, String> partSpec;
  public partitionDesc() { }
  public partitionDesc(
    final tableDesc table,
    final java.util.LinkedHashMap<String, String> partSpec) {
    this.table = table;
    this.partSpec = partSpec;
  }
  
  @explain(displayName="")
  public tableDesc getTableDesc() {
    return this.table;
  }
  public void setTableDesc(final tableDesc table) {
    this.table = table;
  }
  
  @explain(displayName="partition values")
  public java.util.LinkedHashMap<String, String> getPartSpec() {
    return this.partSpec;
  }
  public void setPartSpec(final java.util.LinkedHashMap<String, String> partSpec) {
    this.partSpec=partSpec;
  }
}
