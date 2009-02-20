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

@explain(displayName="Explosion")
public class explosionDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  private String fieldName;
  private int position;
  public explosionDesc() { }
  public explosionDesc(
    final String fieldName,
    final int position) {
    this.fieldName = fieldName;
    this.position = position;
  }
  public String getFieldName() {
    return this.fieldName;
  }
  public void setFieldName(final String fieldName) {
    this.fieldName=fieldName;
  }
  public int getPosition() {
    return this.position;
  }
  public void setPosition(final int position) {
    this.position=position;
  }
}
