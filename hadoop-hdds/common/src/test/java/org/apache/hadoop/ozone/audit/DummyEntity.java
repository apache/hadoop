/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.audit;

import java.util.HashMap;
import java.util.Map;

/**
 * DummyEntity that implements Auditable for test purpose.
 */
public class DummyEntity implements Auditable {

  private String key1;
  private String key2;

  public DummyEntity(){
    this.key1 = "value1";
    this.key2 = "value2";
  }
  public String getKey1() {
    return key1;
  }

  public void setKey1(String key1) {
    this.key1 = key1;
  }

  public String getKey2() {
    return key2;
  }

  public void setKey2(String key2) {
    this.key2 = key2;
  }

  @Override
  public Map<String, String> toAuditMap() {
    Map<String, String> auditMap = new HashMap<>();
    auditMap.put("key1", this.key1);
    auditMap.put("key2", this.key2);
    return auditMap;
  }
}
