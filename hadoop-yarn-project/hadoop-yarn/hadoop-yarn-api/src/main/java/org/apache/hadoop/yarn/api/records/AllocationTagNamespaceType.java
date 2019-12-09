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

package org.apache.hadoop.yarn.api.records;

/**
 * Class to describe all supported forms of namespaces for an allocation tag.
 */
public enum AllocationTagNamespaceType {

  SELF("self"),
  NOT_SELF("not-self"),
  APP_ID("app-id"),
  APP_TAG("app-tag"),
  ALL("all");

  private String typeKeyword;
  AllocationTagNamespaceType(String keyword) {
    this.typeKeyword = keyword;
  }

  public String getTypeKeyword() {
    return this.typeKeyword;
  }

  @Override
  public String toString() {
    return this.getTypeKeyword();
  }
}
