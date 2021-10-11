/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

/**
 * The Http Request / Response Headers for Rest AbfsClient.
 */
public class AbfsHttpHeader {
  private final String name;
  private final String value;

  public AbfsHttpHeader(final String name, final String value) {
    this.name = name;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public String getValue() {
    return value;
  }

  @Override
  public boolean equals(Object header) {
    Preconditions.checkArgument(header instanceof AbfsHttpHeader);
    AbfsHttpHeader httpHeader = (AbfsHttpHeader) header;
    return httpHeader.getName().equals(name) && httpHeader.getValue()
        .equals(value);
  }
}
