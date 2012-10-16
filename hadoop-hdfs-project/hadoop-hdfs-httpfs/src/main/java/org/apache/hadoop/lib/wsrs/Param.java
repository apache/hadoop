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

package org.apache.hadoop.lib.wsrs;

import org.apache.hadoop.classification.InterfaceAudience;

import java.text.MessageFormat;

@InterfaceAudience.Private
public abstract class Param<T> {
  private String name;
  protected T value;

  public Param(String name, T defaultValue) {
    this.name = name;
    this.value = defaultValue;
  }

  public String getName() {
    return name;
  }

  public T parseParam(String str) {
    try {
      value = (str != null && str.trim().length() > 0) ? parse(str) : value;
    } catch (Exception ex) {
      throw new IllegalArgumentException(
        MessageFormat.format("Parameter [{0}], invalid value [{1}], value must be [{2}]",
                             name, str, getDomain()));
    }
    return value;
  }

  public T value() {
    return value;
  }

  protected abstract String getDomain();

  protected abstract T parse(String str) throws Exception;

  @Override
  public String toString() {
    return (value != null) ? value.toString() : "NULL";
  }

}
