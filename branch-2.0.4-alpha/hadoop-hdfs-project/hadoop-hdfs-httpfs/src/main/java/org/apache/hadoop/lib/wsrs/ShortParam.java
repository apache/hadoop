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

@InterfaceAudience.Private
public abstract class ShortParam extends Param<Short> {

  private int radix;

  public ShortParam(String name, Short defaultValue, int radix) {
    super(name, defaultValue);
    this.radix = radix;
  }

  public ShortParam(String name, Short defaultValue) {
    this(name, defaultValue, 10);
  }

  @Override
  protected Short parse(String str) throws Exception {
    return Short.parseShort(str, radix);
  }

  @Override
  protected String getDomain() {
    return "a short";
  }
}
