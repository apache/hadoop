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
import org.apache.hadoop.util.StringUtils;

import java.util.Arrays;

@InterfaceAudience.Private
public abstract class EnumParam<E extends Enum<E>> extends Param<E> {
  Class<E> klass;

  public EnumParam(String name, Class<E> e, E defaultValue) {
    super(name, defaultValue);
    klass = e;
  }

  @Override
  protected E parse(String str) throws Exception {
    return Enum.valueOf(klass, StringUtils.toUpperCase(str));
  }

  @Override
  protected String getDomain() {
    return StringUtils.join(",", Arrays.asList(klass.getEnumConstants()));
  }

}
