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

package org.apache.hadoop.hive.serde2.dynamic_type;

// basically just a container for the real type so more like a proxy
public class DynamicSerDeFieldType extends DynamicSerDeSimpleNode {

  // production: this.name | BaseType() | MapType() | SetType() | ListType()

  private final int FD_FIELD_TYPE = 0;
  public DynamicSerDeFieldType(int i) {
    super(i);
  }
  public DynamicSerDeFieldType(thrift_grammar p, int i) {
    super(p,i);
  }

  protected DynamicSerDeTypeBase getMyType() {
    // bugbug, need to deal with a named type here - i.e., look it up and proxy to it
    // should raise an exception if this is a typedef since won't be any children
    // and thus we can quickly find this comment and limitation.
    return (DynamicSerDeTypeBase)this.jjtGetChild(FD_FIELD_TYPE);
  }


}
