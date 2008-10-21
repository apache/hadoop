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

import com.facebook.thrift.TException;
import com.facebook.thrift.protocol.*;
import org.apache.hadoop.hive.serde2.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import com.facebook.thrift.protocol.TType;

public class DynamicSerDeTypeBool extends DynamicSerDeTypeBase {

  // production is: bool

  public DynamicSerDeTypeBool(int i) {
    super(i);
  }

  public DynamicSerDeTypeBool(thrift_grammar p, int i) {
    super(p, i);
  }

  public String toString() {
    return "bool";
  }

  @Override
  public Object deserialize(Object reuse, TProtocol iprot)
      throws SerDeException, TException, IllegalAccessException {
    return Boolean.valueOf(iprot.readBool());
  }

  @Override
  public void serialize(Object o, ObjectInspector oi, TProtocol oprot)
      throws TException, SerDeException, NoSuchFieldException,
      IllegalAccessException {
    assert (oi.getCategory() == ObjectInspector.Category.PRIMITIVE);
    assert (((PrimitiveObjectInspector) oi).getPrimitiveClass()
        .equals(Boolean.class));
    oprot.writeBool((Boolean) o);
  }

  public byte getType() {
    return TType.BOOL;
  }
}
