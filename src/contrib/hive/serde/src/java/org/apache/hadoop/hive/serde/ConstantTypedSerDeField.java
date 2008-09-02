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

package org.apache.hadoop.hive.serde;

import org.apache.hadoop.hive.serde.*;
import org.apache.hadoop.hive.serde.thrift.*;
import java.lang.reflect.*;

/**
 * Implementation of ConstantTypedSerDeField
 */


public class ConstantTypedSerDeField implements SerDeField {

  protected String _fieldName;
  protected Object _value;

  public String toString() {
    return "[fieldName=" + _fieldName + ",value=" + _value.toString() + "]";
  }

  public ConstantTypedSerDeField() throws SerDeException {
    throw new SerDeException("ConstantTypedSerDeField::empty constructor not implemented!");
  }


  public ConstantTypedSerDeField(String fieldName, Object value) throws SerDeException {
    _fieldName = fieldName;
    _value = value;
  }

  public Object get(Object obj) throws SerDeException {
    return _value;
  }

  public boolean isList() {
    return false;
  }

  public boolean isMap() {
    return false;
  }

  public boolean isPrimitive() {
    return true;
  }

  public Class getType() {
    // total hack. Since fieldName is a String, this does the right thing :) pw 2/5/08
    return this._value.getClass();
  }

  public Class getListElementType() {
    throw new RuntimeException("Not a list field ");
  }

  public Class getMapKeyType() {
    throw new RuntimeException("Not a map field ");
  }

  public Class getMapValueType() {
    throw new RuntimeException("Not a map field ");
  }

  public String getName() {
    return _fieldName;
  }
}
