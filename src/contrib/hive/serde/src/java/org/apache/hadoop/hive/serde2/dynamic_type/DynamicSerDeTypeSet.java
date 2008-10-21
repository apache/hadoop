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
import com.facebook.thrift.TApplicationException;
import com.facebook.thrift.protocol.*;
import com.facebook.thrift.server.*;
import com.facebook.thrift.transport.*;
import java.util.*;
import java.io.*;
import org.apache.hadoop.hive.serde2.*;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.lang.reflect.*;
import com.facebook.thrift.protocol.TType;

public class DynamicSerDeTypeSet extends DynamicSerDeTypeBase {

  // production is: set<FieldType()>

  static final private int FD_TYPE = 0;

  public DynamicSerDeTypeSet(int i) {
    super(i);
  }
  public DynamicSerDeTypeSet(thrift_grammar p, int i) {
    super(p,i);
  }

  // returns Set<?>
  public Class getRealType() {
    try {
      Class c = this.getElementType().getRealType();
      Object o = c.newInstance();
      Set<?> l = Collections.singleton(o);
      return l.getClass();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public DynamicSerDeTypeBase getElementType() {
    return (DynamicSerDeTypeBase)((DynamicSerDeFieldType)this.jjtGetChild(FD_TYPE)).getMyType();
  }

  public String toString() {
    return "set<" + this.getElementType().toString()  + ">";
  }

  public byte getType() {
    return TType.SET;
  }

  /** NOTE: Set is not supported by Hive yet.
   */
  @Override
  public Object deserialize(Object reuse, TProtocol iprot)
  throws SerDeException, TException, IllegalAccessException {
    TSet theset = iprot.readSetBegin();
    Set<Object> result;
    if (reuse != null) {
      result = (Set<Object>)reuse;
      result.clear();
    } else {
      result = new HashSet<Object>();
    }
    for(int i = 0; i < theset.size; i++) {
      Object elem = this.getElementType().deserialize(null, iprot);
      result.add(elem);
    }
    // in theory, the below call isn't needed in non thrift_mode, but let's not get too crazy
    iprot.readSetEnd();
    return result;
  }

  /** NOTE: Set is not supported by Hive yet.
   *  The code uses ListObjectInspector right now. We need to change it to 
   *  SetObjectInspector when that is done.
   */
  @Override
  public void serialize(Object o, ObjectInspector oi, TProtocol oprot)
  throws TException, SerDeException, NoSuchFieldException,
  IllegalAccessException {

    ListObjectInspector loi = (ListObjectInspector)oi;

    Set<Object> set = (Set<Object>)o;
    DynamicSerDeTypeBase mt = this.getElementType();
    oprot.writeSetBegin(new TSet(mt.getType(),set.size()));
    for(Object element: set) {
      mt.serialize(element, loi.getListElementObjectInspector(), oprot);
    }
    // in theory, the below call isn't needed in non thrift_mode, but let's not get too crazy
    oprot.writeSetEnd();
  }
}
