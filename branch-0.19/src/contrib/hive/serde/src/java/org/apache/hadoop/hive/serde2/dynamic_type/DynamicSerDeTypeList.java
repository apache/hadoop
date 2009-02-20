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
import java.util.*;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.*;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import com.facebook.thrift.protocol.TType;

public class DynamicSerDeTypeList extends DynamicSerDeTypeBase {

  public boolean isPrimitive() { return false; }
  public boolean isList() { return true; }

  // production is: list<FieldType()>

  static final private int FD_TYPE = 0;

  public Class getRealType() {
    return java.util.ArrayList.class;
  }

  public DynamicSerDeTypeList(int i) {
    super(i);
  }
  public DynamicSerDeTypeList(thrift_grammar p, int i) {
    super(p,i);
  }

  public DynamicSerDeTypeBase getElementType() {
    return (DynamicSerDeTypeBase)((DynamicSerDeFieldType)this.jjtGetChild(FD_TYPE)).getMyType();
  }

  public String toString() {
    return Constants.LIST_TYPE_NAME + "<" + this.getElementType().toString()  + ">";
  }

  @Override
  public ArrayList<Object> deserialize(Object reuse, TProtocol iprot)  throws SerDeException, TException, IllegalAccessException {
    TList thelist = iprot.readListBegin();
    ArrayList<Object> deserializeReuse;
    if (reuse != null) {
      deserializeReuse = (ArrayList<Object>)reuse;
      // Trim to the size needed
      while (deserializeReuse.size() > thelist.size) {
        deserializeReuse.remove(deserializeReuse.size()-1);
      }
    } else {
      deserializeReuse = new ArrayList<Object>();
    }
    deserializeReuse.ensureCapacity(thelist.size);
    for(int i = 0; i < thelist.size; i++) {
      if (i+1 > deserializeReuse.size()) {
        deserializeReuse.add(this.getElementType().deserialize(null, iprot));
      } else {
        deserializeReuse.set(i, 
            this.getElementType().deserialize(deserializeReuse.get(i), iprot));
      }
    }
    // in theory, the below call isn't needed in non thrift_mode, but let's not get too crazy
    iprot.readListEnd();
    return deserializeReuse;
  }

  @Override
  public void serialize(Object o, ObjectInspector oi, TProtocol oprot) throws TException, SerDeException, NoSuchFieldException,IllegalAccessException  {
    ListObjectInspector loi = (ListObjectInspector)oi;
    ObjectInspector elementObjectInspector = loi.getListElementObjectInspector();
    DynamicSerDeTypeBase mt = this.getElementType();

    if (o instanceof List) {
      List<?> list = (List<?>)o;
      oprot.writeListBegin(new TList(mt.getType(),list.size()));
      for (Object element: list) {
        mt.serialize(element, elementObjectInspector, oprot);
      }
    } else {
      Object[] list = (Object[])o;
      oprot.writeListBegin(new TList(mt.getType(),list.length));
      for (Object element: list) {
        mt.serialize(element, elementObjectInspector, oprot);
      }
    }
    // in theory, the below call isn't needed in non thrift_mode, but let's not get too crazy
    oprot.writeListEnd();
  }

  public byte getType() {
    return TType.LIST;
  }

}
