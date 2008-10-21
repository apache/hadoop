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

public class DynamicSerDeTypeByte extends DynamicSerDeTypeBase {

  // production is: byte


  public DynamicSerDeTypeByte(int i) {
    super(i);
  }
  public DynamicSerDeTypeByte(thrift_grammar p, int i) {
    super(p,i);
  }

  public String toString() { return "byte"; }

  public Byte deserialize(TProtocol iprot)  throws SerDeException, TException, IllegalAccessException {
    return Byte.valueOf(iprot.readByte());
  }
  public Object deserialize(Object reuse, TProtocol iprot)  throws SerDeException, TException, IllegalAccessException {
    return Byte.valueOf(iprot.readByte());
  }

  public void serialize(Object s, TProtocol oprot) throws TException, SerDeException, NoSuchFieldException,IllegalAccessException  {
    // bugbug need to use object of byte type!!!
    oprot.writeByte((Byte)s);
  }

  public void serialize(Object o, ObjectInspector oi, TProtocol oprot) throws TException, SerDeException, NoSuchFieldException,IllegalAccessException  {
    assert(oi.getCategory() == ObjectInspector.Category.PRIMITIVE);
    assert(((PrimitiveObjectInspector)oi).getPrimitiveClass().equals(Byte.class));
    oprot.writeByte((Byte)o);
  }

  public byte getType() {
    return TType.BYTE;
  }
}
