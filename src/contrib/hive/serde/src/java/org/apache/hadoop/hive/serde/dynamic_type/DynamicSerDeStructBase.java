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

package org.apache.hadoop.hive.serde.dynamic_type;

import com.facebook.thrift.TException;
import com.facebook.thrift.TApplicationException;
import com.facebook.thrift.protocol.TBinaryProtocol;
import com.facebook.thrift.protocol.*;
import com.facebook.thrift.protocol.TProtocol;
import com.facebook.thrift.server.TServer;
import com.facebook.thrift.server.*;
import com.facebook.thrift.transport.*;
import com.facebook.thrift.transport.TServerTransport;
import java.util.*;
import java.io.*;
import org.apache.hadoop.hive.serde.*;
import java.lang.reflect.*;
import com.facebook.thrift.protocol.TType.*;

abstract public class DynamicSerDeStructBase extends DynamicSerDeTypeBase implements Serializable {

  DynamicSerDeFieldList fieldList;

  public DynamicSerDeStructBase(int i) {
    super(i);
  }
  public DynamicSerDeStructBase(thrift_grammar p, int i) {
    super(p,i);
  }

  abstract public DynamicSerDeFieldList getFieldList();

  public void initialize() {
    fieldList = getFieldList();
    fieldList.initialize();
  }


  public Class getRealType() {
    return DynamicSerDeTypeContainer.class;
  }

  public DynamicSerDeTypeContainer deserialize(TProtocol iprot)  throws SerDeException, TException, IllegalAccessException {
    if(thrift_mode) {
      iprot.readStructBegin();
    }

    DynamicSerDeTypeContainer container = fieldList.deserialize(iprot);

    if(thrift_mode) {
      iprot.readStructEnd();
    }
    return container;
  }

  /**
   * serialize
   *
   * The way to serialize a Thrift "table" which in thrift land is really a function and thus this class's name.
   *
   * @param o - this list should be in the order of the function's params for now. If we wanted to remove this requirement,
   * we'd need to make it a List<Pair<String, Object>> with the String being the field name.
   *
   */
  public void serialize(DynamicSerDeTypeContainer fields, TProtocol oprot) throws TException, SerDeException, NoSuchFieldException,IllegalAccessException  {
    final int seqid_ = 0; // bugbug set it right - todo pw

    if(thrift_mode) {
      oprot.writeStructBegin(new TStruct(this.name));
    }

    fieldList.serialize(fields.fields, oprot);

    if(thrift_mode) {
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }
  }
}
