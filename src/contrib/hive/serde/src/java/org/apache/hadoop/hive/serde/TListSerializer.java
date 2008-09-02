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

// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

package org.apache.hadoop.hive.serde;

import java.util.ArrayList;
import java.util.Iterator;

import com.facebook.thrift.protocol.*;
import com.facebook.thrift.transport.*;
import com.facebook.thrift.*;

public class TListSerializer<T extends TBase> {

  private TProtocol iprot;
  private TProtocol oprot;
  private Class<T> tclass;
  private TField cField = new TField("tarray", TType.LIST, (short)1);
  private TList cList = new TList(TType.STRUCT, 0);
  private TStruct cStruct = new TStruct("TListSerializer");

  public TListSerializer (Class<T> arg_tclass, TProtocol arg_iprot, TProtocol arg_oprot) {
    iprot = arg_iprot;
    oprot = arg_oprot;
    tclass = arg_tclass;
  }

  public void read(ArrayList<T> tarray) throws TException {
    TField field;
    iprot.readStructBegin();
    while (true) {
      field = iprot.readFieldBegin();
      if (field.type == TType.STOP) {
        break;
      }
      switch (field.id) {
      case 1:
        if (field.type == TType.LIST) {
          TList _list = iprot.readListBegin();
          tarray.ensureCapacity(_list.size);
          for (int _i = 0; _i < _list.size; ++_i) {
            T _elem;
            try {
              _elem = tclass.newInstance();
            } catch (Exception e) {
              throw new TException(e);
            }
            _elem.read(iprot);
            tarray.add(_elem);
          }
          iprot.readListEnd();
        } else {
          TProtocolUtil.skip(iprot, field.type);
        }
        break;
      default:
        TProtocolUtil.skip(iprot, field.type);
        break;
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();
  }

  public void write(ArrayList<T> tarray) throws TException {
    oprot.writeStructBegin(cStruct);
    oprot.writeFieldBegin(cField);
    cList.size = tarray.size();
    oprot.writeListBegin(cList);
    for (T _iter : tarray) {
      _iter.write(oprot);
    }
    oprot.writeListEnd();
    oprot.writeFieldEnd();
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }
}
