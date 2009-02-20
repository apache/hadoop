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

import java.util.ArrayList;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import com.facebook.thrift.*;

import com.facebook.thrift.protocol.*;
import com.facebook.thrift.transport.*;

public class ColumnSet implements TBase, java.io.Serializable  {
  public ArrayList<String> col;

  public ColumnSet() {
  }

  public ColumnSet(ArrayList<String> col)
  {
    this();
    this.col = col;
  }

  public String toString() {
    return col.toString();
  }
  
  public void read(TProtocol iprot) throws TException { 
    throw new RuntimeException ("Should not be called");
  }

  public void write(TProtocol oprot) throws TException { 
    TStruct struct = new TStruct("columnset");
    oprot.writeStructBegin(struct);
    TField field = new TField();
    if (this.col != null) {
      field.name = "col";
      field.type = TType.LIST;
      field.id = 1;
      oprot.writeFieldBegin(field);
      {
        oprot.writeListBegin(new TList(TType.STRING, this.col.size()));
        for (String _iter3 : this.col) {
          oprot.writeString(_iter3);
        }
        oprot.writeListEnd();
      }
      oprot.writeFieldEnd();
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }
}

