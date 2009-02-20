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

public class DynamicSerDeFieldList extends DynamicSerDeSimpleNode implements Serializable {

  //  private void writeObject(ObjectOutputStream out) throws IOException {
  //    out.writeObject(types_by_column_name);
  //    out.writeObject(ordered_types);
  //  }

    // production: Field()*

    // mapping of the fieldid to the field
    private Map<Integer,DynamicSerDeTypeBase> types_by_id = null;
    private Map<String,DynamicSerDeTypeBase> types_by_column_name = null;
    private DynamicSerDeTypeBase ordered_types[] = null;

    public DynamicSerDeFieldList(int i) {
        super(i);
    }

    public DynamicSerDeFieldList(thrift_grammar p, int i) {
     super(p,i);
    }

    private DynamicSerDeField getField(int i) {
        return (DynamicSerDeField)this.jjtGetChild(i);
    }

    final protected DynamicSerDeField [] getChildren() {
        int size = this.jjtGetNumChildren();
        DynamicSerDeField result [] = new DynamicSerDeField[size];
        for(int i = 0; i < size; i++) {
            result[i] = (DynamicSerDeField)this.jjtGetChild(i);
        }
        return result;
    }

    private int getNumFields() {
        return this.jjtGetNumChildren();
    }

    public void initialize() {
        if(types_by_id == null) {
            // multiple means of lookup
            types_by_id = new HashMap<Integer, DynamicSerDeTypeBase> ();
            types_by_column_name = new HashMap<String, DynamicSerDeTypeBase> ();
            ordered_types = new DynamicSerDeTypeBase[this.jjtGetNumChildren()];

            // put them in and also roll them up while we're at it
            // a Field contains a FieldType which in turn contains a type
            for(int i = 0 ; i < this.jjtGetNumChildren(); i++) {
                DynamicSerDeField mt = this.getField(i);
                DynamicSerDeTypeBase type = mt.getFieldType().getMyType();
                type.fieldid = mt.fieldid;
                type.name = mt.name;

                types_by_id.put(Integer.valueOf(mt.fieldid) , type);
                types_by_column_name.put(mt.name, type);
                ordered_types[i] = type;
            }
        }
    }

    private DynamicSerDeTypeBase getFieldByFieldId(int i) {
        return types_by_id.get(i);
    }

    protected DynamicSerDeTypeBase getFieldByName(String fieldname) {
        return types_by_column_name.get(fieldname);
    }

    public DynamicSerDeTypeContainer deserialize(TProtocol iprot)  throws SerDeException, TException, IllegalAccessException {

        DynamicSerDeTypeContainer result = new DynamicSerDeTypeContainer();
        for(int i = 0; i < this.getNumFields(); i++) {
          DynamicSerDeTypeBase mt = null;
            TField field = null;

            if(thrift_mode) {
              field = iprot.readFieldBegin();

              if(field.type >= 0) {
                if(field.type == TType.STOP) {
                  break;
                }
                mt = this.getFieldByFieldId(field.id);
                if(mt == null) {
                    System.err.println("ERROR for fieldid: " + field.id + " system has no knowledge of this field which is of type : " + field.type);
                    TProtocolUtil.skip(iprot,field.type);
                    continue;
                }
              }
            }

            if(!thrift_mode || field.type < 0) {
                mt = this.ordered_types[i];
            }

            result.fields.put(mt.name, mt.deserialize(iprot));

            if(thrift_mode) {
                iprot.readFieldEnd();
            }
        }
        return result;
    }

    public void serialize(Map<String, Object> obj_fields, TProtocol oprot) throws TException, SerDeException, NoSuchFieldException,IllegalAccessException  {

        int i = 0;
        TField field = new TField();

        for(Iterator it = obj_fields.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry entry = (Map.Entry)it.next();
            String key = (String)entry.getKey();
            Object f = entry.getValue();
            DynamicSerDeTypeBase mt = this.types_by_column_name.get(key);

            if(thrift_mode) {
                field.name = mt.name;
                field.type = mt.getType();
                field.id = (short)mt.fieldid;
                oprot.writeFieldBegin(field);
            }

            mt.serialize(f, oprot);
            if(thrift_mode) {
                oprot.writeFieldEnd();
            }
            i++;
        }
        if(thrift_mode) {
            oprot.writeFieldStop();
        }
    }

    public String toString() {
        StringBuffer result = new StringBuffer();
        String prefix = "";
        for(DynamicSerDeField t: this.getChildren()) {
            result.append(prefix + t.fieldid + ":" + t.getFieldType().getMyType().toString() + " " + t.name);
            prefix = ",";
        }
        return result.toString();
    }
}
