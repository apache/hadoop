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
import com.facebook.thrift.protocol.TType;
import java.lang.Integer;


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
        return "list<" + this.getElementType().toString()  + ">";
    }

    public ArrayList<Object> deserialize(TProtocol iprot)  throws SerDeException, TException, IllegalAccessException {
            TList thelist = iprot.readListBegin();
            ArrayList<Object> result = new ArrayList<Object> () ;
            for(int i = 0; i < thelist.size; i++) {
                Object elem = this.getElementType().deserialize(iprot);
                result.add(elem);
            }
            // in theory, the below call isn't needed in non thrift_mode, but let's not get too crazy
            iprot.readListEnd();
            return result;
    }

    public void serialize(Object obj, TProtocol oprot) throws TException, SerDeException, NoSuchFieldException,IllegalAccessException  {
        List<Object> list = (List<Object>)obj;
        DynamicSerDeTypeBase mt = this.getElementType();
            oprot.writeListBegin(new TList(mt.getType(),list.size()));
            for(Object o: list) {
                mt.serialize(o,oprot);
            }
            // in theory, the below call isn't needed in non thrift_mode, but let's not get too crazy
            oprot.writeListEnd();
    }

    public byte getType() {
        return TType.LIST;
    }

}
