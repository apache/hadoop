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

    public Set<Object> deserialize(TProtocol iprot)  throws SerDeException, TException, IllegalAccessException {
            TSet theset = iprot.readSetBegin();
            Set<Object> result = new HashSet<Object> () ;
            for(int i = 0; i < theset.size; i++) {
                Object elem = this.getElementType().deserialize(iprot);
                result.add(elem);
            }
            // in theory, the below call isn't needed in non thrift_mode, but let's not get too crazy
            iprot.readSetEnd();
            return result;
    }

    public void serialize(Object obj, TProtocol oprot) throws TException, SerDeException, NoSuchFieldException,IllegalAccessException  {
        Set<Object> set = (Set<Object>)obj;
            DynamicSerDeTypeBase mt = this.getElementType();
            oprot.writeSetBegin(new TSet(mt.getType(),set.size()));
            for(Object o: set) {
                mt.serialize(o,oprot);
            }
            // in theory, the below call isn't needed in non thrift_mode, but let's not get too crazy
            oprot.writeSetEnd();
    }
    public byte getType() {
        return TType.SET;
    }
}
