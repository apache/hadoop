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

package org.apache.hadoop.hive.serde2;

import java.lang.reflect.Type;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.Writable;

import com.facebook.thrift.TBase;
import com.facebook.thrift.protocol.TProtocol;
import com.facebook.thrift.protocol.TProtocolFactory;
import com.facebook.thrift.transport.TIOStreamTransport;

public class ThriftByteStreamTypedSerDe extends ByteStreamTypedSerDe {

  protected TIOStreamTransport outTransport, inTransport;
  protected TProtocol outProtocol, inProtocol;

  private void init(TProtocolFactory inFactory, TProtocolFactory outFactory) throws Exception {
    outTransport = new TIOStreamTransport(bos);
    inTransport = new TIOStreamTransport(bis);
    outProtocol = outFactory.getProtocol(outTransport);
    inProtocol = inFactory.getProtocol(inTransport);
  }

  public void initialize(Configuration job, Properties tbl) throws SerDeException {
    throw new SerDeException("ThriftByteStreamTypedSerDe is still semi-abstract");
  }

  public ThriftByteStreamTypedSerDe(Type objectType, TProtocolFactory inFactory,
                                    TProtocolFactory outFactory) throws SerDeException {
    super(objectType);
    try {
      init(inFactory, outFactory);
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  protected ObjectInspectorFactory.ObjectInspectorOptions getObjectInspectorOptions() {
    return ObjectInspectorFactory.ObjectInspectorOptions.THRIFT;
  }
  
  public Object deserialize(Writable field) throws SerDeException {
    Object obj = super.deserialize(field);
    try {
      ((TBase)obj).read(inProtocol);
    } catch (Exception e) {
      throw new SerDeException(e);
    }
    return obj;
  }

}
