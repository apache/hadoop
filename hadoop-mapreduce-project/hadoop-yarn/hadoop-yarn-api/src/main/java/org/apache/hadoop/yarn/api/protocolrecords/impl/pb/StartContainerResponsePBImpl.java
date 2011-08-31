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

package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;


import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.List;


import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainerResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainerResponseProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.StringBytesMapProto;
    
public class StartContainerResponsePBImpl extends ProtoBase<StartContainerResponseProto> implements StartContainerResponse {
  StartContainerResponseProto proto = StartContainerResponseProto.getDefaultInstance();
  StartContainerResponseProto.Builder builder = null;
  boolean viaProto = false;
 
  private Map<String, ByteBuffer> serviceResponse = null;

  public StartContainerResponsePBImpl() {
    builder = StartContainerResponseProto.newBuilder();
  }

  public StartContainerResponsePBImpl(StartContainerResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public StartContainerResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.serviceResponse != null) {
      addServiceResponseToProto();
    }
  }
  
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = StartContainerResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
   

  @Override
  public Map<String, ByteBuffer> getAllServiceResponse() {
    initServiceResponse();
    return this.serviceResponse;
  }
  @Override
  public ByteBuffer getServiceResponse(String key) {
    initServiceResponse();
    return this.serviceResponse.get(key);
  }
  
  private void initServiceResponse() {
    if (this.serviceResponse != null) {
      return;
    }
    StartContainerResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<StringBytesMapProto> list = p.getServiceResponseList();
    this.serviceResponse = new HashMap<String, ByteBuffer>();

    for (StringBytesMapProto c : list) {
      this.serviceResponse.put(c.getKey(), convertFromProtoFormat(c.getValue()));
    }
  }
  
  @Override
  public void addAllServiceResponse(final Map<String, ByteBuffer> serviceResponse) {
    if (serviceResponse == null)
      return;
    initServiceResponse();
    this.serviceResponse.putAll(serviceResponse);
  }
  
  private void addServiceResponseToProto() {
    maybeInitBuilder();
    builder.clearServiceResponse();
    if (serviceResponse == null)
      return;
    Iterable<StringBytesMapProto> iterable = new Iterable<StringBytesMapProto>() {
      
      @Override
      public Iterator<StringBytesMapProto> iterator() {
        return new Iterator<StringBytesMapProto>() {
          
          Iterator<String> keyIter = serviceResponse.keySet().iterator();
          
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
          
          @Override
          public StringBytesMapProto next() {
            String key = keyIter.next();
            return StringBytesMapProto.newBuilder().setKey(key).setValue(convertToProtoFormat(serviceResponse.get(key))).build();
          }
          
          @Override
          public boolean hasNext() {
            return keyIter.hasNext();
          }
        };
      }
    };
    builder.addAllServiceResponse(iterable);
  }
  @Override
  public void setServiceResponse(String key, ByteBuffer val) {
    initServiceResponse();
    this.serviceResponse.put(key, val);
  }
  @Override
  public void removeServiceResponse(String key) {
    initServiceResponse();
    this.serviceResponse.remove(key);
  }
  @Override
  public void clearServiceResponse() {
    initServiceResponse();
    this.serviceResponse.clear();
  }
}  
