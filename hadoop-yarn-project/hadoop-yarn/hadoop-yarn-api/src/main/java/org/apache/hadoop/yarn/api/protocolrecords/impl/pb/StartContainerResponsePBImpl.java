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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.proto.YarnProtos.StringBytesMapProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainerResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainerResponseProtoOrBuilder;
import org.apache.hadoop.yarn.util.ProtoUtils;

import com.google.protobuf.ByteString;
    
public class StartContainerResponsePBImpl extends StartContainerResponse {
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
  
  public synchronized StartContainerResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return getProto().toString().replaceAll("\\n", ", ").replaceAll("\\s+", " ");
  }

  private synchronized void mergeLocalToBuilder() {
    if (this.serviceResponse != null) {
      addServiceResponseToProto();
    }
  }
  
  protected final ByteBuffer convertFromProtoFormat(ByteString byteString) {
    return ProtoUtils.convertFromProtoFormat(byteString);
  }

  protected final ByteString convertToProtoFormat(ByteBuffer byteBuffer) {
    return ProtoUtils.convertToProtoFormat(byteBuffer);
  }

  private synchronized void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = StartContainerResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
   

  @Override
  public synchronized Map<String, ByteBuffer> getAllServiceResponse() {
    initServiceResponse();
    return this.serviceResponse;
  }
  @Override
  public synchronized void setAllServiceResponse(
      Map<String, ByteBuffer> serviceResponses) {
    if(serviceResponses == null) {
      return;
    }
    initServiceResponse();
    this.serviceResponse.clear();
    this.serviceResponse.putAll(serviceResponses);
  }
  
  private synchronized void initServiceResponse() {
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
  
  private synchronized void addServiceResponseToProto() {
    maybeInitBuilder();
    builder.clearServiceResponse();
    if (serviceResponse == null)
      return;
    Iterable<StringBytesMapProto> iterable = new Iterable<StringBytesMapProto>() {
      
      @Override
      public synchronized Iterator<StringBytesMapProto> iterator() {
        return new Iterator<StringBytesMapProto>() {
          
          Iterator<String> keyIter = serviceResponse.keySet().iterator();
          
          @Override
          public synchronized void remove() {
            throw new UnsupportedOperationException();
          }
          
          @Override
          public synchronized StringBytesMapProto next() {
            String key = keyIter.next();
            return StringBytesMapProto.newBuilder().setKey(key).setValue(convertToProtoFormat(serviceResponse.get(key))).build();
          }
          
          @Override
          public synchronized boolean hasNext() {
            return keyIter.hasNext();
          }
        };
      }
    };
    builder.addAllServiceResponse(iterable);
  }
}  
