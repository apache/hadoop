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

package org.apache.hadoop.yarn.api.records.impl.pb;


import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.proto.YarnProtos.URLProto;
import org.apache.hadoop.yarn.proto.YarnProtos.URLProtoOrBuilder;


    
public class URLPBImpl extends ProtoBase<URLProto> implements URL {
  URLProto proto = URLProto.getDefaultInstance();
  URLProto.Builder builder = null;
  boolean viaProto = false;
  
  public URLPBImpl() {
    builder = URLProto.newBuilder();
  }

  public URLPBImpl(URLProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public URLProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = URLProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public String getFile() {
    URLProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasFile()) {
      return null;
    }
    return (p.getFile());
  }

  @Override
  public void setFile(String file) {
    maybeInitBuilder();
    if (file == null) { 
      builder.clearFile();
      return;
    }
    builder.setFile((file));
  }
  @Override
  public String getScheme() {
    URLProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasScheme()) {
      return null;
    }
    return (p.getScheme());
  }

  @Override
  public void setScheme(String scheme) {
    maybeInitBuilder();
    if (scheme == null) { 
      builder.clearScheme();
      return;
    }
    builder.setScheme((scheme));
  }
  @Override
  public String getHost() {
    URLProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasHost()) {
      return null;
    }
    return (p.getHost());
  }

  @Override
  public void setHost(String host) {
    maybeInitBuilder();
    if (host == null) { 
      builder.clearHost();
      return;
    }
    builder.setHost((host));
  }
  @Override
  public int getPort() {
    URLProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getPort());
  }

  @Override
  public void setPort(int port) {
    maybeInitBuilder();
    builder.setPort((port));
  }



}  
