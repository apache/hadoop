/*
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

package org.apache.hadoop.chukwa.datacollection.adaptor;

import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;

public class ChukwaTestAdaptor implements Adaptor {

  private long adaptorId = 0l;
  private String type = null;
  private String params = null;
  private long startOffset = 0l;
  private ChunkReceiver dest = null;
  
  @Override
  public String getCurrentStatus() throws AdaptorException {
    // TODO Auto-generated method stub
    return type+ " "+ params + " "+ startOffset;
  }

  @Override
  public String getStreamName() {
    // TODO Auto-generated method stub
    return "";
  }

  @Override
  public void hardStop() throws AdaptorException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public long shutdown() throws AdaptorException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void start(long adaptorID, String type, String params, long offset,
      ChunkReceiver dest) throws AdaptorException {
    this.adaptorId = adaptorID;
    this.type = type;
    this.params = params;
    this.startOffset = offset;
    this.dest = dest;
    System.out.println("adaptorId [" +adaptorId + "]");
    System.out.println("type [" +type+ "]");
    System.out.println("params [" +params+ "]");
    System.out.println("startOffset [" +startOffset+ "]");
    
    
  }
  

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getParams() {
    return params;
  }

  public void setParams(String params) {
    this.params = params;
  }

  public long getStartOffset() {
    return startOffset;
  }

  public void setStartOffset(long startOffset) {
    this.startOffset = startOffset;
  }

  public long getAdaptorId() {
    return adaptorId;
  }

  public void setAdaptorId(long adaptorId) {
    this.adaptorId = adaptorId;
  }

  public ChunkReceiver getDest() {
    return dest;
  }

  public void setDest(ChunkReceiver dest) {
    this.dest = dest;
  }
  
}
