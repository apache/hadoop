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

package org.apache.hadoop.fs.s3a.audit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;

/**
 * Log entry which can be serialized as java Serializable or hadoop Writable
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class SerializableLogEntryRecord implements Serializable, Writable {

  private String owner;
  private String bucket;
  private String timestamp;
  private String remoteip;
  private String requester;
  private String requestid;
  private String verb;
  private String key;
  private String requesturi;
  private String http;
  private String awserrorcode;
  private java.lang.Long bytessent;
  private java.lang.Long objectsize;
  private java.lang.Long totaltime;
  private java.lang.Long turnaroundtime;
  private String referrer;
  private String useragent;
  private String version;
  private String hostid;
  private String sigv;
  private String cypher;
  private String auth;
  private String endpoint;
  private String tls;
  private String tail;
  private java.util.Map<String,String> referrerMap;
  
  public SerializableLogEntryRecord() {
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    
  }

  @Override
  public void readFields(final DataInput in) throws IOException {

  }
}
