/*
 * Copyright 2009 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.stargate;

public interface Constants {
  public static final String MIMETYPE_TEXT = "text/plain";
  public static final String MIMETYPE_XML = "text/xml";
  public static final String MIMETYPE_BINARY = "application/octet-stream";
  public static final String MIMETYPE_PROTOBUF = "application/x-protobuf";
  public static final String MIMETYPE_JSON = "application/json";
  public static final String MIMETYPE_JAVASCRIPT = "application/x-javascript";
  
  public static final String PATH_STATUS_CLUSTER = "/status/cluster";
  public static final String PATH_VERSION = "/version";
  public static final String PATH_VERSION_CLUSTER = "/version/cluster";
}
