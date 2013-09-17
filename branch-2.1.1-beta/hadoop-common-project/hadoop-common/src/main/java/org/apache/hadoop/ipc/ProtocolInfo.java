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
package org.apache.hadoop.ipc;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;


/**
 * The protocol name that is used when a client and server connect.
 * By default the class name of the protocol interface is the protocol name.
 * 
 * Why override the default name (i.e. the class name)?
 * One use case overriding the default name (i.e. the class name) is when
 * there are multiple implementations of the same protocol, each with say a
 *  different version/serialization.
 * In Hadoop this is used to allow multiple server and client adapters
 * for different versions of the same protocol service.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface ProtocolInfo {
  String protocolName();  // the name of the protocol (i.e. rpc service)
  long protocolVersion() default -1; // default means not defined use old way
}
