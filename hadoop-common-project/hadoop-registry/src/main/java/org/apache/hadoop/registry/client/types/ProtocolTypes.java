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

package org.apache.hadoop.registry.client.types;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * some common protocol types
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface ProtocolTypes {

  /**
   * Addresses are URIs of Hadoop Filesystem paths: {@value}.
   */
  String PROTOCOL_FILESYSTEM = "hadoop/filesystem";

  /**
   * Hadoop IPC,  "classic" or protobuf : {@value}.
   */
  String PROTOCOL_HADOOP_IPC = "hadoop/IPC";

  /**
   * Corba IIOP: {@value}.
   */
  String PROTOCOL_IIOP = "IIOP";

  /**
   * REST: {@value}.
   */
  String PROTOCOL_REST = "REST";

  /**
   * Java RMI: {@value}.
   */
  String PROTOCOL_RMI = "RMI";

  /**
   * SunOS RPC, as used by NFS and similar: {@value}.
   */
  String PROTOCOL_SUN_RPC = "sunrpc";

  /**
   * Thrift-based protocols: {@value}.
   */
  String PROTOCOL_THRIFT = "thrift";

  /**
   * Custom TCP protocol: {@value}.
   */
  String PROTOCOL_TCP = "tcp";

  /**
   * Custom UPC-based protocol : {@value}.
   */
  String PROTOCOL_UDP = "udp";

  /**
   * Default value —the protocol is unknown : "{@value}"
   */
  String PROTOCOL_UNKNOWN = "";

  /**
   * Web page: {@value}.
   *
   * This protocol implies that the URLs are designed for
   * people to view via web browsers.
   */
  String PROTOCOL_WEBUI = "webui";

  /**
   * Web Services: {@value}.
   */
  String PROTOCOL_WSAPI = "WS-*";

  /**
   * A zookeeper binding: {@value}.
   */
  String PROTOCOL_ZOOKEEPER_BINDING = "zookeeper";

}
