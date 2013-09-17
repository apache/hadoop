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
package org.apache.hadoop.portmap;

import org.apache.hadoop.oncrpc.XDR;

/**
 * Represents a mapping entry for in the Portmap service for binding RPC
 * protocols. See RFC 1833 for details.
 * 
 * This maps a program to a port number.
 */
public class PortmapMapping {
  public static final int TRANSPORT_TCP = 6;
  public static final int TRANSPORT_UDP = 17;

  private final int program;
  private final int version;
  private final int transport;
  private final int port;

  public PortmapMapping(int program, int version, int transport, int port) {
    this.program = program;
    this.version = version;
    this.transport = transport;
    this.port = port;
  }

  public XDR serialize(XDR xdr) {
    xdr.writeInt(program);
    xdr.writeInt(version);
    xdr.writeInt(transport);
    xdr.writeInt(port);
    return xdr;
  }

  public static PortmapMapping deserialize(XDR xdr) {
    return new PortmapMapping(xdr.readInt(), xdr.readInt(), xdr.readInt(),
        xdr.readInt());
  }

  public int getPort() {
    return port;
  }

  public static String key(PortmapMapping mapping) {
    return mapping.program + " " + mapping.version + " " + mapping.transport;
  }
  
  @Override
  public String toString() {
    return String.format("(PortmapMapping-%d:%d:%d:%d)", program, version,
        transport, port);
  }
}
