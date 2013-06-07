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
 * Methods that need to be implemented to provide Portmap RPC program.
 * See RFC 1833 for details.
 */
public interface PortmapInterface {
  public enum Procedure {
    PMAPPROC_NULL(0),
    PMAPPROC_SET(1),
    PMAPPROC_UNSET(2),
    PMAPPROC_GETPORT(3),
    PMAPPROC_DUMP(4),
    PMAPPROC_CALLIT(5),
    PMAPPROC_GETTIME(6),
    PMAPPROC_UADDR2TADDR(7),
    PMAPPROC_TADDR2UADDR(8),
    PMAPPROC_GETVERSADDR(9),
    PMAPPROC_INDIRECT(10),
    PMAPPROC_GETADDRLIST(11),
    PMAPPROC_GETSTAT(12);
    
    private final int value;
    
    Procedure(int value) {
      this.value = value;
    }
    
    public int getValue() {
      return value;
    }
    
    public static Procedure fromValue(int value) {
      return values()[value];
    }
  }

  /**
   * This procedure does no work. By convention, procedure zero of any protocol
   * takes no parameters and returns no results.
   */
  public XDR nullOp(int xidd, XDR in, XDR out);
  
  /**
   * When a program first becomes available on a machine, it registers itself
   * with the port mapper program on the same machine. The program passes its
   * program number "prog", version number "vers", transport protocol number
   * "prot", and the port "port" on which it awaits service request. The
   * procedure returns a boolean reply whose value is "TRUE" if the procedure
   * successfully established the mapping and "FALSE" otherwise. The procedure
   * refuses to establish a mapping if one already exists for the tuple
   * "(prog, vers, prot)".
   */
  public XDR set(int xid, XDR in, XDR out);
  
  /**
   * When a program becomes unavailable, it should unregister itself with the
   * port mapper program on the same machine. The parameters and results have
   * meanings identical to those of "PMAPPROC_SET". The protocol and port number
   * fields of the argument are ignored.
   */
  public XDR unset(int xid, XDR in, XDR out);
  
  /**
   * Given a program number "prog", version number "vers", and transport
   * protocol number "prot", this procedure returns the port number on which the
   * program is awaiting call requests. A port value of zeros means the program
   * has not been registered. The "port" field of the argument is ignored.
   */
  public XDR getport(int xid, XDR in, XDR out);
  
  /**
   * This procedure enumerates all entries in the port mapper's database. The
   * procedure takes no parameters and returns a list of program, version,
   * protocol, and port values.
   */
  public XDR dump(int xid, XDR in, XDR out);
}
