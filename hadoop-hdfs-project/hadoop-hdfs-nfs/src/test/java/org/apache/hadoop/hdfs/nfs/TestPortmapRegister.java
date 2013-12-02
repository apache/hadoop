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

package org.apache.hadoop.hdfs.nfs;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.nfs.mount.RpcProgramMountd;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.oncrpc.RegistrationClient;
import org.apache.hadoop.oncrpc.RpcCall;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.CredentialsNone;
import org.apache.hadoop.oncrpc.security.VerifierNone;
import org.apache.hadoop.portmap.PortmapMapping;
import org.apache.hadoop.portmap.PortmapRequest;

public class TestPortmapRegister {
  
  public static final Log LOG = LogFactory.getLog(TestPortmapRegister.class);
  
  static void testRequest(XDR request, XDR request2) {
    RegistrationClient registrationClient = new RegistrationClient(
        "localhost", Nfs3Constant.SUN_RPCBIND, request);
    registrationClient.run();
  }
 
  public static void main(String[] args) throws InterruptedException {
    PortmapMapping mapEntry = new PortmapMapping(RpcProgramMountd.PROGRAM,
        RpcProgramMountd.VERSION_1, PortmapMapping.TRANSPORT_UDP,
        RpcProgramMountd.PORT);
    XDR mappingRequest = PortmapRequest.create(mapEntry);
    RegistrationClient registrationClient = new RegistrationClient(
        "localhost", Nfs3Constant.SUN_RPCBIND, mappingRequest);
    registrationClient.run();
        
    Thread t1 = new Runtest1();
    //Thread t2 = testa.new Runtest2();
    t1.start();
    //t2.start();
    t1.join();
    //t2.join();
    //testDump();
  }
  
  static class Runtest1 extends Thread {
    @Override
    public void run() {
      //testGetportMount();
      PortmapMapping mapEntry = new PortmapMapping(RpcProgramMountd.PROGRAM,
          RpcProgramMountd.VERSION_1, PortmapMapping.TRANSPORT_UDP,
          RpcProgramMountd.PORT);
      XDR req = PortmapRequest.create(mapEntry);
      testRequest(req, req);
    }
  }
  
  static class Runtest2 extends Thread {
    @Override
    public void run() {
      testDump();
    }
  }
  
  static void createPortmapXDRheader(XDR xdr_out, int procedure) {
    // TODO: Move this to RpcRequest
    RpcCall.getInstance(0, 100000, 2, procedure, new CredentialsNone(),
        new VerifierNone()).write(xdr_out);
    
    /*
    xdr_out.putInt(1); //unix auth
    xdr_out.putVariableOpaque(new byte[20]);
    xdr_out.putInt(0);
    xdr_out.putInt(0);
*/
  }
 
  static void testGetportMount() {
    XDR xdr_out = new XDR();

    createPortmapXDRheader(xdr_out, 3);

    xdr_out.writeInt(100005);
    xdr_out.writeInt(1);
    xdr_out.writeInt(6);
    xdr_out.writeInt(0);

    XDR request2 = new XDR();

    createPortmapXDRheader(xdr_out, 3);
    request2.writeInt(100005);
    request2.writeInt(1);
    request2.writeInt(6);
    request2.writeInt(0);

    testRequest(xdr_out, request2);
  }
  
  static void testGetport() {
    XDR xdr_out = new XDR();

    createPortmapXDRheader(xdr_out, 3);

    xdr_out.writeInt(100003);
    xdr_out.writeInt(3);
    xdr_out.writeInt(6);
    xdr_out.writeInt(0);

    XDR request2 = new XDR();

    createPortmapXDRheader(xdr_out, 3);
    request2.writeInt(100003);
    request2.writeInt(3);
    request2.writeInt(6);
    request2.writeInt(0);

    testRequest(xdr_out, request2);
  }
  
  static void testDump() {
    XDR xdr_out = new XDR();
    createPortmapXDRheader(xdr_out, 4);
    testRequest(xdr_out, xdr_out);
  }
}
