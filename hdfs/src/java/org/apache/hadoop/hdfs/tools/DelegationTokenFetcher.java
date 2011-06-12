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
package org.apache.hadoop.hdfs.tools;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLConnection;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.DelegationTokenServlet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.TokenStorage;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;

/**
 * Fetch a DelegationToken from the current Namenode and store it in the
 * specified file.
 */
@InterfaceAudience.Private
public class DelegationTokenFetcher {
  private static final String USAGE =
    "fetchdt retrieves delegation tokens (optionally over http)\n" +
    "and writes them to specified file.\n" +
    "Usage: fetchdt [--webservice <namenode http addr>] <output filename>";
  
  private final DistributedFileSystem dfs;
  private final UserGroupInformation ugi;
  private final DataOutputStream out;

  /**
   * Command-line interface
   */
  public static void main(final String [] args) throws Exception {
    // Login the current user
    UserGroupInformation.getCurrentUser().doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        
        if(args.length == 3 && "--webservice".equals(args[0])) {
          getDTfromRemote(args[1], args[2]);
          return null;
        }
        // avoid annoying mistake
        if(args.length == 1 && "--webservice".equals(args[0])) {
          System.out.println(USAGE);
          return null;
        }
        if(args.length != 1 || args[0].isEmpty()) {
          System.out.println(USAGE);
          return null;
        }
        
        DataOutputStream out = null;
        
        try {
          Configuration conf = new HdfsConfiguration();
          DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(conf);
          out = new DataOutputStream(new FileOutputStream(args[0]));
          UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

          new DelegationTokenFetcher(dfs, out, ugi).go();
          
          out.flush();
          System.out.println("Succesfully wrote token of size " + 
              out.size() + " bytes to "+ args[0]);
        } catch (IOException ioe) {
          System.out.println("Exception encountered:\n" +
              StringUtils.stringifyException(ioe));
        } finally {
          if(out != null) out.close();
        }
        return null;
      }
    });

  }
  
  public DelegationTokenFetcher(DistributedFileSystem dfs, 
      DataOutputStream out, UserGroupInformation ugi) {
    checkNotNull("dfs", dfs); this.dfs = dfs;
    checkNotNull("out", out); this.out = out;
    checkNotNull("ugi", ugi); this.ugi = ugi;
  }
  
  private void checkNotNull(String s, Object o) {
    if(o == null) throw new IllegalArgumentException(s + " cannot be null.");
  }

  public void go() throws IOException {
    String fullName = ugi.getUserName();
    String shortName = ugi.getShortUserName();
    Token<DelegationTokenIdentifier> token = 
      dfs.getDelegationToken(new Text(fullName));
    
    // Reconstruct the ip:port of the Namenode
    String nnAddress = 
      InetAddress.getByName(dfs.getUri().getHost()).getHostAddress() 
      + ":" + dfs.getUri().getPort();
    token.setService(new Text(nnAddress));
    
    TokenStorage ts = new TokenStorage();
    ts.addToken(new Text(shortName), token);
    ts.write(out);
  }
  
  /**
   * Utility method to obtain a delegation token over http
   * @param nnHttpAddr Namenode http addr, such as http://namenode:50070
   * @param filename Name of file to store token in
   */
   static private void getDTfromRemote(String nnAddr, String filename) 
   throws IOException {
     // Enable Kerberos sockets
    System.setProperty("https.cipherSuites", "TLS_KRB5_WITH_3DES_EDE_CBC_SHA");
    String ugiPostfix = "";
    DataOutputStream file = null;
    DataInputStream dis = null;
    
    if(nnAddr.startsWith("http:"))
      ugiPostfix = "?ugi=" + UserGroupInformation.getCurrentUser().getShortUserName();
    
    try {
      System.out.println("Retrieving token from: " + 
          nnAddr + DelegationTokenServlet.PATH_SPEC + ugiPostfix);
      URL remoteURL = new URL(nnAddr + DelegationTokenServlet.PATH_SPEC + ugiPostfix);
      URLConnection connection = remoteURL.openConnection();
      
      InputStream in = connection.getInputStream();
      TokenStorage ts = new TokenStorage();
      dis = new DataInputStream(in);
      ts.readFields(dis);
      file = new DataOutputStream(new FileOutputStream(filename));
      ts.write(file);
      file.flush();
      System.out.println("Successfully wrote token of " + file.size() 
          + " bytes  to " + filename);
    } catch (Exception e) {
      throw new IOException("Unable to obtain remote token", e);
    } finally {
      if(dis != null) dis.close();
      if(file != null) file.close();
    }
  }
}
