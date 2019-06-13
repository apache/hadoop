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
package org.apache.hadoop.util;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

/**
 * Utilities for working with ZooKeeper.
 */
@InterfaceAudience.Private
public class ZKUtil {
  
  /**
   * Parse ACL permission string, partially borrowed from
   * ZooKeeperMain private method
   */
  private static int getPermFromString(String permString) {
    int perm = 0;
    for (int i = 0; i < permString.length(); i++) {
      char c = permString.charAt(i); 
      switch (c) {
      case 'r':
        perm |= ZooDefs.Perms.READ;
        break;
      case 'w':
        perm |= ZooDefs.Perms.WRITE;
        break;
      case 'c':
        perm |= ZooDefs.Perms.CREATE;
        break;
      case 'd':
        perm |= ZooDefs.Perms.DELETE;
        break;
      case 'a':
        perm |= ZooDefs.Perms.ADMIN;
        break;
      default:
        throw new BadAclFormatException(
            "Invalid permission '" + c + "' in permission string '" +
            permString + "'");
      }
    }
    return perm;
  }

  /**
   * Helper method to remove a subset of permissions (remove) from a
   * given set (perms).
   * @param perms The permissions flag to remove from. Should be an OR of a
   *              some combination of {@link ZooDefs.Perms}
   * @param remove The permissions to be removed. Should be an OR of a
   *              some combination of {@link ZooDefs.Perms}
   * @return A permissions flag that is an OR of {@link ZooDefs.Perms}
   * present in perms and not present in remove
   */
  public static int removeSpecificPerms(int perms, int remove) {
    return perms ^ remove;
  }

  /**
   * Parse comma separated list of ACL entries to secure generated nodes, e.g.
   * <code>sasl:hdfs/host1@MY.DOMAIN:cdrwa,sasl:hdfs/host2@MY.DOMAIN:cdrwa</code>
   *
   * @return ACL list
   * @throws {@link BadAclFormatException} if an ACL is invalid
   */
  public static List<ACL> parseACLs(String aclString) throws
      BadAclFormatException {
    List<ACL> acl = Lists.newArrayList();
    if (aclString == null) {
      return acl;
    }
    
    List<String> aclComps = Lists.newArrayList(
        Splitter.on(',').omitEmptyStrings().trimResults()
        .split(aclString));
    for (String a : aclComps) {
      // from ZooKeeperMain private method
      int firstColon = a.indexOf(':');
      int lastColon = a.lastIndexOf(':');
      if (firstColon == -1 || lastColon == -1 || firstColon == lastColon) {
        throw new BadAclFormatException(
            "ACL '" + a + "' not of expected form scheme:id:perm");
      }

      ACL newAcl = new ACL();
      newAcl.setId(new Id(a.substring(0, firstColon), a.substring(
          firstColon + 1, lastColon)));
      newAcl.setPerms(getPermFromString(a.substring(lastColon + 1)));
      acl.add(newAcl);
    }
    
    return acl;
  }
  
  /**
   * Parse a comma-separated list of authentication mechanisms. Each
   * such mechanism should be of the form 'scheme:auth' -- the same
   * syntax used for the 'addAuth' command in the ZK CLI.
   * 
   * @param authString the comma-separated auth mechanisms
   * @return a list of parsed authentications
   * @throws {@link BadAuthFormatException} if the auth format is invalid
   */
  public static List<ZKAuthInfo> parseAuth(String authString) throws
      BadAuthFormatException{
    List<ZKAuthInfo> ret = Lists.newArrayList();
    if (authString == null) {
      return ret;
    }
    
    List<String> authComps = Lists.newArrayList(
        Splitter.on(',').omitEmptyStrings().trimResults()
        .split(authString));
    
    for (String comp : authComps) {
      String parts[] = comp.split(":", 2);
      if (parts.length != 2) {
        throw new BadAuthFormatException(
            "Auth '" + comp + "' not of expected form scheme:auth");
      }
      ret.add(new ZKAuthInfo(parts[0],
          parts[1].getBytes(Charsets.UTF_8)));
    }
    return ret;
  }
  
  /**
   * Because ZK ACLs and authentication information may be secret,
   * allow the configuration values to be indirected through a file
   * by specifying the configuration as "@/path/to/file". If this
   * syntax is used, this function will return the contents of the file
   * as a String.
   * 
   * @param valInConf the value from the Configuration 
   * @return either the same value, or the contents of the referenced
   * file if the configured value starts with "@"
   * @throws IOException if the file cannot be read
   */
  public static String resolveConfIndirection(String valInConf)
      throws IOException {
    if (valInConf == null) return null;
    if (!valInConf.startsWith("@")) {
      return valInConf;
    }
    String path = valInConf.substring(1).trim();
    return Files.asCharSource(new File(path), Charsets.UTF_8).read().trim();
  }

  /**
   * An authentication token passed to ZooKeeper.addAuthInfo
   */
  @InterfaceAudience.Private
  public static class ZKAuthInfo {
    private final String scheme;
    private final byte[] auth;
    
    public ZKAuthInfo(String scheme, byte[] auth) {
      super();
      this.scheme = scheme;
      this.auth = auth;
    }

    public String getScheme() {
      return scheme;
    }

    public byte[] getAuth() {
      return auth;
    }
  }

  @InterfaceAudience.Private
  public static class BadAclFormatException extends
      HadoopIllegalArgumentException {
    private static final long serialVersionUID = 1L;

    public BadAclFormatException(String message) {
      super(message);
    }
  }

  @InterfaceAudience.Private
  public static class BadAuthFormatException extends
      HadoopIllegalArgumentException {
    private static final long serialVersionUID = 1L;

    public BadAuthFormatException(String message) {
      super(message);
    }
  }
}
