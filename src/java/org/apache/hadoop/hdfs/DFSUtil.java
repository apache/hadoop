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

package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.NodeBase;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

@InterfaceAudience.Private
public class DFSUtil {
  
  /**
   * Whether the pathname is valid.  Currently prohibits relative paths, 
   * and names which contain a ":" or "/" 
   */
  public static boolean isValidName(String src) {
      
    // Path must be absolute.
    if (!src.startsWith(Path.SEPARATOR)) {
      return false;
    }
      
    // Check for ".." "." ":" "/"
    StringTokenizer tokens = new StringTokenizer(src, Path.SEPARATOR);
    while(tokens.hasMoreTokens()) {
      String element = tokens.nextToken();
      if (element.equals("..") || 
          element.equals(".")  ||
          (element.indexOf(":") >= 0)  ||
          (element.indexOf("/") >= 0)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Utility class to facilitate junit test error simulation.
   */
  @InterfaceAudience.Private
  public static class ErrorSimulator {
    private static boolean[] simulation = null; // error simulation events
    public static void initializeErrorSimulationEvent(int numberOfEvents) {
      simulation = new boolean[numberOfEvents]; 
      for (int i = 0; i < numberOfEvents; i++) {
        simulation[i] = false;
      }
    }
    
    public static boolean getErrorSimulation(int index) {
      if(simulation == null)
        return false;
      assert(index < simulation.length);
      return simulation[index];
    }
    
    public static void setErrorSimulation(int index) {
      assert(index < simulation.length);
      simulation[index] = true;
    }
    
    public static void clearErrorSimulation(int index) {
      assert(index < simulation.length);
      simulation[index] = false;
    }
  }

  /**
   * Converts a byte array to a string using UTF8 encoding.
   */
  public static String bytes2String(byte[] bytes) {
    try {
      return new String(bytes, "UTF8");
    } catch(UnsupportedEncodingException e) {
      assert false : "UTF8 encoding is not supported ";
    }
    return null;
  }

  /**
   * Converts a string to a byte array using UTF8 encoding.
   */
  public static byte[] string2Bytes(String str) {
    try {
      return str.getBytes("UTF8");
    } catch(UnsupportedEncodingException e) {
      assert false : "UTF8 encoding is not supported ";
    }
    return null;
  }

  /**
   * Given a list of path components returns a path as a UTF8 String
   */
  public static String byteArray2String(byte[][] pathComponents) {
    if (pathComponents.length == 0)
      return "";
    if (pathComponents.length == 1 && pathComponents[0].length == 0) {
      return Path.SEPARATOR;
    }
    try {
      StringBuilder result = new StringBuilder();
      for (int i = 0; i < pathComponents.length; i++) {
        result.append(new String(pathComponents[i], "UTF-8"));
        if (i < pathComponents.length - 1) {
          result.append(Path.SEPARATOR_CHAR);
        }
      }
      return result.toString();
    } catch (UnsupportedEncodingException ex) {
      assert false : "UTF8 encoding is not supported ";
    }
    return null;
  }

  /**
   * Splits the array of bytes into array of arrays of bytes
   * on byte separator
   * @param bytes the array of bytes to split
   * @param separator the delimiting byte
   */
  public static byte[][] bytes2byteArray(byte[] bytes, byte separator) {
    return bytes2byteArray(bytes, bytes.length, separator);
  }

  /**
   * Splits first len bytes in bytes to array of arrays of bytes
   * on byte separator
   * @param bytes the byte array to split
   * @param len the number of bytes to split
   * @param separator the delimiting byte
   */
  public static byte[][] bytes2byteArray(byte[] bytes,
                                         int len,
                                         byte separator) {
    assert len <= bytes.length;
    int splits = 0;
    if (len == 0) {
      return new byte[][]{null};
    }
    // Count the splits. Omit multiple separators and the last one
    for (int i = 0; i < len; i++) {
      if (bytes[i] == separator) {
        splits++;
      }
    }
    int last = len - 1;
    while (last > -1 && bytes[last--] == separator) {
      splits--;
    }
    if (splits == 0 && bytes[0] == separator) {
      return new byte[][]{null};
    }
    splits++;
    byte[][] result = new byte[splits][];
    int startIndex = 0;
    int nextIndex = 0;
    int index = 0;
    // Build the splits
    while (index < splits) {
      while (nextIndex < len && bytes[nextIndex] != separator) {
        nextIndex++;
      }
      result[index] = new byte[nextIndex - startIndex];
      System.arraycopy(bytes, startIndex, result[index], 0, nextIndex
              - startIndex);
      index++;
      startIndex = nextIndex + 1;
      nextIndex = startIndex;
    }
    return result;
  }
  
  /**
   * Convert a LocatedBlocks to BlockLocations[]
   * @param blocks a LocatedBlocks
   * @return an array of BlockLocations
   */
  public static BlockLocation[] locatedBlocks2Locations(LocatedBlocks blocks) {
    if (blocks == null) {
      return new BlockLocation[0];
    }
    int nrBlocks = blocks.locatedBlockCount();
    BlockLocation[] blkLocations = new BlockLocation[nrBlocks];
    int idx = 0;
    for (LocatedBlock blk : blocks.getLocatedBlocks()) {
      assert idx < nrBlocks : "Incorrect index";
      DatanodeInfo[] locations = blk.getLocations();
      String[] hosts = new String[locations.length];
      String[] names = new String[locations.length];
      String[] racks = new String[locations.length];
      for (int hCnt = 0; hCnt < locations.length; hCnt++) {
        hosts[hCnt] = locations[hCnt].getHostName();
        names[hCnt] = locations[hCnt].getName();
        NodeBase node = new NodeBase(names[hCnt], 
                                     locations[hCnt].getNetworkLocation());
        racks[hCnt] = node.toString();
      }
      blkLocations[idx] = new BlockLocation(names, hosts, racks,
                                            blk.getStartOffset(),
                                            blk.getBlockSize());
      idx++;
    }
    return blkLocations;
  }

  private static URI getDefaultNamenode(Configuration conf) {
    return FileSystem.getDefaultUri(conf);
  }
  
  /**
   * Returns collection of nameservice Ids from the configuration.
   * 
   * @param conf configuration
   * @return collection of nameservice Ids
   */
  public static Collection<String> getNameServiceIds(Configuration conf) {
    return conf.getStringCollection(DFS_FEDERATION_NAMESERVICES);
  }
  
  /**
   * Returns the InetSocketAddresses of namenodes from the configuration.
   * Note this is to be used by datanodes to get the list of namenode addresses
   * to talk to.
   * 
   * If namenode address specifically configured for datanodes (using service
   * ports) is found, it is returned. If not, regular RPC address configured 
   * for other clients is returned.
   * 
   * @param conf configuration
   * @return list of InetSocketAddresses
   * @throws IOException on error
   */
  public static List<InetSocketAddress> getNNServiceRpcAddresses(
      Configuration conf) throws IOException {
    Collection<String> nameserviceIds = getNameServiceIds(conf);
    List<InetSocketAddress> isas = new ArrayList<InetSocketAddress>();

    // Configuration with a single namenode
    if (nameserviceIds == null || nameserviceIds.isEmpty()) {
      String namenodeAddress = conf
          .get(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY);
      if (namenodeAddress == null) {
        // Fall back to regular rpc address
        namenodeAddress = conf.get(DFS_NAMENODE_RPC_ADDRESS_KEY);
      }
      if (namenodeAddress == null) {
        isas.add(NameNode.getAddress(getDefaultNamenode(conf)));
      } else {
        isas.add(NameNode.getAddress(namenodeAddress));
      }
    } else {
      // Get the namenodes for all the configured nameServiceIds
      for (String nameserviceId : nameserviceIds) {
        String namenodeAddress = conf.get(getNameServiceIdKey(
            DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, nameserviceId));
        if (namenodeAddress == null) {
          // Fallback to regular rpc address
          namenodeAddress = conf.get(getNameServiceIdKey(
              DFS_NAMENODE_RPC_ADDRESS_KEY, nameserviceId));
        }
        if (namenodeAddress == null) {
          throw new IOException("Incorrect configuration: "
              + "No namenode rpc address configured for nameserviceId "
              + nameserviceId);
        }
        isas.add(NameNode.getAddress(namenodeAddress));
      }
    }
    return isas;
  }
  
  /**
   * @return key specific to a nameserviceId from a generic key
   */
  public static String getNameServiceIdKey(String key, String nameserviceId) {
    return key + "." + nameserviceId;
  }
  
  /**
   * Sets the node specific setting into generic configuration key. Looks up
   * value of "key.nameserviceId" and if found sets that value into generic key 
   * in the conf. Note that this only modifies the runtime conf.
   * 
   * @param conf
   *          Configuration object to lookup specific key and to set the value
   *          to the key passed. Note the conf object is modified.
   * @param nameserviceId
   *          nameservice Id to construct the node specific key.
   * @param keys
   *          The key for which node specific value is looked up
   */
  public static void setGenericConf(Configuration conf,
      String nameserviceId, String... keys) {
    for (String key : keys) {
      String value = conf.get(getNameServiceIdKey(key, nameserviceId));
      if (value != null) {
        conf.set(key, value);
      }
    }
  }
  
  /**
   * Returns the configured nameservice Id
   * 
   * @param conf
   *          Configuration object to lookup the nameserviceId
   * @return nameserviceId string from conf
   */
  public static String getNameServiceId(Configuration conf) {
    return conf.get(DFS_FEDERATION_NAMESERVICE_ID);
  }
  
  /** Return used as percentage of capacity */
  public static float getPercentUsed(long used, long capacity) {
    return capacity <= 0 ? 100 : ((float)used * 100.0f)/(float)capacity; 
  }
  
  /** Return remaining as percentage of capacity */
  public static float getPercentRemaining(long remaining, long capacity) {
    return capacity <= 0 ? 0 : ((float)remaining * 100.0f)/(float)capacity; 
  }

  /**
   * @param address address of format host:port
   * @return InetSocketAddress for the address
   */
  public static InetSocketAddress getSocketAddress(String address) {
    int colon = address.indexOf(":");
    if (colon < 0) {
      return new InetSocketAddress(address, 0);
    }
    return new InetSocketAddress(address.substring(0, colon), 
        Integer.parseInt(address.substring(colon + 1)));
  }
}

