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

package org.apache.hadoop.registry.client.binding;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.registry.client.exceptions.InvalidRecordException;
import org.apache.hadoop.registry.client.types.AddressTypes;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ProtocolTypes;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Static methods to work with registry types —primarily endpoints and the
 * list representation of addresses.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RegistryTypeUtils {

  /**
   * Create a URL endpoint from a list of URIs
   * @param api implemented API
   * @param protocolType protocol type
   * @param uris URIs
   * @return a new endpoint
   */
  public static Endpoint urlEndpoint(String api,
      String protocolType,
      URI... uris) {
    return new Endpoint(api, protocolType, uris);
  }

  /**
   * Create a REST endpoint from a list of URIs
   * @param api implemented API
   * @param uris URIs
   * @return a new endpoint
   */
  public static Endpoint restEndpoint(String api,
      URI... uris) {
    return urlEndpoint(api, ProtocolTypes.PROTOCOL_REST, uris);
  }

  /**
   * Create a Web UI endpoint from a list of URIs
   * @param api implemented API
   * @param uris URIs
   * @return a new endpoint
   */
  public static Endpoint webEndpoint(String api,
      URI... uris) {
    return urlEndpoint(api, ProtocolTypes.PROTOCOL_WEBUI, uris);
  }

  /**
   * Create an internet address endpoint from a list of URIs
   * @param api implemented API
   * @param protocolType protocol type
   * @param hostname hostname/FQDN
   * @param port port
   * @return a new endpoint
   */

  public static Endpoint inetAddrEndpoint(String api,
      String protocolType,
      String hostname,
      int port) {
    Preconditions.checkArgument(api != null, "null API");
    Preconditions.checkArgument(protocolType != null, "null protocolType");
    Preconditions.checkArgument(hostname != null, "null hostname");
    return new Endpoint(api,
        AddressTypes.ADDRESS_HOSTNAME_AND_PORT,
        protocolType,
        tuplelist(hostname, Integer.toString(port)));
  }

  /**
   * Create an IPC endpoint
   * @param api API
   * @param protobuf flag to indicate whether or not the IPC uses protocol
   * buffers
   * @param address the address as a tuple of (hostname, port)
   * @return the new endpoint
   */
  public static Endpoint ipcEndpoint(String api,
      boolean protobuf, List<String> address) {
    ArrayList<List<String>> addressList = new ArrayList<List<String>>();
    if (address != null) {
      addressList.add(address);
    }
    return new Endpoint(api,
        AddressTypes.ADDRESS_HOSTNAME_AND_PORT,
        protobuf ? ProtocolTypes.PROTOCOL_HADOOP_IPC_PROTOBUF
                 : ProtocolTypes.PROTOCOL_HADOOP_IPC,
        addressList);
  }

  /**
   * Create a single-element list of tuples from the input.
   * that is, an input ("a","b","c") is converted into a list
   * in the form [["a","b","c"]]
   * @param t1 tuple elements
   * @return a list containing a single tuple
   */
  public static List<List<String>> tuplelist(String... t1) {
    List<List<String>> outer = new ArrayList<List<String>>();
    outer.add(tuple(t1));
    return outer;
  }

  /**
   * Create a tuples from the input.
   * that is, an input ("a","b","c") is converted into a list
   * in the form ["a","b","c"]
   * @param t1 tuple elements
   * @return a single tuple as a list
   */
  public static List<String> tuple(String... t1) {
    return Arrays.asList(t1);
  }

  /**
   * Create a tuples from the input, converting all to Strings in the process
   * that is, an input ("a", 7, true) is converted into a list
   * in the form ["a","7,"true"]
   * @param t1 tuple elements
   * @return a single tuple as a list
   */
  public static List<String> tuple(Object... t1) {
    List<String> l = new ArrayList<String>(t1.length);
    for (Object t : t1) {
      l.add(t.toString());
    }
    return l;
  }

  /**
   * Convert a socket address pair into a string tuple, (host, port).
   * TODO JDK7: move to InetAddress.getHostString() to avoid DNS lookups.
   * @param address an address
   * @return an element for the address list
   */
  public static List<String> marshall(InetSocketAddress address) {
    return tuple(address.getHostName(), address.getPort());
  }

  /**
   * Require a specific address type on an endpoint
   * @param required required type
   * @param epr endpoint
   * @throws InvalidRecordException if the type is wrong
   */
  public static void requireAddressType(String required, Endpoint epr) throws
      InvalidRecordException {
    if (!required.equals(epr.addressType)) {
      throw new InvalidRecordException(
          epr.toString(),
          "Address type of " + epr.addressType
          + " does not match required type of "
          + required);
    }
  }

  /**
   * Get a single URI endpoint
   * @param epr endpoint
   * @return the uri of the first entry in the address list. Null if the endpoint
   * itself is null
   * @throws InvalidRecordException if the type is wrong, there are no addresses
   * or the payload ill-formatted
   */
  public static List<String> retrieveAddressesUriType(Endpoint epr)
      throws InvalidRecordException {
    if (epr == null) {
      return null;
    }
    requireAddressType(AddressTypes.ADDRESS_URI, epr);
    List<List<String>> addresses = epr.addresses;
    if (addresses.size() < 1) {
      throw new InvalidRecordException(epr.toString(),
          "No addresses in endpoint");
    }
    List<String> results = new ArrayList<String>(addresses.size());
    for (List<String> address : addresses) {
      if (address.size() != 1) {
        throw new InvalidRecordException(epr.toString(),
            "Address payload invalid: wrong element count: " +
            address.size());
      }
      results.add(address.get(0));
    }
    return results;
  }

  /**
   * Get the address URLs. Guranteed to return at least one address.
   * @param epr endpoint
   * @return the address as a URL
   * @throws InvalidRecordException if the type is wrong, there are no addresses
   * or the payload ill-formatted
   * @throws MalformedURLException address can't be turned into a URL
   */
  public static List<URL> retrieveAddressURLs(Endpoint epr)
      throws InvalidRecordException, MalformedURLException {
    if (epr == null) {
      throw new InvalidRecordException("", "Null endpoint");
    }
    List<String> addresses = retrieveAddressesUriType(epr);
    List<URL> results = new ArrayList<URL>(addresses.size());
    for (String address : addresses) {
      results.add(new URL(address));
    }
    return results;
  }
}
