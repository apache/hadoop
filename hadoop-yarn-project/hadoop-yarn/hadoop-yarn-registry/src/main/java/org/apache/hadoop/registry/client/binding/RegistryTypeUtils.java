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
import static org.apache.hadoop.registry.client.types.AddressTypes.*;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ProtocolTypes;
import org.apache.hadoop.registry.client.types.ServiceRecord;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        ADDRESS_HOSTNAME_AND_PORT,
        protocolType,
        hostnamePortPair(hostname, port));
  }

  /**
   * Create an IPC endpoint
   * @param api API
   * @param address the address as a tuple of (hostname, port)
   * @return the new endpoint
   */
  public static Endpoint ipcEndpoint(String api, InetSocketAddress address) {
    return new Endpoint(api,
        ADDRESS_HOSTNAME_AND_PORT,
        ProtocolTypes.PROTOCOL_HADOOP_IPC,
        address== null ? null: hostnamePortPair(address));
  }

  /**
   * Create a single entry map
   * @param key map entry key
   * @param val map entry value
   * @return a 1 entry map.
   */
  public static Map<String, String> map(String key, String val) {
    Map<String, String> map = new HashMap<String, String>(1);
    map.put(key, val);
    return map;
  }

  /**
   * Create a URI
   * @param uri value
   * @return a 1 entry map.
   */
  public static Map<String, String> uri(String uri) {
    return map(ADDRESS_URI, uri);
  }

  /**
   * Create a (hostname, port) address pair
   * @param hostname hostname
   * @param port port
   * @return a 1 entry map.
   */
  public static Map<String, String> hostnamePortPair(String hostname, int port) {
    Map<String, String> map =
        map(ADDRESS_HOSTNAME_FIELD, hostname);
    map.put(ADDRESS_PORT_FIELD, Integer.toString(port));
    return map;
  }

  /**
   * Create a (hostname, port) address pair
   * @param address socket address whose hostname and port are used for the
   * generated address.
   * @return a 1 entry map.
   */
  public static Map<String, String> hostnamePortPair(InetSocketAddress address) {
    return hostnamePortPair(address.getHostName(), address.getPort());
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
    requireAddressType(ADDRESS_URI, epr);
    List<Map<String, String>> addresses = epr.addresses;
    if (addresses.size() < 1) {
      throw new InvalidRecordException(epr.toString(),
          "No addresses in endpoint");
    }
    List<String> results = new ArrayList<String>(addresses.size());
    for (Map<String, String> address : addresses) {
      results.add(getAddressField(address, ADDRESS_URI));
    }
    return results;
  }

  /**
   * Get a specific field from an address -raising an exception if
   * the field is not present
   * @param address address to query
   * @param field field to resolve
   * @return the resolved value. Guaranteed to be non-null.
   * @throws InvalidRecordException if the field did not resolve
   */
  public static String getAddressField(Map<String, String> address,
      String field) throws InvalidRecordException {
    String val = address.get(field);
    if (val == null) {
      throw new InvalidRecordException("", "Missing address field: " + field);
    }
    return val;
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

  /**
   * Validate the record by checking for null fields and other invalid
   * conditions
   * @param path path for exceptions
   * @param record record to validate. May be null
   * @throws InvalidRecordException on invalid entries
   */
  public static void validateServiceRecord(String path, ServiceRecord record)
      throws InvalidRecordException {
    if (record == null) {
      throw new InvalidRecordException(path, "Null record");
    }
    if (!ServiceRecord.RECORD_TYPE.equals(record.type)) {
      throw new InvalidRecordException(path,
          "invalid record type field: \"" + record.type + "\"");
    }

    if (record.external != null) {
      for (Endpoint endpoint : record.external) {
        validateEndpoint(path, endpoint);
      }
    }
    if (record.internal != null) {
      for (Endpoint endpoint : record.internal) {
        validateEndpoint(path, endpoint);
      }
    }
  }

  /**
   * Validate the endpoint by checking for null fields and other invalid
   * conditions
   * @param path path for exceptions
   * @param endpoint endpoint to validate. May be null
   * @throws InvalidRecordException on invalid entries
   */
  public static void validateEndpoint(String path, Endpoint endpoint)
      throws InvalidRecordException {
    if (endpoint == null) {
      throw new InvalidRecordException(path, "Null endpoint");
    }
    try {
      endpoint.validate();
    } catch (RuntimeException e) {
      throw new InvalidRecordException(path, e.toString());
    }
  }

}
