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

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.registry.client.binding.JsonSerDeser;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Description of a single service/component endpoint.
 * It is designed to be marshalled as JSON.
 * <p>
 * Every endpoint can have more than one address entry, such as
 * a list of URLs to a replicated service, or a (hostname, port)
 * pair. Each of these address entries is represented as a string list,
 * as that is the only reliably marshallable form of a tuple JSON can represent.
 *
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public final class Endpoint implements Cloneable {

  /**
   * API implemented at the end of the binding
   */
  public String api;

  /**
   * Type of address. The standard types are defined in
   * {@link AddressTypes}
   */
  public String addressType;

  /**
   * Protocol type. Some standard types are defined in
   * {@link ProtocolTypes}
   */
  public String protocolType;

  /**
   * a list of address tuples —tuples whose format depends on the address type
   */
  public List<Map<String, String>> addresses;

  /**
   * Create an empty instance.
   */
  public Endpoint() {
  }

  /**
   * Create an endpoint from another endpoint.
   * This is a deep clone with a new list of addresses.
   * @param that the endpoint to copy from
   */
  public Endpoint(Endpoint that) {
    this.api = that.api;
    this.addressType = that.addressType;
    this.protocolType = that.protocolType;
    this.addresses = newAddresses(that.addresses.size());
    for (Map<String, String> address : that.addresses) {
      Map<String, String> addr2 = new HashMap<String, String>(address.size());
      addr2.putAll(address);
      addresses.add(addr2);
    }
  }

  /**
   * Build an endpoint with a list of addresses
   * @param api API name
   * @param addressType address type
   * @param protocolType protocol type
   * @param addrs addresses
   */
  public Endpoint(String api,
      String addressType,
      String protocolType,
      List<Map<String, String>> addrs) {
    this.api = api;
    this.addressType = addressType;
    this.protocolType = protocolType;
    this.addresses = newAddresses(0);
    if (addrs != null) {
      addresses.addAll(addrs);
    }
  }

  /**
   * Build an endpoint with an empty address list
   * @param api API name
   * @param addressType address type
   * @param protocolType protocol type
   */
  public Endpoint(String api,
      String addressType,
      String protocolType) {
    this.api = api;
    this.addressType = addressType;
    this.protocolType = protocolType;
    this.addresses = newAddresses(0);
  }

  /**
   * Build an endpoint with a single address entry.
   * <p>
   * This constructor is superfluous given the varags constructor is equivalent
   * for a single element argument. However, type-erasure in java generics
   * causes javac to warn about unchecked generic array creation. This
   * constructor, which represents the common "one address" case, does
   * not generate compile-time warnings.
   * @param api API name
   * @param addressType address type
   * @param protocolType protocol type
   * @param addr address. May be null —in which case it is not added
   */
  public Endpoint(String api,
      String addressType,
      String protocolType,
      Map<String, String> addr) {
    this(api, addressType, protocolType);
    if (addr != null) {
      addresses.add(addr);
    }
  }

  /**
   * Build an endpoint with a list of addresses
   * @param api API name
   * @param addressType address type
   * @param protocolType protocol type
   * @param addrs addresses. Null elements will be skipped
   */
  public Endpoint(String api,
      String addressType,
      String protocolType,
      Map<String, String>...addrs) {
    this(api, addressType, protocolType);
    for (Map<String, String> addr : addrs) {
      if (addr!=null) {
        addresses.add(addr);
      }
    }
  }

  /**
   * Create a new address structure of the requested size
   * @param size size to create
   * @return the new list
   */
  private List<Map<String, String>> newAddresses(int size) {
    return new ArrayList<Map<String, String>>(size);
  }

  /**
   * Build an endpoint from a list of URIs; each URI
   * is ASCII-encoded and added to the list of addresses.
   * @param api API name
   * @param protocolType protocol type
   * @param uris URIs to convert to a list of tup;les
   */
  public Endpoint(String api,
      String protocolType,
      URI... uris) {
    this.api = api;
    this.addressType = AddressTypes.ADDRESS_URI;

    this.protocolType = protocolType;
    List<Map<String, String>> addrs = newAddresses(uris.length);
    for (URI uri : uris) {
      addrs.add(RegistryTypeUtils.uri(uri.toString()));
    }
    this.addresses = addrs;
  }

  @Override
  public String toString() {
      return marshalToString.toString(this);
  }

  /**
   * Validate the record by checking for null fields and other invalid
   * conditions
   * @throws NullPointerException if a field is null when it
   * MUST be set.
   * @throws RuntimeException on invalid entries
   */
  public void validate() {
    Preconditions.checkNotNull(api, "null API field");
    Preconditions.checkNotNull(addressType, "null addressType field");
    Preconditions.checkNotNull(protocolType, "null protocolType field");
    Preconditions.checkNotNull(addresses, "null addresses field");
    for (Map<String, String> address : addresses) {
      Preconditions.checkNotNull(address, "null element in address");
    }
  }

  /**
   * Shallow clone: the lists of addresses are shared
   * @return a cloned instance
   * @throws CloneNotSupportedException
   */
  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }


  /**
   * Static instance of service record marshalling
   */
  private static class Marshal extends JsonSerDeser<Endpoint> {
    private Marshal() {
      super(Endpoint.class);
    }
  }

  private static final Marshal marshalToString = new Marshal();
}
