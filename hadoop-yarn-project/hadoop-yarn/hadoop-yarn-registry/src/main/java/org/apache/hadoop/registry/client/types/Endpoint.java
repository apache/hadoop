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
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
  public List<List<String>> addresses;

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
    this.addresses = new ArrayList<List<String>>(that.addresses.size());
    for (List<String> address : addresses) {
      List<String> addr2 = new ArrayList<String>(address.size());
      Collections.copy(address, addr2);
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
      List<List<String>> addrs) {
    this.api = api;
    this.addressType = addressType;
    this.protocolType = protocolType;
    this.addresses = new ArrayList<List<String>>();
    if (addrs != null) {
      addresses.addAll(addrs);
    }
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
    List<List<String>> addrs = new ArrayList<List<String>>(uris.length);
    for (URI uri : uris) {
      addrs.add(RegistryTypeUtils.tuple(uri.toString()));
    }
    this.addresses = addrs;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("Endpoint{");
    sb.append("api='").append(api).append('\'');
    sb.append(", addressType='").append(addressType).append('\'');
    sb.append(", protocolType='").append(protocolType).append('\'');

    sb.append(", addresses=");
    if (addresses != null) {
      sb.append("[ ");
      for (List<String> address : addresses) {
        sb.append("[ ");
        if (address == null) {
          sb.append("NULL entry in address list");
        } else {
          for (String elt : address) {
            sb.append('"').append(elt).append("\" ");
          }
        }
        sb.append("] ");
      };
      sb.append("] ");
    } else {
      sb.append("(null) ");
    }
    sb.append('}');
    return sb.toString();
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
    for (List<String> address : addresses) {
      Preconditions.checkNotNull(address, "null element in address");
    }
  }

  /**
   * Shallow clone: the lists of addresses are shared
   * @return a cloned instance
   * @throws CloneNotSupportedException
   */
  @Override
  protected Object clone() throws CloneNotSupportedException {
    return super.clone();
  }
}
