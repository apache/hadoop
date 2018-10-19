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
package org.apache.hadoop.registry.server.dns;

import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.types.AddressTypes;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.xbill.DNS.Name;
import org.xbill.DNS.ReverseMap;
import org.xbill.DNS.TextParseException;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Provides common service record processing logic.
 */
public abstract class BaseServiceRecordProcessor
    implements ServiceRecordProcessor {

  private final ZoneSelector zoneSelctor;
  private Map<Integer, List<RecordDescriptor>> typeToDescriptorMap =
      new HashMap<>();
  private String path;
  private String domain;

  private static final String YARN_SERVICE_API_PREFIX =
      "classpath:org.apache.hadoop.yarn.service.";
  private static final String HTTP_API_TYPE = "http://";

  /**
   * Creates a service record processor.
   *
   * @param record       the service record.
   * @param path         the node path for the record in the registry.
   * @param domain       the target DNS domain for the service record
   *                     associated DNS records.
   * @param zoneSelector A selector of the best zone for a given DNS name.
   * @throws Exception if an issue is generated during instantiation.
   */
  public BaseServiceRecordProcessor(ServiceRecord record, String path,
      String domain, ZoneSelector zoneSelector)
      throws Exception {
    this.setPath(path);
    this.domain = domain;
    this.zoneSelctor = zoneSelector;
    initTypeToInfoMapping(record);
  }

  /**
   * Return the IPv6 mapped address for the provided IPv4 address. Utilized
   * to create corresponding AAAA records.
   *
   * @param address the IPv4 address.
   * @return the mapped IPv6 address.
   * @throws UnknownHostException
   */
  static InetAddress getIpv6Address(InetAddress address)
      throws UnknownHostException {
    String[] octets = address.getHostAddress().split("\\.");
    byte[] octetBytes = new byte[4];
    for (int i = 0; i < 4; ++i) {
      octetBytes[i] = (byte) Integer.parseInt(octets[i]);
    }

    byte[] ipv4asIpV6addr = new byte[16];
    ipv4asIpV6addr[10] = (byte) 0xff;
    ipv4asIpV6addr[11] = (byte) 0xff;
    ipv4asIpV6addr[12] = octetBytes[0];
    ipv4asIpV6addr[13] = octetBytes[1];
    ipv4asIpV6addr[14] = octetBytes[2];
    ipv4asIpV6addr[15] = octetBytes[3];

    return Inet6Address.getByAddress(null, ipv4asIpV6addr, 0);
  }

  /**
   * Reverse the string representation of the input IP address.
   *
   * @param ip the string representation of the IP address.
   * @return the reversed IP address.
   * @throws UnknownHostException if the ip is unknown.
   */
  protected Name reverseIP(String ip) throws UnknownHostException {
    return ReverseMap.fromAddress(ip);
  }

  /**
   * Manages the creation and registration of service record generated DNS
   * records.
   *
   * @param command the DNS registration command object (e.g. add_record,
   *                remove record)
   * @throws IOException if the creation or registration generates an issue.
   */
  @SuppressWarnings({"unchecked"})
  public void manageDNSRecords(RegistryDNS.RegistryCommand command)
      throws IOException {
    for (Map.Entry<Integer, List<RecordDescriptor>> entry :
        typeToDescriptorMap.entrySet()) {
      for (RecordDescriptor recordDescriptor : entry.getValue()) {
        for (Name name : recordDescriptor.getNames()) {
          RecordCreatorFactory.RecordCreator recordCreator =
              RecordCreatorFactory.getRecordCreator(entry.getKey());
          command.exec(zoneSelctor.findBestZone(name),
              recordCreator.create(name, recordDescriptor.getTarget()));
        }
      }
    }
  }

  /**
   * Add the DNS record descriptor object to the record type to descriptor
   * mapping.
   *
   * @param type             the DNS record type.
   * @param recordDescriptor the DNS record descriptor
   */
  protected void registerRecordDescriptor(int type,
      RecordDescriptor recordDescriptor) {
    List<RecordDescriptor> infos = new ArrayList<>();
    infos.add(recordDescriptor);
    typeToDescriptorMap.put(type, infos);
  }

  /**
   * Add the DNS record descriptor objects to the record type to descriptor
   * mapping.
   *
   * @param type              the DNS record type.
   * @param recordDescriptors the DNS record descriptors
   */
  protected void registerRecordDescriptor(int type,
      List<RecordDescriptor> recordDescriptors) {
    typeToDescriptorMap.put(type, recordDescriptors);
  }

  /**
   * Return the path associated with the record.
   * @return the path.
   */
  protected String getPath() {
    return path;
  }

  /**
   * Set the path associated with the record.
   * @param path the path.
   */
  protected void setPath(String path) {
    this.path = path;
  }

  /**
   * A descriptor container the information to be populated into a DNS record.
   *
   * @param <T> the DNS record type/class.
   */
  abstract class RecordDescriptor<T> {
    private final ServiceRecord record;
    private Name[] names;
    private T target;

    /**
     * Creates a DNS record descriptor.
     *
     * @param record the associated service record.
     */
    public RecordDescriptor(ServiceRecord record) {
      this.record = record;
    }

    /**
     * Returns the DNS names associated with the record type and information.
     *
     * @return the array of names.
     */
    public Name[] getNames() {
      return names;
    }

    /**
     * Return the target object for the DNS record.
     *
     * @return the DNS record target.
     */
    public T getTarget() {
      return target;
    }

    /**
     * Initializes the names and information for this DNS record descriptor.
     *
     * @param serviceRecord the service record.
     * @throws Exception
     */
    protected abstract void init(ServiceRecord serviceRecord) throws Exception;

    /**
     * Returns the service record.
     * @return the service record.
     */
    public ServiceRecord getRecord() {
      return record;
    }

    /**
     * Sets the names associated with the record type and information.
     * @param names the names.
     */
    public void setNames(Name[] names) {
      this.names = names;
    }

    /**
     * Sets the target object associated with the record.
     * @param target the target.
     */
    public void setTarget(T target) {
      this.target = target;
    }
  }

  /**
   * A container-based DNS record descriptor.
   *
   * @param <T> the DNS record type/class.
   */
  abstract class ContainerRecordDescriptor<T> extends RecordDescriptor<T> {

    public ContainerRecordDescriptor(String path, ServiceRecord record)
        throws Exception {
      super(record);
      init(record);
    }

    /**
     * Returns the DNS name constructed from the YARN container ID.
     *
     * @return the container ID name.
     * @throws TextParseException
     */
    protected Name getContainerIDName() throws TextParseException {
      String containerID = RegistryPathUtils.lastPathEntry(getPath());
      return Name.fromString(String.format("%s.%s", containerID, domain));
    }

    /**
     * Returns the DNS name constructed from the container role/component name.
     *
     * @return the DNS naem.
     * @throws PathNotFoundException
     * @throws TextParseException
     */
    protected Name getContainerName()
        throws PathNotFoundException, TextParseException {
      String service = RegistryPathUtils.lastPathEntry(
          RegistryPathUtils.parentOf(RegistryPathUtils.parentOf(getPath())));
      String description = getRecord().description.toLowerCase();
      String user = RegistryPathUtils.getUsername(getPath());
      return Name.fromString(MessageFormat.format("{0}.{1}.{2}.{3}",
          description,
          service,
          user,
          domain));
    }

    /**
     * Return the DNS name constructed from the component name.
     *
     * @return the DNS naem.
     * @throws PathNotFoundException
     * @throws TextParseException
     */
    protected Name getComponentName()
        throws PathNotFoundException, TextParseException {
      String service = RegistryPathUtils.lastPathEntry(
          RegistryPathUtils.parentOf(RegistryPathUtils.parentOf(getPath())));
      String component = getRecord().get("yarn:component").toLowerCase();
      String user = RegistryPathUtils.getUsername(getPath());
      return Name.fromString(MessageFormat.format("{0}.{1}.{2}.{3}",
          component,
          service,
          user,
          domain));
    }

  }

  /**
   * An application-based DNS record descriptor.
   *
   * @param <T> the DNS record type/class.
   */
  abstract class ApplicationRecordDescriptor<T> extends RecordDescriptor<T> {

    private Endpoint srEndpoint;

    /**
     * Creates an application associated DNS record descriptor.
     *
     * @param record the service record.
     * @throws Exception
     */
    public ApplicationRecordDescriptor(ServiceRecord record)
        throws Exception {
      this(record, null);
    }

    /**
     * Creates an application associated DNS record descriptor.  The endpoint
     * is leverated to create an associated application API record.
     *
     * @param record   the service record.
     * @param endpoint an API endpoint.
     * @throws Exception
     */
    public ApplicationRecordDescriptor(ServiceRecord record,
        Endpoint endpoint) throws Exception {
      super(record);
      this.setEndpoint(endpoint);
      init(record);
    }

    /**
     * Get the service's DNS name for registration.
     *
     * @return the service DNS name.
     * @throws TextParseException
     */
    protected Name getServiceName() throws TextParseException {
      String user = RegistryPathUtils.getUsername(getPath());
      String service =
          String.format("%s.%s.%s",
              RegistryPathUtils.lastPathEntry(getPath()),
              user,
              domain);
      return Name.fromString(service);
    }

    /**
     * Get the host from the provided endpoint record.
     *
     * @param endpoint the endpoint info.
     * @return the host name.
     */
    protected String getHost(Endpoint endpoint) {
      String host = null;
      // assume one address for now
      Map<String, String> address = endpoint.addresses.get(0);
      if (endpoint.addressType.equals(AddressTypes.ADDRESS_HOSTNAME_AND_PORT)) {
        host = address.get(AddressTypes.ADDRESS_HOSTNAME_FIELD);
      } else if (endpoint.addressType.equals(AddressTypes.ADDRESS_URI)) {
        URI uri = URI.create(address.get("uri"));
        host = uri.getHost();
      }
      return host;
    }

    /**
     * Get the post from the provided endpoint record.
     *
     * @param endpoint the endpoint info.
     * @return the port.
     */
    protected int getPort(Endpoint endpoint) {
      int port = -1;
      // assume one address for now
      Map<String, String> address = endpoint.addresses.get(0);
      if (endpoint.addressType.equals(AddressTypes.ADDRESS_HOSTNAME_AND_PORT)) {
        port = Integer.parseInt(address.get(AddressTypes.ADDRESS_PORT_FIELD));
      } else if (endpoint.addressType.equals(AddressTypes.ADDRESS_URI)) {
        URI uri = URI.create(address.get("uri"));
        port = uri.getPort();
      }
      return port;
    }

    /**
     * Get the list of strings that can be related in a TXT record for the given
     * endpoint.
     *
     * @param endpoint the endpoint information.
     * @return the list of strings relating endpoint info.
     */
    protected List<String> getTextRecords(Endpoint endpoint) {
      Map<String, String> address = endpoint.addresses.get(0);
      List<String> txtRecs = new ArrayList<String>();
      txtRecs.add("api=" + getDNSApiFragment(endpoint.api));
      if (endpoint.addressType.equals(AddressTypes.ADDRESS_URI)) {
        URI uri = URI.create(address.get("uri"));
        txtRecs.add("path=" + uri.getPath());
      }
      return txtRecs;
    }

    /**
     * Get an API name that is compatible with DNS standards (and shortened).
     *
     * @param api the api indicator.
     * @return the shortened and compatible api name.
     */
    protected String getDNSApiFragment(String api) {
      String dnsApi = null;
      if (api.startsWith(YARN_SERVICE_API_PREFIX)) {
        dnsApi = api.substring(YARN_SERVICE_API_PREFIX.length());
      } else if (api.startsWith(HTTP_API_TYPE)) {
        dnsApi = "http";
      }
      assert dnsApi != null;
      dnsApi = dnsApi.replace('.', '-');
      return dnsApi;
    }

    /**
     * Return the DNS name associated with the API endpoint.
     *
     * @return the name.
     * @throws TextParseException
     */
    protected Name getEndpointName() throws TextParseException {
      return Name.fromString(String.format("%s-api.%s",
          getDNSApiFragment(
              getEndpoint().api),
          getServiceName()));
    }

    /**
     * Returns the endpoint.
     * @return the endpoint.
     */
    public Endpoint getEndpoint() {
      return srEndpoint;
    }

    /**
     * Sets the endpoint.
     * @param endpoint the endpoint.
     */
    public void setEndpoint(
        Endpoint endpoint) {
      this.srEndpoint = endpoint;
    }
  }
}
