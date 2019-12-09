/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.Name;
import org.xbill.DNS.Type;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * A processor for generating application DNS records from registry service
 * records.
 */
public class ApplicationServiceRecordProcessor extends
    BaseServiceRecordProcessor {
  private static final Logger LOG =
      LoggerFactory.getLogger(ApplicationServiceRecordProcessor.class);
  /**
   * Create an application service record processor.
   *
   * @param record       the service record
   * @param path         the service record registry node path
   * @param domain       the DNS zone/domain name
   * @param zoneSelector returns the zone associated with the provided name.
   * @throws Exception  if an issue is generated during instantiation.
   */
  public ApplicationServiceRecordProcessor(
      ServiceRecord record, String path, String domain,
      ZoneSelector zoneSelector) throws Exception {
    super(record, path, domain, zoneSelector);
  }

  /**
   * Initializes the DNS record type to descriptor mapping based on the
   * provided service record.
   *
   * @param serviceRecord the registry service record.
   * @throws Exception if an issue is encountered.
   */
  @Override public void initTypeToInfoMapping(ServiceRecord serviceRecord)
      throws Exception {
    if (serviceRecord.external.isEmpty()) {
      LOG.info(serviceRecord.description + ": No external endpoints defined.");
      return;
    }
    for (int type : getRecordTypes()) {
      switch (type) {
      case Type.A:
        createAInfo(serviceRecord);
        break;
      case Type.AAAA:
        createAAAAInfo(serviceRecord);
        break;
      case Type.TXT:
        createTXTInfo(serviceRecord);
        break;
      case Type.CNAME:
        createCNAMEInfo(serviceRecord);
        break;
      case Type.SRV:
        createSRVInfo(serviceRecord);
        break;
      default:
        throw new IllegalArgumentException("Unknown type " + type);

      }
    }
  }

  /**
   * Create an application TXT record descriptor.
   *
   * @param serviceRecord the service record.
   * @throws Exception if there is an issue during descriptor creation.
   */
  protected void createTXTInfo(ServiceRecord serviceRecord) throws Exception {
    List<Endpoint> endpoints = serviceRecord.external;
    List<RecordDescriptor> recordDescriptors = new ArrayList<>();
    TXTApplicationRecordDescriptor txtInfo;
    for (Endpoint endpoint : endpoints) {
      txtInfo = new TXTApplicationRecordDescriptor(
          serviceRecord, endpoint);
      recordDescriptors.add(txtInfo);
    }
    registerRecordDescriptor(Type.TXT, recordDescriptors);
  }

  /**
   * Create an application SRV record descriptor.
   *
   * @param serviceRecord the service record.
   * @throws Exception if there is an issue during descriptor creation.
   */
  protected void createSRVInfo(ServiceRecord serviceRecord) throws Exception {
    List<Endpoint> endpoints = serviceRecord.external;
    List<RecordDescriptor> recordDescriptors = new ArrayList<>();
    SRVApplicationRecordDescriptor srvInfo;
    for (Endpoint endpoint : endpoints) {
      srvInfo = new SRVApplicationRecordDescriptor(
          serviceRecord, endpoint);
      recordDescriptors.add(srvInfo);
    }
    registerRecordDescriptor(Type.SRV, recordDescriptors);
  }

  /**
   * Create an application CNAME record descriptor.
   *
   * @param serviceRecord the service record.
   * @throws Exception if there is an issue during descriptor creation.
   */
  protected void createCNAMEInfo(ServiceRecord serviceRecord) throws Exception {
    List<Endpoint> endpoints = serviceRecord.external;
    List<RecordDescriptor> recordDescriptors = new ArrayList<>();
    CNAMEApplicationRecordDescriptor cnameInfo;
    for (Endpoint endpoint : endpoints) {
      cnameInfo = new CNAMEApplicationRecordDescriptor(
          serviceRecord, endpoint);
      recordDescriptors.add(cnameInfo);
    }
    registerRecordDescriptor(Type.CNAME, recordDescriptors);
  }

  /**
   * Create an application AAAA record descriptor.
   *
   * @param record the service record.
   * @throws Exception if there is an issue during descriptor creation.
   */
  protected void createAAAAInfo(ServiceRecord record)
      throws Exception {
    AAAAApplicationRecordDescriptor
        recordInfo = new AAAAApplicationRecordDescriptor(
        getPath(), record);
    registerRecordDescriptor(Type.AAAA, recordInfo);
  }

  /**
   * Create an application A record descriptor.
   *
   * @param record the service record.
   * @throws Exception if there is an issue during descriptor creation.
   */
  protected void createAInfo(ServiceRecord record) throws Exception {
    AApplicationRecordDescriptor recordInfo = new AApplicationRecordDescriptor(
        getPath(), record);
    registerRecordDescriptor(Type.A, recordInfo);
  }

  /**
   * Returns the record types associated with a container service record.
   *
   * @return the record type array
   */
  @Override public int[] getRecordTypes() {
    return new int[] {Type.A, Type.AAAA, Type.CNAME, Type.SRV, Type.TXT};
  }

  /**
   * An application TXT record descriptor.
   */
  class TXTApplicationRecordDescriptor
      extends ApplicationRecordDescriptor<List<String>> {

    /**
     * Creates an application TXT record descriptor.
     *
     * @param record service record
     * @throws Exception
     */
    public TXTApplicationRecordDescriptor(ServiceRecord record,
        Endpoint endpoint) throws Exception {
      super(record, endpoint);
    }

    /**
     * Initializes the descriptor parameters.
     *
     * @param serviceRecord the service record.
     */
    @Override protected void init(ServiceRecord serviceRecord)
        throws Exception {
      if (getEndpoint() != null) {
        this.setNames(new Name[] {getServiceName(), getEndpointName()});
        this.setTarget(getTextRecords(getEndpoint()));
      }
    }

  }

  /**
   * An application SRV record descriptor.
   */
  class SRVApplicationRecordDescriptor extends
      ApplicationRecordDescriptor<RecordCreatorFactory.HostPortInfo> {

    /**
     * Creates an application SRV record descriptor.
     *
     * @param record service record
     * @throws Exception
     */
    public SRVApplicationRecordDescriptor(ServiceRecord record,
        Endpoint endpoint) throws Exception {
      super(record, endpoint);
    }

    /**
     * Initializes the descriptor parameters.
     *
     * @param serviceRecord the service record.
     */
    @Override protected void init(ServiceRecord serviceRecord)
        throws Exception {
      if (getEndpoint() != null) {
        this.setNames(new Name[] {getServiceName(), getEndpointName()});
        this.setTarget(new RecordCreatorFactory.HostPortInfo(
            Name.fromString(getHost(getEndpoint()) + "."), getPort(
            getEndpoint())));
      }
    }

  }

  /**
   * An application CNAME record descriptor.
   */
  class CNAMEApplicationRecordDescriptor extends
      ApplicationRecordDescriptor<Name> {

    /**
     * Creates an application CNAME record descriptor.
     *
     * @param path   registry path for service record
     * @param record service record
     * @throws Exception
     */
    public CNAMEApplicationRecordDescriptor(String path,
        ServiceRecord record) throws Exception {
      super(record);
    }

    /**
     * Creates an application CNAME record descriptor.  This descriptor is the
     * source for API related CNAME records.
     *
     * @param record   service record
     * @param endpoint the API endpoint
     * @throws Exception
     */
    public CNAMEApplicationRecordDescriptor(ServiceRecord record,
        Endpoint endpoint) throws Exception {
      super(record, endpoint);
    }

    /**
     * Initializes the descriptor parameters.
     *
     * @param serviceRecord the service record.
     */
    @Override protected void init(ServiceRecord serviceRecord)
        throws Exception {
      if (getEndpoint() != null) {
        this.setNames(new Name[] {getEndpointName()});
        this.setTarget(getServiceName());
      }
    }

  }

  /**
   * An application A record descriptor.
   */
  class AApplicationRecordDescriptor
      extends ApplicationRecordDescriptor<InetAddress> {

    /**
     * Creates an application A record descriptor.
     *
     * @param path   registry path for service record
     * @param record service record
     * @throws Exception
     */
    public AApplicationRecordDescriptor(String path,
        ServiceRecord record) throws Exception {
      super(record);
    }

    /**
     * Initializes the descriptor parameters.
     *
     * @param serviceRecord the service record.
     */
    @Override protected void init(ServiceRecord serviceRecord)
        throws Exception {
      this.setNames(new Name[] {getServiceName()});
      List<Endpoint> endpoints = serviceRecord.external;
      if (endpoints.isEmpty()) {
        return;
      }
      // TODO:  do we need a "hostname" attribute for an application record or
      // can we rely on the first endpoint record.
      this.setTarget(InetAddress.getByName(
          getHost(endpoints.get(0))));
    }

  }

  /**
   * An application AAAA record descriptor.
   */
  class AAAAApplicationRecordDescriptor extends AApplicationRecordDescriptor {

    /**
     * Creates an application AAAA record descriptor.
     *
     * @param path   registry path for service record
     * @param record service record
     * @throws Exception
     */
    public AAAAApplicationRecordDescriptor(String path,
        ServiceRecord record) throws Exception {
      super(path, record);
    }

    /**
     * Initializes the descriptor parameters.
     *
     * @param serviceRecord the service record.
     */
    @Override protected void init(ServiceRecord serviceRecord)
        throws Exception {
      super.init(serviceRecord);
      if (getTarget() == null) {
        return;
      }
      try {
        this.setTarget(getIpv6Address(getTarget()));
      } catch (UnknownHostException e) {
        throw new IllegalStateException(e);
      }
    }
  }

}
