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

import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.client.types.yarn.YarnRegistryAttributes;
import org.xbill.DNS.Name;
import org.xbill.DNS.TextParseException;
import org.xbill.DNS.Type;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * A processor for generating container DNS records from registry service
 * records.
 */
public class ContainerServiceRecordProcessor extends
    BaseServiceRecordProcessor {

  /**
   * Create a container service record processor.
   * @param record the service record
   * @param path the service record registry node path
   * @param domain the DNS zone/domain name
   * @param zoneSelector returns the zone associated with the provided name.
   * @throws Exception if an issue is generated during instantiation.
   */
  public ContainerServiceRecordProcessor(
      ServiceRecord record, String path, String domain,
      ZoneSelector zoneSelector) throws Exception {
    super(record, path, domain, zoneSelector);
  }

  /**
   * Initializes the DNS record type to descriptor mapping based on the
   * provided service record.
   * @param serviceRecord  the registry service record.
   * @throws Exception if an issue arises.
   */
  @Override public void initTypeToInfoMapping(ServiceRecord serviceRecord)
      throws Exception {
    if (serviceRecord.get(YarnRegistryAttributes.YARN_IP) != null) {
      for (int type : getRecordTypes()) {
        switch (type) {
        case Type.A:
          createAInfo(serviceRecord);
          break;
        case Type.AAAA:
          createAAAAInfo(serviceRecord);
          break;
        case Type.PTR:
          createPTRInfo(serviceRecord);
          break;
        case Type.TXT:
          createTXTInfo(serviceRecord);
          break;
        default:
          throw new IllegalArgumentException("Unknown type " + type);

        }
      }
    }
  }

  /**
   * Create a container TXT record descriptor.
   * @param serviceRecord the service record.
   * @throws Exception if the descriptor creation yields an issue.
   */
  protected void createTXTInfo(ServiceRecord serviceRecord) throws Exception {
    TXTContainerRecordDescriptor txtInfo =
        new TXTContainerRecordDescriptor(getPath(), serviceRecord);
    registerRecordDescriptor(Type.TXT, txtInfo);
  }

  /**
   * Creates a container PTR record descriptor.
   * @param record the service record.
   * @throws Exception if the descriptor creation yields an issue.
   */
  protected void createPTRInfo(ServiceRecord record) throws Exception {
    PTRContainerRecordDescriptor
        ptrInfo = new PTRContainerRecordDescriptor(getPath(), record);
    registerRecordDescriptor(Type.PTR, ptrInfo);
  }

  /**
   * Creates a container AAAA (IPv6) record descriptor.
   * @param record the service record
   * @throws Exception if the descriptor creation yields an issue.
   */
  protected void createAAAAInfo(ServiceRecord record)
      throws Exception {
    AAAAContainerRecordDescriptor
        recordInfo = new AAAAContainerRecordDescriptor(
        getPath(), record);
    registerRecordDescriptor(Type.AAAA, recordInfo);
  }

  /**
   * Creates a container A (IPv4) record descriptor.
   * @param record service record.
   * @throws Exception if the descriptor creation yields an issue.
   */
  protected void createAInfo(ServiceRecord record) throws Exception {
    AContainerRecordDescriptor recordInfo = new AContainerRecordDescriptor(
        getPath(), record);
    registerRecordDescriptor(Type.A, recordInfo);
  }

  /**
   * Returns the record types associated with a container service record.
   * @return the record type array
   */
  @Override public int[] getRecordTypes() {
    return new int[] {Type.A, Type.AAAA, Type.PTR, Type.TXT};
  }

  /**
   * A container TXT record descriptor.
   */
  class TXTContainerRecordDescriptor
      extends ContainerRecordDescriptor<List<String>> {

    /**
     * Creates a container TXT record descriptor.
     * @param path registry path for service record
     * @param record service record
     * @throws Exception
     */
    public TXTContainerRecordDescriptor(String path,
        ServiceRecord record) throws Exception {
      super(path, record);
    }

    /**
     * Initializes the descriptor parameters.
     * @param serviceRecord  the service record.
     */
    @Override protected void init(ServiceRecord serviceRecord) {
      try {
        this.setNames(new Name[] {getContainerName()});
      } catch (TextParseException e) {
        // log
      } catch (PathNotFoundException e) {
        // log
      }
      List<String> txts = new ArrayList<>();
      txts.add("id=" + serviceRecord.get(YarnRegistryAttributes.YARN_ID));
      this.setTarget(txts);
    }

  }

  /**
   * A container PTR record descriptor.
   */
  class PTRContainerRecordDescriptor extends ContainerRecordDescriptor<Name> {

    /**
     * Creates a container PTR record descriptor.
     * @param path registry path for service record
     * @param record service record
     * @throws Exception
     */
    public PTRContainerRecordDescriptor(String path,
        ServiceRecord record) throws Exception {
      super(path, record);
    }

    /**
     * Initializes the descriptor parameters.
     * @param serviceRecord  the service record.
     */
    @Override protected void init(ServiceRecord serviceRecord) {
      String host = serviceRecord.get(YarnRegistryAttributes.YARN_HOSTNAME);
      String ip = serviceRecord.get(YarnRegistryAttributes.YARN_IP);
      Name reverseLookupName = null;
      if (host != null && ip != null) {
        try {
          reverseLookupName = reverseIP(ip);
        } catch (UnknownHostException e) {
          //LOG
        }
      }
      this.setNames(new Name[] {reverseLookupName});
      try {
        this.setTarget(getContainerName());
      } catch (TextParseException e) {
        //LOG
      } catch (PathNotFoundException e) {
        //LOG
      }
    }

  }


  /**
   * A container A record descriptor.
   */
  class AContainerRecordDescriptor
      extends ContainerRecordDescriptor<InetAddress> {

    /**
     * Creates a container A record descriptor.
     * @param path registry path for service record
     * @param record service record
     * @throws Exception
     */
    public AContainerRecordDescriptor(String path,
        ServiceRecord record) throws Exception {
      super(path, record);
    }

    /**
     * Initializes the descriptor parameters.
     * @param serviceRecord  the service record.
     */
    @Override protected void init(ServiceRecord serviceRecord) {
      String ip = serviceRecord.get(YarnRegistryAttributes.YARN_IP);
      if (ip == null) {
        throw new IllegalArgumentException("No IP specified");
      }
      try {
        this.setTarget(InetAddress.getByName(ip));
        this.setNames(new Name[] {getContainerName(), getContainerIDName(),
            getComponentName()});
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }

    }

  }

  /**
   * A container AAAA record descriptor.
   */
  class AAAAContainerRecordDescriptor extends AContainerRecordDescriptor {

    /**
     * Creates a container AAAA record descriptor.
     * @param path registry path for service record
     * @param record service record
     * @throws Exception
     */
    public AAAAContainerRecordDescriptor(String path,
        ServiceRecord record) throws Exception {
      super(path, record);
    }

    /**
     * Initializes the descriptor parameters.
     * @param serviceRecord  the service record.
     */
    @Override protected void init(ServiceRecord serviceRecord) {
      super.init(serviceRecord);
      try {
        this.setTarget(getIpv6Address(getTarget()));
      } catch (UnknownHostException e) {
        throw new IllegalStateException(e);
      }
    }
  }
}
