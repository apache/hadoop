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

import org.xbill.DNS.AAAARecord;
import org.xbill.DNS.ARecord;
import org.xbill.DNS.CNAMERecord;
import org.xbill.DNS.DClass;
import org.xbill.DNS.Name;
import org.xbill.DNS.PTRRecord;
import org.xbill.DNS.Record;
import org.xbill.DNS.SRVRecord;
import org.xbill.DNS.TXTRecord;

import java.net.InetAddress;
import java.util.List;

import static org.xbill.DNS.Type.*;

/**
 * A factory for creating DNS records.
 */
public final class RecordCreatorFactory {
  private static long ttl;

  /**
   * Private constructor.
   */
  private RecordCreatorFactory() {
  }

  /**
   * Returns the DNS record creator for the provided type.
   *
   * @param type the DNS record type.
   * @return the record creator.
   */
  static RecordCreator getRecordCreator(int type) {
    switch (type) {
    case A:
      return new ARecordCreator();
    case CNAME:
      return new CNAMERecordCreator();
    case TXT:
      return new TXTRecordCreator();
    case AAAA:
      return new AAAARecordCreator();
    case PTR:
      return new PTRRecordCreator();
    case SRV:
      return new SRVRecordCreator();
    default:
      throw new IllegalArgumentException("No type " + type);

    }
  }

  /**
   * Set the TTL value for the records created by the factory.
   *
   * @param ttl the ttl value, in seconds.
   */
  public static void setTtl(long ttl) {
    RecordCreatorFactory.ttl = ttl;
  }

  /**
   * A DNS Record creator.
   *
   * @param <R> the record type
   * @param <T> the record's target type
   */
  public interface RecordCreator<R extends Record, T> {
    R create(Name name, T target);
  }

  /**
   * An A Record creator.
   */
  static class ARecordCreator implements RecordCreator<ARecord, InetAddress> {
    /**
     * Creates an A record creator.
     */
    public ARecordCreator() {
    }

    /**
     * Creates a DNS A record.
     *
     * @param name   the record name.
     * @param target the record target/value.
     * @return an A record.
     */
    @Override public ARecord create(Name name, InetAddress target) {
      return new ARecord(name, DClass.IN, ttl, target);
    }
  }

  /**
   * An AAAA Record creator.
   */
  static class AAAARecordCreator
      implements RecordCreator<AAAARecord, InetAddress> {
    /**
     * Creates an AAAA record creator.
     */
    public AAAARecordCreator() {
    }

    /**
     * Creates a DNS AAAA record.
     *
     * @param name   the record name.
     * @param target the record target/value.
     * @return an A record.
     */
    @Override public AAAARecord create(Name name, InetAddress target) {
      return new AAAARecord(name, DClass.IN, ttl, target);
    }
  }

  static class CNAMERecordCreator implements RecordCreator<CNAMERecord, Name> {
    /**
     * Creates a CNAME record creator.
     */
    public CNAMERecordCreator() {
    }

    /**
     * Creates a DNS CNAME record.
     *
     * @param name   the record name.
     * @param target the record target/value.
     * @return an A record.
     */
    @Override public CNAMERecord create(Name name, Name target) {
      return new CNAMERecord(name, DClass.IN, ttl, target);
    }
  }

  /**
   * A TXT Record creator.
   */
  static class TXTRecordCreator
      implements RecordCreator<TXTRecord, List<String>> {
    /**
     * Creates a TXT record creator.
     */
    public TXTRecordCreator() {
    }

    /**
     * Creates a DNS TXT record.
     *
     * @param name   the record name.
     * @param target the record target/value.
     * @return an A record.
     */
    @Override public TXTRecord create(Name name, List<String> target) {
      return new TXTRecord(name, DClass.IN, ttl, target);
    }
  }

  /**
   * A PTR Record creator.
   */
  static class PTRRecordCreator implements RecordCreator<PTRRecord, Name> {
    /**
     * Creates a PTR record creator.
     */
    public PTRRecordCreator() {
    }

    /**
     * Creates a DNS PTR record.
     *
     * @param name   the record name.
     * @param target the record target/value.
     * @return an A record.
     */
    @Override public PTRRecord create(Name name, Name target) {
      return new PTRRecord(name, DClass.IN, ttl, target);
    }
  }

  /**
   * A SRV Record creator.
   */
  static class SRVRecordCreator
      implements RecordCreator<SRVRecord, HostPortInfo> {
    /**
     * Creates a SRV record creator.
     */
    public SRVRecordCreator() {
    }

    /**
     * Creates a DNS SRV record.
     *
     * @param name   the record name.
     * @param target the record target/value.
     * @return an A record.
     */
    @Override public SRVRecord create(Name name, HostPortInfo target) {
      return new SRVRecord(name, DClass.IN, ttl, 1, 1, target.getPort(),
          target.getHost());
    }
  }

  /**
   * An object for storing the host and port info used to generate SRV records.
   */
  public static class HostPortInfo {
    private Name host;
    private int port;

    /**
     * Creates an object with a host and port pair.
     *
     * @param host the hostname/ip
     * @param port the port value
     */
    public HostPortInfo(Name host, int port) {
      this.setHost(host);
      this.setPort(port);
    }

    /**
     * Return the host name.
     * @return the host name.
     */
    Name getHost() {
      return host;
    }

    /**
     * Set the host name.
     * @param host the host name.
     */
    void setHost(Name host) {
      this.host = host;
    }

    /**
     * Get the port.
     * @return the port.
     */
    int getPort() {
      return port;
    }

    /**
     * Set the port.
     * @param port the port.
     */
    void setPort(int port) {
      this.port = port;
    }
  }

}
