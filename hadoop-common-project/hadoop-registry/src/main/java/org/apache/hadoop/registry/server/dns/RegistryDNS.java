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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.net.util.Base64;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.registry.client.api.DNSOperations;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.client.types.yarn.YarnRegistryAttributes;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.CNAMERecord;
import org.xbill.DNS.DClass;
import org.xbill.DNS.DNSKEYRecord;
import org.xbill.DNS.DNSSEC;
import org.xbill.DNS.DSRecord;
import org.xbill.DNS.ExtendedFlags;
import org.xbill.DNS.ExtendedResolver;
import org.xbill.DNS.Flags;
import org.xbill.DNS.Header;
import org.xbill.DNS.Lookup;
import org.xbill.DNS.Message;
import org.xbill.DNS.NSRecord;
import org.xbill.DNS.Name;
import org.xbill.DNS.NameTooLongException;
import org.xbill.DNS.OPTRecord;
import org.xbill.DNS.Opcode;
import org.xbill.DNS.RRSIGRecord;
import org.xbill.DNS.RRset;
import org.xbill.DNS.Rcode;
import org.xbill.DNS.Record;
import org.xbill.DNS.Resolver;
import org.xbill.DNS.ResolverConfig;
import org.xbill.DNS.SOARecord;
import org.xbill.DNS.Section;
import org.xbill.DNS.SetResponse;
import org.xbill.DNS.SimpleResolver;
import org.xbill.DNS.TSIG;
import org.xbill.DNS.TSIGRecord;
import org.xbill.DNS.TextParseException;
import org.xbill.DNS.Type;
import org.xbill.DNS.Zone;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPrivateKeySpec;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.registry.client.api.RegistryConstants.*;

/**
 * A DNS service reflecting the state of the YARN registry.  Records are created
 * based on service records available in the YARN ZK-based registry.
 */
public class RegistryDNS extends AbstractService implements DNSOperations,
    ZoneSelector {

  public static final String CONTAINER = "container";

  static final int FLAG_DNSSECOK = 1;
  static final int FLAG_SIGONLY = 2;

  private static final Logger LOG =
      LoggerFactory.getLogger(RegistryDNS.class);
  public static final String IN_ADDR_ARPA = "in-addr.arpa.";
  public static final String ZONE_SUFFIX = ".zone";

  private ExecutorService executor;
  private ReentrantReadWriteLock zoneLock = new ReentrantReadWriteLock();
  private CloseableLock readLock = new CloseableLock(zoneLock.readLock());
  private CloseableLock writeLock = new CloseableLock(zoneLock.writeLock());
  private String domainName;
  private long ttl = 0L;

  private static final Pattern USER_NAME = Pattern.compile("/users/(\\w*)/?");
  private Boolean dnssecEnabled;
  private PrivateKey privateKey;

  private ConcurrentMap<Name, DNSKEYRecord> dnsKeyRecs =
      new ConcurrentHashMap<>();
  private ConcurrentMap<Name, Zone> zones = new ConcurrentHashMap<>();
  private Name bindHost;

  private boolean channelsInitialized = false;

  /**
   * Lock to update resolver only once per request.
   */
  private final Object resolverUpdateLock = new Object();

  /**
   * Whether resolver update has been requested.
   */
  private boolean resolverUpdateRequested = true;

  /**
   * Construct the service.
   *
   * @param name service name
   */
  public RegistryDNS(String name) {
    super(name);
    executor = HadoopExecutors.newCachedThreadPool(
        new ThreadFactory() {
          private AtomicInteger counter = new AtomicInteger(1);

          @Override
          public Thread newThread(Runnable r) {
            return new Thread(r,
                "RegistryDNS "
                    + counter.getAndIncrement());
          }
        });
  }

  public void initializeChannels(Configuration conf) throws Exception {
    if (channelsInitialized) {
      return;
    }
    channelsInitialized = true;
    int port = conf.getInt(KEY_DNS_PORT, DEFAULT_DNS_PORT);
    InetAddress addr = InetAddress.getLocalHost();

    String bindAddress = conf.get(KEY_DNS_BIND_ADDRESS);
    if (bindAddress != null) {
      addr = InetAddress.getByName(bindAddress);
    }

    LOG.info("Opening TCP and UDP channels on {} port {}", addr, port);
    addNIOUDP(addr, port);
    addNIOTCP(addr, port);
  }

  /**
   * Initialize registryDNS to use /etc/resolv.conf values
   * as default resolvers.
   */
  private void updateDNSServer(Configuration conf) {
    synchronized (resolverUpdateLock) {
      if (!resolverUpdateRequested) {
        return;
      }
      int port = conf.getInt(KEY_DNS_PORT, DEFAULT_DNS_PORT);
      resolverUpdateRequested = false;
      List<InetAddress> list = new ArrayList<InetAddress>();
      try {
        // If resolv.conf contains the server's own IP address,
        // and RegistryDNS handles the lookup.  Local IP address
        // must be filter out from default resolvers to prevent
        // self recursive loop.
        if (port != 53) {
          // When registryDNS is not running on default port,
          // registryDNS can utilize local DNS server as upstream lookup.
          throw new SocketException("Bypass filtering local DNS server.");
        }
        Enumeration<NetworkInterface> net =
            NetworkInterface.getNetworkInterfaces();
        while(net.hasMoreElements()) {
          NetworkInterface n = (NetworkInterface) net.nextElement();
          Enumeration<InetAddress> ee = n.getInetAddresses();
          while (ee.hasMoreElements()) {
            InetAddress i = (InetAddress) ee.nextElement();
            list.add(i);
          }
        }
      } catch (SocketException e) {
      }
      ResolverConfig.refresh();
      ExtendedResolver resolver;
      try {
        resolver = new ExtendedResolver();
      } catch (UnknownHostException e) {
        LOG.error("Can not resolve DNS servers: ", e);
        return;
      }
      for (Resolver check : resolver.getResolvers()) {
        if (check instanceof SimpleResolver) {
          InetAddress address = ((SimpleResolver) check).getAddress()
              .getAddress();
          if (list.contains(address)) {
            resolver.deleteResolver(check);
            continue;
          } else {
            check.setTimeout(30);
          }
        } else {
          LOG.error("Not simple resolver!!!?" + check);
        }
      }
      synchronized (Lookup.class) {
        Lookup.setDefaultResolver(resolver);
        Lookup.setDefaultSearchPath(ResolverConfig.getCurrentConfig()
            .searchPath());
      }
      StringBuilder message = new StringBuilder();
      message.append("DNS servers: ");
      if (ResolverConfig.getCurrentConfig().servers() != null) {
        for (String server : ResolverConfig.getCurrentConfig()
            .servers()) {
          message.append(server);
          message.append(" ");
        }
      }
      LOG.info(message.toString());
    }
  }
  /**
   * Initializes the registry.
   *
   * @param conf the hadoop configuration
   * @throws Exception if there are tcp/udp issues
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);

    // create the zone.  for now create a "dummy" SOA record
    try {
      updateDNSServer(conf);
      setDomainName(conf);

      initializeZones(conf);

      initializeChannels(conf);
    } catch (IOException e) {
      LOG.error("Error initializing Registry DNS Server", e);
      throw e;
    }
  }

  /**
   * Initializes the registry based on available parameters in the hadoop
   * configuration.
   *
   * @param conf the hadoop configuration
   * @return the listener port
   * @throws IOException
   */
  void initializeZones(Configuration conf) throws IOException {
    ttl = conf.getTimeDuration(KEY_DNS_TTL, 1L, TimeUnit.SECONDS);
    RecordCreatorFactory.setTtl(ttl);

    setDNSSECEnabled(conf);

    initializeZonesFromFiles(conf);

    Zone registryZone = configureZone(Name.fromString(domainName), conf);
    zones.put(registryZone.getOrigin(), registryZone);

    initializeReverseLookupZone(conf);

    StringBuilder builder = new StringBuilder();
    builder.append("DNS zones: ").append(System.lineSeparator());
    for (Map.Entry<Name, Zone> entry : zones.entrySet()) {
      builder.append(System.lineSeparator()).append(entry.getValue());
    }
    LOG.info(builder.toString());
  }

  /**
   * Signs zone records if necessary (DNSSEC enabled).  Zones may not have
   * their NS and SOA records signed if they were initialized from master files.
   */
  private void signZones() throws IOException {
    if (isDNSSECEnabled()) {
      Collection<Zone> zoneCollection = zones.values();
      for (Zone zone : zoneCollection) {
        Iterator itor = zone.iterator();
        while (itor.hasNext()) {
          RRset rRset = (RRset) itor.next();
          Iterator sigs = rRset.sigs();
          if (!sigs.hasNext()) {
            try {
              signSiteRecord(zone, rRset.first());
            } catch (DNSSEC.DNSSECException e) {
              throw new IOException(e);
            }
          }
        }
      }
    }
  }

  /**
   * Initializes a zone by reading any zone file by the same name in the
   * designated zone file directory.
   *
   * @param conf the Hadoop configuration object.
   * @throws IOException
   */
  private void initializeZonesFromFiles(Configuration conf) throws IOException {
    // should this be in HDFS?
    String zonesDir = conf.get(KEY_DNS_ZONES_DIR);
    if (zonesDir != null) {
      Iterator<File> iterator = FileUtils.iterateFiles(new File(zonesDir),
          new IOFileFilter() {
            @Override
            public boolean accept(
                File file) {
              return file.getName().endsWith(
                  ZONE_SUFFIX);
            }

            @Override
            public boolean accept(
                File file,
                String s) {
              return s.endsWith(
                  ZONE_SUFFIX);
            }
          }, null);
      while (iterator.hasNext()) {
        File file = iterator.next();
        String name = file.getName();
        name = name.substring(0, name.indexOf(ZONE_SUFFIX) + 1);
        Zone zone = new SecureableZone(Name.fromString(name),
            file.getAbsolutePath());
        zones.putIfAbsent(zone.getOrigin(), zone);
      }
    }
  }

  /**
   * Return the number of zones in the map.
   *
   * @return number of zones in the map
   */
  @VisibleForTesting
  protected int getZoneCount() {
    return zones.size();
  }

  /**
   * Initializes the reverse lookup zone (mapping IP to name).
   *
   * @param conf the Hadoop configuration.
   * @throws IOException if the DNSSEC key can not be read.
   */
  private void initializeReverseLookupZone(Configuration conf)
      throws IOException {
    // Determine if the subnet should be split into
    // multiple reverse zones, this can be necessary in
    // network configurations where the hosts and containers
    // are part of the same subnet (i.e. the containers only use
    // part of the subnet).
    Boolean shouldSplitReverseZone = conf.getBoolean(KEY_DNS_SPLIT_REVERSE_ZONE,
        DEFAULT_DNS_SPLIT_REVERSE_ZONE);
    if (shouldSplitReverseZone) {
      long subnetCount = ReverseZoneUtils.getSubnetCountForReverseZones(conf);
      addSplitReverseZones(conf, subnetCount);
      // Single reverse zone
    } else {
      Name reverseLookupZoneName = getReverseZoneName(conf);
      if (reverseLookupZoneName == null) {
        // reverse lookup disabled
        return;
      }
      Zone reverseLookupZone = configureZone(reverseLookupZoneName, conf);
      zones.put(reverseLookupZone.getOrigin(), reverseLookupZone);
    }
  }

  /**
   * Create the zones based on the zone count.
   *
   * @param conf        the Hadoop configuration.
   * @param subnetCount number of subnets to create reverse zones for.
   * @throws IOException if the DNSSEC key can not be read.
   */
  @VisibleForTesting
  protected void addSplitReverseZones(Configuration conf, long subnetCount)
      throws IOException {
    String subnet = conf.get(KEY_DNS_ZONE_SUBNET);
    String range = conf.get(KEY_DNS_SPLIT_REVERSE_ZONE_RANGE);

    // Add the split reverse zones
    for (int idx = 0; idx < subnetCount; idx++) {
      Name reverseLookupZoneName = getReverseZoneName(ReverseZoneUtils
          .getReverseZoneNetworkAddress(subnet, Integer.parseInt(range), idx));
      Zone reverseLookupZone = configureZone(reverseLookupZoneName, conf);
      zones.put(reverseLookupZone.getOrigin(), reverseLookupZone);
    }
  }

  /**
   * Returns the list of reverse lookup zones.
   *
   * @param conf the hadoop configuration.
   * @return the list of reverse zone names required based on the configuration
   * properties.
   */
  protected Name getReverseZoneName(Configuration conf) {
    Name name = null;
    String zoneSubnet = getZoneSubnet(conf);
    if (zoneSubnet == null) {
      LOG.warn("Zone subnet is not configured.  Reverse lookups disabled");
    } else {
      // is there a netmask
      String mask = conf.get(KEY_DNS_ZONE_MASK);
      if (mask != null) {
        // get the range of IPs
        SubnetUtils utils = new SubnetUtils(zoneSubnet, mask);
        name = getReverseZoneName(utils, zoneSubnet);
      } else {
        name = getReverseZoneName(zoneSubnet);
      }
    }
    return name;
  }

  /**
   * Return the subnet for the zone.  this should be a network address for the
   * subnet (ends in ".0").
   *
   * @param conf the hadoop configuration.
   * @return the zone subnet.
   */
  private String getZoneSubnet(Configuration conf) {
    String subnet = conf.get(KEY_DNS_ZONE_SUBNET);
    if (subnet != null) {
      final String[] bytes = subnet.split("\\.");
      if (bytes.length == 3) {
        subnet += ".0";
      }
    }
    return subnet;
  }

  /**
   * Return the reverse zone name based on the address.
   *
   * @param networkAddress the network address.
   * @return the reverse zone name.
   */
  private Name getReverseZoneName(String networkAddress) {
    return getReverseZoneName(null, networkAddress);
  }

  /**
   * Return the reverse zone name based on the address.
   *
   * @param utils          subnet utils
   * @param networkAddress the network address.
   * @return the reverse zone name.
   */
  private Name getReverseZoneName(SubnetUtils utils, String networkAddress) {
    Name reverseZoneName = null;
    boolean isLargeNetwork = false;
    if (utils != null) {
      isLargeNetwork = utils.getInfo().getAddressCountLong() > 256;
    }
    final String[] bytes = networkAddress.split("\\.");
    if (bytes.length == 4) {
      String reverseLookupZoneName = null;
      if (isLargeNetwork) {
        reverseLookupZoneName =
            String.format("%s.%s.%s",
                bytes[1],
                bytes[0],
                IN_ADDR_ARPA);
      } else {
        reverseLookupZoneName =
            String.format("%s.%s.%s.%s",
                bytes[2],
                bytes[1],
                bytes[0],
                IN_ADDR_ARPA);
      }
      try {
        reverseZoneName = Name.fromString(reverseLookupZoneName);
      } catch (TextParseException e) {
        LOG.warn("Unable to convert {} to DNS name", reverseLookupZoneName);
      }
    }
    return reverseZoneName;
  }

  /**
   * Create the zone and its related zone associated DNS records  (NS, SOA).
   *
   * @param zoneName domain name of the zone
   * @param conf     configuration reference.
   * @return the zone.
   * @throws IOException
   */
  private Zone configureZone(Name zoneName, Configuration conf)
      throws IOException {
    bindHost = Name.fromString(
        InetAddress.getLocalHost().getCanonicalHostName() + ".");
    SOARecord soaRecord = new SOARecord(zoneName, DClass.IN, ttl,
        bindHost,
        bindHost, getSerial(), 86000, 7200,
        1209600, 600);
    NSRecord nsRecord = new NSRecord(zoneName, DClass.IN, ttl, bindHost);
    Zone zone = zones.get(zoneName);
    if (zone == null) {
      zone = new SecureableZone(zoneName, new Record[] {soaRecord, nsRecord});
    }

    try {
      enableDNSSECIfNecessary(zone, conf, soaRecord, nsRecord);
    } catch (NoSuchAlgorithmException e) {
      throw new IOException(e);
    } catch (InvalidKeySpecException e) {
      throw new IOException(e);
    } catch (DNSSEC.DNSSECException e) {
      throw new IOException(e);
    }

    return zone;
  }

  /**
   * Return a serial number based on the current date and time.
   *
   * @return the serial number.
   */
  private long getSerial() {
    Date curDate = new Date();
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHH");
    String serial = simpleDateFormat.format(curDate);
    return Long.parseLong(serial);
  }

  /**
   * Set the value of the DNSSEC enabled property.
   *
   * @param conf the Hadoop configuration.
   */
  @VisibleForTesting
  protected void setDNSSECEnabled(Configuration conf) {
    dnssecEnabled = conf.getBoolean(KEY_DNSSEC_ENABLED, false);
  }

  /**
   * Is DNSSEC enabled?
   *
   * @return true if enabled, false otherwise.
   */
  private boolean isDNSSECEnabled() {
    return dnssecEnabled;
  }

  /**
   * Load the required public/private keys, create the zone DNSKEY record, and
   * sign the zone level records.
   *
   * @param zone      the zone.
   * @param conf      the configuration.
   * @param soaRecord the SOA record.
   * @param nsRecord  the NS record.
   * @throws IOException
   * @throws NoSuchAlgorithmException
   * @throws InvalidKeySpecException
   * @throws DNSSEC.DNSSECException
   */
  private void enableDNSSECIfNecessary(Zone zone, Configuration conf,
      SOARecord soaRecord,
      NSRecord nsRecord)
      throws IOException, NoSuchAlgorithmException, InvalidKeySpecException,
      DNSSEC.DNSSECException {
    if (isDNSSECEnabled()) {
      // read in the DNSKEY and create the DNSKEYRecord
      // TODO:  reading these out of config seems wrong...
      String publicKey = conf.get(KEY_DNSSEC_PUBLIC_KEY);
      if (publicKey == null) {
        throw new IOException("DNSSEC Key not configured");
      }
      //TODO - perhaps read in actual DNSKEY record structure?
      Name zoneName = zone.getOrigin();
      DNSKEYRecord dnskeyRecord = dnsKeyRecs.get(zoneName);
      if (dnskeyRecord == null) {
        byte[] key = Base64.decodeBase64(publicKey.getBytes("UTF-8"));
        dnskeyRecord = new DNSKEYRecord(zoneName,
            DClass.IN, ttl,
            DNSKEYRecord.Flags.ZONE_KEY,
            DNSKEYRecord.Protocol.DNSSEC,
            DNSSEC.Algorithm.RSASHA256, key);
        dnsKeyRecs.putIfAbsent(zoneName, dnskeyRecord);
      }
      LOG.info("Registering {}", dnskeyRecord);
      try (CloseableLock lock = writeLock.lock()) {
        zone.addRecord(dnskeyRecord);

        String privateKeyFile = conf.get(KEY_DNSSEC_PRIVATE_KEY_FILE,
            DEFAULT_DNSSEC_PRIVATE_KEY_FILE);

        Properties props = new Properties();
        try (
            FileInputStream inputStream = new FileInputStream(privateKeyFile)) {
          props.load(inputStream);
        }

        String privateModulus = props.getProperty("Modulus");
        String privateExponent = props.getProperty("PrivateExponent");

        RSAPrivateKeySpec privateSpec = new RSAPrivateKeySpec(
            new BigInteger(1, Base64.decodeBase64(privateModulus)),
            new BigInteger(1, Base64.decodeBase64(privateExponent)));

        KeyFactory factory = KeyFactory.getInstance("RSA");
        privateKey = factory.generatePrivate(privateSpec);

        signSiteRecord(zone, dnskeyRecord);
        signSiteRecord(zone, soaRecord);
        signSiteRecord(zone, nsRecord);
      }
      // create required DS records

      // domain
//      DSRecord dsRecord = new DSRecord(zoneName, DClass.IN, ttl,
//                                       DSRecord.Digest.SHA1, dnskeyRecord);
//      zone.addRecord(dsRecord);
//      signSiteRecord(zone, dsRecord);
    }
  }

  /**
   * Sign a DNS record.
   *
   * @param zone   the zone reference
   * @param record the record to sign.
   * @throws DNSSEC.DNSSECException
   */
  private void signSiteRecord(Zone zone, Record record)
      throws DNSSEC.DNSSECException {
    RRset rrset = zone.findExactMatch(record.getName(),
        record.getType());
    Calendar cal = Calendar.getInstance();
    Date inception = cal.getTime();
    cal.add(Calendar.YEAR, 1);
    Date expiration = cal.getTime();
    RRSIGRecord rrsigRecord =
        DNSSEC.sign(rrset, dnsKeyRecs.get(zone.getOrigin()),
            privateKey, inception, expiration);
    LOG.info("Adding {}", record);
    rrset.addRR(rrsigRecord);
  }

  /**
   * Sets the zone/domain name.  The name will be read from the configuration
   * and the code will ensure the name is absolute.
   *
   * @param conf the configuration.
   * @throws IOException
   */
  void setDomainName(Configuration conf) throws IOException {
    domainName = conf.get(KEY_DNS_DOMAIN);
    if (domainName == null) {
      throw new IOException("No DNS domain name specified");
    }
    if (!domainName.endsWith(".")) {
      domainName += ".";
    }
  }

  /**
   * Stops the registry.
   *
   * @throws Exception if the service stop generates an issue.
   */
  @Override
  protected void serviceStop() throws Exception {
    stopExecutor();
    super.serviceStop();
  }

  /**
   * Shuts down the leveraged executor service.
   */
  protected synchronized void stopExecutor() {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  /**
   * Creates a DNS error response.
   *
   * @param in the byte array detailing the error.
   * @return the error message, in bytes
   */
  public byte[] formErrorMessage(byte[] in) {
    Header header;
    try {
      header = new Header(in);
    } catch (IOException e) {
      return null;
    }
    return buildErrorMessage(header, Rcode.FORMERR, null);
  }

  /**
   * Process a TCP request.
   *
   * @param ch the socket channel for the request.
   * @throws IOException if the tcp processing generates an issue.
   */
  public void nioTCPClient(SocketChannel ch) throws IOException {
    try {
      // query sizes are small, so the following two lines should work
      // in all instances
      ByteBuffer buf = ByteBuffer.allocate(1024);
      ch.read(buf);
      buf.flip();
      int messageLength = getMessgeLength(buf);

      byte[] in = new byte[messageLength];

      buf.get(in, 0, messageLength);

      Message query;
      byte[] response;
      try {
        query = new Message(in);
        LOG.info("received TCP query {}", query.getQuestion());
        response = generateReply(query, ch.socket());
        if (response == null) {
          return;
        }
      } catch (IOException e) {
        response = formErrorMessage(in);
      }

      ByteBuffer out = ByteBuffer.allocate(response.length + 2);
      out.clear();
      byte[] data = new byte[2];

      data[1] = (byte)(response.length & 0xFF);
      data[0] = (byte)((response.length >> 8) & 0xFF);
      out.put(data);
      out.put(response);
      out.flip();

      while(out.hasRemaining()) {
        ch.write(out);
      }

    } catch (IOException e) {
      throw NetUtils.wrapException(ch.socket().getInetAddress().getHostName(),
          ch.socket().getPort(),
          ch.socket().getLocalAddress().getHostName(),
          ch.socket().getLocalPort(), e);
    } catch (BufferUnderflowException e) {
      // Ignore system monitor ping packets
    } finally {
      IOUtils.closeStream(ch);
    }

  }

  /**
   * Calculate the inbound message length, which is related in the message as an
   * unsigned short value.
   *
   * @param buf the byte buffer containing the message.
   * @return the message length
   * @throws EOFException
   */
  private int getMessgeLength(ByteBuffer buf) throws EOFException {
    int ch1 = buf.get();
    int ch2 = buf.get();
    if ((ch1 | ch2) < 0) {
      throw new EOFException();
    }
    return (ch1 << 8) + (ch2 & 0xff);
  }

  /**
   * Monitor the TCP socket for inbound requests.
   *
   * @param serverSocketChannel the server socket channel
   * @param addr                the local inet address
   * @param port                the listener (local) port
   * @throws Exception if the tcp processing fails.
   */
  public void serveNIOTCP(ServerSocketChannel serverSocketChannel,
      InetAddress addr, int port) throws Exception {
    try {

      while (true) {
        final SocketChannel socketChannel = serverSocketChannel.accept();
        if (socketChannel != null) {
          executor.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
              nioTCPClient(socketChannel);
              return true;
            }
          });

        } else {
          Thread.sleep(500);
        }
      }
    } catch (IOException e) {
      throw NetUtils.wrapException(addr.getHostName(), port,
          addr.getHostName(), port, e);
    }
  }

  /**
   * Open the TCP listener.
   *
   * @param addr the host address.
   * @param port the host port.
   * @return the created server socket channel.
   * @throws IOException
   */
  private ServerSocketChannel openTCPChannel(InetAddress addr, int port)
      throws IOException {
    ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
    try {
      serverSocketChannel.socket().bind(new InetSocketAddress(addr, port));
      serverSocketChannel.configureBlocking(false);
    } catch (IOException e) {
      throw NetUtils.wrapException(null, 0,
          InetAddress.getLocalHost().getHostName(),
          port, e);
    }
    return serverSocketChannel;
  }

  /**
   * Create the thread (Callable) monitoring the TCP listener.
   *
   * @param addr host address.
   * @param port host port.
   * @throws Exception if the tcp listener generates an error.
   */
  public void addNIOTCP(final InetAddress addr, final int port)
      throws Exception {
    final ServerSocketChannel tcpChannel = openTCPChannel(addr, port);
    executor.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          serveNIOTCP(tcpChannel, addr, port);
        } catch (Exception e) {
          LOG.error("Error initializing DNS TCP listener", e);
          throw e;
        }

        return true;
      }

    });
  }

  /**
   * Create the thread monitoring the socket for inbound UDP requests.
   *
   * @param addr host address.
   * @param port host port.
   * @throws Exception if the UDP listener creation generates an error.
   */
  public void addNIOUDP(final InetAddress addr, final int port)
      throws Exception {
    final DatagramChannel udpChannel = openUDPChannel(addr, port);
    executor.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          serveNIOUDP(udpChannel, addr, port);
        } catch (Exception e) {
          LOG.error("Error initializing DNS UDP listener", e);
          throw e;
        }
        return true;
      }
    });
  }

  /**
   * Process an inbound UDP request.
   *
   * @param channel the UDP datagram channel.
   * @param addr    local host address.
   * @param port    local port.
   * @throws IOException if the UDP processing fails.
   */
  private synchronized void serveNIOUDP(DatagramChannel channel,
      InetAddress addr, int port) throws Exception {
    SocketAddress remoteAddress = null;
    try {

      ByteBuffer input = ByteBuffer.allocate(4096);
      ByteBuffer output = ByteBuffer.allocate(4096);
      byte[] in = null;

      while (true) {
        input.clear();
        try {
          remoteAddress = channel.receive(input);
        } catch (IOException e) {
          LOG.debug("Error during message receipt", e);
          continue;
        }
        Message query;
        byte[] response = null;
        try {
          int position = input.position();
          in = new byte[position];
          input.flip();
          input.get(in);
          query = new Message(in);
          LOG.info("{}: received UDP query {}", remoteAddress,
              query.getQuestion());
          response = generateReply(query, null);
          if (response == null) {
            continue;
          }
        } catch (IOException e) {
          response = formErrorMessage(in);
        }
        output.clear();
        output.put(response);
        output.flip();

        LOG.debug("{}:  sending response", remoteAddress);
        channel.send(output, remoteAddress);
      }
    } catch (Exception e) {
      if (e instanceof IOException && remoteAddress != null) {
        throw NetUtils.wrapException(addr.getHostName(),
            port,
            ((InetSocketAddress) remoteAddress).getHostName(),
            ((InetSocketAddress) remoteAddress).getPort(),
            (IOException) e);
      } else {
        throw e;
      }
    }
  }

  /**
   * Create and UDP listener socket.
   *
   * @param addr host address.
   * @param port host port.
   * @return
   * @throws IOException if listener creation fails.
   */
  private DatagramChannel openUDPChannel(InetAddress addr, int port)
      throws IOException {
    DatagramChannel channel = DatagramChannel.open();
    try {
      channel.socket().bind(new InetSocketAddress(addr, port));
    } catch (IOException e) {
      throw NetUtils.wrapException(null, 0,
          InetAddress.getLocalHost().getHostName(),
          port, e);
    }
    return channel;
  }

  /**
   * Create an error message.
   *
   * @param header   the response header.
   * @param rcode    the response code.
   * @param question the question record.
   * @return  the error message.
   */
  byte[] buildErrorMessage(Header header, int rcode, Record question) {
    Message response = new Message();
    response.setHeader(header);
    for (int i = 0; i < 4; i++) {
      response.removeAllRecords(i);
    }
    response.addRecord(question, Section.QUESTION);
    header.setRcode(rcode);
    return response.toWire();
  }

  /**
   * Generate an error message based on inbound query.
   *
   * @param query the query.
   * @param rcode the response code for the specific error.
   * @return the error message.
   */
  public byte[] errorMessage(Message query, int rcode) {
    return buildErrorMessage(query.getHeader(), rcode,
        query.getQuestion());
  }

  /**
   * Generate the response for the inbound DNS query.
   *
   * @param query the query.
   * @param s     the socket associated with the query.
   * @return the response, in bytes.
   * @throws IOException if reply generation fails.
   */
  byte[] generateReply(Message query, Socket s)
      throws IOException {
    Header header;
    boolean badversion;
    int maxLength;
    int flags = 0;

    OPTRecord queryOPT = query.getOPT();
    maxLength = getMaxLength(s, queryOPT);

    header = query.getHeader();
    if (header.getFlag(Flags.QR)) {
      LOG.debug("returning null");
      return null;
    }
    if (header.getRcode() != Rcode.NOERROR) {
      return errorMessage(query, Rcode.FORMERR);
    }
    if (header.getOpcode() != Opcode.QUERY) {
      return errorMessage(query, Rcode.NOTIMP);
    }

    Record queryRecord = query.getQuestion();

    if (queryOPT != null && (queryOPT.getFlags() & ExtendedFlags.DO) != 0) {
      flags = FLAG_DNSSECOK;
    }

    Message response = new Message(query.getHeader().getID());
    response.getHeader().setFlag(Flags.QR);
    if (query.getHeader().getFlag(Flags.RD)) {
      response.getHeader().setFlag(Flags.RD);
      response.getHeader().setFlag(Flags.RA);
    }
    response.addRecord(queryRecord, Section.QUESTION);

    Name name = queryRecord.getName();
    int type = queryRecord.getType();
    int dclass = queryRecord.getDClass();

    TSIGRecord queryTSIG = query.getTSIG();
    if (type == Type.AXFR && s != null) {
      return doAXFR(name, query, null, queryTSIG, s);
    }
    if (!Type.isRR(type) && type != Type.ANY) {
      return errorMessage(query, Rcode.NOTIMP);
    }

    LOG.debug("calling addAnswer");
    byte rcode = addAnswer(response, name, type, dclass, 0, flags);
    if (rcode != Rcode.NOERROR) {
      rcode = remoteLookup(response, name, type, 0);
      response.getHeader().setRcode(rcode);
    }
    addAdditional(response, flags);

    if (queryOPT != null) {
      int optflags = (flags == FLAG_DNSSECOK) ? ExtendedFlags.DO : 0;
      OPTRecord opt = new OPTRecord((short) 4096, rcode >>> 16, (byte) 0,
          optflags);
      response.addRecord(opt, Section.ADDITIONAL);
    }

    return response.toWire(maxLength);
  }

  /**
   * Lookup record from upstream DNS servers.
   */
  private byte remoteLookup(Message response, Name name, int type,
      int iterations) {
    // If retrieving the root zone, query for NS record type
    if (name.toString().equals(".")) {
      type = Type.NS;
    }

    // Always add any CNAMEs to the response first
    if (type != Type.CNAME) {
      Record[] cnameAnswers = getRecords(name, Type.CNAME);
      if (cnameAnswers != null) {
        for (Record cnameR : cnameAnswers) {
          if (!response.findRecord(cnameR)) {
            response.addRecord(cnameR, Section.ANSWER);
          }
        }
      }
    }

    // Forward lookup to primary DNS servers
    Record[] answers = getRecords(name, type);
    try {
      for (Record r : answers) {
        if (!response.findRecord(r)) {
          if (r.getType() == Type.SOA) {
            response.addRecord(r, Section.AUTHORITY);
          } else {
            response.addRecord(r, Section.ANSWER);
          }
        }
        if (r.getType() == Type.CNAME) {
          Name cname = ((CNAMERecord) r).getAlias();
          if (iterations < 6) {
            remoteLookup(response, cname, type, iterations + 1);
          }
        }
      }
    } catch (NullPointerException e) {
      return Rcode.NXDOMAIN;
    } catch (Throwable e) {
      return Rcode.SERVFAIL;
    }
    return Rcode.NOERROR;
  }

  /**
   * Requests records for the given resource name.
   *
   * @param name - query string
   * @param type - type of DNS record to lookup
   * @return DNS records
   */
  protected Record[] getRecords(Name name, int type) {
    Record[] result = null;
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<Record[]> future = executor.submit(new LookupTask(name, type));
    try {
      result = future.get(1500, TimeUnit.MILLISECONDS);
      return result;
    } catch (InterruptedException | ExecutionException |
        TimeoutException | NullPointerException |
        ExceptionInInitializerError e) {
      LOG.warn("Failed to lookup: {} type: {}", name, Type.string(type), e);
      return result;
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Create a query to forward to the primary DNS server (if configured).
   * NOTE:  Experimental
   *
   * @param query the inbound query.
   * @return the query to forward to the primary server.
   * @throws NameTooLongException
   * @throws TextParseException if query creation fails.
   */
  private Message createPrimaryQuery(Message query)
      throws NameTooLongException, TextParseException {
    Name name = query.getQuestion().getName();
    if (name.labels() > 0 && name.labels() <= 2) {
      // short relative or absolute name.  this code may not be necessary -
      // OS resolution utilities probably append the search paths defined
      // in resolv.conf prior to the lookup
      int id = query.getHeader().getID();
      String queryName = name.getLabelString(0);
      Name qualifiedName = Name.concatenate(Name.fromString(queryName),
          Name.fromString(domainName));
      LOG.info("Received query {}.  Forwarding query {}", name, qualifiedName);
      Record question = Record.newRecord(qualifiedName,
          query.getQuestion().getType(),
          query.getQuestion().getDClass());
      query = Message.newQuery(question);
      query.getHeader().setID(id);
    }
    return query;
  }

  /**
   * Calculate the max length for a response.
   *
   * @param s        the request socket.
   * @param queryOPT describes Extended DNS (EDNS) properties of a Message.
   * @return  the length of the response.
   */
  private int getMaxLength(Socket s, OPTRecord queryOPT) {
    int maxLength;
    if (s != null) {
      maxLength = 65535;
    } else if (queryOPT != null) {
      maxLength = Math.max(queryOPT.getPayloadSize(), 512);
    } else {
      maxLength = 512;
    }
    return maxLength;
  }

  /**
   * Add additional information to a DNS response section if a glue name is
   * specified.
   *
   * @param response the response message.
   * @param section  the section of the response (e.g. ANSWER, AUTHORITY)
   * @param flags the flags.
   */
  private void addAdditional2(Message response, int section, int flags) {
    Record[] records = response.getSectionArray(section);
    for (int i = 0; i < records.length; i++) {
      Record r = records[i];
      Name glueName = r.getAdditionalName();
      if (glueName != null) {
        addGlue(response, glueName, flags);
      }
    }
  }

  /**
   * Process any additional records indicated for both the ANSWER and AUTHORITY
   * sections of the response.
   *
   * @param response the response message.
   * @param flags the flags.
   */
  private void addAdditional(Message response, int flags) {
    addAdditional2(response, Section.ANSWER, flags);
    addAdditional2(response, Section.AUTHORITY, flags);
  }

  /**
   * Add the specific record indicated by the "glue", or the mapping to a
   * specific host.
   *
   * @param response the response message.
   * @param name     the name of the glue record.
   * @param flags    the flags.
   */
  private void addGlue(Message response, Name name, int flags) {
    RRset a = findExactMatch(name, Type.A);
    if (a == null) {
      return;
    }
    addRRset(name, response, a, Section.ADDITIONAL, flags);
  }

  /**
   * Find the record set that matches the requested name and type.
   *
   * @param name the requested name.
   * @param type the record type.
   * @return the set of records with the given name and type.
   */
  public RRset findExactMatch(Name name, int type) {
    try (CloseableLock lock = readLock.lock()) {
      Zone zone = findBestZone(name);
      if (zone != null) {
        return zone.findExactMatch(name, type);
      }
    }

    return null;
  }

  /**
   * Find the zone that correlates to the provided name.
   *
   * @param name the name to be matched to a zone.
   * @return the zone.
   */
  @Override public Zone findBestZone(Name name) {
    Zone foundzone = null;
    foundzone = zones.get(name);
    if (foundzone != null) {
      return foundzone;
    }
    int labels = name.labels();
    for (int i = 1; i < labels; i++) {
      Name tname = new Name(name, i);
      foundzone = zones.get(tname);
      if (foundzone != null) {
        return foundzone;
      }
    }
    return null;
  }

  /**
   * Add the answer section to the response.
   *
   * @param response   the response message.
   * @param name       the name of the answer record.
   * @param type       the type of record.
   * @param dclass     the DNS class.
   * @param iterations iteration count.
   * @param flags
   * @return the response code.
   */
  byte addAnswer(Message response, Name name, int type, int dclass,
      int iterations, int flags) {
    SetResponse sr = null;
    byte rcode = Rcode.NOERROR;

    if (iterations > 6) {
      return Rcode.NOERROR;
    }

    if (type == Type.SIG || type == Type.RRSIG) {
      type = Type.ANY;
      flags |= FLAG_SIGONLY;
    }

    Zone zone = findBestZone(name);

    LOG.debug("finding record");
    try (CloseableLock lock = readLock.lock()) {
      if (zone != null) {
        sr = zone.findRecords(name, type);
      } else {
        rcode = Rcode.NOTAUTH;
      }
    }
    LOG.info("found local record? {}", sr != null && sr.isSuccessful());

    if (sr != null) {
      if (sr.isCNAME()) {
        CNAMERecord cname = sr.getCNAME();
        RRset rrset = zone.findExactMatch(cname.getName(), Type.CNAME);
        addRRset(name, response, rrset, Section.ANSWER, flags);
        if (iterations == 0) {
          response.getHeader().setFlag(Flags.AA);
        }
        rcode = addAnswer(response, cname.getTarget(),
            type, dclass, iterations + 1, flags);
      }
      if (sr.isNXDOMAIN()) {
        response.getHeader().setRcode(Rcode.NXDOMAIN);
        if (isDNSSECEnabled()) {
          try {
            addNXT(response, flags);
          } catch (Exception e) {
            LOG.warn("Unable to add NXTRecord to AUTHORITY Section", e);
          }
        }
        addSOA(response, zone, flags);
        if (iterations == 0) {
          response.getHeader().setFlag(Flags.AA);
        }
        rcode = Rcode.NXDOMAIN;
      } else if (sr.isNXRRSET()) {
        LOG.info("No data found the given name {} and type {}", name, type);
        addSOA(response, zone, flags);
        if (iterations == 0) {
          response.getHeader().setFlag(Flags.AA);
        }
      } else if (sr.isSuccessful()) {
        RRset[] rrsets = sr.answers();
        LOG.info("found answers {}", rrsets);
        for (int i = 0; i < rrsets.length; i++) {
          addRRset(name, response, rrsets[i],
              Section.ANSWER, flags);
        }
        addNS(response, zone, flags);
        if (iterations == 0) {
          response.getHeader().setFlag(Flags.AA);
        }
      }
    } else {
      if (zone != null) {
        Name defaultDomain = null;
        try {
          defaultDomain = Name.fromString(domainName);
          zone = zones.get(defaultDomain);
          addNS(response, zone, flags);
          if (iterations == 0) {
            response.getHeader().setFlag(Flags.AA);
          }
        } catch (TextParseException e) {
          LOG.warn("Unable to obtain default zone for unknown name response",
              e);
        }
      }
    }

    return rcode;
  }

  /**
   * Add the SOA record (describes the properties of the zone) to the authority
   * section of the response.
   *
   * @param response the response message.
   * @param zone     the DNS zone.
   */
  private void addSOA(Message response, Zone zone, int flags) {
    RRset soa = zone.findExactMatch(zone.getOrigin(), Type.SOA);
    addRRset(soa.getName(), response, soa,
        Section.AUTHORITY, flags);
  }

  /**
   * Add the NXT record to the authority
   * section of the response.
   *
   * @param response the response message.
   */
  private void addNXT(Message response, int flags)
      throws DNSSEC.DNSSECException, IOException {
    Record nxtRecord = getNXTRecord(
        response.getSectionArray(Section.QUESTION)[0]);
    Zone zone = findBestZone(nxtRecord.getName());
    addRecordCommand.exec(zone, nxtRecord);
    RRset nxtRR = zone.findExactMatch(nxtRecord.getName(), Type.NXT);
    addRRset(nxtRecord.getName(), response, nxtRR, Section.AUTHORITY, flags);

    removeRecordCommand.exec(zone, nxtRecord);
  }

  /**
   * Return an NXT record required to validate negative responses.  If there is
   * an issue returning the NXT record, a SOA record will be returned.
   *
   * @param query the query record.
   * @return an NXT record.
   */
  private Record getNXTRecord(Record query) {
    Record response = null;
    SecureableZone zone = (SecureableZone) findBestZone(query.getName());
    if (zone != null) {
      response = zone.getNXTRecord(query, zone);
      if (response == null) {
        response = zone.getSOA();
      }
    }

    return response;
  }

  /**
   * Add the name server info to the authority section.
   *
   * @param response the response message.
   * @param zone     the DNS zone.
   * @param flags    the flags.
   */
  private void addNS(Message response, Zone zone, int flags) {
    RRset nsRecords = zone.getNS();
    addRRset(nsRecords.getName(), response, nsRecords,
        Section.AUTHORITY, flags);
  }

  /**
   * Add the provided record set to the response section specified.
   *
   * @param name     the name associated with the record set.
   * @param response the response message.
   * @param rrset    the record set.
   * @param section  the response section to which the record set will be added.
   * @param flags    the flags.
   */
  private void addRRset(Name name, Message response, RRset rrset, int section,
      int flags) {
    for (int s = 1; s <= section; s++) {
      if (response.findRRset(name, rrset.getType(), s)) {
        return;
      }
    }
    if ((flags & FLAG_SIGONLY) == 0) {
      Iterator it = rrset.rrs();
      while (it.hasNext()) {
        Record r = (Record) it.next();
        if (r.getName().isWild() && !name.isWild()) {
          r = r.withName(name);
        }
        response.addRecord(r, section);
      }
    }
    if ((flags & (FLAG_SIGONLY | FLAG_DNSSECOK)) != 0) {
      Iterator it = rrset.sigs();
      while (it.hasNext()) {
        Record r = (Record) it.next();
        if (r.getName().isWild() && !name.isWild()) {
          r = r.withName(name);
        }
        response.addRecord(r, section);
      }
    }
  }

  /**
   * Perform a zone transfer.
   *
   * @param name  the zone name.
   * @param query the query.
   * @param tsig  the query signature.
   * @param qtsig the signature record.
   * @param s     the connection socket.
   * @return      an error message if there is no matching zone
   * or null due to error.
   */
  byte[] doAXFR(Name name, Message query, TSIG tsig, TSIGRecord qtsig,
      Socket s) {
    boolean first = true;
    Zone zone = findBestZone(name);
    if (zone == null) {
      return errorMessage(query, Rcode.REFUSED);
    }
    Iterator it = zone.AXFR();
    try {
      DataOutputStream dataOut;
      dataOut = new DataOutputStream(s.getOutputStream());
      int id = query.getHeader().getID();
      while (it.hasNext()) {
        RRset rrset = (RRset) it.next();
        Message response = new Message(id);
        Header header = response.getHeader();
        header.setFlag(Flags.QR);
        header.setFlag(Flags.AA);
        addRRset(rrset.getName(), response, rrset,
            Section.ANSWER, FLAG_DNSSECOK);
        if (tsig != null) {
          tsig.applyStream(response, qtsig, first);
          qtsig = response.getTSIG();
        }
        first = false;
        byte[] out = response.toWire();
        dataOut.writeShort(out.length);
        dataOut.write(out);
      }
    } catch (IOException ex) {
      System.out.println("AXFR failed");
    }
    try {
      s.close();
    } catch (IOException ex) {
    }
    return null;
  }

  /**
   * Perform the registry operation (register or delete).  This method will take
   * the provided service record and either add or remove the DNS records
   * indicated.
   *
   * @param path    the ZK path for the service record.
   * @param record  the service record.
   * @param command the registry command (REGISTER or DELETE).
   * @throws IOException if the is an error performing registry operation.
   */
  private void op(String path, ServiceRecord record, RegistryCommand command)
      throws IOException {
    ServiceRecordProcessor processor;
    try {
      String yarnPersistanceValue = record.get(
                                    YarnRegistryAttributes.YARN_PERSISTENCE);
      if (yarnPersistanceValue != null) {
        if (yarnPersistanceValue.equals(CONTAINER)) {
          // container registration.  the logic to identify and create the
          // container entry needs to be enhanced/more accurate and associate
          // to correct host
          processor =
               new ContainerServiceRecordProcessor(record, path, domainName,
                   this);
        } else {
          LOG.debug("Creating ApplicationServiceRecordProcessor for {}",
                    yarnPersistanceValue);
          processor =
               new ApplicationServiceRecordProcessor(record, path, domainName,
                   this);
        }
        processor.manageDNSRecords(command);
      } else {
        LOG.warn("Yarn Registry record {} does not contain {} attribute ",
                  record.toString(), YarnRegistryAttributes.YARN_PERSISTENCE);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }

  }

  /**
   * Return the username found in the ZK path.
   *
   * @param path the ZK path.
   * @return the user name.
   */
  private String getUsername(String path) {
    String user = "anonymous";
    Matcher matcher = USER_NAME.matcher(path);
    if (matcher.find()) {
      user = matcher.group(1);
    }
    return user;
  }

  /**
   * Register DNS records based on the provided service record.
   *
   * @param path   the ZK path of the service record.
   * @param record record providing DNS registration info.
   * @throws IOException if registration causes an error.
   */
  @Override
  public void register(String path, ServiceRecord record) throws IOException {
    op(path, record, addRecordCommand);
  }

  /**
   * Delete the DNS records generated by the provided service record.
   *
   * @param path   the ZK path for the given record.
   * @param record the service record
   * @throws IOException if deletion causes and error.
   */
  @Override
  public void delete(String path, ServiceRecord record) throws IOException {
    op(path, record, removeRecordCommand);
  }

  /**
   * An interface representing a registry associated function/command (see
   * command pattern).
   */
  interface RegistryCommand {
    void exec(Zone zone, Record record) throws IOException;

    String getLogDescription();
  }

  /**
   * The "add record" command.
   */
  private final RegistryCommand addRecordCommand = new RegistryCommand() {
    @Override
    public void exec(Zone zone, Record record) throws IOException {
      if (zone != null) {
        try (CloseableLock lock = writeLock.lock()) {
          zone.addRecord(record);
          LOG.info("Registered {}", record);
          if (isDNSSECEnabled()) {
            Calendar cal = Calendar.getInstance();
            Date inception = cal.getTime();
            cal.add(Calendar.YEAR, 1);
            Date expiration = cal.getTime();
            RRset rRset =
                zone.findExactMatch(record.getName(), record.getType());
            try {
              DNSKEYRecord dnskeyRecord = dnsKeyRecs.get(zone.getOrigin());
              RRSIGRecord rrsigRecord =
                  DNSSEC.sign(rRset, dnskeyRecord, privateKey,
                      inception, expiration);
              LOG.info("Adding {}", rrsigRecord);
              rRset.addRR(rrsigRecord);

              //addDSRecord(zone, record.getName(), record.getDClass(),
              //  record.getTTL(), inception, expiration);

            } catch (DNSSEC.DNSSECException e) {
              throw new IOException(e);
            }
          }
        }
      } else {
        LOG.warn("Unable to find zone matching record {}", record);
      }
    }

    /**
     * Add a DS record associated with the input name.
     * @param zone  the zone.
     * @param name  the record name.
     * @param dClass the DNS class.
     * @param dsTtl the ttl value.
     * @param inception  the time of inception of the record.
     * @param expiration  the expiry time of the record.
     * @throws DNSSEC.DNSSECException if the addition of DS record fails.
     */
    private void addDSRecord(Zone zone,
        Name name, int dClass, long dsTtl,
        Date inception,
        Date expiration) throws DNSSEC.DNSSECException {
      RRset rRset;
      RRSIGRecord rrsigRecord;

      DNSKEYRecord dnskeyRecord = dnsKeyRecs.get(zone.getOrigin());
      DSRecord dsRecord = new DSRecord(name, dClass,
          dsTtl, DSRecord.Digest.SHA1,
          dnskeyRecord);
      zone.addRecord(dsRecord);
      LOG.info("Adding {}", dsRecord);
      rRset = zone.findExactMatch(dsRecord.getName(), dsRecord.getType());

      rrsigRecord = DNSSEC.sign(rRset, dnskeyRecord, privateKey,
          inception, expiration);
      rRset.addRR(rrsigRecord);
    }

    @Override
    public String getLogDescription() {
      return "Registering ";
    }
  };

  /**
   * The "remove record" command.
   */
  private final RegistryCommand removeRecordCommand = new RegistryCommand() {
    @Override
    public void exec(Zone zone, Record record) throws IOException {
      if (zone == null) {
        LOG.error("Unable to remove record because zone is null: {}", record);
        return;
      }
      zone.removeRecord(record);
      LOG.info("Removed {}", record);
      if (isDNSSECEnabled()) {
        RRset rRset = zone.findExactMatch(record.getName(), Type.DS);
        if (rRset != null) {
          zone.removeRecord(rRset.first());
        }
      }
    }

    @Override
    public String getLogDescription() {
      return "Deleting ";
    }
  };

  /**
   * An implementation allowing for obtaining and releasing a lock.
   */
  public static class CloseableLock implements AutoCloseable {
    private Lock lock;

    public CloseableLock(Lock lock) {
      this.lock = lock;
    }

    public CloseableLock lock() {
      lock.lock();
      return this;
    }

    @Override
    public void close() {
      lock.unlock();
    }
  }
}
