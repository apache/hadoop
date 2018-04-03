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

import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xbill.DNS.AAAARecord;
import org.xbill.DNS.ARecord;
import org.xbill.DNS.CNAMERecord;
import org.xbill.DNS.DClass;
import org.xbill.DNS.DNSKEYRecord;
import org.xbill.DNS.DNSSEC;
import org.xbill.DNS.Flags;
import org.xbill.DNS.Message;
import org.xbill.DNS.Name;
import org.xbill.DNS.OPTRecord;
import org.xbill.DNS.PTRRecord;
import org.xbill.DNS.RRSIGRecord;
import org.xbill.DNS.RRset;
import org.xbill.DNS.Rcode;
import org.xbill.DNS.Record;
import org.xbill.DNS.SRVRecord;
import org.xbill.DNS.Section;
import org.xbill.DNS.Type;

import java.io.IOException;
import java.math.BigInteger;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.RSAPrivateKeySpec;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.registry.client.api.RegistryConstants.*;

/**
 *
 */
public class TestRegistryDNS extends Assert {

  private RegistryDNS registryDNS;
  private RegistryUtils.ServiceRecordMarshal marshal;

  private static final String APPLICATION_RECORD = "{\n"
      + "  \"type\" : \"JSONServiceRecord\",\n"
      + "  \"description\" : \"Slider Application Master\",\n"
      + "  \"external\" : [ {\n"
      + "    \"api\" : \"classpath:org.apache.hadoop.yarn.service.appmaster.ipc"
      + "\",\n"
      + "    \"addressType\" : \"host/port\",\n"
      + "    \"protocolType\" : \"hadoop/IPC\",\n"
      + "    \"addresses\" : [ {\n"
      + "      \"host\" : \"192.168.1.5\",\n"
      + "      \"port\" : \"1026\"\n"
      + "    } ]\n"
      + "  }, {\n"
      + "    \"api\" : \"http://\",\n"
      + "    \"addressType\" : \"uri\",\n"
      + "    \"protocolType\" : \"webui\",\n"
      + "    \"addresses\" : [ {\n"
      + "      \"uri\" : \"http://192.168.1.5:1027\"\n"
      + "    } ]\n"
      + "  }, {\n"
      + "    \"api\" : \"classpath:org.apache.hadoop.yarn.service.management\""
      + ",\n"
      + "    \"addressType\" : \"uri\",\n"
      + "    \"protocolType\" : \"REST\",\n"
      + "    \"addresses\" : [ {\n"
      + "      \"uri\" : \"http://192.168.1.5:1027/ws/v1/slider/mgmt\"\n"
      + "    } ]\n"
      + "  } ],\n"
      + "  \"internal\" : [ {\n"
      + "    \"api\" : \"classpath:org.apache.hadoop.yarn.service.agents.secure"
      + "\",\n"
      + "    \"addressType\" : \"uri\",\n"
      + "    \"protocolType\" : \"REST\",\n"
      + "    \"addresses\" : [ {\n"
      + "      \"uri\" : \"https://192.168.1.5:47700/ws/v1/slider/agents\"\n"
      + "    } ]\n"
      + "  }, {\n"
      + "    \"api\" : \"classpath:org.apache.hadoop.yarn.service.agents.oneway"
      + "\",\n"
      + "    \"addressType\" : \"uri\",\n"
      + "    \"protocolType\" : \"REST\",\n"
      + "    \"addresses\" : [ {\n"
      + "      \"uri\" : \"https://192.168.1.5:35531/ws/v1/slider/agents\"\n"
      + "    } ]\n"
      + "  } ],\n"
      + "  \"yarn:id\" : \"application_1451931954322_0016\",\n"
      + "  \"yarn:persistence\" : \"application\"\n"
      + "}\n";
  static final String CONTAINER_RECORD = "{\n"
      + "  \"type\" : \"JSONServiceRecord\",\n"
      + "  \"description\" : \"COMP-NAME\",\n"
      + "  \"external\" : [ ],\n"
      + "  \"internal\" : [ ],\n"
      + "  \"yarn:id\" : \"container_e50_1451931954322_0016_01_000002\",\n"
      + "  \"yarn:persistence\" : \"container\",\n"
      + "  \"yarn:ip\" : \"172.17.0.19\",\n"
      + "  \"yarn:hostname\" : \"0a134d6329ba\"\n"
      + "}\n";

  private static final String CONTAINER_RECORD_NO_IP = "{\n"
      + "  \"type\" : \"JSONServiceRecord\",\n"
      + "  \"description\" : \"COMP-NAME\",\n"
      + "  \"external\" : [ ],\n"
      + "  \"internal\" : [ ],\n"
      + "  \"yarn:id\" : \"container_e50_1451931954322_0016_01_000002\",\n"
      + "  \"yarn:persistence\" : \"container\"\n"
      + "}\n";

  private static final String CONTAINER_RECORD_YARN_PERSISTANCE_ABSENT = "{\n"
      + "  \"type\" : \"JSONServiceRecord\",\n"
      + "  \"description\" : \"COMP-NAME\",\n"
      + "  \"external\" : [ ],\n"
      + "  \"internal\" : [ ],\n"
      + "  \"yarn:id\" : \"container_e50_1451931954322_0016_01_000003\",\n"
      + "  \"yarn:ip\" : \"172.17.0.19\",\n"
      + "  \"yarn:hostname\" : \"0a134d6329bb\"\n"
      + "}\n";

  @Before
  public void initialize() throws Exception {
    setRegistryDNS(new RegistryDNS("TestRegistry"));
    Configuration conf = createConfiguration();

    getRegistryDNS().setDomainName(conf);
    getRegistryDNS().initializeZones(conf);

    setMarshal(new RegistryUtils.ServiceRecordMarshal());
  }

  protected Configuration createConfiguration() {
    Configuration conf = new Configuration();
    conf.set(RegistryConstants.KEY_DNS_DOMAIN, "dev.test");
    conf.set(RegistryConstants.KEY_DNS_ZONE_SUBNET, "172.17.0");
    conf.setTimeDuration(RegistryConstants.KEY_DNS_TTL, 30L, TimeUnit.SECONDS);
    return conf;
  }

  protected boolean isSecure() {
    return false;
  }

  @After
  public void closeRegistry() throws Exception {
    getRegistryDNS().stopExecutor();
  }

  @Test
  public void testAppRegistration() throws Exception {
    ServiceRecord record = getMarshal().fromBytes("somepath",
        APPLICATION_RECORD.getBytes());
    getRegistryDNS().register(
        "/registry/users/root/services/org-apache-slider/test1/", record);

    // start assessing whether correct records are available
    Record[] recs = assertDNSQuery("test1.root.dev.test.");
    assertEquals("wrong result", "192.168.1.5",
        ((ARecord) recs[0]).getAddress().getHostAddress());

    recs = assertDNSQuery("management-api.test1.root.dev.test.", 2);
    assertEquals("wrong target name", "test1.root.dev.test.",
        ((CNAMERecord) recs[0]).getTarget().toString());
    assertTrue("not an ARecord", recs[isSecure() ? 2 : 1] instanceof ARecord);

    recs = assertDNSQuery("appmaster-ipc-api.test1.root.dev.test.",
        Type.SRV, 1);
    assertTrue("not an SRV record", recs[0] instanceof SRVRecord);
    assertEquals("wrong port", 1026, ((SRVRecord) recs[0]).getPort());

    recs = assertDNSQuery("appmaster-ipc-api.test1.root.dev.test.", 2);
    assertEquals("wrong target name", "test1.root.dev.test.",
        ((CNAMERecord) recs[0]).getTarget().toString());
    assertTrue("not an ARecord", recs[isSecure() ? 2 : 1] instanceof ARecord);

    recs = assertDNSQuery("http-api.test1.root.dev.test.", 2);
    assertEquals("wrong target name", "test1.root.dev.test.",
        ((CNAMERecord) recs[0]).getTarget().toString());
    assertTrue("not an ARecord", recs[isSecure() ? 2 : 1] instanceof ARecord);

    recs = assertDNSQuery("http-api.test1.root.dev.test.", Type.SRV,
        1);
    assertTrue("not an SRV record", recs[0] instanceof SRVRecord);
    assertEquals("wrong port", 1027, ((SRVRecord) recs[0]).getPort());

    assertDNSQuery("test1.root.dev.test.", Type.TXT, 3);
    assertDNSQuery("appmaster-ipc-api.test1.root.dev.test.", Type.TXT, 1);
    assertDNSQuery("http-api.test1.root.dev.test.", Type.TXT, 1);
    assertDNSQuery("management-api.test1.root.dev.test.", Type.TXT, 1);
  }

  @Test
  public void testContainerRegistration() throws Exception {
    ServiceRecord record = getMarshal().fromBytes("somepath",
        CONTAINER_RECORD.getBytes());
    getRegistryDNS().register(
        "/registry/users/root/services/org-apache-slider/test1/components/"
            + "ctr-e50-1451931954322-0016-01-000002",
        record);

    // start assessing whether correct records are available
    Record[] recs =
        assertDNSQuery("ctr-e50-1451931954322-0016-01-000002.dev.test.");
    assertEquals("wrong result", "172.17.0.19",
        ((ARecord) recs[0]).getAddress().getHostAddress());

    recs = assertDNSQuery("comp-name.test1.root.dev.test.", 1);
    assertTrue("not an ARecord", recs[0] instanceof ARecord);
  }

  @Test
  public void testContainerRegistrationPersistanceAbsent() throws Exception {
    ServiceRecord record = marshal.fromBytes("somepath",
        CONTAINER_RECORD_YARN_PERSISTANCE_ABSENT.getBytes());
    registryDNS.register(
        "/registry/users/root/services/org-apache-slider/test1/components/"
            + "ctr-e50-1451931954322-0016-01-000003",
         record);

    Name name =
        Name.fromString("ctr-e50-1451931954322-0016-01-000002.dev.test.");
    Record question = Record.newRecord(name, Type.A, DClass.IN);
    Message query = Message.newQuery(question);
    byte[] responseBytes = registryDNS.generateReply(query, null);
    Message response = new Message(responseBytes);
    assertEquals("Excepting NXDOMAIN as Record must not have regsisterd wrong",
        Rcode.NXDOMAIN, response.getRcode());
  }

  @Test
  public void testRecordTTL() throws Exception {
    ServiceRecord record = getMarshal().fromBytes("somepath",
        CONTAINER_RECORD.getBytes());
    getRegistryDNS().register(
        "/registry/users/root/services/org-apache-slider/test1/components/"
            + "ctr-e50-1451931954322-0016-01-000002",
        record);

    // start assessing whether correct records are available
    Record[] recs = assertDNSQuery(
        "ctr-e50-1451931954322-0016-01-000002.dev.test.");
    assertEquals("wrong result", "172.17.0.19",
        ((ARecord) recs[0]).getAddress().getHostAddress());
    assertEquals("wrong ttl", 30L, recs[0].getTTL());

    recs = assertDNSQuery("comp-name.test1.root.dev.test.", 1);
    assertTrue("not an ARecord", recs[0] instanceof ARecord);

    assertEquals("wrong ttl", 30L, recs[0].getTTL());
  }

  @Test
  public void testReverseLookup() throws Exception {
    ServiceRecord record = getMarshal().fromBytes("somepath",
        CONTAINER_RECORD.getBytes());
    getRegistryDNS().register(
        "/registry/users/root/services/org-apache-slider/test1/components/"
            + "ctr-e50-1451931954322-0016-01-000002",
        record);

    // start assessing whether correct records are available
    Record[] recs = assertDNSQuery("19.0.17.172.in-addr.arpa.", Type.PTR, 1);
    assertEquals("wrong result",
        "comp-name.test1.root.dev.test.",
        ((PTRRecord) recs[0]).getTarget().toString());
  }

  @Test
  public void testReverseLookupInLargeNetwork() throws Exception {
    setRegistryDNS(new RegistryDNS("TestRegistry"));
    Configuration conf = createConfiguration();
    conf.set(RegistryConstants.KEY_DNS_DOMAIN, "dev.test");
    conf.set(KEY_DNS_ZONE_SUBNET, "172.17.0.0");
    conf.set(KEY_DNS_ZONE_MASK, "255.255.224.0");
    conf.setTimeDuration(RegistryConstants.KEY_DNS_TTL, 30L, TimeUnit.SECONDS);

    getRegistryDNS().setDomainName(conf);
    getRegistryDNS().initializeZones(conf);

    ServiceRecord record = getMarshal().fromBytes("somepath",
        CONTAINER_RECORD.getBytes());
    getRegistryDNS().register(
        "/registry/users/root/services/org-apache-slider/test1/components/"
            + "ctr-e50-1451931954322-0016-01-000002",
        record);

    // start assessing whether correct records are available
    Record[] recs = assertDNSQuery("19.0.17.172.in-addr.arpa.", Type.PTR, 1);
    assertEquals("wrong result",
        "comp-name.test1.root.dev.test.",
        ((PTRRecord) recs[0]).getTarget().toString());
  }

  @Test
  public void testMissingReverseLookup() throws Exception {
    ServiceRecord record = getMarshal().fromBytes("somepath",
        CONTAINER_RECORD.getBytes());
    getRegistryDNS().register(
        "/registry/users/root/services/org-apache-slider/test1/components/"
            + "ctr-e50-1451931954322-0016-01-000002",
        record);

    // start assessing whether correct records are available
    Name name = Name.fromString("19.1.17.172.in-addr.arpa.");
    Record question = Record.newRecord(name, Type.PTR, DClass.IN);
    Message query = Message.newQuery(question);
    OPTRecord optRecord = new OPTRecord(4096, 0, 0, Flags.DO, null);
    query.addRecord(optRecord, Section.ADDITIONAL);
    byte[] responseBytes = getRegistryDNS().generateReply(query, null);
    Message response = new Message(responseBytes);
    assertEquals("Missing record should be: ", Rcode.NXDOMAIN,
        response.getRcode());
  }

  @Test
  public void testNoContainerIP() throws Exception {
    ServiceRecord record = getMarshal().fromBytes("somepath",
        CONTAINER_RECORD_NO_IP.getBytes());
    getRegistryDNS().register(
        "/registry/users/root/services/org-apache-slider/test1/components/"
            + "ctr-e50-1451931954322-0016-01-000002",
        record);

    // start assessing whether correct records are available
    Name name =
        Name.fromString("ctr-e50-1451931954322-0016-01-000002.dev.test.");
    Record question = Record.newRecord(name, Type.A, DClass.IN);
    Message query = Message.newQuery(question);

    byte[] responseBytes = getRegistryDNS().generateReply(query, null);
    Message response = new Message(responseBytes);
    assertEquals("wrong status", Rcode.NXDOMAIN, response.getRcode());
  }

  private Record[] assertDNSQuery(String lookup) throws IOException {
    return assertDNSQuery(lookup, Type.A, 1);
  }

  private Record[] assertDNSQuery(String lookup, int numRecs)
      throws IOException {
    return assertDNSQuery(lookup, Type.A, numRecs);
  }

  Record[] assertDNSQuery(String lookup, int type, int numRecs)
      throws IOException {
    Name name = Name.fromString(lookup);
    Record question = Record.newRecord(name, type, DClass.IN);
    Message query = Message.newQuery(question);
    OPTRecord optRecord = new OPTRecord(4096, 0, 0, Flags.DO, null);
    query.addRecord(optRecord, Section.ADDITIONAL);
    byte[] responseBytes = getRegistryDNS().generateReply(query, null);
    Message response = new Message(responseBytes);
    assertEquals("not successful", Rcode.NOERROR, response.getRcode());
    assertNotNull("Null response", response);
    assertEquals("Questions do not match", query.getQuestion(),
        response.getQuestion());
    Record[] recs = response.getSectionArray(Section.ANSWER);
    assertEquals("wrong number of answer records",
        isSecure() ? numRecs * 2 : numRecs, recs.length);
    if (isSecure()) {
      boolean signed = false;
      for (Record record : recs) {
        signed = record.getType() == Type.RRSIG;
        if (signed) {
          break;
        }
      }
      assertTrue("No signatures found", signed);
    }
    return recs;
  }

  Record[] assertDNSQueryNotNull(String lookup, int type)
      throws IOException {
    Name name = Name.fromString(lookup);
    Record question = Record.newRecord(name, type, DClass.IN);
    Message query = Message.newQuery(question);
    OPTRecord optRecord = new OPTRecord(4096, 0, 0, Flags.DO, null);
    query.addRecord(optRecord, Section.ADDITIONAL);
    byte[] responseBytes = getRegistryDNS().generateReply(query, null);
    Message response = new Message(responseBytes);
    assertEquals("not successful", Rcode.NOERROR, response.getRcode());
    assertNotNull("Null response", response);
    assertEquals("Questions do not match", query.getQuestion(),
        response.getQuestion());
    Record[] recs = response.getSectionArray(Section.ANSWER);
    boolean found = false;
    for (Record r : recs) {
      if (r.getType()==Type.A) {
        found = true;
      }
    }
    assertTrue("No A records in answer", found);
    return recs;
  }

  @Test
  public void testDNSKEYRecord() throws Exception {
    String publicK =
        "AwEAAe1Jev0Az1khlQCvf0nud1/CNHQwwPEu8BNchZthdDxKPVn29yrD "
            + "CHoAWjwiGsOSw3SzIPrawSbHzyJsjn0oLBhGrH6QedFGnydoxjNsw3m/ "
            + "SCmOjR/a7LGBAMDFKqFioi4gOyuN66svBeY+/5uw72+0ei9AQ20gqf6q "
            + "l9Ozs5bV";
    //    byte[] publicBytes = Base64.decodeBase64(publicK);
    //    X509EncodedKeySpec keySpec = new X509EncodedKeySpec(publicBytes);
    //    KeyFactory keyFactory = KeyFactory.getInstance("RSA");
    //    PublicKey pubKey = keyFactory.generatePublic(keySpec);
    DNSKEYRecord dnskeyRecord =
        new DNSKEYRecord(Name.fromString("dev.test."), DClass.IN, 0,
            DNSKEYRecord.Flags.ZONE_KEY,
            DNSKEYRecord.Protocol.DNSSEC,
            DNSSEC.Algorithm.RSASHA256,
            Base64.decodeBase64(publicK.getBytes()));
    assertNotNull(dnskeyRecord);
    RSAPrivateKeySpec privateSpec = new RSAPrivateKeySpec(new BigInteger(1,
        Base64.decodeBase64(
            "7Ul6/QDPWSGVAK9/Se53X8I0dDDA8S7wE1yFm2F0PEo9Wfb3KsMIegBaPCIaw5LDd"
                + "LMg+trBJsfPImyOfSgsGEasfpB50UafJ2jGM2zDeb9IKY6NH9rssYEAwMUq"
                + "oWKiLiA7K43rqy8F5j7/m7Dvb7R6L0BDbSCp/qqX07OzltU=")),
        new BigInteger(1, Base64.decodeBase64(
            "MgbQ6DBYhskeufNGGdct0cGG/4wb0X183ggenwCv2dopDyOTPq+5xMb4Pz9Ndzgk/"
                + "yCY7mpaWIu9rttGOzrR+LBRR30VobPpMK1bMnzu2C0x08oYAguVwZB79DLC"
                + "705qmZpiaaFB+LnhG7VtpPiOBm3UzZxdrBfeq/qaKrXid60=")));
    KeyFactory factory = KeyFactory.getInstance("RSA");
    PrivateKey priv = factory.generatePrivate(privateSpec);

    ARecord aRecord = new ARecord(Name.fromString("some.test."), DClass.IN, 0,
        InetAddress.getByName("192.168.0.1"));
    Calendar cal = Calendar.getInstance();
    Date inception = cal.getTime();
    cal.add(Calendar.YEAR, 1);
    Date expiration = cal.getTime();
    RRset rrset = new RRset(aRecord);
    RRSIGRecord rrsigRecord = DNSSEC.sign(rrset,
        dnskeyRecord,
        priv,
        inception,
        expiration);
    DNSSEC.verify(rrset, rrsigRecord, dnskeyRecord);

  }

  @Test
  public void testIpv4toIpv6() throws Exception {
    InetAddress address =
        BaseServiceRecordProcessor
            .getIpv6Address(InetAddress.getByName("172.17.0.19"));
    assertTrue("not an ipv6 address", address instanceof Inet6Address);
    assertEquals("wrong IP", "172.17.0.19",
        InetAddress.getByAddress(address.getAddress()).getHostAddress());
  }

  @Test
  public void testAAAALookup() throws Exception {
    ServiceRecord record = getMarshal().fromBytes("somepath",
        CONTAINER_RECORD.getBytes());
    getRegistryDNS().register(
        "/registry/users/root/services/org-apache-slider/test1/components/"
            + "ctr-e50-1451931954322-0016-01-000002",
        record);

    // start assessing whether correct records are available
    Record[] recs = assertDNSQuery(
        "ctr-e50-1451931954322-0016-01-000002.dev.test.", Type.AAAA, 1);
    assertEquals("wrong result", "172.17.0.19",
        ((AAAARecord) recs[0]).getAddress().getHostAddress());

    recs = assertDNSQuery("comp-name.test1.root.dev.test.", Type.AAAA, 1);
    assertTrue("not an ARecord", recs[0] instanceof AAAARecord);
  }

  @Test
  public void testNegativeLookup() throws Exception {
    ServiceRecord record = getMarshal().fromBytes("somepath",
        CONTAINER_RECORD.getBytes());
    getRegistryDNS().register(
        "/registry/users/root/services/org-apache-slider/test1/components/"
            + "ctr-e50-1451931954322-0016-01-000002",
        record);

    // start assessing whether correct records are available
    Name name = Name.fromString("missing.dev.test.");
    Record question = Record.newRecord(name, Type.A, DClass.IN);
    Message query = Message.newQuery(question);

    byte[] responseBytes = getRegistryDNS().generateReply(query, null);
    Message response = new Message(responseBytes);
    assertEquals("not successful", Rcode.NXDOMAIN, response.getRcode());
    assertNotNull("Null response", response);
    assertEquals("Questions do not match", query.getQuestion(),
        response.getQuestion());
    Record[] sectionArray = response.getSectionArray(Section.AUTHORITY);
    assertEquals("Wrong number of recs in AUTHORITY", isSecure() ? 2 : 1,
        sectionArray.length);
    boolean soaFound = false;
    for (Record rec : sectionArray) {
      soaFound = rec.getType() == Type.SOA;
      if (soaFound) {
        break;
      }
    }
    assertTrue("wrong record type",
        soaFound);

  }

  @Test
  public void testReadMasterFile() throws Exception {
    setRegistryDNS(new RegistryDNS("TestRegistry"));
    Configuration conf = new Configuration();
    conf.set(RegistryConstants.KEY_DNS_DOMAIN, "dev.test");
    conf.set(RegistryConstants.KEY_DNS_ZONE_SUBNET, "172.17.0");
    conf.setTimeDuration(RegistryConstants.KEY_DNS_TTL, 30L, TimeUnit.SECONDS);
    conf.set(RegistryConstants.KEY_DNS_ZONES_DIR,
        getClass().getResource("/").getFile());
    if (isSecure()) {
      conf.setBoolean(RegistryConstants.KEY_DNSSEC_ENABLED, true);
      conf.set(RegistryConstants.KEY_DNSSEC_PUBLIC_KEY,
          "AwEAAe1Jev0Az1khlQCvf0nud1/CNHQwwPEu8BNchZthdDxKPVn29yrD "
              + "CHoAWjwiGsOSw3SzIPrawSbHzyJsjn0oLBhGrH6QedFGnydoxjNsw3m/ "
              + "SCmOjR/a7LGBAMDFKqFioi4gOyuN66svBeY+/5uw72+0ei9AQ20gqf6q "
              + "l9Ozs5bV");
      conf.set(RegistryConstants.KEY_DNSSEC_PRIVATE_KEY_FILE,
          getClass().getResource("/test.private").getFile());
    }

    getRegistryDNS().setDomainName(conf);
    getRegistryDNS().initializeZones(conf);

    ServiceRecord record = getMarshal().fromBytes("somepath",
        CONTAINER_RECORD.getBytes());
    getRegistryDNS().register(
        "/registry/users/root/services/org-apache-slider/test1/components/"
            + "ctr-e50-1451931954322-0016-01-000002",
        record);

    // start assessing whether correct records are available
    Record[] recs =
        assertDNSQuery("ctr-e50-1451931954322-0016-01-000002.dev.test.");
    assertEquals("wrong result", "172.17.0.19",
        ((ARecord) recs[0]).getAddress().getHostAddress());

    recs = assertDNSQuery("comp-name.test1.root.dev.test.", 1);
    assertTrue("not an ARecord", recs[0] instanceof ARecord);

    // lookup dyanmic reverse records
    recs = assertDNSQuery("19.0.17.172.in-addr.arpa.", Type.PTR, 1);
    assertEquals("wrong result",
        "comp-name.test1.root.dev.test.",
        ((PTRRecord) recs[0]).getTarget().toString());

    // now lookup static reverse records
    Name name = Name.fromString("5.0.17.172.in-addr.arpa.");
    Record question = Record.newRecord(name, Type.PTR, DClass.IN);
    Message query = Message.newQuery(question);
    OPTRecord optRecord = new OPTRecord(4096, 0, 0, Flags.DO, null);
    query.addRecord(optRecord, Section.ADDITIONAL);
    byte[] responseBytes = getRegistryDNS().generateReply(query, null);
    Message response = new Message(responseBytes);
    recs = response.getSectionArray(Section.ANSWER);
    assertEquals("wrong result", "cn005.dev.test.",
        ((PTRRecord) recs[0]).getTarget().toString());
  }

  @Test
  public void testReverseZoneNames() throws Exception {
    Configuration conf = new Configuration();
    conf.set(KEY_DNS_ZONE_SUBNET, "172.26.32.0");
    conf.set(KEY_DNS_ZONE_MASK, "255.255.224.0");

    Name name = getRegistryDNS().getReverseZoneName(conf);
    assertEquals("wrong name", "26.172.in-addr.arpa.", name.toString());
  }

  @Test
  public void testSplitReverseZoneNames() throws Exception {
    Configuration conf = new Configuration();
    registryDNS = new RegistryDNS("TestRegistry");
    conf.set(RegistryConstants.KEY_DNS_DOMAIN, "example.com");
    conf.set(KEY_DNS_SPLIT_REVERSE_ZONE, "true");
    conf.set(KEY_DNS_SPLIT_REVERSE_ZONE_RANGE, "256");
    conf.set(KEY_DNS_ZONE_SUBNET, "172.26.32.0");
    conf.set(KEY_DNS_ZONE_MASK, "255.255.224.0");
    conf.setTimeDuration(RegistryConstants.KEY_DNS_TTL, 30L, TimeUnit.SECONDS);
    conf.set(RegistryConstants.KEY_DNS_ZONES_DIR,
        getClass().getResource("/").getFile());
    if (isSecure()) {
      conf.setBoolean(RegistryConstants.KEY_DNSSEC_ENABLED, true);
      conf.set(RegistryConstants.KEY_DNSSEC_PUBLIC_KEY,
          "AwEAAe1Jev0Az1khlQCvf0nud1/CNHQwwPEu8BNchZthdDxKPVn29yrD "
              + "CHoAWjwiGsOSw3SzIPrawSbHzyJsjn0oLBhGrH6QedFGnydoxjNsw3m/ "
              + "SCmOjR/a7LGBAMDFKqFioi4gOyuN66svBeY+/5uw72+0ei9AQ20gqf6q "
              + "l9Ozs5bV");
      conf.set(RegistryConstants.KEY_DNSSEC_PRIVATE_KEY_FILE,
          getClass().getResource("/test.private").getFile());
    }
    registryDNS.setDomainName(conf);
    registryDNS.setDNSSECEnabled(conf);
    registryDNS.addSplitReverseZones(conf, 4);
    assertEquals(4, registryDNS.getZoneCount());
  }

  @Test
  public void testExampleDotCom() throws Exception {
    Name name = Name.fromString("example.com.");
    Record[] records = getRegistryDNS().getRecords(name, Type.SOA);
    assertNotNull("example.com exists:", records);
  }

  @Test
  public void testExternalCNAMERecord() throws Exception {
    setRegistryDNS(new RegistryDNS("TestRegistry"));
    Configuration conf = new Configuration();
    conf.set(RegistryConstants.KEY_DNS_DOMAIN, "dev.test");
    conf.set(RegistryConstants.KEY_DNS_ZONE_SUBNET, "172.17.0");
    conf.setTimeDuration(RegistryConstants.KEY_DNS_TTL, 30L, TimeUnit.SECONDS);
    conf.set(RegistryConstants.KEY_DNS_ZONES_DIR,
        getClass().getResource("/").getFile());
    getRegistryDNS().setDomainName(conf);
    getRegistryDNS().initializeZones(conf);

    // start assessing whether correct records are available
    Record[] recs =
        assertDNSQueryNotNull("mail.yahoo.com.", Type.CNAME);
  }

  public RegistryDNS getRegistryDNS() {
    return registryDNS;
  }

  public void setRegistryDNS(
      RegistryDNS registryDNS) {
    this.registryDNS = registryDNS;
  }

  public RegistryUtils.ServiceRecordMarshal getMarshal() {
    return marshal;
  }

  public void setMarshal(
      RegistryUtils.ServiceRecordMarshal marshal) {
    this.marshal = marshal;
  }
}
