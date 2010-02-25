/*
 * Copyright 2009 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.stargate.model;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.apache.hadoop.hbase.util.Base64;

import junit.framework.TestCase;

public class TestVersionModel extends TestCase {
  private static final String STARGATE_VERSION = "0.0.1";
  private static final String OS_VERSION = 
    "Linux 2.6.18-128.1.6.el5.centos.plusxen amd64";
  private static final String JVM_VERSION =
    "Sun Microsystems Inc. 1.6.0_13-11.3-b02";
  private static final String JETTY_VERSION = "6.1.14";
  private static final String JERSEY_VERSION = "1.1.0-ea";
  
  private static final String AS_XML =
    "<Version Stargate=\"" + STARGATE_VERSION + "\"" +
    " OS=\"" + OS_VERSION + "\"" +
    " JVM=\"" + JVM_VERSION + "\"" +
    " Server=\"" + JETTY_VERSION + "\"" +
    " Jersey=\"" + JERSEY_VERSION + "\"/>";

  private static final String AS_PB = 
    "CgUwLjAuMRInU3VuIE1pY3Jvc3lzdGVtcyBJbmMuIDEuNi4wXzEzLTExLjMtYjAyGi1MaW51eCAy" +
    "LjYuMTgtMTI4LjEuNi5lbDUuY2VudG9zLnBsdXN4ZW4gYW1kNjQiBjYuMS4xNCoIMS4xLjAtZWE=";

  private JAXBContext context;

  public TestVersionModel() throws JAXBException {
    super();
    context = JAXBContext.newInstance(VersionModel.class);
  }

  private VersionModel buildTestModel() {
    VersionModel model = new VersionModel();
    model.setStargateVersion(STARGATE_VERSION);
    model.setOsVersion(OS_VERSION);
    model.setJvmVersion(JVM_VERSION);
    model.setServerVersion(JETTY_VERSION);
    model.setJerseyVersion(JERSEY_VERSION);
    return model;
  }

  @SuppressWarnings("unused")
  private String toXML(VersionModel model) throws JAXBException {
    StringWriter writer = new StringWriter();
    context.createMarshaller().marshal(model, writer);
    return writer.toString();
  }

  private VersionModel fromXML(String xml) throws JAXBException {
    return (VersionModel)
      context.createUnmarshaller().unmarshal(new StringReader(xml));
  }

  @SuppressWarnings("unused")
  private byte[] toPB(VersionModel model) {
    return model.createProtobufOutput();
  }

  private VersionModel fromPB(String pb) throws IOException {
    return (VersionModel) 
      new VersionModel().getObjectFromMessage(Base64.decode(AS_PB));
  }

  private void checkModel(VersionModel model) {
    assertEquals(model.getStargateVersion(), STARGATE_VERSION);
    assertEquals(model.getOsVersion(), OS_VERSION);
    assertEquals(model.getJvmVersion(), JVM_VERSION);
    assertEquals(model.getServerVersion(), JETTY_VERSION);
    assertEquals(model.getJerseyVersion(), JERSEY_VERSION);
  }

  public void testBuildModel() throws Exception {
    checkModel(buildTestModel());
  }

  public void testFromXML() throws Exception {
    checkModel(fromXML(AS_XML));
  }

  public void testFromPB() throws Exception {
    checkModel(fromPB(AS_PB));
  }
}
