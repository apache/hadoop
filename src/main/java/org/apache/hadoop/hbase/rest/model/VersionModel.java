/*
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.rest.model;

import java.io.IOException;
import java.io.Serializable;

import javax.servlet.ServletContext;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.hbase.rest.ProtobufMessageHandler;
import org.apache.hadoop.hbase.rest.RESTServlet;
import org.apache.hadoop.hbase.rest.protobuf.generated.VersionMessage.Version;

import com.sun.jersey.spi.container.servlet.ServletContainer;

/**
 * A representation of the collection of versions of the REST gateway software
 * components.
 * <ul>
 * <li>restVersion: REST gateway revision</li>
 * <li>jvmVersion: the JVM vendor and version information</li>
 * <li>osVersion: the OS type, version, and hardware architecture</li>
 * <li>serverVersion: the name and version of the servlet container</li>
 * <li>jerseyVersion: the version of the embedded Jersey framework</li>
 * </ul>
 */
@XmlRootElement(name="Version")
public class VersionModel implements Serializable, ProtobufMessageHandler {

	private static final long serialVersionUID = 1L;

	private String restVersion;
  private String jvmVersion;
  private String osVersion;
  private String serverVersion;
  private String jerseyVersion;

  /**
   * Default constructor. Do not use.
   */
  public VersionModel() {}
  
  /**
   * Constructor
   * @param context the servlet context
   */
	public VersionModel(ServletContext context) {
	  restVersion = RESTServlet.VERSION_STRING;
	  jvmVersion = System.getProperty("java.vm.vendor") + ' ' +
      System.getProperty("java.version") + '-' +
      System.getProperty("java.vm.version");
	  osVersion = System.getProperty("os.name") + ' ' +
      System.getProperty("os.version") + ' ' +
      System.getProperty("os.arch");
	  serverVersion = context.getServerInfo();
	  jerseyVersion = ServletContainer.class.getPackage()
      .getImplementationVersion();
	}

	/**
	 * @return the REST gateway version
	 */
	@XmlAttribute(name="REST")
	public String getRESTVersion() {
    return restVersion;
  }

	/**
	 * @return the JVM vendor and version
	 */
  @XmlAttribute(name="JVM")
  public String getJVMVersion() {
    return jvmVersion;
  }

  /**
   * @return the OS name, version, and hardware architecture
   */
  @XmlAttribute(name="OS")
  public String getOSVersion() {
    return osVersion;
  }

  /**
   * @return the servlet container version
   */
  @XmlAttribute(name="Server")
  public String getServerVersion() {
    return serverVersion;
  }

  /**
   * @return the version of the embedded Jersey framework
   */
  @XmlAttribute(name="Jersey")
  public String getJerseyVersion() {
    return jerseyVersion;
  }

  /**
   * @param version the REST gateway version string
   */
  public void setRESTVersion(String version) {
    this.restVersion = version;
  }

  /**
   * @param version the OS version string
   */
  public void setOSVersion(String version) {
    this.osVersion = version;
  }

  /**
   * @param version the JVM version string
   */
  public void setJVMVersion(String version) {
    this.jvmVersion = version;
  }

  /**
   * @param version the servlet container version string
   */
  public void setServerVersion(String version) {
    this.serverVersion = version;
  }

  /**
   * @param version the Jersey framework version string
   */
  public void setJerseyVersion(String version) {
    this.jerseyVersion = version;
  }

  /* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
	  StringBuilder sb = new StringBuilder();
	  sb.append("rest ");
	  sb.append(restVersion);
	  sb.append(" [JVM: ");
	  sb.append(jvmVersion);
	  sb.append("] [OS: ");
	  sb.append(osVersion);
	  sb.append("] [Server: ");
	  sb.append(serverVersion);
	  sb.append("] [Jersey: ");
    sb.append(jerseyVersion);
	  sb.append("]\n");
	  return sb.toString();
	}

	@Override
  public byte[] createProtobufOutput() {
	  Version.Builder builder = Version.newBuilder();
	  builder.setRestVersion(restVersion);
	  builder.setJvmVersion(jvmVersion);
	  builder.setOsVersion(osVersion);
	  builder.setServerVersion(serverVersion);
	  builder.setJerseyVersion(jerseyVersion);
	  return builder.build().toByteArray();
  }

  @Override
  public ProtobufMessageHandler getObjectFromMessage(byte[] message)
      throws IOException {
    Version.Builder builder = Version.newBuilder();
    builder.mergeFrom(message);
    if (builder.hasRestVersion()) {
      restVersion = builder.getRestVersion();
    }
    if (builder.hasJvmVersion()) {
      jvmVersion = builder.getJvmVersion();
    }
    if (builder.hasOsVersion()) {
      osVersion = builder.getOsVersion();
    }
    if (builder.hasServerVersion()) {
      serverVersion = builder.getServerVersion();
    }
    if (builder.hasJerseyVersion()) {
      jerseyVersion = builder.getJerseyVersion();
    }
    return this;
  }
}
