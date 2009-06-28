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
import java.io.Serializable;

import javax.servlet.ServletContext;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.hbase.stargate.RESTServlet;
import org.apache.hadoop.hbase.stargate.protobuf.generated.VersionMessage.Version;

import com.sun.jersey.spi.container.servlet.ServletContainer;

@XmlRootElement(name="Version")
public class VersionModel implements Serializable, IProtobufWrapper {

	private static final long serialVersionUID = 1L;

	private String stargateVersion;
  private String jvmVersion;
  private String osVersion;
  private String serverVersion;
  private String jerseyVersion;

  public VersionModel() {}
  
	public VersionModel(ServletContext context) {
	  stargateVersion = RESTServlet.VERSION_STRING;
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

	@XmlAttribute(name="Stargate")
	public String getStargateVersion() {
    return stargateVersion;
  }

  @XmlAttribute(name="JVM")
  public String getJvmVersion() {
    return jvmVersion;
  }

  @XmlAttribute(name="OS")
  public String getOsVersion() {
    return osVersion;
  }

  @XmlAttribute(name="Server")
  public String getServerVersion() {
    return serverVersion;
  }

  @XmlAttribute(name="Jersey")
  public String getJerseyVersion() {
    return jerseyVersion;
  }

  public void setStargateVersion(String version) {
    this.stargateVersion = version;
  }

  public void setOsVersion(String version) {
    this.osVersion = version;
  }

  public void setJvmVersion(String version) {
    this.jvmVersion = version;
  }

  public void setServerVersion(String version) {
    this.serverVersion = version;
  }

  public void setJerseyVersion(String version) {
    this.jerseyVersion = version;
  }

  /* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
	  StringBuilder sb = new StringBuilder();
	  sb.append("Stargate ");
	  sb.append(stargateVersion);
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
	  builder.setStargateVersion(stargateVersion);
	  builder.setJvmVersion(jvmVersion);
	  builder.setOsVersion(osVersion);
	  builder.setServerVersion(serverVersion);
	  builder.setJerseyVersion(jerseyVersion);
	  return builder.build().toByteArray();
  }

  @Override
  public IProtobufWrapper getObjectFromMessage(byte[] message)
      throws IOException {
    Version.Builder builder = Version.newBuilder();
    builder.mergeFrom(message);
    if (builder.hasStargateVersion()) {
      stargateVersion = builder.getStargateVersion();
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
