/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.server.BaseHttpServer;

import java.io.IOException;

/**
 * HttpServer2 wrapper for the Ozone Storage Container Manager.
 */
public class StorageContainerManagerHttpServer extends BaseHttpServer {

  public StorageContainerManagerHttpServer(Configuration conf)
      throws IOException {
    super(conf, "scm");
  }

  @Override protected String getHttpAddressKey() {
    return ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY;
  }

  @Override protected String getHttpBindHostKey() {
    return ScmConfigKeys.OZONE_SCM_HTTP_BIND_HOST_KEY;
  }

  @Override protected String getHttpsAddressKey() {
    return ScmConfigKeys.OZONE_SCM_HTTPS_ADDRESS_KEY;
  }

  @Override protected String getHttpsBindHostKey() {
    return ScmConfigKeys.OZONE_SCM_HTTPS_BIND_HOST_KEY;
  }

  @Override protected String getBindHostDefault() {
    return ScmConfigKeys.OZONE_SCM_HTTP_BIND_HOST_DEFAULT;
  }

  @Override protected int getHttpBindPortDefault() {
    return ScmConfigKeys.OZONE_SCM_HTTP_BIND_PORT_DEFAULT;
  }

  @Override protected int getHttpsBindPortDefault() {
    return ScmConfigKeys.OZONE_SCM_HTTPS_BIND_PORT_DEFAULT;
  }

  @Override protected String getKeytabFile() {
    return ScmConfigKeys.HDDS_SCM_HTTP_KERBEROS_KEYTAB_FILE_KEY;
  }

  @Override protected String getSpnegoPrincipal() {
    return ScmConfigKeys.HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY;
  }

  @Override protected String getEnabledKey() {
    return ScmConfigKeys.OZONE_SCM_HTTP_ENABLED_KEY;
  }

}
