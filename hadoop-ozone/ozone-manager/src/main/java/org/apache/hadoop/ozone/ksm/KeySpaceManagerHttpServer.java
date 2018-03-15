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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.ksm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.web.OzoneHttpServer;

import java.io.IOException;

/**
 * HttpServer wrapper for the KeySpaceManager.
 */
public class KeySpaceManagerHttpServer extends OzoneHttpServer {

  public KeySpaceManagerHttpServer(Configuration conf, KeySpaceManager ksm)
      throws IOException {
    super(conf, "ksm");
    addServlet("serviceList", "/serviceList", ServiceListJSONServlet.class);
    getWebAppContext().setAttribute(OzoneConsts.KSM_CONTEXT_ATTRIBUTE, ksm);
  }

  @Override protected String getHttpAddressKey() {
    return KSMConfigKeys.OZONE_KSM_HTTP_ADDRESS_KEY;
  }

  @Override protected String getHttpBindHostKey() {
    return KSMConfigKeys.OZONE_KSM_HTTP_BIND_HOST_KEY;
  }

  @Override protected String getHttpsAddressKey() {
    return KSMConfigKeys.OZONE_KSM_HTTPS_ADDRESS_KEY;
  }

  @Override protected String getHttpsBindHostKey() {
    return KSMConfigKeys.OZONE_KSM_HTTPS_BIND_HOST_KEY;
  }

  @Override protected String getBindHostDefault() {
    return KSMConfigKeys.OZONE_KSM_HTTP_BIND_HOST_DEFAULT;
  }

  @Override protected int getHttpBindPortDefault() {
    return KSMConfigKeys.OZONE_KSM_HTTP_BIND_PORT_DEFAULT;
  }

  @Override protected int getHttpsBindPortDefault() {
    return KSMConfigKeys.OZONE_KSM_HTTPS_BIND_PORT_DEFAULT;
  }

  @Override protected String getKeytabFile() {
    return KSMConfigKeys.OZONE_KSM_KEYTAB_FILE;
  }

  @Override protected String getSpnegoPrincipal() {
    return OzoneConfigKeys.OZONE_SCM_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL;
  }

  @Override protected String getEnabledKey() {
    return KSMConfigKeys.OZONE_KSM_HTTP_ENABLED_KEY;
  }
}
