/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.freon;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.server.BaseHttpServer;
import org.apache.hadoop.ozone.OzoneConfigKeys;

/**
 * Http server to provide metrics + profile endpoint.
 */
public class FreonHttpServer extends BaseHttpServer {
  public FreonHttpServer(Configuration conf) throws IOException {
    super(conf, "freon");
  }


  @Override protected String getHttpAddressKey() {
    return OzoneConfigKeys.OZONE_FREON_HTTP_ADDRESS_KEY;
  }

  @Override protected String getHttpBindHostKey() {
    return OzoneConfigKeys.OZONE_FREON_HTTP_BIND_HOST_KEY;
  }

  @Override protected String getHttpsAddressKey() {
    return OzoneConfigKeys.OZONE_FREON_HTTPS_ADDRESS_KEY;
  }

  @Override protected String getHttpsBindHostKey() {
    return OzoneConfigKeys.OZONE_FREON_HTTPS_BIND_HOST_KEY;
  }

  @Override protected String getBindHostDefault() {
    return OzoneConfigKeys.OZONE_FREON_HTTP_BIND_HOST_DEFAULT;
  }

  @Override protected int getHttpBindPortDefault() {
    return OzoneConfigKeys.OZONE_FREON_HTTP_BIND_PORT_DEFAULT;
  }

  @Override protected int getHttpsBindPortDefault() {
    return OzoneConfigKeys.OZONE_FREON_HTTPS_BIND_PORT_DEFAULT;
  }

  @Override protected String getKeytabFile() {
    return OzoneConfigKeys.OZONE_FREON_HTTP_KERBEROS_KEYTAB_FILE_KEY;
  }

  @Override protected String getSpnegoPrincipal() {
    return OzoneConfigKeys.OZONE_FREON_HTTP_KERBEROS_PRINCIPAL_KEY;
  }

  @Override protected String getEnabledKey() {
    return OzoneConfigKeys.OZONE_FREON_HTTP_ENABLED_KEY;
  }
}
