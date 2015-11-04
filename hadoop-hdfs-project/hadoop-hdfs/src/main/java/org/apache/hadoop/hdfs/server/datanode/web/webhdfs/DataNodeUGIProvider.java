/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.hdfs.server.datanode.web.webhdfs;

import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * Create UGI from the request for the WebHDFS requests for the DNs. Note that
 * the DN does not authenticate the UGI -- the NN will authenticate them in
 * subsequent operations.
 */
class DataNodeUGIProvider {
  private final ParameterParser params;

  DataNodeUGIProvider(ParameterParser params) {
    this.params = params;
  }

  UserGroupInformation ugi() throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      return tokenUGI();
    }

    final String usernameFromQuery = params.userName();
    final String doAsUserFromQuery = params.doAsUser();
    final String remoteUser = usernameFromQuery == null
        ? JspHelper.getDefaultWebUserName(params.conf()) // not specified in
        // request
        : usernameFromQuery;

    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(remoteUser);
    JspHelper.checkUsername(ugi.getShortUserName(), usernameFromQuery);
    if (doAsUserFromQuery != null) {
      // create and attempt to authorize a proxy user
      ugi = UserGroupInformation.createProxyUser(doAsUserFromQuery, ugi);
    }
    return ugi;
  }

  private UserGroupInformation tokenUGI() throws IOException {
    Token<DelegationTokenIdentifier> token = params.delegationToken();
    ByteArrayInputStream buf =
      new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    DelegationTokenIdentifier id = new DelegationTokenIdentifier();
    id.readFields(in);
    UserGroupInformation ugi = id.getUser();
    ugi.addToken(token);
    return ugi;
  }

}
