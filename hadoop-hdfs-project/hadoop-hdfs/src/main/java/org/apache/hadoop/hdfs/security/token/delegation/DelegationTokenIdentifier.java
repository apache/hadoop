/**
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

package org.apache.hadoop.hdfs.security.token.delegation;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.web.SWebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;

/**
 * A delegation token identifier that is specific to HDFS.
 */
@InterfaceAudience.Private
public class DelegationTokenIdentifier 
    extends AbstractDelegationTokenIdentifier {
  public static final Text HDFS_DELEGATION_KIND = new Text("HDFS_DELEGATION_TOKEN");

  /**
   * Create an empty delegation token identifier for reading into.
   */
  public DelegationTokenIdentifier() {
  }

  /**
   * Create a new delegation token identifier
   * @param owner the effective username of the token owner
   * @param renewer the username of the renewer
   * @param realUser the real username of the token owner
   */
  public DelegationTokenIdentifier(Text owner, Text renewer, Text realUser) {
    super(owner, renewer, realUser);
  }

  @Override
  public Text getKind() {
    return HDFS_DELEGATION_KIND;
  }

  @Override
  public String toString() {
    return getKind() + " token " + getSequenceNumber()
        + " for " + getUser().getShortUserName();
  }

  /** @return a string representation of the token */
  public static String stringifyToken(final Token<?> token) throws IOException {
    DelegationTokenIdentifier ident = new DelegationTokenIdentifier();
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);  
    ident.readFields(in);

    if (token.getService().getLength() > 0) {
      return ident + " on " + token.getService();
    } else {
      return ident.toString();
    }
  }
  
  public static class WebHdfsDelegationTokenIdentifier
      extends DelegationTokenIdentifier {
    public WebHdfsDelegationTokenIdentifier() {
      super();
    }
    @Override
    public Text getKind() {
      return WebHdfsFileSystem.TOKEN_KIND;
    }
  }
  
  public static class SWebHdfsDelegationTokenIdentifier
      extends WebHdfsDelegationTokenIdentifier {
    public SWebHdfsDelegationTokenIdentifier() {
      super();
    }
    @Override
    public Text getKind() {
      return SWebHdfsFileSystem.TOKEN_KIND;
    }
  }
}
