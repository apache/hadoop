/*
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

package org.apache.hadoop.fs.s3a.auth.delegation.providers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.s3a.auth.delegation.AbstractS3ATokenIdentifier;
import org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecrets;
import org.apache.hadoop.io.Text;

import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.INJECTING_TOKEN_KIND;

/**
 * A token identifier for the injecting binding.
 * This contains no credentials and cannot authenticate.
 */
public final class InjectingTokenIdentifier extends
    AbstractS3ATokenIdentifier {

  private long issueNumber;

  /**
   * Constructor for service loader use.
   * Created with the kind {@link DelegationConstants#INJECTING_TOKEN_KIND}.
   */
  public InjectingTokenIdentifier() {
    super(INJECTING_TOKEN_KIND);
  }

  /**
   * Constructor.
   * @param uri filesystem URI.
   * @param owner token owner.
   * @param renewer token renewer.
   * @param encryptionSecrets encryption secrets
   * @param issueNumber issue number
   */
  public InjectingTokenIdentifier(
      final URI uri,
      final Text owner,
      final Text renewer,
      final EncryptionSecrets encryptionSecrets,
      final long issueNumber) {
    super(INJECTING_TOKEN_KIND, uri, owner, renewer,
        createDefaultOriginMessage(), encryptionSecrets);
    this.issueNumber = issueNumber;
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    super.write(out);
    out.writeLong(issueNumber);
  }

  @Override
  public void readFields(final DataInput in)
      throws IOException {
    super.readFields(in);
    issueNumber = in.readLong();
  }

  public long getIssueNumber() {
    return issueNumber;
  }

  public void setIssueNumber(final long issueNumber) {
    this.issueNumber = issueNumber;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "InjectingTokenIdentifier{");
    sb.append("counter=").append(issueNumber);
    sb.append('}');
    return sb.toString();
  }
}
