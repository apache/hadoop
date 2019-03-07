/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.security.x509.certificate.authority;

import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.PKIProfiles.PKIProfile;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;

import java.io.IOException;
import java.security.PrivateKey;
import java.util.Date;
import java.util.concurrent.CompletableFuture;

/**
 * A test approver class that makes testing easier.
 */
public class MockApprover extends BaseApprover {

  public MockApprover(PKIProfile pkiProfile, SecurityConfig config) {
    super(pkiProfile, config);
  }

  @Override
  public CompletableFuture<X509CertificateHolder>
      inspectCSR(PKCS10CertificationRequest csr) {
    return super.inspectCSR(csr);
  }

  @Override
  public X509CertificateHolder sign(SecurityConfig config, PrivateKey caPrivate,
      X509CertificateHolder caCertificate,
      Date validFrom, Date validTill,
      PKCS10CertificationRequest request,
      String scmId, String clusterId)
      throws IOException, OperatorCreationException {
    return null;
  }

}
