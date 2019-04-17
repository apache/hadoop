/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.security.x509.certificate.authority;

import java.io.IOException;
import java.math.BigInteger;
import java.security.cert.X509Certificate;

/**
 * This interface allows the DefaultCA to be portable and use different DB
 * interfaces later. It also allows us define this interface in the SCM layer
 * by which we don't have to take a circular dependency between hdds-common
 * and the SCM.
 *
 * With this interface, DefaultCA server read and write DB or persistence
 * layer and we can write to SCM's Metadata DB.
 */
public interface CertificateStore {

  /**
   * Writes a new certificate that was issued to the persistent store.
   * @param serialID - Certificate Serial Number.
   * @param certificate - Certificate to persist.
   * @throws IOException - on Failure.
   */
  void storeValidCertificate(BigInteger serialID,
                             X509Certificate certificate) throws IOException;

  /**
   * Moves a certificate in a transactional manner from valid certificate to
   * revoked certificate state.
   * @param serialID - Serial ID of the certificate.
   * @throws IOException
   */
  void revokeCertificate(BigInteger serialID) throws IOException;

  /**
   * Deletes an expired certificate from the store. Please note: We don't
   * remove revoked certificates, we need that information to generate the
   * CRLs.
   * @param serialID - Certificate ID.
   */
  void removeExpiredCertificate(BigInteger serialID) throws IOException;

  /**
   * Retrieves a Certificate based on the Serial number of that certificate.
   * @param serialID - ID of the certificate.
   * @param certType
   * @return X509Certificate
   * @throws IOException
   */
  X509Certificate getCertificateByID(BigInteger serialID, CertType certType)
      throws IOException;

  /**
   * Different kind of Certificate stores.
   */
  enum CertType {
    VALID_CERTS,
    REVOKED_CERTS
  }

}
