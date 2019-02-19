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

package org.apache.hadoop.hdds.scm.metadata;

import java.io.IOException;
import java.nio.charset.Charset;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.utils.db.Codec;

/**
 * Encodes and Decodes X509Certificate Class.
 */
public class X509CertificateCodec implements Codec<X509Certificate> {
  @Override
  public byte[] toPersistedFormat(X509Certificate object) throws IOException {
    try {
      return CertificateCodec.getPEMEncodedString(object)
          .getBytes(Charset.forName("UTF-8"));
    } catch (SCMSecurityException exp) {
      throw new IOException(exp);
    }
  }

  @Override
  public X509Certificate fromPersistedFormat(byte[] rawData)
      throws IOException {
    try{
      String s = new String(rawData, Charset.forName("UTF-8"));
      return CertificateCodec.getX509Certificate(s);
    } catch (CertificateException exp) {
      throw new IOException(exp);
    }
  }
}
