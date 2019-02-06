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

package org.apache.hadoop.hdds.security.x509.certificate.authority.PKIProfiles;

import org.bouncycastle.asn1.x509.Extension;

import java.util.function.BiFunction;

import static java.lang.Boolean.TRUE;

/**
 * CA Profile, this is needed when SCM does HA.
 * A place holder class indicating what we need to do when we support issuing
 * CA certificates to other SCMs in HA mode.
 */
public class DefaultCAProfile extends DefaultProfile {
  static final BiFunction<Extension, PKIProfile, Boolean>
      VALIDATE_BASIC_CONSTRAINTS = (e, b) -> TRUE;
  static final BiFunction<Extension, PKIProfile, Boolean>
      VALIDATE_CRL_NUMBER = (e, b) -> TRUE;
  static final BiFunction<Extension, PKIProfile, Boolean>
      VALIDATE_REASON_CODE = (e, b) -> TRUE;
  static final BiFunction<Extension, PKIProfile, Boolean>
      VALIDATE_DELTA_CRL_INDICATOR = (e, b) -> TRUE;
  static final BiFunction<Extension, PKIProfile, Boolean>
      VALIDATE_NAME_CONSTRAINTS = (e, b) -> TRUE;
  static final BiFunction<Extension, PKIProfile, Boolean>
      VALIDATE_CRL_DISTRIBUTION_POINTS = (e, b) -> TRUE;
}
