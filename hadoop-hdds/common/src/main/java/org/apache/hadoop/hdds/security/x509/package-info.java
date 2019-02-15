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


/**
 * This package contains common routines used in creating an x509 based identity
 * framework for HDDS.
 */
package org.apache.hadoop.hdds.security.x509;
/*

Architecture of Certificate Infrastructure for SCM.
====================================================

The certificate infrastructure has two main parts, the certificate server or
the Certificate authority and the clients who want certificates. The CA is
responsible for issuing certificates to participating entities.

To issue a certificate the CA has to verify the identity and the assertions
in the certificate. The client starts off making a request to CA for a
certificate.  This request is called Certificate Signing Request or CSR
(PKCS#10).

When a CSR arrives on the CA, CA will decode the CSR and verify that all the
fields in the CSR are in line with what the system expects. Since there are
lots of possible ways to construct an X.509 certificate, we rely on PKI
profiles.

Generally, PKI profiles are policy documents or general guidelines that get
followed by the requester and CA. However, most of the PKI profiles that are
commonly available are general purpose and offers too much surface area.

SCM CA infrastructure supports the notion of a PKI profile class which can
codify the RDNs, Extensions and other certificate policies. The CA when
issuing a certificate will invoke a certificate approver class, based on the
authentication method used. For example, out of the box, we support manual,
Kerberos, trusted network and testing authentication mechanisms.

If there is no authentication mechanism in place, then when CA receives the
CSR, it runs the standard PKI profile over it verify that all the fields are
in expected ranges. Once that is done, The signing request is sent for human
review and approval. This form of certificate approval is called Manual,  Of
all the certificate approval process this is the ** most secure **. This
approval needs to be done once for each data node.

For existing clusters, where data nodes already have a Kerberos keytab,  we
can leverage the Kerberos identity mechanism to identify the data node that
is requesting the certificate. In this case, users can configure the system
to leverage Kerberos while issuing certificates and SCM CA will be able to
verify the data nodes identity and issue certificates automatically.

In environments like Kubernetes, we can leverage the base system services to
pass on a shared secret securely. In this model also, we can rely on these
secrets to make sure that is the right data node that is talking to us. This
kind of approval is called a Trusted network approval. In this process, each
data node not only sends the CSR but signs the request with a shared secret
with SCM. SCM then can issue a certificate without the intervention of a
human administrator.

The last, TESTING method which never should be used other than in development
 and testing clusters, is merely a mechanism to bypass all identity checks. If
this flag is setup, then CA will issue a CSR if the base approves all fields.

 * Please do not use this mechanism(TESTING) for any purpose other than
 * testing.

CA - Certificate Approval and Code Layout (as of Dec, 1st, 2018)
=================================================================
The CA implementation ( as of now it is called DefaultCA) receives a CSR from
 the network layer. The network also tells the system what approver type to
 use, that is if Kerberos or Shared secrets mechanism is used, it reports
 that to Default CA.

The default CA instantiates the approver based on the type of the approver
indicated by the network layer. This approver creates an instance of the PKI
profile and passes each field from the certificate signing request. The PKI
profile (as of today Dec 1st, 2018, we have one profile called Ozone profile)
 verifies that each field in the CSR meets the approved set of values.

Once the PKI Profile validates the request, it is either auto approved or
queued for manual review.

 */