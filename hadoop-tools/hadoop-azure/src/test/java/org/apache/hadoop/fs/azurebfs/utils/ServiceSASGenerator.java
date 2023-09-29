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

package org.apache.hadoop.fs.azurebfs.utils;

import java.time.Instant;

import org.apache.hadoop.fs.azurebfs.services.AbfsUriQueryBuilder;

/**
 * Test Service SAS generator.
 */
public class ServiceSASGenerator extends SASGenerator {

  /**
   * Creates a SAS Generator for Service SAS
   * (https://docs.microsoft.com/en-us/rest/api/storageservices/create-service-sas).
   * @param accountKey - the storage account key
   */
  public ServiceSASGenerator(byte[] accountKey) {
    super(accountKey);
  }

  public String getContainerSASWithFullControl(String accountName, String containerName) {
    String sp = "rcwdl";
    String sv = AuthenticationVersion.Feb20.toString();
    String sr = "c";
    String st = ISO_8601_FORMATTER.format(Instant.now().minus(FIVE_MINUTES));
    String se = ISO_8601_FORMATTER.format(Instant.now().plus(ONE_DAY));

    String signature = computeSignatureForSAS(sp, st, se, sv, "c",
        accountName, containerName, null);

    AbfsUriQueryBuilder qb = new AbfsUriQueryBuilder();
    qb.addQuery("sp", sp);
    qb.addQuery("st", st);
    qb.addQuery("se", se);
    qb.addQuery("sv", sv);
    qb.addQuery("sr", sr);
    qb.addQuery("sig", signature);
    return qb.toString().substring(1);
  }

  private String computeSignatureForSAS(String sp, String st, String se, String sv,
      String sr, String accountName, String containerName, String path) {

    StringBuilder sb = new StringBuilder();
    sb.append(sp);
    sb.append("\n");
    sb.append(st);
    sb.append("\n");
    sb.append(se);
    sb.append("\n");
    // canonicalized resource
    sb.append("/blob/");
    sb.append(accountName);
    sb.append("/");
    sb.append(containerName);
    if (path != null && !sr.equals("c")) {
      //sb.append("/");
      sb.append(path);
    }
    sb.append("\n");
    sb.append("\n"); // si
    sb.append("\n"); // sip
    sb.append("\n"); // spr
    sb.append(sv);
    sb.append("\n");
    sb.append(sr);
    sb.append("\n");
    sb.append("\n"); // - For optional : rscc - ResponseCacheControl
    sb.append("\n"); // - For optional : rscd - ResponseContentDisposition
    sb.append("\n"); // - For optional : rsce - ResponseContentEncoding
    sb.append("\n"); // - For optional : rscl - ResponseContentLanguage
    sb.append("\n"); // - For optional : rsct - ResponseContentType

    String stringToSign = sb.toString();
    LOG.debug("Service SAS stringToSign: " + stringToSign.replace("\n", "."));
    return computeHmac256(stringToSign);
  }
}