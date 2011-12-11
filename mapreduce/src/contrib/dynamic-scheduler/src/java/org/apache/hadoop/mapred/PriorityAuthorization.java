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
package org.apache.hadoop.mapred;

import java.security.SignatureException;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.net.URLDecoder;


import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.commons.codec.binary.Base64;


/**
 * This class implements symmetric key HMAC/SHA1 signature
 * based authorization of users and admins.
 */
public class PriorityAuthorization {
  public static final int USER = 0;
  public static final int ADMIN = 1;
  public static final int NO_ACCESS = 2;
  private HashMap<String,UserACL> acl = new HashMap<String,UserACL>();
  private long lastSuccessfulReload = 0;
  public static final long START_TIME = System.currentTimeMillis();
  private String aclFile;
  private static final Log LOG = LogFactory.getLog(PriorityAuthorization.class);
  private static final boolean debug = LOG.isDebugEnabled();
  private static final String HMAC_SHA1_ALGORITHM = "HmacSHA1";

  /**
   * Initializes authorization configuration
   * @param conf MapReduce configuration handle 
   */
  public void init(Configuration conf) {
    aclFile = conf.get("mapred.priority-scheduler.acl-file","/etc/hadoop.acl");
  }

  /**
   * Adapted from AWS Query Authentication cookbook:
   * Computes RFC 2104-compliant HMAC signature.
   *
   * @param data
   *     The data to be signed.
   * @param key
   *     The signing key.
   * @return
   *     The base64-encoded RFC 2104-compliant HMAC signature.
   * @throws
   *     java.security.SignatureException when signature generation fails
   */
  public static String hmac(String data, String key)
    throws java.security.SignatureException {
    String result;
    try {
      // get an hmac_sha1 key from the raw key bytes
      SecretKeySpec signingKey = new SecretKeySpec(key.getBytes(), 
          HMAC_SHA1_ALGORITHM);
           
      // get an hmac_sha1 Mac instance and initialize with the signing key
      Mac mac = Mac.getInstance(HMAC_SHA1_ALGORITHM);
      mac.init(signingKey);
            
      // compute the hmac on input data bytes
      byte[] rawHmac = mac.doFinal(data.getBytes());
            
      // base64-encode the hmac
      result = new String(Base64.encodeBase64(rawHmac));
    } 
    catch (Exception e) {
      throw new SignatureException("Failed to generate HMAC : " + e, e);
    }
    return result;
  }

  class UserACL {
    String user;
    String role;
    String key;
    // for replay detection
    long lastTimestamp = START_TIME;
    UserACL(String user, String role, String key) {
      this.user = user;
      this.role = role;
      this.key = key;
    }
  }

  private void reloadACL() {
    BufferedReader in = null;
    try {
      in = new BufferedReader(new FileReader(aclFile));
      String line = in.readLine();
      while (line != null) {
        String[] nameValue = line.split(" ");
        if (nameValue.length != 3) {
          continue;
        }
        acl.put(nameValue[0], new UserACL(nameValue[0], nameValue[1], nameValue[2]));
        if (debug) {
          LOG.debug("Loading " + line);
        }
        line = in.readLine();
      }
    } catch (Exception e) {
      LOG.error(e);
    }
    try {
      in.close();
    } catch (Exception e) {
      LOG.error(e);
    }
  }

  private void loadACL() {
    long time = System.currentTimeMillis();
    try {
      File file = new File(aclFile);
      long lastModified = file.lastModified();
      if (lastModified > lastSuccessfulReload) {
        reloadACL();
        lastSuccessfulReload = time;
      }
    } catch (Exception e) {
      LOG.error("Failed to reload acl file", e);
    }
  }

  private boolean isReplay(String timestamp, String signature, UserACL userACL) {
    long signatureTime = Long.parseLong(timestamp);
    if (debug) {
      LOG.debug("signaturetime: " + Long.toString(signatureTime));
      LOG.debug("lasttime: " + Long.toString(userACL.lastTimestamp));
    }
    if (signatureTime <= userACL.lastTimestamp) {
        return true;
    }
    userACL.lastTimestamp = signatureTime;   
    return false;
  }

  /**
   * Returns authorized role for user.
   * Checks whether signature obtained by user was made by key stored in local acl.
   * Also checks for replay attacks.
   * @param data data that was signed by user
   * @param signature user-provided signature
   * @param user-provided nonce/timestamp of signature 
   * @return the authorized role of the user:
   *   ADMIN, USER or NO_ACCESS
   */
  public int authorize(String data, String signature, String user, String timestamp) {
    try {
      signature = URLDecoder.decode(signature, "UTF-8");
    } catch (Exception e) {
      LOG.error("Authorization exception:",e);
      return NO_ACCESS;
    }
    if (debug) {
      LOG.debug(data + " sig: " + signature + " user: " + user + " time: " + timestamp);
    }
    try {
      loadACL();
      UserACL userACL = acl.get(user);  
      if (userACL == null) {
        return NO_ACCESS;
      }
      String signatureTest = hmac(data, userACL.key);
      if (debug) {
        LOG.debug("SignatureTest " + signatureTest);
        LOG.debug("Signature " + signature);
      }
      if (signatureTest.equals(signature) && !isReplay(timestamp, signature, userACL)) {
        return (userACL.role.equals("admin")) ? ADMIN : USER; 
      }
    } catch (Exception e) {
      LOG.error("Athorization exception:", e);
    }
    return NO_ACCESS;
  }
}
