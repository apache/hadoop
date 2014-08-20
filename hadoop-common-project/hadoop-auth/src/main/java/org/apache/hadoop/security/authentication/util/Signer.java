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
package org.apache.hadoop.security.authentication.util;

import org.apache.commons.codec.binary.Base64;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Signs strings and verifies signed strings using a SHA digest.
 */
public class Signer {
  private static final String SIGNATURE = "&s=";

  private SignerSecretProvider secretProvider;

  /**
   * Creates a Signer instance using the specified SignerSecretProvider.  The
   * SignerSecretProvider should already be initialized.
   *
   * @param secretProvider The SignerSecretProvider to use
   */
  public Signer(SignerSecretProvider secretProvider) {
    if (secretProvider == null) {
      throw new IllegalArgumentException("secretProvider cannot be NULL");
    }
    this.secretProvider = secretProvider;
  }

  /**
   * Returns a signed string.
   * <p/>
   * The signature '&s=SIGNATURE' is appended at the end of the string.
   *
   * @param str string to sign.
   *
   * @return the signed string.
   */
  public synchronized String sign(String str) {
    if (str == null || str.length() == 0) {
      throw new IllegalArgumentException("NULL or empty string to sign");
    }
    byte[] secret = secretProvider.getCurrentSecret();
    String signature = computeSignature(secret, str);
    return str + SIGNATURE + signature;
  }

  /**
   * Verifies a signed string and extracts the original string.
   *
   * @param signedStr the signed string to verify and extract.
   *
   * @return the extracted original string.
   *
   * @throws SignerException thrown if the given string is not a signed string or if the signature is invalid.
   */
  public String verifyAndExtract(String signedStr) throws SignerException {
    int index = signedStr.lastIndexOf(SIGNATURE);
    if (index == -1) {
      throw new SignerException("Invalid signed text: " + signedStr);
    }
    String originalSignature = signedStr.substring(index + SIGNATURE.length());
    String rawValue = signedStr.substring(0, index);
    checkSignatures(rawValue, originalSignature);
    return rawValue;
  }

  /**
   * Returns then signature of a string.
   *
   * @param secret The secret to use
   * @param str string to sign.
   *
   * @return the signature for the string.
   */
  protected String computeSignature(byte[] secret, String str) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA");
      md.update(str.getBytes());
      md.update(secret);
      byte[] digest = md.digest();
      return new Base64(0).encodeToString(digest);
    } catch (NoSuchAlgorithmException ex) {
      throw new RuntimeException("It should not happen, " + ex.getMessage(), ex);
    }
  }

  protected void checkSignatures(String rawValue, String originalSignature)
      throws SignerException {
    boolean isValid = false;
    byte[][] secrets = secretProvider.getAllSecrets();
    for (int i = 0; i < secrets.length; i++) {
      byte[] secret = secrets[i];
      if (secret != null) {
        String currentSignature = computeSignature(secret, rawValue);
        if (originalSignature.equals(currentSignature)) {
          isValid = true;
          break;
        }
      }
    }
    if (!isValid) {
      throw new SignerException("Invalid signature");
    }
  }
}
