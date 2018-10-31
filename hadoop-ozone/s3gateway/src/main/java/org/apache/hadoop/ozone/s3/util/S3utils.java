package org.apache.hadoop.ozone.s3.util;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;

import java.nio.charset.StandardCharsets;

/**
 * Utility class for S3.
 */
public final class S3utils {

  private S3utils() {

  }
  private static final String CONTINUE_TOKEN_SEPERATOR = "-";

  /**
   * Generate a continuation token which is used in get Bucket.
   * @param key
   * @return if key is not null return continuation token, else returns null.
   */
  public static String generateContinueToken(String key) {
    if (key != null) {
      byte[] byteData = key.getBytes(StandardCharsets.UTF_8);
      String hex = Hex.encodeHexString(byteData);
      String digest = DigestUtils.sha256Hex(key);
      return hex + CONTINUE_TOKEN_SEPERATOR + digest;
    } else {
      return null;
    }
  }

  /**
   * Decode a continuation token which is used in get Bucket.
   * @param key
   * @return if key is not null return decoded token, otherwise returns null.
   * @throws OS3Exception
   */
  public static String decodeContinueToken(String key) throws OS3Exception {
    if (key != null) {
      int indexSeparator = key.indexOf(CONTINUE_TOKEN_SEPERATOR);
      if (indexSeparator == -1) {
        throw S3ErrorTable.newError(S3ErrorTable.INVALID_ARGUMENT, key);
      }
      String hex = key.substring(0, indexSeparator);
      String digest = key.substring(indexSeparator + 1);
      try {
        byte[] actualKeyBytes = Hex.decodeHex(hex);
        String digestActualKey = DigestUtils.sha256Hex(actualKeyBytes);
        if (digest.equals(digestActualKey)) {
          return new String(actualKeyBytes);
        } else {
          OS3Exception ex = S3ErrorTable.newError(S3ErrorTable
              .INVALID_ARGUMENT, key);
          ex.setErrorMessage("The continuation token provided is incorrect");
          throw ex;
        }
      } catch (DecoderException ex) {
        OS3Exception os3Exception = S3ErrorTable.newError(S3ErrorTable
            .INVALID_ARGUMENT, key);
        os3Exception.setErrorMessage("The continuation token provided is " +
            "incorrect");
        throw os3Exception;
      }
    } else {
      return null;
    }
  }
}
