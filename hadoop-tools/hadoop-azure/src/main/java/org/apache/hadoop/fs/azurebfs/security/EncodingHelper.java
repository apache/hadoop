package org.apache.hadoop.fs.azurebfs.security;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class EncodingHelper {

    public static byte[] getSHA256Hash(byte[] key) {
        try {
            final MessageDigest digester = MessageDigest.getInstance("SHA-256");
            return digester.digest(key);
        } catch (NoSuchAlgorithmException ignored) {
            /**
             * This exception can be ignored. Reason being SHA-256 is a valid algorithm, and it is constant for all
             * method calls.
             */
            return null;
        }
    }

    public static String getBase64EncodedString(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }
}
