package org.apache.hadoop.fs.azurebfs.security;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.util.Arrays;

public class ABFSKey implements SecretKey {
    private byte[] bytes;
    private String base64Encoding;
    private byte[] sha256Hash;
    public ABFSKey(byte[] bytes) {
        if(bytes != null) {
            this.bytes = bytes.clone();
            base64Encoding = EncodingHelper.getBase64EncodedString(this.bytes);
            sha256Hash = EncodingHelper.getSHA256Hash(this.bytes);
        }
    }

    @Override
    public String getAlgorithm() {
        return null;
    }

    @Override
    public String getFormat() {
        return null;
    }

    /**
     * This method to be called by implementations of EncryptionContextProvider interface.
     * Method returns clone of the original bytes array to prevent findbugs flags.
     * */
    @Override
    public byte[] getEncoded() {
        if(bytes == null) {
            return null;
        }
        return bytes.clone();
    }

    public String getBase64EncodedString() {
        return base64Encoding;
    }

    public byte[] getSHA256Hash() {
        return sha256Hash.clone();
    }

    @Override
    public void destroy() {
        Arrays.fill(bytes, (byte) 0);
    }
}
