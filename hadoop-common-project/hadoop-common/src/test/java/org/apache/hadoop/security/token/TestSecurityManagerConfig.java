package org.apache.hadoop.security.token;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.crypto.KeyGenerator;
import javax.crypto.Mac;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestSecurityManagerConfig {

    private final String defaultAlgorithm =
            CommonConfigurationKeysPublic.HADOOP_SECURITY_SECRET_MANAGER_KEY_GENERATOR_ALGORITHM_DEFAULT;
    private final int defaultLength =
            CommonConfigurationKeysPublic.HADOOP_SECURITY_SECRET_MANAGER_KEY_LENGTH_DEFAULT;
    private final String strongAlgorithm = "HmacSHA256";
    private final int strongLength = 256;

    @Test
    public void testDefaults() {
        assertEquals(defaultAlgorithm, SecretManagerConfig.getSelectedAlgorithm());
        assertEquals(defaultLength, SecretManagerConfig.getSelectedLength());
    }

    @Test
    public void testUpdateByConfig() {
        Configuration conf = new Configuration();
        conf.set(
                CommonConfigurationKeysPublic.HADOOP_SECURITY_SECRET_MANAGER_KEY_GENERATOR_ALGORITHM_KEY,
                strongAlgorithm);
        conf.setInt(
                CommonConfigurationKeysPublic.HADOOP_SECURITY_SECRET_MANAGER_KEY_LENGTH_KEY,
                strongLength);
        SecretManagerConfig.update(conf);
        assertEquals(strongAlgorithm, SecretManagerConfig.getSelectedAlgorithm());
        assertEquals(strongLength, SecretManagerConfig.getSelectedLength());
    }

    @Test
    public void testUpdateAlgorithmBySetter() {
        SecretManagerConfig.setSelectedAlgorithm(strongAlgorithm);
        assertEquals(strongAlgorithm, SecretManagerConfig.getSelectedAlgorithm());
        assertEquals(defaultLength, SecretManagerConfig.getSelectedLength());
    }

    @Test
    public void testUpdateLengthBySetter() {
        SecretManagerConfig.setSelectedLength(strongLength);
        assertEquals(defaultAlgorithm, SecretManagerConfig.getSelectedAlgorithm());
        assertEquals(strongLength, SecretManagerConfig.getSelectedLength());
    }

    @Test
    public void testMacCreation() {
        SecretManagerConfig.setSelectedAlgorithm(strongAlgorithm);
        Mac mac = SecretManagerConfig.createMac();
        assertEquals(strongAlgorithm, mac.getAlgorithm());
    }

    @Test
    public void testMacCreationUnknownAlgorithm() {
        SecretManagerConfig.setSelectedAlgorithm("testMacCreationUnknownAlgorithm_NO_ALG");
        assertThrows(IllegalArgumentException.class, SecretManagerConfig::createMac);
    }

    @Test
    public void testKeygenCreation() {
        SecretManagerConfig.setSelectedAlgorithm(strongAlgorithm);
        KeyGenerator keyGenerator = SecretManagerConfig.createKeyGenerator();
        assertEquals(strongAlgorithm, keyGenerator.getAlgorithm());
    }

    @Test
    public void testKeygenCreationUnknownAlgorithm() {
        SecretManagerConfig.setSelectedAlgorithm("testKeygenCreationUnknownAlgorithm_NO_ALG");
        assertThrows(IllegalArgumentException.class, SecretManagerConfig::createKeyGenerator);
    }

    @AfterEach
    public void tearDown() {
        SecretManagerConfig.setSelectedAlgorithm(defaultAlgorithm);
        SecretManagerConfig.setSelectedLength(defaultLength);
    }
}
