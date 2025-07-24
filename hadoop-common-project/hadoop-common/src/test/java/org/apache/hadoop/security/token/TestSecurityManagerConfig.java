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
        SecretManagerConfig.update(createConfiguration(strongAlgorithm, strongLength));
        assertEquals(strongAlgorithm, SecretManagerConfig.getSelectedAlgorithm());
        assertEquals(strongLength, SecretManagerConfig.getSelectedLength());
    }

    @Test
    public void testMacCreation() {
        SecretManagerConfig.update(createConfiguration(strongAlgorithm, strongLength));
        Mac mac = SecretManagerConfig.createMac();
        assertEquals(strongAlgorithm, mac.getAlgorithm());
    }

    @Test
    public void testMacCreationUnknownAlgorithm() {
        SecretManagerConfig.update(
                createConfiguration("testMacCreationUnknownAlgorithm_NO_ALG", defaultLength));
        assertThrows(IllegalArgumentException.class, SecretManagerConfig::createMac);
    }

    @Test
    public void testKeygenCreation() {
        SecretManagerConfig.update(createConfiguration(strongAlgorithm, strongLength));
        KeyGenerator keyGenerator = SecretManagerConfig.createKeyGenerator();
        assertEquals(strongAlgorithm, keyGenerator.getAlgorithm());
    }

    @Test
    public void testKeygenCreationUnknownAlgorithm() {
        SecretManagerConfig.update(
                createConfiguration("testKeygenCreationUnknownAlgorithm_NO_ALG", defaultLength));
        assertThrows(IllegalArgumentException.class, SecretManagerConfig::createKeyGenerator);
    }

    @AfterEach
    public void tearDown() {
        SecretManagerConfig.update(createConfiguration(defaultAlgorithm, defaultLength));
    }

    private Configuration createConfiguration(String algorithm, int length) {
        Configuration conf = new Configuration();
        conf.set(
                CommonConfigurationKeysPublic.HADOOP_SECURITY_SECRET_MANAGER_KEY_GENERATOR_ALGORITHM_KEY,
                algorithm);
        conf.setInt(
                CommonConfigurationKeysPublic.HADOOP_SECURITY_SECRET_MANAGER_KEY_LENGTH_KEY,
                length);
        return conf;
    }
}
