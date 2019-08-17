/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
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
package org.apache.hadoop.hdds.security.x509.keys;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;
import org.bouncycastle.util.io.pem.PemWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;

/**
 * We store all Key material in good old PEM files. This helps in avoiding
 * dealing will persistent Java KeyStore issues. Also when debugging, general
 * tools like OpenSSL can be used to read and decode these files.
 */
public class KeyCodec {
  public final static String PRIVATE_KEY = "PRIVATE KEY";
  public final static String PUBLIC_KEY = "PUBLIC KEY";
  public final static Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
  private final static  Logger LOG =
      LoggerFactory.getLogger(KeyCodec.class);
  private final Path location;
  private final SecurityConfig securityConfig;
  private Set<PosixFilePermission> permissionSet =
      Stream.of(OWNER_READ, OWNER_WRITE, OWNER_EXECUTE)
          .collect(Collectors.toSet());
  private Supplier<Boolean> isPosixFileSystem;

  /**
   * Creates an KeyCodec.
   *
   * @param config - Security Config.
   * @param component - Component String.
   */
  public KeyCodec(SecurityConfig config, String component) {
    this.securityConfig = config;
    isPosixFileSystem = KeyCodec::isPosix;
    this.location = securityConfig.getKeyLocation(component);
  }

  /**
   * Creates an KeyCodec.
   *
   * @param config - Security Config.
   */
  public KeyCodec(SecurityConfig config) {
    this.securityConfig = config;
    isPosixFileSystem = KeyCodec::isPosix;
    this.location = securityConfig.getKeyLocation();
  }

  /**
   * Creates an HDDS Key Writer.
   *
   * @param configuration - Configuration
   */
  public KeyCodec(Configuration configuration) {
    Preconditions.checkNotNull(configuration, "Config cannot be null");
    this.securityConfig = new SecurityConfig(configuration);
    isPosixFileSystem = KeyCodec::isPosix;
    this.location = securityConfig.getKeyLocation();
  }

  /**
   * Checks if File System supports posix style security permissions.
   *
   * @return True if it supports posix.
   */
  private static Boolean isPosix() {
    return FileSystems.getDefault().supportedFileAttributeViews()
        .contains("posix");
  }

  /**
   * Returns the Permission set.
   *
   * @return Set
   */
  @VisibleForTesting
  public Set<PosixFilePermission> getPermissionSet() {
    return permissionSet;
  }

  /**
   * Returns the Security config used for this object.
   *
   * @return SecurityConfig
   */
  public SecurityConfig getSecurityConfig() {
    return securityConfig;
  }

  /**
   * This function is used only for testing.
   *
   * @param isPosixFileSystem - Sets a boolean function for mimicking files
   * systems that are not posix.
   */
  @VisibleForTesting
  public void setIsPosixFileSystem(Supplier<Boolean> isPosixFileSystem) {
    this.isPosixFileSystem = isPosixFileSystem;
  }

  /**
   * Writes a given key using the default config options.
   *
   * @param keyPair - Key Pair to write to file.
   * @throws IOException - On I/O failure.
   */
  public void writeKey(KeyPair keyPair) throws IOException {
    writeKey(location, keyPair, securityConfig.getPrivateKeyFileName(),
        securityConfig.getPublicKeyFileName(), false);
  }

  /**
   * Writes a given private key using the default config options.
   *
   * @param key - Key to write to file.
   * @throws IOException - On I/O failure.
   */
  public void writePrivateKey(PrivateKey key) throws IOException {
    File privateKeyFile =
        Paths.get(location.toString(),
            securityConfig.getPrivateKeyFileName()).toFile();

    if (Files.exists(privateKeyFile.toPath())) {
      throw new IOException("Private key already exist.");
    }

    try (PemWriter privateKeyWriter = new PemWriter(new
        FileWriterWithEncoding(privateKeyFile, DEFAULT_CHARSET))) {
      privateKeyWriter.writeObject(
          new PemObject(PRIVATE_KEY, key.getEncoded()));
    }
    Files.setPosixFilePermissions(privateKeyFile.toPath(), permissionSet);
  }

  /**
   * Writes a given public key using the default config options.
   *
   * @param key - Key to write to file.
   * @throws IOException - On I/O failure.
   */
  public void writePublicKey(PublicKey key) throws IOException {
    File publicKeyFile = Paths.get(location.toString(),
        securityConfig.getPublicKeyFileName()).toFile();

    if (Files.exists(publicKeyFile.toPath())) {
      throw new IOException("Private key already exist.");
    }

    try (PemWriter keyWriter = new PemWriter(new
        FileWriterWithEncoding(publicKeyFile, DEFAULT_CHARSET))) {
      keyWriter.writeObject(
          new PemObject(PUBLIC_KEY, key.getEncoded()));
    }
    Files.setPosixFilePermissions(publicKeyFile.toPath(), permissionSet);
  }

  /**
   * Writes a given key using default config options.
   *
   * @param keyPair - Key pair to write
   * @param overwrite - Overwrites the keys if they already exist.
   * @throws IOException - On I/O failure.
   */
  public void writeKey(KeyPair keyPair, boolean overwrite) throws IOException {
    writeKey(location, keyPair, securityConfig.getPrivateKeyFileName(),
        securityConfig.getPublicKeyFileName(), overwrite);
  }

  /**
   * Writes a given key using default config options.
   *
   * @param basePath - The location to write to, override the config values.
   * @param keyPair - Key pair to write
   * @param overwrite - Overwrites the keys if they already exist.
   * @throws IOException - On I/O failure.
   */
  public void writeKey(Path basePath, KeyPair keyPair, boolean overwrite)
      throws IOException {
    writeKey(basePath, keyPair, securityConfig.getPrivateKeyFileName(),
        securityConfig.getPublicKeyFileName(), overwrite);
  }

  /**
   * Reads a Private Key from the PEM Encoded Store.
   *
   * @param basePath - Base Path, Directory where the Key is stored.
   * @param keyFileName - File Name of the private key
   * @return PrivateKey Object.
   * @throws IOException - on Error.
   */
  private PKCS8EncodedKeySpec readKey(Path basePath, String keyFileName)
      throws IOException {
    File fileName = Paths.get(basePath.toString(), keyFileName).toFile();
    String keyData = FileUtils.readFileToString(fileName, DEFAULT_CHARSET);
    final byte[] pemContent;
    try (PemReader pemReader = new PemReader(new StringReader(keyData))) {
      PemObject keyObject = pemReader.readPemObject();
      pemContent = keyObject.getContent();
    }
    return new PKCS8EncodedKeySpec(pemContent);
  }

  /**
   * Returns a Private Key from a PEM encoded file.
   *
   * @param basePath - base path
   * @param privateKeyFileName - private key file name.
   * @return PrivateKey
   * @throws InvalidKeySpecException  - on Error.
   * @throws NoSuchAlgorithmException - on Error.
   * @throws IOException              - on Error.
   */
  public PrivateKey readPrivateKey(Path basePath, String privateKeyFileName)
      throws InvalidKeySpecException, NoSuchAlgorithmException, IOException {
    PKCS8EncodedKeySpec encodedKeySpec = readKey(basePath, privateKeyFileName);
    final KeyFactory keyFactory =
        KeyFactory.getInstance(securityConfig.getKeyAlgo());
    return
        keyFactory.generatePrivate(encodedKeySpec);
  }

  /**
   * Read the Public Key using defaults.
   * @return PublicKey.
   * @throws InvalidKeySpecException - On Error.
   * @throws NoSuchAlgorithmException - On Error.
   * @throws IOException - On Error.
   */
  public PublicKey readPublicKey() throws InvalidKeySpecException,
      NoSuchAlgorithmException, IOException {
    return readPublicKey(this.location.toAbsolutePath(),
        securityConfig.getPublicKeyFileName());
  }

  /**
   * Returns a public key from a PEM encoded file.
   *
   * @param basePath - base path.
   * @param publicKeyFileName - public key file name.
   * @return PublicKey
   * @throws NoSuchAlgorithmException - on Error.
   * @throws InvalidKeySpecException  - on Error.
   * @throws IOException              - on Error.
   */
  public PublicKey readPublicKey(Path basePath, String publicKeyFileName)
      throws NoSuchAlgorithmException, InvalidKeySpecException, IOException {
    PKCS8EncodedKeySpec encodedKeySpec = readKey(basePath, publicKeyFileName);
    final KeyFactory keyFactory =
        KeyFactory.getInstance(securityConfig.getKeyAlgo());
    return
        keyFactory.generatePublic(
            new X509EncodedKeySpec(encodedKeySpec.getEncoded()));

  }


  /**
   * Returns the private key  using defaults.
   * @return PrivateKey.
   * @throws InvalidKeySpecException - On Error.
   * @throws NoSuchAlgorithmException - On Error.
   * @throws IOException - On Error.
   */
  public PrivateKey readPrivateKey() throws InvalidKeySpecException,
      NoSuchAlgorithmException, IOException {
    return readPrivateKey(this.location.toAbsolutePath(),
        securityConfig.getPrivateKeyFileName());
  }


  /**
   * Helper function that actually writes data to the files.
   *
   * @param basePath - base path to write key
   * @param keyPair - Key pair to write to file.
   * @param privateKeyFileName - private key file name.
   * @param publicKeyFileName - public key file name.
   * @param force - forces overwriting the keys.
   * @throws IOException - On I/O failure.
   */
  private synchronized void writeKey(Path basePath, KeyPair keyPair,
      String privateKeyFileName, String publicKeyFileName, boolean force)
      throws IOException {
    checkPreconditions(basePath);

    File privateKeyFile =
        Paths.get(location.toString(), privateKeyFileName).toFile();
    File publicKeyFile =
        Paths.get(location.toString(), publicKeyFileName).toFile();
    checkKeyFile(privateKeyFile, force, publicKeyFile);

    try (PemWriter privateKeyWriter = new PemWriter(new
        FileWriterWithEncoding(privateKeyFile, DEFAULT_CHARSET))) {
      privateKeyWriter.writeObject(
          new PemObject(PRIVATE_KEY, keyPair.getPrivate().getEncoded()));
    }

    try (PemWriter publicKeyWriter = new PemWriter(new
        FileWriterWithEncoding(publicKeyFile, DEFAULT_CHARSET))) {
      publicKeyWriter.writeObject(
          new PemObject(PUBLIC_KEY, keyPair.getPublic().getEncoded()));
    }
    Files.setPosixFilePermissions(privateKeyFile.toPath(), permissionSet);
    Files.setPosixFilePermissions(publicKeyFile.toPath(), permissionSet);
  }

  /**
   * Checks if private and public key file already exists. Throws IOException if
   * file exists and force flag is set to false, else will delete the existing
   * file.
   *
   * @param privateKeyFile - Private key file.
   * @param force - forces overwriting the keys.
   * @param publicKeyFile - public key file.
   * @throws IOException - On I/O failure.
   */
  private void checkKeyFile(File privateKeyFile, boolean force,
                            File publicKeyFile) throws IOException {
    if (privateKeyFile.exists() && force) {
      if (!privateKeyFile.delete()) {
        throw new IOException("Unable to delete private key file.");
      }
    }

    if (publicKeyFile.exists() && force) {
      if (!publicKeyFile.delete()) {
        throw new IOException("Unable to delete public key file.");
      }
    }

    if (privateKeyFile.exists()) {
      throw new IOException("Private Key file already exists.");
    }

    if (publicKeyFile.exists()) {
      throw new IOException("Public Key file already exists.");
    }
  }

  /**
   * Checks if base path exists and sets file permissions.
   *
   * @param basePath - base path to write key
   * @throws IOException - On I/O failure.
   */
  private void checkPreconditions(Path basePath) throws IOException {
    Preconditions.checkNotNull(basePath, "Base path cannot be null");
    if (!isPosixFileSystem.get()) {
      LOG.error("Keys cannot be stored securely without POSIX file system "
          + "support for now.");
      throw new IOException("Unsupported File System for pem file.");
    }

    if (Files.exists(basePath)) {
      // Not the end of the world if we reset the permissions on an existing
      // directory.
      Files.setPosixFilePermissions(basePath, permissionSet);
    } else {
      boolean success = basePath.toFile().mkdirs();
      if (!success) {
        LOG.error("Unable to create the directory for the "
            + "location. Location: {}", basePath);
        throw new IOException("Unable to create the directory for the "
            + "location. Location:" + basePath);
      }
      Files.setPosixFilePermissions(basePath, permissionSet);
    }
  }

}
