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
package org.apache.hadoop.hdds.security.x509;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.hadoop.conf.Configuration;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.security.KeyPair;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;

/**
 * We store all Key material in good old PEM files.
 * This helps in avoiding dealing will persistent
 * Java KeyStore issues. Also when debugging,
 * general tools like OpenSSL can be used to read and
 * decode these files.
 */
public class HDDSKeyPEMWriter {
  private static final Logger LOG =
      LoggerFactory.getLogger(HDDSKeyPEMWriter.class);
  private final Path location;
  private final SecurityConfig securityConfig;
  private Set<PosixFilePermission> permissionSet =
      Stream.of(OWNER_READ, OWNER_WRITE,  OWNER_EXECUTE)
          .collect(Collectors.toSet());
  private Supplier<Boolean> isPosixFileSystem;
  public final static String PRIVATE_KEY = "PRIVATE KEY";
  public final static String PUBLIC_KEY = "PUBLIC KEY";
  public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
  /*
    Creates an HDDS Key Writer.

    @param configuration - Configuration
   */
  public HDDSKeyPEMWriter(Configuration configuration) throws IOException {
    Preconditions.checkNotNull(configuration, "Config cannot be null");
    this.securityConfig = new SecurityConfig(configuration);
    isPosixFileSystem = HDDSKeyPEMWriter::isPosix;
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
   * @return Set
   */
  @VisibleForTesting
  public Set<PosixFilePermission> getPermissionSet() {
    return permissionSet;
  }

  /**
   * Returns the Security config used for this object.
   * @return SecurityConfig
   */
  public SecurityConfig getSecurityConfig() {
    return securityConfig;
  }

  /**
   * This function is used only for testing.
   *
   * @param isPosixFileSystem - Sets a boolean function for mimicking
   * files systems that are not posix.
   */
  @VisibleForTesting
  public void setIsPosixFileSystem(Supplier<Boolean> isPosixFileSystem) {
    this.isPosixFileSystem = isPosixFileSystem;
  }

  /**
   * Writes a given key using the default config options.
   *
   * @param keyPair - Key Pair to write to file.
   * @throws IOException
   */
  public void writeKey(KeyPair keyPair) throws IOException {
    writeKey(location, keyPair, securityConfig.getPrivateKeyName(),
        securityConfig.getPublicKeyName(), false);
  }

  /**
   * Writes a given key using default config options.
   *
   * @param keyPair - Key pair to write
   * @param overwrite - Overwrites the keys if they already exist.
   * @throws IOException
   */
  public void writeKey(KeyPair keyPair, boolean overwrite) throws IOException {
    writeKey(location, keyPair, securityConfig.getPrivateKeyName(),
        securityConfig.getPublicKeyName(), overwrite);
  }

  /**
   * Writes a given key using default config options.
   *
   * @param basePath - The location to write to, override the config values.
   * @param keyPair - Key pair to write
   * @param overwrite - Overwrites the keys if they already exist.
   * @throws IOException
   */
  public void writeKey(Path basePath, KeyPair keyPair, boolean overwrite)
      throws IOException {
    writeKey(basePath, keyPair, securityConfig.getPrivateKeyName(),
        securityConfig.getPublicKeyName(), overwrite);
  }

  /**
   * Helper function that actually writes data to the files.
   *
   * @param basePath - base path to write key
   * @param keyPair - Key pair to write to file.
   * @param privateKeyFileName - private key file name.
   * @param publicKeyFileName - public key file name.
   * @param force - forces overwriting the keys.
   * @throws IOException
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
   * Checks if private and public key file already exists. Throws IOException
   * if file exists and force flag is set to false, else will delete the
   * existing file.
   *
   * @param privateKeyFile - Private key file.
   * @param force - forces overwriting the keys.
   * @param publicKeyFile - public key file.
   * @throws IOException
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
   * @throws IOException
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
