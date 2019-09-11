/**
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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.container.keyvalue;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;

import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerPacker;

import com.google.common.base.Preconditions;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.IOUtils;

/**
 * Compress/uncompress KeyValueContainer data to a tar.gz archive.
 */
public class TarContainerPacker
    implements ContainerPacker<KeyValueContainerData> {

  private static final String CHUNKS_DIR_NAME = OzoneConsts.STORAGE_DIR_CHUNKS;

  private static final String DB_DIR_NAME = "db";

  private static final String CONTAINER_FILE_NAME = "container.yaml";



  /**
   * Given an input stream (tar file) extract the data to the specified
   * directories.
   *
   * @param container container which defines the destination structure.
   * @param inputStream the input stream.
   * @throws IOException
   */
  @Override
  public byte[] unpackContainerData(Container<KeyValueContainerData> container,
      InputStream inputStream)
      throws IOException {
    byte[] descriptorFileContent = null;
    try {
      KeyValueContainerData containerData = container.getContainerData();
      CompressorInputStream compressorInputStream =
          new CompressorStreamFactory()
              .createCompressorInputStream(CompressorStreamFactory.GZIP,
                  inputStream);

      TarArchiveInputStream tarInput =
          new TarArchiveInputStream(compressorInputStream);

      TarArchiveEntry entry = tarInput.getNextTarEntry();
      while (entry != null) {
        String name = entry.getName();
        if (name.startsWith(DB_DIR_NAME + "/")) {
          Path destinationPath = containerData.getDbFile().toPath()
              .resolve(name.substring(DB_DIR_NAME.length() + 1));
          extractEntry(tarInput, entry.getSize(), destinationPath);
        } else if (name.startsWith(CHUNKS_DIR_NAME + "/")) {
          Path destinationPath = Paths.get(containerData.getChunksPath())
              .resolve(name.substring(CHUNKS_DIR_NAME.length() + 1));
          extractEntry(tarInput, entry.getSize(), destinationPath);
        } else if (name.equals(CONTAINER_FILE_NAME)) {
          //Don't do anything. Container file should be unpacked in a
          //separated step by unpackContainerDescriptor call.
          descriptorFileContent = readEntry(tarInput, entry);
        } else {
          throw new IllegalArgumentException(
              "Unknown entry in the tar file: " + "" + name);
        }
        entry = tarInput.getNextTarEntry();
      }
      return descriptorFileContent;

    } catch (CompressorException e) {
      throw new IOException(
          "Can't uncompress the given container: " + container
              .getContainerData().getContainerID(),
          e);
    }
  }

  private void extractEntry(TarArchiveInputStream tarInput, long size,
      Path path) throws IOException {
    Preconditions.checkNotNull(path, "Path element should not be null");
    Path parent = Preconditions.checkNotNull(path.getParent(),
        "Path element should have a parent directory");
    Files.createDirectories(parent);
    try (BufferedOutputStream bos = new BufferedOutputStream(
        new FileOutputStream(path.toAbsolutePath().toString()))) {
      int bufferSize = 1024;
      byte[] buffer = new byte[bufferSize + 1];
      long remaining = size;
      while (remaining > 0) {
        int read =
            tarInput.read(buffer, 0, (int) Math.min(remaining, bufferSize));
        if (read >= 0) {
          remaining -= read;
          bos.write(buffer, 0, read);
        } else {
          remaining = 0;
        }
      }
    }

  }

  /**
   * Given a containerData include all the required container data/metadata
   * in a tar file.
   *
   * @param container Container to archive (data + metadata).
   * @param destination   Destination tar file/stream.
   * @throws IOException
   */
  @Override
  public void pack(Container<KeyValueContainerData> container,
      OutputStream destination)
      throws IOException {

    KeyValueContainerData containerData = container.getContainerData();

    try (CompressorOutputStream gzippedOut = new CompressorStreamFactory()
          .createCompressorOutputStream(CompressorStreamFactory.GZIP,
              destination)) {

      try (ArchiveOutputStream archiveOutputStream = new TarArchiveOutputStream(
          gzippedOut)) {

        includePath(containerData.getDbFile().toString(), DB_DIR_NAME,
            archiveOutputStream);

        includePath(containerData.getChunksPath(), CHUNKS_DIR_NAME,
            archiveOutputStream);

        includeFile(container.getContainerFile(),
            CONTAINER_FILE_NAME,
            archiveOutputStream);
      }
    } catch (CompressorException e) {
      throw new IOException(
          "Can't compress the container: " + containerData.getContainerID(),
          e);
    }

  }

  @Override
  public byte[] unpackContainerDescriptor(InputStream inputStream)
      throws IOException {
    try {
      CompressorInputStream compressorInputStream =
          new CompressorStreamFactory()
              .createCompressorInputStream(CompressorStreamFactory.GZIP,
                  inputStream);

      TarArchiveInputStream tarInput =
          new TarArchiveInputStream(compressorInputStream);

      TarArchiveEntry entry = tarInput.getNextTarEntry();
      while (entry != null) {
        String name = entry.getName();
        if (name.equals(CONTAINER_FILE_NAME)) {
          return readEntry(tarInput, entry);
        }
        entry = tarInput.getNextTarEntry();
      }

    } catch (CompressorException e) {
      throw new IOException(
          "Can't read the container descriptor from the container archive",
          e);
    }
    throw new IOException(
        "Container descriptor is missing from the container archive.");
  }

  private byte[] readEntry(TarArchiveInputStream tarInput,
      TarArchiveEntry entry) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    int bufferSize = 1024;
    byte[] buffer = new byte[bufferSize + 1];
    long remaining = entry.getSize();
    while (remaining > 0) {
      int read =
          tarInput.read(buffer, 0, (int) Math.min(remaining, bufferSize));
      remaining -= read;
      bos.write(buffer, 0, read);
    }
    return bos.toByteArray();
  }

  private void includePath(String containerPath, String subdir,
      ArchiveOutputStream archiveOutputStream) throws IOException {

    for (Path path : Files.list(Paths.get(containerPath))
        .collect(Collectors.toList())) {

      includeFile(path.toFile(), subdir + "/" + path.getFileName(),
          archiveOutputStream);
    }
  }

  private void includeFile(File file, String entryName,
      ArchiveOutputStream archiveOutputStream) throws IOException {
    ArchiveEntry archiveEntry =
        archiveOutputStream.createArchiveEntry(file, entryName);
    archiveOutputStream.putArchiveEntry(archiveEntry);
    try (FileInputStream fis = new FileInputStream(file)) {
      IOUtils.copy(fis, archiveOutputStream);
    }
    archiveOutputStream.closeArchiveEntry();
  }

}
