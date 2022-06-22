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

package org.apache.hadoop.runc.squashfs;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;

public final class SquashFsConverter {

  private static final Logger LOG
      = LoggerFactory.getLogger(SquashFsConverter.class);

  private SquashFsConverter() {
  }

  public static void convertToSquashFs(File inputFile, File outputFile)
      throws IOException {

    LOG.debug("Converting {} -> {}..",
        inputFile.getAbsolutePath(), outputFile.getAbsolutePath());

    try (
        FileInputStream fis = new FileInputStream(inputFile);
        GZIPInputStream gis = new GZIPInputStream(fis);
        TarArchiveInputStream tis = new TarArchiveInputStream(gis)) {

      long fileCount = 0L;
      try (SquashFsWriter writer = new SquashFsWriter(outputFile)) {
        TarArchiveEntry entry;
        AtomicReference<Date> modDate = new AtomicReference<>(new Date(0));

        while ((entry = tis.getNextTarEntry()) != null) {
          processTarEntry(tis, entry, writer, modDate);
          fileCount++;
        }
        writer.setModificationTime((int) (modDate.get().getTime() / 1000L));
        writer.finish();
      }

      LOG.debug("Converted image containing {} files", fileCount);
    }
  }

  private static void processTarEntry(
      TarArchiveInputStream tis,
      TarArchiveEntry entry,
      SquashFsWriter writer,
      AtomicReference<Date> modDate) throws IOException {

    int userId = (int) entry.getLongUserId();
    int groupId = (int) entry.getLongGroupId();

    String name = entry.getName()
        .replaceAll("/+", "/")
        .replaceAll("^/", "")
        .replaceAll("/$", "")
        .replaceAll("^", "/");

    short permissions = (short) (entry.getMode() & 07777);

    Date lastModified = entry.getLastModifiedDate();
    if (lastModified.after(modDate.get())) {
      modDate.set(lastModified);
    }

    SquashFsEntryBuilder tb = writer.entry(name)
        .uid(userId)
        .gid(groupId)
        .permissions(permissions)
        .fileSize(entry.getSize())
        .lastModified(lastModified);

    if (entry.isSymbolicLink()) {
      tb.symlink(entry.getLinkName());
    } else if (entry.isDirectory()) {
      tb.directory();
    } else if (entry.isFile()) {
      tb.file();
    } else if (entry.isBlockDevice()) {
      tb.blockDev(entry.getDevMajor(), entry.getDevMinor());
    } else if (entry.isCharacterDevice()) {
      tb.charDev(entry.getDevMajor(), entry.getDevMinor());
    } else if (entry.isFIFO()) {
      tb.fifo();
    } else {
      throw new IOException(
          String.format("Unknown file type for '%s'", entry.getName()));
    }

    if (entry.isLink()) {
      String target = entry.getLinkName()
          .replaceAll("/+", "/")
          .replaceAll("^/", "")
          .replaceAll("/$", "")
          .replaceAll("^", "/");
      tb.hardlink(target);
    }

    if (entry.isFile() && !entry.isLink()) {
      tb.content(tis, entry.getSize());
    }

    tb.build();
  }

}
