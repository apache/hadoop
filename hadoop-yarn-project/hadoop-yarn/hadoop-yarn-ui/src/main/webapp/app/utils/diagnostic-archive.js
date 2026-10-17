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

const ISSUE_ID = 'application_diagnostic';

export function buildBundleFolderName(generatedAt, appId, issueId) {
  const safeTs = generatedAt.replace(/[:.]/g, '-');
  const safeAppId = String(appId).replace(/[^\w.-]+/g, '_');
  const safeIssueId = String(issueId || ISSUE_ID).replace(/[^\w.-]+/g, '_');
  return `${safeAppId}_${safeIssueId}_${safeTs}`;
}

export function normalizeIssueFiles(issueData) {
  if (!issueData || !issueData.file) {
    return [];
  }

  const files = issueData.file;
  return Array.isArray(files) ? files : [files];
}

export function resolveEntryFileName(filename, contentType, content) {

  if (contentType.indexOf('xml') >= 0 || content.startsWith('<?xml')) {
    return `${filename}.xml`;
  }
  if (contentType.indexOf('json') >= 0 || content.startsWith('{') || content.startsWith('[')) {
    return `${filename}.json`;
  }
  if (filename.indexOf('log') >= 0) {
    return `${filename}.log`;
  }

  return `${filename}.txt`;
}

function stringToUtf8Bytes(str) {
  if (typeof TextEncoder !== 'undefined') {
    return new TextEncoder().encode(str);
  }

  return utf8EncodeFallback(str);
}

function utf8EncodeFallback(str) {
  const bytes = [];
  for (let i = 0; i < str.length; i++) {
    let codePoint = str.charCodeAt(i);
    if (codePoint >= 0xd800 && codePoint <= 0xdbff && i + 1 < str.length) {
      const next = str.charCodeAt(i + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        codePoint = ((codePoint - 0xd800) << 10) + (next - 0xdc00) + 0x10000;
        i++;
      }
    }

    if (codePoint < 0x80) {
      bytes.push(codePoint);
    } else if (codePoint < 0x800) {
      bytes.push(0xc0 | (codePoint >> 6), 0x80 | (codePoint & 0x3f));
    } else if (codePoint < 0x10000) {
      bytes.push(
        0xe0 | (codePoint >> 12),
        0x80 | ((codePoint >> 6) & 0x3f),
        0x80 | (codePoint & 0x3f),
      );
    } else {
      bytes.push(
        0xf0 | (codePoint >> 18),
        0x80 | ((codePoint >> 12) & 0x3f),
        0x80 | ((codePoint >> 6) & 0x3f),
        0x80 | (codePoint & 0x3f),
      );
    }
  }
  return new Uint8Array(bytes);
}

const CRC32_TABLE = (function buildCrc32Table() {
  const table = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let j = 0; j < 8; j++) {
      c = (c & 1) ? (0xedb88320 ^ (c >>> 1)) : (c >>> 1);
    }
    table[i] = c >>> 0;
  }
  return table;
}());

function crc32(bytes) {
  let crc = 0xffffffff;
  for (let i = 0; i < bytes.length; i++) {
    crc = CRC32_TABLE[(crc ^ bytes[i]) & 0xff] ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function writeUint32LE(view, offset, value) {
  view.setUint32(offset, value, true);
}

function writeUint16LE(view, offset, value) {
  view.setUint16(offset, value, true);
}

/**
 * Build a ZIP archive (stored, no compression) containing one top-level folder.
 */
export function buildDiagnosticZipBlob(folderName, entries) {
  const localParts = [];
  const centralParts = [];
  let offset = 0;

  entries.forEach((entry) => {
    const nameBytes = stringToUtf8Bytes(entry.path);
    const dataBytes = entry.data instanceof Uint8Array ? entry.data : stringToUtf8Bytes(entry.data);
    const crc = crc32(dataBytes);

    const localHeader = new ArrayBuffer(30 + nameBytes.length);
    const localView = new DataView(localHeader);
    writeUint32LE(localView, 0, 0x04034b50);
    writeUint16LE(localView, 4, 20);
    writeUint16LE(localView, 6, 0);
    writeUint16LE(localView, 8, 0);
    writeUint16LE(localView, 10, 0);
    writeUint16LE(localView, 12, 0);
    writeUint32LE(localView, 14, crc);
    writeUint32LE(localView, 18, dataBytes.length);
    writeUint32LE(localView, 22, dataBytes.length);
    writeUint16LE(localView, 26, nameBytes.length);
    writeUint16LE(localView, 28, 0);
    new Uint8Array(localHeader, 30).set(nameBytes);

    localParts.push(new Uint8Array(localHeader), dataBytes);

    const centralHeader = new ArrayBuffer(46 + nameBytes.length);
    const centralView = new DataView(centralHeader);
    writeUint32LE(centralView, 0, 0x02014b50);
    writeUint16LE(centralView, 4, 20);
    writeUint16LE(centralView, 6, 20);
    writeUint16LE(centralView, 8, 0);
    writeUint16LE(centralView, 10, 0);
    writeUint16LE(centralView, 12, 0);
    writeUint16LE(centralView, 14, 0);
    writeUint32LE(centralView, 16, crc);
    writeUint32LE(centralView, 20, dataBytes.length);
    writeUint32LE(centralView, 24, dataBytes.length);
    writeUint16LE(centralView, 28, nameBytes.length);
    writeUint16LE(centralView, 30, 0);
    writeUint16LE(centralView, 32, 0);
    writeUint16LE(centralView, 34, 0);
    writeUint16LE(centralView, 36, 0);
    writeUint32LE(centralView, 38, 0);
    writeUint32LE(centralView, 42, offset);
    new Uint8Array(centralHeader, 46).set(nameBytes);

    centralParts.push(new Uint8Array(centralHeader));
    offset += localHeader.byteLength + dataBytes.length;
  });

  const centralSize = centralParts.reduce((sum, part) => sum + part.length, 0);
  const endRecord = new ArrayBuffer(22);
  const endView = new DataView(endRecord);
  writeUint32LE(endView, 0, 0x06054b50);
  writeUint16LE(endView, 4, 0);
  writeUint16LE(endView, 6, 0);
  writeUint16LE(endView, 8, entries.length);
  writeUint16LE(endView, 10, entries.length);
  writeUint32LE(endView, 12, centralSize);
  writeUint32LE(endView, 16, offset);
  writeUint16LE(endView, 20, 0);

  const totalLength = localParts.reduce((sum, part) => sum + part.length, 0)
    + centralSize
    + endRecord.byteLength;
  const output = new Uint8Array(totalLength);
  let writeOffset = 0;

  localParts.forEach((part) => {
    output.set(part, writeOffset);
    writeOffset += part.length;
  });
  centralParts.forEach((part) => {
    output.set(part, writeOffset);
    writeOffset += part.length;
  });
  output.set(new Uint8Array(endRecord), writeOffset);

  return new Blob([output], { type: 'application/zip' });
}

export function buildDiagnosticArchiveEntries(folderName, issueData) {
  const entries = [];
  const usedNames = {};

  normalizeIssueFiles(issueData).forEach((fileEntry, index) => {
    const content = fileEntry.content != null ? String(fileEntry.content) : '';
    const relativeName = resolveEntryFileName(
      fileEntry.filename || `file_${index + 1}`,
      fileEntry.contentType,
      content,
    );
    let zipPath = `${folderName}/${relativeName.replace(/^\/+/, '')}`;

    if (usedNames[zipPath]) {
      const dot = zipPath.lastIndexOf('.');
      if (dot > 0) {
        zipPath = `${zipPath.slice(0, dot)}_${index + 1}${zipPath.slice(dot)}`;
      } else {
        zipPath = `${zipPath}_${index + 1}`;
      }
    }
    usedNames[zipPath] = true;

    entries.push({
      path: zipPath,
      data: content,
    });
  });

  return entries;
}
