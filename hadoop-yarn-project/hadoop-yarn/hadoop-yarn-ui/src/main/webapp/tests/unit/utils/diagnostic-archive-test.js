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

import {
  buildBundleFolderName,
  buildDiagnosticArchiveEntries,
  buildDiagnosticZipBlob,
  buildDiagnosticZipBytes,
  normalizeIssueFiles,
  resolveEntryFileName,
  sanitizeEntryPath,
  toDosDateTime,
  utf8EncodeFallback,
} from 'yarn-ui/utils/diagnostic-archive';
import { module, test } from 'qunit';

module('Unit | Utility | diagnostic archive');

test('buildBundleFolderName sanitizes timestamp', function(assert) {
  const name = buildBundleFolderName(
    '2026-09-29T02:24:30.345Z',
    'application_1789971760389_0001',
    'application_diagnostic',
  );

  assert.equal(
    name,
    'application_1789971760389_0001_application_diagnostic_2026-09-29T02-24-30-345Z',
  );
});

test('normalizeIssueFiles accepts single file object', function(assert) {
  const files = normalizeIssueFiles({
    file: {
      filename: 'application_info',
      contentType: 'text/plain',
      content: '<xml/>',
    },
  });

  assert.equal(files.length, 1);
  assert.equal(files[0].filename, 'application_info');
});

test('resolveEntryFileName adds xml extension for xml payload', function(assert) {
  const name = resolveEntryFileName(
    'application_attempts',
    'text/plain',
    '<?xml version="1.0"?><appAttempts/>',
  );

  assert.equal(name, 'application_attempts.xml');
});

test('resolveEntryFileName adds log extension for _log file names', function(assert) {
  assert.equal(
    resolveEntryFileName('resourcemanager_log', 'text/plain', 'plain log line'),
    'resourcemanager_log.log',
  );
  assert.equal(
    resolveEntryFileName('nodemanager_log', 'text/plain', 'plain log line'),
    'nodemanager_log.log',
  );
});

test('resolveEntryFileName adds txt extension for non-xml payload', function(assert) {
  const name = resolveEntryFileName('application_logs', 'text/plain', 'plain log line');

  assert.equal(name, 'application_logs.txt');
});

test('resolveEntryFileName does not treat embedded log as a log file', function(assert) {
  assert.equal(resolveEntryFileName('catalog', 'text/plain', 'entries'), 'catalog.txt');
  assert.equal(resolveEntryFileName('dialog', 'text/plain', 'entries'), 'dialog.txt');
});

test('buildDiagnosticArchiveEntries includes collected files only', function(assert) {
  const folderName = 'application_1_application_diagnostic_2026-09-29T02-24-30-345Z';
  const entries = buildDiagnosticArchiveEntries(folderName, {
    file: [{
      filename: 'application_attempts',
      contentType: 'text/plain',
      content: '<?xml version="1.0"?><appAttempts/>',
    }],
  });

  assert.equal(entries.length, 1);
  assert.equal(entries[0].path, `${folderName}/application_attempts.xml`);
});

test('buildDiagnosticArchiveEntries sanitizes host:port file names', function(assert) {
  const folderName = 'application_1_application_diagnostic_2026-09-29T02-24-30-345Z';
  const entries = buildDiagnosticArchiveEntries(folderName, {
    file: [{
      filename: 'nm_host1.example.com:8042_jstack',
      contentType: 'text/plain',
      content: 'thread dump',
    }],
  });

  assert.equal(entries.length, 1);
  assert.equal(entries[0].path, `${folderName}/nm_host1.example.com_8042_jstack.txt`);
});

test('sanitizeEntryPath sanitizes each segment and drops traversal', function(assert) {
  assert.equal(sanitizeEntryPath('nm/host1:8042_jstack.txt'), 'nm/host1_8042_jstack.txt');
  assert.equal(sanitizeEntryPath('/leading/slash.txt'), 'leading/slash.txt');
  assert.equal(sanitizeEntryPath('../../etc/passwd.txt'), 'etc/passwd.txt');
});

test('buildDiagnosticZipBlob produces a zip blob', function(assert) {
  const folderName = 'application_1_application_diagnostic_2026-09-29T02-24-30-345Z';
  const entries = buildDiagnosticArchiveEntries(folderName, {
    file: [{
      filename: 'application_info',
      contentType: 'text/plain',
      content: 'hello',
    }],
  });
  const blob = buildDiagnosticZipBlob(entries, new Date(2026, 8, 29, 2, 24, 30));

  assert.ok(blob instanceof Blob);
  assert.equal(blob.type, 'application/zip');
  assert.ok(blob.size > 0);
});

function readUint16(bytes, offset) {
  return new DataView(bytes.buffer, bytes.byteOffset).getUint16(offset, true);
}

function readUint32(bytes, offset) {
  return new DataView(bytes.buffer, bytes.byteOffset).getUint32(offset, true);
}

function toArray(bytes) {
  return Array.prototype.slice.call(bytes);
}

test('toDosDateTime encodes local date and time', function(assert) {
  const dos = toDosDateTime(new Date(2026, 8, 29, 2, 24, 30));

  assert.equal(dos.time, (2 << 11) | (24 << 5) | 15);
  assert.equal(dos.date, (46 << 9) | (9 << 5) | 29);
});

test('buildDiagnosticZipBytes writes valid headers', function(assert) {
  const modifiedAt = new Date(2026, 8, 29, 2, 24, 30);
  const dos = toDosDateTime(modifiedAt);
  const bytes = buildDiagnosticZipBytes([{ path: 'b/hello.txt', data: 'hello' }], modifiedAt);
  const nameLength = 'b/hello.txt'.length;
  const centralOffset = 30 + nameLength + 5;
  const centralSize = 46 + nameLength;
  const end = bytes.length - 22;

  assert.equal(readUint32(bytes, 0), 0x04034b50, 'local header signature');
  assert.equal(readUint16(bytes, 6), 0x0800, 'local header UTF-8 flag');
  assert.equal(readUint16(bytes, 10), dos.time, 'local header time');
  assert.equal(readUint16(bytes, 12), dos.date, 'local header date');
  assert.equal(readUint32(bytes, 14), 0x3610a686, 'local header CRC-32 of "hello"');
  assert.equal(readUint32(bytes, 18), 5, 'local header compressed size');
  assert.equal(readUint32(bytes, 22), 5, 'local header uncompressed size');

  assert.equal(readUint32(bytes, centralOffset), 0x02014b50, 'central directory signature');
  assert.equal(readUint16(bytes, centralOffset + 8), 0x0800, 'central directory UTF-8 flag');
  assert.equal(readUint16(bytes, centralOffset + 12), dos.time, 'central directory time');
  assert.equal(readUint16(bytes, centralOffset + 14), dos.date, 'central directory date');
  assert.equal(readUint32(bytes, centralOffset + 16), 0x3610a686, 'central directory CRC-32');
  assert.equal(readUint32(bytes, centralOffset + 42), 0, 'central directory local header offset');

  assert.equal(readUint32(bytes, end), 0x06054b50, 'end of central directory signature');
  assert.equal(readUint16(bytes, end + 8), 1, 'entries on this disk');
  assert.equal(readUint16(bytes, end + 10), 1, 'total entries');
  assert.equal(readUint32(bytes, end + 12), centralSize, 'central directory size');
  assert.equal(readUint32(bytes, end + 16), centralOffset, 'central directory offset');
});

test('buildDiagnosticZipBytes encodes non-ASCII names as UTF-8', function(assert) {
  const bytes = buildDiagnosticZipBytes([{ path: 'b/ünïcode.txt', data: 'x' }], new Date(2026, 8, 29));
  const expectedName = [0x62, 0x2f, 0xc3, 0xbc, 0x6e, 0xc3, 0xaf, 0x63, 0x6f, 0x64, 0x65, 0x2e, 0x74, 0x78, 0x74];

  assert.equal(readUint16(bytes, 26), expectedName.length, 'name length counts bytes, not characters');
  assert.deepEqual(toArray(bytes.subarray(30, 30 + expectedName.length)), expectedName);
});

test('utf8EncodeFallback matches UTF-8 for multi-byte characters', function(assert) {
  assert.deepEqual(toArray(utf8EncodeFallback('A')), [0x41]);
  assert.deepEqual(toArray(utf8EncodeFallback('ü')), [0xc3, 0xbc]);
  assert.deepEqual(toArray(utf8EncodeFallback('€')), [0xe2, 0x82, 0xac]);
  assert.deepEqual(toArray(utf8EncodeFallback('\ud83d\ude00')), [0xf0, 0x9f, 0x98, 0x80]);
});
