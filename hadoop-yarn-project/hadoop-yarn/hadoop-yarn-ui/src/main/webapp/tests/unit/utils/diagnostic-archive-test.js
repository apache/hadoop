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
  normalizeIssueFiles,
  resolveEntryFileName,
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

test('resolveEntryFileName adds txt extension for non-xml payload', function(assert) {
  const name = resolveEntryFileName('application_logs', 'text/plain', 'plain log line');

  assert.equal(name, 'application_logs.txt');
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

test('buildDiagnosticZipBlob produces a zip blob', function(assert) {
  const folderName = 'application_1_application_diagnostic_2026-09-29T02-24-30-345Z';
  const entries = buildDiagnosticArchiveEntries(folderName, {
    file: [{
      filename: 'application_info',
      contentType: 'text/plain',
      content: 'hello',
    }],
  });
  const blob = buildDiagnosticZipBlob(folderName, entries);

  assert.ok(blob instanceof Blob);
  assert.equal(blob.type, 'application/zip');
  assert.ok(blob.size > 0);
});
