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

import Ember from 'ember';
import {
  buildBundleFolderName,
  buildDiagnosticArchiveEntries,
  buildDiagnosticZipBlob,
} from '../utils/diagnostic-archive';

const ISSUE_ID = 'application_diagnostic';
const COLLECT_TIMEOUT_MS = 300000;

export default Ember.Service.extend({
  hosts: Ember.inject.service('hosts'),
  env: Ember.inject.service('env'),

  buildCollectUrl(appId) {
    const host = this.get('hosts.rmWebAddress');
    const namespace = this.get('env.app.namespaces.cluster');
    const base = `${host}/${namespace}/common-issues/collect`;
    const query = `issueId=${encodeURIComponent(ISSUE_ID)}&args=${encodeURIComponent(appId)}`;
    return `${base}?${query}`;
  },

  collectApplicationDiagnostic(appId) {
    const url = this.buildCollectUrl(appId);

    return new Ember.RSVP.Promise((resolve, reject) => {
      Ember.$.ajax({
        url,
        method: 'GET',
        dataType: 'json',
        timeout: COLLECT_TIMEOUT_MS,
        crossDomain: true,
        xhrFields: {
          withCredentials: true
        },
        headers: {
          Accept: 'application/json'
        },
        success: (payload) => resolve(payload),
        error: (jqXHR) => reject(this.parseAjaxError(jqXHR))
      });
    });
  },

  parseAjaxError(jqXHR) {
    let message = 'Failed to collect application diagnostics.';

    if (jqXHR.responseJSON && jqXHR.responseJSON.RemoteException) {
      message = jqXHR.responseJSON.RemoteException.message || message;
      return new Error(message);
    }

    if (jqXHR.responseText) {
      try {
        const parsed = JSON.parse(jqXHR.responseText);
        if (parsed.RemoteException && parsed.RemoteException.message) {
          return new Error(parsed.RemoteException.message);
        }
      } catch (e) {
        // fall through to status-based message
      }
    }

    if (jqXHR.status === 0) {
      return new Error('Not able to connect to the Resource Manager.');
    }

    if (jqXHR.status) {
      return new Error(`HTTP ${jqXHR.status}: ${jqXHR.statusText || message}`);
    }

    return new Error(message);
  },

  downloadDiagnosticArchive(appId, issueData) {
    const generatedAt = new Date().toISOString();
    const folderName = buildBundleFolderName(generatedAt, appId, ISSUE_ID);
    const entries = buildDiagnosticArchiveEntries(folderName, issueData);
    const blob = buildDiagnosticZipBlob(folderName, entries);
    const objectUrl = URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = objectUrl;
    anchor.download = `${folderName}.zip`;
    document.body.appendChild(anchor);
    anchor.click();
    document.body.removeChild(anchor);
    URL.revokeObjectURL(objectUrl);
  }
});
