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


import { HydratedRouter } from 'react-router/dom';
import { startTransition, StrictMode } from 'react';
import { hydrateRoot } from 'react-dom/client';
import { API_CONFIG } from '~/lib/api/config';

async function enableMocking() {
  if (!import.meta.env.DEV) {
    return;
  }

  if (API_CONFIG.mockMode !== 'static') {
    return;
  }

  const { worker } = await import('~/lib/api/mocks/browser');

  // Start the worker
  return worker.start({
    onUnhandledRequest: 'bypass',
    serviceWorker: {
      url: '/scheduler-ui/mockServiceWorker.js',
    },
  });
}

enableMocking().then(() => {
  // Handle servlet welcome-file redirect to index.html
  // React Router doesn't need index.html in the URL, so redirect without it
  if (window.location.pathname.endsWith('/index.html')) {
    const newPath = window.location.pathname.replace(/\/index\.html$/, '/');
    window.history.replaceState(null, '', newPath + window.location.search + window.location.hash);
  }

  startTransition(() => {
    hydrateRoot(
      document,
      <StrictMode>
        <HydratedRouter />
      </StrictMode>,
    );
  });
});
