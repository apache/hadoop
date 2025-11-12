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


import { Links, Meta, Outlet, Scripts, ScrollRestoration } from 'react-router';
import { Toaster } from '~/components/ui/sonner';
import { ThemeProvider } from '~/components/providers/theme-provider';
import { ValidationProvider } from '~/contexts/ValidationContext';

import './app.css';

// eslint-disable-next-line react-refresh/only-export-components
export { links } from './root.links';

export function Layout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <meta charSet="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <Meta />
        <Links />
      </head>
      <body suppressHydrationWarning>
        <ThemeProvider defaultTheme="system" storageKey="yarn-scheduler-theme">
          {children}
          <Toaster />
        </ThemeProvider>
        <ScrollRestoration />
        <Scripts />
      </body>
    </html>
  );
}

export default function App() {
  return (
    <ValidationProvider>
      <Outlet />
    </ValidationProvider>
  );
}

export function HydrateFallback() {
  return (
    <div className="flex h-screen items-center justify-center bg-background">
      <div className="text-center space-y-4">
        <div
          className="inline-block h-8 w-8 animate-spin rounded-full border-4 border-solid border-primary border-r-transparent align-[-0.125em] motion-reduce:animate-[spin_1.5s_linear_infinite]"
          role="status"
        >
          <span className="sr-only">Loading...</span>
        </div>
        <p className="text-sm text-muted-foreground">
          Loading YARN Capacity Scheduler UI...
        </p>
      </div>
    </div>
  );
}
