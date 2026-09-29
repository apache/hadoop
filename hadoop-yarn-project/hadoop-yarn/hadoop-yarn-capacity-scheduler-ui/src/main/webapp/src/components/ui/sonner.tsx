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


import { Toaster as Sonner } from 'sonner';

export function Toaster() {
  return (
    <Sonner
      position="top-center"
      className="toaster group"
      toastOptions={{
        classNames: {
          toast:
            'group toast group-[.toaster]:bg-background group-[.toaster]:text-foreground group-[.toaster]:border-border group-[.toaster]:shadow-xl group-[.toaster]:rounded-xl',
          description: 'group-[.toast]:text-muted-foreground',
          actionButton:
            'group-[.toast]:bg-primary group-[.toast]:text-primary-foreground group-[.toast]:shadow-md group-[.toast]:font-medium',
          cancelButton:
            'group-[.toast]:bg-muted group-[.toast]:text-muted-foreground group-[.toast]:font-medium',
          success:
            'group-[.toaster]:border-l-4 group-[.toaster]:border-l-green-500 group-[.toaster]:bg-gradient-to-r group-[.toaster]:from-green-50 group-[.toaster]:to-background dark:group-[.toaster]:from-green-950/30',
          error:
            'group-[.toaster]:border-l-4 group-[.toaster]:border-l-destructive group-[.toaster]:bg-gradient-to-r group-[.toaster]:from-red-50 group-[.toaster]:to-background dark:group-[.toaster]:from-red-950/30',
          warning:
            'group-[.toaster]:border-l-4 group-[.toaster]:border-l-amber-500 group-[.toaster]:bg-gradient-to-r group-[.toaster]:from-amber-50 group-[.toaster]:to-background dark:group-[.toaster]:from-amber-950/30',
          info: 'group-[.toaster]:border-l-4 group-[.toaster]:border-l-blue-500 group-[.toaster]:bg-gradient-to-r group-[.toaster]:from-blue-50 group-[.toaster]:to-background dark:group-[.toaster]:from-blue-950/30',
        },
      }}
    />
  );
}
