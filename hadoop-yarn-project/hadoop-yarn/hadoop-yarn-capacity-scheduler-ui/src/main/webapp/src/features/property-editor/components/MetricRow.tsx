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


import type { ReactNode } from 'react';

export type MetricRowProps = {
  label: string;
  value: ReactNode;
  tooltip?: string;
};

/**
 * MetricRow component for displaying label-value pairs consistently
 * Used in queue overview and info tabs
 */
export const MetricRow = ({ label, value, tooltip }: MetricRowProps) => {
  return (
    <div className="flex items-center justify-between py-1.5 text-sm">
      <span className="text-muted-foreground" title={tooltip}>
        {label}:
      </span>
      <span className="font-medium text-foreground">{value}</span>
    </div>
  );
};
