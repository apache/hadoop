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


import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { QueueCapacityProgress } from './QueueCapacityProgress';

describe('QueueCapacityProgress', () => {
  it('renders usage header and default formatter', () => {
    render(
      <QueueCapacityProgress capacity={60} maxCapacity={80} usedCapacity={70} className="test" />,
    );

    expect(screen.getByText('Live Resource Usage')).toBeInTheDocument();
    expect(screen.getByText('70.0% used')).toBeInTheDocument();
  });

  it('shows capacity and max capacity markers with distinct styling', () => {
    render(<QueueCapacityProgress capacity={40} maxCapacity={85} usedCapacity={92} showHeader />);

    const capacityMarker = screen.getByText('40%');
    const maxCapacityMarker = screen.getByText('85%');
    expect(capacityMarker).toBeInTheDocument();
    expect(maxCapacityMarker).toBeInTheDocument();

    const usageBar = document.querySelector('.bg-destructive');
    expect(usageBar).not.toBeNull();
  });
});
