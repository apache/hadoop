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

import { describe, it, expect } from 'vitest';
import { render, screen } from '~/testing/setup/setup';
import { InheritedValueIndicator } from '../PropertyFieldHelpers';

describe('InheritedValueIndicator', () => {
  it('renders nothing when inheritanceInfo is null', () => {
    const { container } = render(<InheritedValueIndicator inheritanceInfo={null} />);
    expect(container).toBeEmptyDOMElement();
  });

  it('shows parent queue source with value', () => {
    render(
      <InheritedValueIndicator
        inheritanceInfo={{ value: 'true', source: 'queue', sourcePath: 'root.production' }}
      />,
    );
    expect(screen.getByText(/inherited from/i)).toBeInTheDocument();
    expect(screen.getByText(/root\.production/)).toBeInTheDocument();
  });

  it('shows global default source', () => {
    render(
      <InheritedValueIndicator inheritanceInfo={{ value: '100', source: 'global' }} />,
    );
    expect(screen.getByText(/global default/i)).toBeInTheDocument();
  });

  it('shows "overrides" message when field has explicit value', () => {
    render(
      <InheritedValueIndicator
        inheritanceInfo={{ value: 'true', source: 'queue', sourcePath: 'root.production' }}
        hasExplicitValue
      />,
    );
    expect(screen.getByText(/overrides/i)).toBeInTheDocument();
    expect(screen.getByText(/root\.production/)).toBeInTheDocument();
  });

  it('shows scaled message for scaled-from-global properties', () => {
    render(
      <InheritedValueIndicator
        inheritanceInfo={{ value: '500', source: 'queue', sourcePath: 'root.production', isScaled: true }}
      />,
    );
    expect(screen.getByText(/root\.production/)).toBeInTheDocument();
    expect(screen.getByText(/scaled by queue capacity/i)).toBeInTheDocument();
  });

  it('shows scaled override message when explicit and scaled-from-global', () => {
    render(
      <InheritedValueIndicator
        inheritanceInfo={{ value: '500', source: 'queue', sourcePath: 'root.production', isScaled: true }}
        hasExplicitValue
      />,
    );
    expect(screen.getByText(/overrides/i)).toBeInTheDocument();
    expect(screen.getByText(/scaled by queue capacity/i)).toBeInTheDocument();
  });
});
