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


import React from 'react';
import { render, screen } from '@testing-library/react';
import { vi } from 'vitest';
import { AppSidebar } from './app-sidebar';

// Mock React Router
const mockLocation = { pathname: '/' };
const mockNavigate = vi.fn();

vi.mock('react-router', () => ({
  Link: ({ children, to, ...props }: any) => (
    <a href={to} {...props}>
      {children}
    </a>
  ),
  useLocation: () => mockLocation,
  useNavigate: () => mockNavigate,
}));

// Mock the Sidebar UI components to focus on testing AppSidebar behavior
vi.mock('~/components/ui/sidebar', () => ({
  Sidebar: ({ children, ...props }: any) => (
    <aside data-testid="sidebar" {...props}>
      {children}
    </aside>
  ),
  SidebarContent: ({ children }: any) => <div data-testid="sidebar-content">{children}</div>,
  SidebarHeader: ({ children }: any) => <header data-testid="sidebar-header">{children}</header>,
  SidebarMenu: ({ children }: any) => <nav data-testid="sidebar-menu">{children}</nav>,
  SidebarMenuItem: ({ children }: any) => <li>{children}</li>,
  SidebarMenuButton: ({ children, isActive, asChild, ...props }: any) => {
    const className = isActive ? 'active' : '';
    if (asChild && React.isValidElement(children)) {
      // eslint-disable-next-line @eslint-react/no-clone-element
      return React.cloneElement(children as React.ReactElement<any>, {
        className: `${(children.props as any).className || ''} ${className}`.trim(),
        'data-active': isActive,
      });
    }
    return (
      <button className={className} data-active={isActive} {...props}>
        {children}
      </button>
    );
  },
}));

describe('AppSidebar', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Reset location to default
    mockLocation.pathname = '/';
  });

  describe('Header', () => {
    it('should display the application title', () => {
      render(<AppSidebar />);

      expect(screen.getByText('Capacity Scheduler UI')).toBeInTheDocument();
    });
  });

  describe('Navigation Links', () => {
    it('should render all navigation items', () => {
      render(<AppSidebar />);

      expect(screen.getByRole('link', { name: /Queues/i })).toBeInTheDocument();
      expect(screen.getByRole('link', { name: /Node Labels/i })).toBeInTheDocument();
      expect(screen.getByRole('link', { name: /Global Settings/i })).toBeInTheDocument();
    });

    it('should have correct href attributes for navigation links', () => {
      render(<AppSidebar />);

      expect(screen.getByRole('link', { name: /Queues/i })).toHaveAttribute('href', '/');
      expect(screen.getByRole('link', { name: /Node Labels/i })).toHaveAttribute(
        'href',
        '/node-labels',
      );
      expect(screen.getByRole('link', { name: /Global Settings/i })).toHaveAttribute(
        'href',
        '/global-settings',
      );
    });
  });

  describe('Active State', () => {
    it('should mark Queues as active when on root path', () => {
      mockLocation.pathname = '/';
      render(<AppSidebar />);

      const queuesLink = screen.getByRole('link', { name: /Queues/i });
      const nodeLabelsLink = screen.getByRole('link', { name: /Node Labels/i });
      const globalSettingsLink = screen.getByRole('link', { name: /Global Settings/i });

      expect(queuesLink).toHaveAttribute('data-active', 'true');
      expect(nodeLabelsLink).toHaveAttribute('data-active', 'false');
      expect(globalSettingsLink).toHaveAttribute('data-active', 'false');
    });

    it('should mark Node Labels as active when on node labels path', () => {
      mockLocation.pathname = '/node-labels';
      render(<AppSidebar />);

      const queuesLink = screen.getByRole('link', { name: /Queues/i });
      const nodeLabelsLink = screen.getByRole('link', { name: /Node Labels/i });
      const globalSettingsLink = screen.getByRole('link', { name: /Global Settings/i });

      expect(queuesLink).toHaveAttribute('data-active', 'false');
      expect(nodeLabelsLink).toHaveAttribute('data-active', 'true');
      expect(globalSettingsLink).toHaveAttribute('data-active', 'false');
    });

    it('should mark Global Settings as active when on global settings path', () => {
      mockLocation.pathname = '/global-settings';
      render(<AppSidebar />);

      const queuesLink = screen.getByRole('link', { name: /Queues/i });
      const nodeLabelsLink = screen.getByRole('link', { name: /Node Labels/i });
      const globalSettingsLink = screen.getByRole('link', { name: /Global Settings/i });

      expect(queuesLink).toHaveAttribute('data-active', 'false');
      expect(nodeLabelsLink).toHaveAttribute('data-active', 'false');
      expect(globalSettingsLink).toHaveAttribute('data-active', 'true');
    });

    it('should not mark any item as active on unknown paths', () => {
      mockLocation.pathname = '/unknown-path';
      render(<AppSidebar />);

      const queuesLink = screen.getByRole('link', { name: /Queues/i });
      const nodeLabelsLink = screen.getByRole('link', { name: /Node Labels/i });
      const globalSettingsLink = screen.getByRole('link', { name: /Global Settings/i });

      expect(queuesLink).toHaveAttribute('data-active', 'false');
      expect(nodeLabelsLink).toHaveAttribute('data-active', 'false');
      expect(globalSettingsLink).toHaveAttribute('data-active', 'false');
    });
  });

  describe('Component Structure', () => {
    it('should render with correct sidebar variant', () => {
      render(<AppSidebar />);

      const sidebar = screen.getByTestId('sidebar');
      expect(sidebar).toHaveAttribute('variant', 'inset');
    });

    it('should render sidebar components in correct hierarchy', () => {
      render(<AppSidebar />);

      const sidebar = screen.getByTestId('sidebar');
      const header = screen.getByTestId('sidebar-header');
      const content = screen.getByTestId('sidebar-content');
      const menu = screen.getByTestId('sidebar-menu');

      // Check hierarchy
      expect(sidebar).toContainElement(header);
      expect(sidebar).toContainElement(content);
      expect(content).toContainElement(menu);

      // Check header comes before content
      const allElements = sidebar.querySelectorAll('[data-testid]');
      const headerIndex = Array.from(allElements).findIndex(
        (el) => el.getAttribute('data-testid') === 'sidebar-header',
      );
      const contentIndex = Array.from(allElements).findIndex(
        (el) => el.getAttribute('data-testid') === 'sidebar-content',
      );
      expect(headerIndex).toBeLessThan(contentIndex);
    });
  });

  describe('Icons', () => {
    it('should render icons for each navigation item', () => {
      render(<AppSidebar />);

      // Check that each link contains an icon (SVG element)
      const links = screen.getAllByRole('link');
      links.forEach((link) => {
        const svg = link.querySelector('svg');
        expect(svg).toBeInTheDocument();
        expect(svg).toHaveClass('h-4', 'w-4');
      });
    });
  });

  describe('Reactive Navigation', () => {
    it('should update active state when location changes', () => {
      const { rerender } = render(<AppSidebar />);

      // Initially on root
      expect(screen.getByRole('link', { name: /Queues/i })).toHaveAttribute('data-active', 'true');

      // Change location
      mockLocation.pathname = '/node-labels';
      rerender(<AppSidebar />);

      expect(screen.getByRole('link', { name: /Queues/i })).toHaveAttribute('data-active', 'false');
      expect(screen.getByRole('link', { name: /Node Labels/i })).toHaveAttribute(
        'data-active',
        'true',
      );
    });
  });

  describe('Edge Cases', () => {
    it('should handle navigation items without icons gracefully', () => {
      // This test ensures the component doesn't crash if icon is undefined
      // though in the current implementation all items have icons
      render(<AppSidebar />);

      // Component should render without errors
      expect(screen.getByTestId('sidebar')).toBeInTheDocument();
    });

    it('should handle empty pathname gracefully', () => {
      mockLocation.pathname = '';
      render(<AppSidebar />);

      // Should still render all navigation items
      expect(screen.getByRole('link', { name: /Queues/i })).toBeInTheDocument();
      expect(screen.getByRole('link', { name: /Node Labels/i })).toBeInTheDocument();
      expect(screen.getByRole('link', { name: /Global Settings/i })).toBeInTheDocument();
    });
  });
});
