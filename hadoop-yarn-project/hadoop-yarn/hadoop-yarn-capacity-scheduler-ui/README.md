<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->


# Apache Hadoop YARN Capacity Scheduler UI

A modern React-based web interface for managing and configuring the YARN Capacity Scheduler.

## Overview

The YARN Capacity Scheduler UI provides an intuitive graphical interface for:
- **Queue Management**: Visual queue hierarchy with drag-and-drop organization
- **Capacity Configuration**: Interactive capacity allocation and resource management
- **Node Labels**: Partition management and node-to-label assignments
- **Placement Rules**: Configure application placement policies
- **Global Settings**: Manage scheduler-wide configuration parameters
- **Configuration Validation**: Real-time validation with helpful error messages
- **Change Staging**: Review and apply configuration changes in batches

## Technology Stack

- **Framework**: React 19 with TypeScript (strict mode)
- **Build Tool**: Vite 6.4 with React Router v7 (SPA mode)
- **UI Components**: shadcn/ui, Radix UI, Tailwind CSS
- **State Management**: Zustand with Immer
- **Visualization**: XYFlow (React Flow) for queue hierarchy
- **Testing**: Vitest + React Testing Library

## Building

### Prerequisites

- **Maven**: 3.6.0 or later
- **Node.js**: 22.16.0+ (automatically installed by Maven during build)
- **Java**: JDK 8 or later

### Maven Build

Build the UI as part of the YARN build using the `yarn-ui` profile:

```bash
# From hadoop-yarn directory
cd hadoop-yarn-project/hadoop-yarn
mvn clean package -Pyarn-ui

# From hadoop-yarn-capacity-scheduler-ui directory
cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-capacity-scheduler-ui
mvn clean package -Pyarn-ui
```

This will:
1. Install Node.js 22.16.0 and npm locally in `target/webapp/node/`
2. Install npm dependencies
3. Build the React application
4. Package everything into a WAR file at `target/hadoop-yarn-capacity-scheduler-ui-*.war`

### Build Output

The build creates:
- `target/webapp/build/client/` - Built React application (static files)
- `target/hadoop-yarn-capacity-scheduler-ui-*.war` - Deployable WAR file

## Development

### Local Development Setup

```bash
cd src/main/webapp

# Install dependencies
npm install

# Start development server
npm run dev
```

The development server runs at `http://localhost:5173` with hot module replacement.

### Environment Variables

Create a `.env` file in `src/main/webapp/` based on `.env.example`:

```bash
# Mock mode: "static" (use mock data), "cluster" (proxy to real cluster), "off" (no mocking)
VITE_API_MOCK_MODE=static

# YARN ResourceManager URL (required when VITE_API_MOCK_MODE=cluster)
VITE_CLUSTER_PROXY_TARGET=http://localhost:8088

# Development flags
VITE_READONLY_MODE=false
VITE_YARN_USER_NAME=admin
```

### Available Scripts

```bash
npm run dev           # Start development server
npm run build         # Production build
npm run start         # Preview production build
npm run test          # Run tests
npm run test:ui       # Run tests with UI
npm run test:coverage # Generate test coverage report
npm run lint          # Lint code
npm run lint:fix      # Fix linting issues
npm run format        # Format code with Prettier
```

### Running Tests

```bash
# Run all tests
npm test

# Run tests in watch mode
npm run test

# Run tests with coverage
npm run test:coverage

# Run tests with UI
npm run test:ui
```

## Deployment

### WAR Deployment

The built WAR file can be deployed to a servlet container (Tomcat, Jetty, etc.) or integrated into the YARN ResourceManager web application.

### API Endpoints

The UI requires access to the following YARN ResourceManager REST API endpoints:

- `GET /ws/v1/cluster/scheduler` - Get scheduler configuration
- `PUT /ws/v1/cluster/scheduler-conf` - Update scheduler configuration
- `GET /ws/v1/cluster/scheduler-conf` - Get mutable configuration
- `GET /ws/v1/cluster/nodes` - Get cluster nodes
- `GET /conf` - Get Hadoop configuration

### Integration with ResourceManager

When deployed alongside the ResourceManager, the UI can access these endpoints directly. The `web.xml` includes SPA routing configuration to handle client-side routing.

## Architecture

### Directory Structure

```
src/main/webapp/
├── src/
│   ├── app/              # React Router application setup
│   ├── components/       # Reusable UI components
│   ├── config/          # Scheduler configuration schemas
│   ├── contexts/        # React contexts
│   ├── features/        # Feature modules
│   │   ├── global-settings/
│   │   ├── node-labels/
│   │   ├── placement-rules/
│   │   ├── property-editor/
│   │   ├── queue-management/
│   │   ├── staged-changes/
│   │   ├── template-config/
│   │   └── validation/
│   ├── hooks/           # Custom React hooks
│   ├── lib/             # API client, utilities
│   ├── stores/          # Zustand stores
│   ├── types/           # TypeScript type definitions
│   └── utils/           # Helper functions
├── public/              # Static assets
└── WEB-INF/            # Web application descriptor
```

### State Management

The application uses Zustand for global state management with the following stores:

- **SchedulerStore**: Manages scheduler configuration and queue hierarchy
- **NodeLabelStore**: Handles node labels and partitions
- **StagedChangesStore**: Tracks pending configuration changes
- **ValidationStore**: Manages validation state and error messages

## Configuration

### Build Profiles

- **yarn-ui**: Production build (default profile in this module)
- **yarn-ui-dev**: Development build (includes mock data in WAR)

### Maven Properties

- `packagingType`: WAR when profile is active, POM otherwise
- `webappDir`: Build directory (`${basedir}/target/webapp`)
- `nodeExecutable`: Path to locally installed Node.js
- `keepUIBuildCache`: Set to `true` to preserve node_modules between builds

## Browser Support

- Chrome/Edge: Latest 2 versions
- Firefox: Latest 2 versions
- Safari: Latest 2 versions

## Contributing

This module follows Apache Hadoop's contribution guidelines. All source files must include the Apache License header.

### Code Style

- TypeScript strict mode enabled
- ESLint for code quality
- Prettier for code formatting
- Conventional commit messages

## Developer Documentation

Detailed guides for extending and customizing the UI:

- **[Design Document](src/main/webapp/docs/design_doc.md)** - Architecture, design decisions, and technical specifications
- **[Adding Validation Rules](src/main/webapp/docs/development/adding-validation-rules.md)** - Guide for implementing custom validation rules
- **[Extending Scheduler Properties](src/main/webapp/docs/development/extending-scheduler-properties.md)** - Instructions for adding new configuration properties

## License

Licensed under the Apache License, Version 2.0. See the LICENSE file in the Hadoop root directory.

## Related Documentation

- [YARN Capacity Scheduler Documentation](../hadoop-yarn-site/src/site/markdown/CapacityScheduler.md)
- [YARN REST API Documentation](../hadoop-yarn-site/src/site/markdown/ResourceManagerRest.md)
- [Hadoop Main Documentation](https://hadoop.apache.org/docs/current/)
