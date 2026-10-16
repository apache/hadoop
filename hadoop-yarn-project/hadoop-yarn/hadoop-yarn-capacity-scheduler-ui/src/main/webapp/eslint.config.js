/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/


import js from '@eslint/js';
import globals from 'globals';
import eslintReact from '@eslint-react/eslint-plugin';
import reactHooks from 'eslint-plugin-react-hooks';
import reactRefresh from 'eslint-plugin-react-refresh';
import tseslint from 'typescript-eslint';
import prettierConfig from 'eslint-config-prettier';
import reactCompiler from 'eslint-plugin-react-compiler';

export default tseslint.config(
  // 1. Ignores
  {
    ignores: [
      'build',
      '.react-router',
      'node_modules',
      'coverage',
      '*.log',
      'eslint-results.sarif',
      'src/components/ui/**',
    ],
  },

  // 2. Recommended configs
  js.configs.recommended,
  ...tseslint.configs.recommended,
  eslintReact.configs.recommended,

  // 3. TypeScript/React config object
  {
    files: ['**/*.{ts,tsx}'],
    languageOptions: {
      ecmaVersion: 2020,
      globals: globals.browser,
      parserOptions: {
        ecmaFeatures: { jsx: true },
      },
    },
    plugins: {
      '@typescript-eslint': tseslint.plugin,
      'react-hooks': reactHooks,
      'react-refresh': reactRefresh,
      'react-compiler': reactCompiler,
    },
    settings: {
      react: {
        version: 'detect',
      },
    },
    rules: {
      'react-compiler/react-compiler': 'error',

      // React hooks rules
      ...reactHooks.configs.recommended.rules,

      // React Refresh
      'react-refresh/only-export-components': ['warn', { allowConstantExport: true }],

      // General rules
      'no-console': ['warn', { allow: ['warn', 'error'] }],
      'no-debugger': 'warn',
      curly: ['error', 'all'],
      'prefer-const': ['warn', { destructuring: 'all' }],

      // TypeScript rules
      '@typescript-eslint/no-unused-vars': [
        'error',
        {
          vars: 'local',
          args: 'after-used',
          argsIgnorePattern: '^_',
          varsIgnorePattern: '^_|^[A-Z_][A-Z0-9_]*$|^[A-Z][a-zA-Z0-9]*$',
        },
      ],
      '@typescript-eslint/no-explicit-any': 'error',
      '@typescript-eslint/explicit-function-return-type': 'off',

      // Rules to disable from recommended configs
      '@eslint-react/jsx-uses-react': 'off',
      '@eslint-react/react-in-jsx-scope': 'off',
      '@eslint-react/prop-types': 'off',
      '@eslint-react/display-name': 'off',
      '@eslint-react/hooks-extra/no-direct-set-state-in-use-effect': 'off',
    },
  },

  // 4. Mock files config
  {
    files: ['**/mocks/**/*.{ts,tsx,js}'],
    rules: {
      'no-console': 'off',
    },
  },

  // 5. Test files config
  {
    files: ['**/*.test.{ts,tsx}', '**/*.spec.{ts,tsx}'],
    rules: {
      '@typescript-eslint/no-explicit-any': 'off',
    },
  },

  // 6. Prettier config
  prettierConfig,
);
