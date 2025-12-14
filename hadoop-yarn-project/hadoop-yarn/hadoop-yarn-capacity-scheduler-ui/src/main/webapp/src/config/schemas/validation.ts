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


import { z } from 'zod';
import { SPECIAL_VALUES } from '~/types';

export const capacityValueSchema = z.string().refine(
  (value) => {
    if (!value.trim()) return true;

    const trimmedValue = value.trim();

    if (trimmedValue.endsWith('%')) {
      const numericPart = trimmedValue.slice(0, -1);
      const numericValue = parseFloat(numericPart);
      return !isNaN(numericValue) && numericValue >= 0 && numericValue <= 100;
    }

    if (trimmedValue.endsWith('w')) {
      const numericPart = trimmedValue.slice(0, -1);
      const numericValue = parseFloat(numericPart);
      return !isNaN(numericValue) && numericValue > 0;
    }

    if (trimmedValue.startsWith('[') && trimmedValue.endsWith(']')) {
      const resourcePart = trimmedValue.slice(1, -1);
      if (resourcePart.trim() === '') return false;

      const resourcePairs = resourcePart.split(',');
      return resourcePairs.every((pair) => {
        const [resource, val] = pair.split('=');
        return resource && val && !isNaN(parseFloat(val));
      });
    }

    const numericValue = parseFloat(trimmedValue);
    return !isNaN(numericValue) && numericValue >= 0 && numericValue <= 100;
  },
  {
    message:
      'Invalid capacity format. Use percentage (50), weight (2w), or absolute ([memory=1024,vcores=2])',
  },
);

export const percentageSchema = z.string().refine(
  (value) => {
    if (!value.trim()) return true;
    const numericValue = parseFloat(value);
    return !isNaN(numericValue) && numericValue >= 0 && numericValue <= 100;
  },
  { message: 'Must be a number between 0 and 100' },
);

export const positiveNumberSchema = z.string().refine(
  (value) => {
    if (!value.trim()) return true;
    const numericValue = parseFloat(value);
    return !isNaN(numericValue) && numericValue > 0;
  },
  { message: 'Must be a positive number' },
);

export const nonNegativeNumberSchema = z.string().refine(
  (value) => {
    if (!value.trim()) return true;
    const numericValue = parseFloat(value);
    return !isNaN(numericValue) && numericValue >= 0;
  },
  { message: 'Must be a non-negative number' },
);

export const integerSchema = z.string().refine(
  (value) => {
    if (!value.trim()) return true;
    const numericValue = parseFloat(value);
    return !isNaN(numericValue) && Number.isInteger(numericValue) && numericValue > 0;
  },
  { message: 'Must be a positive integer' },
);

export const aclFormatSchema = z.string().refine(
  (value) => {
    if (!value.trim()) return true;

    if (value === SPECIAL_VALUES.ALL_USERS_ACL || value === SPECIAL_VALUES.NO_USERS_ACL)
      return true;

    const parts = value.split(' ');
    if (parts.length > 2) return false;

    return parts.every((part) => {
      if (!part) return true;
      return part
        .split(',')
        .every((item) => item.trim().length > 0 && /^[a-zA-Z0-9_-]+$/.test(item.trim()));
    });
  },
  { message: 'Invalid ACL format. Use "user1,user2 group1,group2" or "*" or " " (space for none)' },
);
