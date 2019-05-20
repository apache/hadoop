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
import { Breadcrumb } from 'antd';
import { withRouter, Link } from 'react-router-dom';
import { RouteComponentProps } from 'react-router';
import { breadcrumbNameMap } from '../../constants/breadcrumbs.constants';

interface Props extends RouteComponentProps<any> {
  collapsed: boolean;
  onCollapse: (arg: boolean) => void;
}

class Breadcrumbs extends React.Component<RouteComponentProps> {

  render() {
    const { location } = this.props;
    const pathSnippets = location.pathname.split('/').filter(i => i);
    const extraBreadcrumbItems = pathSnippets.map((_, index) => {
      const url = `/${pathSnippets.slice(0, index + 1).join('/')}`;
      return (
          <Breadcrumb.Item key={url}>
            <Link to={url}>
              {breadcrumbNameMap[url]}
            </Link>
          </Breadcrumb.Item>
      );
    });
    const breadcrumbItems = [(
        <Breadcrumb.Item key="home">
          <Link to="/">Home</Link>
        </Breadcrumb.Item>
    )].concat(extraBreadcrumbItems);
    return (
        <Breadcrumb>
          {breadcrumbItems}
        </Breadcrumb>
    );
  }
}

export default withRouter<RouteComponentProps>(Breadcrumbs);
