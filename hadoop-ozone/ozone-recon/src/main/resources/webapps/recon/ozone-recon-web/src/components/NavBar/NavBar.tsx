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
import logo from '../../logo.png';
import { Layout, Menu, Icon } from 'antd';
import './NavBar.less';
import { withRouter, Link } from 'react-router-dom';
import { RouteComponentProps } from 'react-router';
const { Sider } = Layout;

interface NavBarProps extends RouteComponentProps<any> {
  collapsed: boolean;
  onCollapse: (arg: boolean) => void;
}

class NavBar extends React.Component<NavBarProps> {
  render() {
    const {location} = this.props;
    return (
        <Sider
            collapsible
            collapsed={this.props.collapsed}
            collapsedWidth={50}
            onCollapse={this.props.onCollapse}
            style={{
              overflow: 'auto', height: '100vh', position: 'fixed', left: 0,
            }}
        >
          <div className="logo">
            <img src={logo} alt="Ozone Recon Logo" width={32} height={32}/>
            <span className="logo-text">Ozone Recon</span>
          </div>
          <Menu theme="dark" defaultSelectedKeys={['/Dashboard']}
                mode="inline" selectedKeys={[location.pathname]}>
            <Menu.Item key="/Dashboard">
              <Icon type="dashboard"/>
              <span>Dashboard</span>
              <Link to="/Dashboard"/>
            </Menu.Item>
            <Menu.Item key="/ContainerBrowser">
              <Icon type="file-search"/>
              <span>Container Browser</span>
              <Link to="/ContainerBrowser"/>
            </Menu.Item>
          </Menu>
        </Sider>
    );
  }
}

export default withRouter<NavBarProps>(NavBar);
