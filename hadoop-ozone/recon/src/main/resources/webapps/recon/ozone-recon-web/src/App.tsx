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

import { Layout } from 'antd';
import './App.less';
import NavBar from './components/NavBar/NavBar';
import Breadcrumbs from './components/Breadcrumbs/Breadcrumbs';
import { BrowserRouter as Router, Switch, Route, Redirect } from 'react-router-dom';
import { routes } from './routes';
import { MakeRouteWithSubRoutes } from './makeRouteWithSubRoutes';

const classNames = require('classnames');
const {
  Header, Content, Footer
} = Layout;

interface Props {
}

interface State {
  collapsed: boolean;
}

class App extends React.Component<Props, State>  {

  constructor(props: Props) {
    super(props);

    this.state = {collapsed: false};
  }

  onCollapse = (collapsed: boolean) => {
    this.setState({ collapsed });
  };

  render() {
    const { collapsed } = this.state;
    const layoutClass = classNames('content-layout', {'sidebar-collapsed': collapsed});

    return (
      <Router>
        <Layout style={{ minHeight: '100vh' }}>
          <NavBar collapsed={collapsed} onCollapse={this.onCollapse}/>
          <Layout className={layoutClass}>
            <Header>
              <div style={{ margin: '16px 0' }}>
                <Breadcrumbs/>
              </div>
            </Header>
            <Content style={{ margin: '0 16px 0', overflow: 'initial' }}>
              <Switch>
                <Route exact path="/">
                  <Redirect to="/Dashboard"/>
                </Route>
                {
                  routes.map(
                      (route, index) => <MakeRouteWithSubRoutes key={index} {...route} />
                  )
                }
              </Switch>
            </Content>
            <Footer style={{ textAlign: 'center' }}>
            </Footer>
          </Layout>
        </Layout>
      </Router>
    );
  }
}

export default App;
