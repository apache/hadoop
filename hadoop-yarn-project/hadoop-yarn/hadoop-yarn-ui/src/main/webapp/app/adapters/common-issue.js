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


import RESTAbstractAdapter from './restabstract';

export default RESTAbstractAdapter.extend({
    address: "rmWebAddress",
    restNameSpace: "cluster",
    serverName: "RM",

    // Call, store.findAll('common-issue')
    urlForFindAll(){
        var url = this._buildURL();
        url = url + '/common-issues/list';
        console.log('diagnostics url list: ', url);
        return url;

    },

    urlForQuery(query){
        let url = this._buildURL();

        if(query.issueId && query.appId){
            let finalUrl = `${url}/common-issues/collect?issueId=${query.issueId}&args=${query.appId}`;
            console.log('diagnostics url problems: ', finalUrl);
            delete query.issueId;
            delete query.appId;

            return finalUrl;
        }

        return `${url}/common-issues`;

    }

})