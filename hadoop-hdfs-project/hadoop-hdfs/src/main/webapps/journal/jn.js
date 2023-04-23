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
(function () {
    "use strict";

    var data = {};

    dust.loadSource(dust.compile($('#tmpl-jn').html(), 'jn'));

    var BEANS = [
        {"name": "jn",      "url": "/jmx?qry=Hadoop:service=JournalNode,name=JournalNodeInfo"},
        {"name": "journals", "url": "/jmx?qry=Hadoop:service=JournalNode,name=Journal-*"}

    ];

    var HELPERS = {
        'helper_date_tostring' : function (chunk, ctx, bodies, params) {
            var value = dust.helpers.tap(params.value, chunk, ctx);
            return chunk.write('' + moment(Number(value)).format('ddd MMM DD HH:mm:ss ZZ YYYY'));
        }
    };

    load_json(
        BEANS,
        guard_with_startup_progress(function(d) {
            for (var k in d) {
                data[k] = k === 'journals' ? workaround(d[k].beans) : d[k].beans[0];
            }
            render();
        }),
        function (url, jqxhr, text, err) {
            show_err_msg('<p>Failed to retrieve data from ' + url + ', cause: ' + err + '</p>');
        });

    function guard_with_startup_progress(fn) {
        return function() {
            try {
                fn.apply(this, arguments);
            } catch (err) {
                if (err instanceof TypeError) {
                    show_err_msg('JournalNode error: ' + err);
                }
            }
        };
    }

    function workaround(journals) {
        for (var i in journals){
            var str= journals[i]['modelerType'];
            var index= str.indexOf("-");
            journals[i]['NameService']= str.substr(index + 1);
        }

        return journals;
    }

    function render() {
        var base = dust.makeBase(HELPERS);
        dust.render('jn', base.push(data), function(err, out) {
            $('#tab-overview').html(out);
            $('#tab-overview').addClass('active');
        });
    }

    function show_err_msg() {
        $('#alert-panel-body').html("Failed to load journalnode information");
        $('#alert-panel').show();
    }
})();
