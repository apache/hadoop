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

function accordionJS(parent, selector){
  window.addEventListener("DOMContentLoaded", () => {
    window.removeEventListener("DOMContentLoaded", arguments.callee, false);
    if (parent.charAt(0) === "#"){
      parent = parent.substring(1);
    }
    let accordion = new Accordion(parent, {
      openTab: 1,
      oneOpen: true,
    }, selector);
  })
}

function getProgressbar(value) {
  let progressBarHTML = "";
  progressBarHTML += `<br title="${value}">`
  progressBarHTML += `<div class="ui-progressbar ui-widget ui-widget-content ui-corner-all" title="${value}">`
  progressBarHTML += `<div class="ui-progressbar ui-widget ui-widget-content ui-corner-all" style="width:${value}"></div>`
  progressBarHTML += "</div>"
  return progressBarHTML
}

function getTableHeadings(id) {
  let headings = [];
  window.addEventListener('DOMContentLoaded', () => {
    window.removeEventListener('DOMContentLoaded', arguments.callee, false);
    headings = [].slice.call(document.getElementById(id).tHead.rows[0].cells);
    return headings.map((x) => { return x.innerText });
  });
}

function DataTableHelper(dtSelector, opts, hasDate, headings) {
  function parseDateVals(data) {
    parsedData = Array.prototype.map.call(data, function(d){
      d[7] = renderHadoopDate(d[7], 'display', true);
      d[8] = renderHadoopDate(d[8], 'display', true);
      d[9] = renderHadoopDate(d[9], 'display', true);
      return d;
    })
    return parsedData;
  }
    // Ensure opts.data.data is always defined, even if empty
  opts.data = opts.data || { data: { data: [] }}
  opts.data.data = opts.data.data || [];
  if (!Array.isArray(opts.data.data)) {
    console.error('opts.data.data is not an array. DataTable initialization will fail.', opts.data);
    return;
  }
  if (!opts.data || !opts.data.data) {
      console.error('opts.data.data is not defined correctly or is empty. DataTable initialization will fail.', opts.data);
      return;
    }

  // Set th as date types
  // Parse date values if data is not empty
  if (Array.isArray(opts.data.data) && opts.data.data.length && hasDate) {
    opts.data.data = parseDateVals(opts.data.data);
  }
  opts["headings"] = headings;
  opts["searchable"] = true;
  dtElem = null;
  document.addEventListener('DOMContentLoaded', () => {
    window.removeEventListener('DOMContentLoaded', arguments.callee, false);
    dtElem = document.querySelector(dtSelector);
    dtElem.classList.add("dataTable");
    dtElem.classList.add("no-footer");
    dtElem.setAttribute('aria-describedby', 'apps_info')
    dtElem.style.width = `${window.screen.width} px`;
    const dataTableInstance = new DataTable(dtElem, opts);

if (!opts.data.data.length) {
      const columnCount = dtElem.querySelectorAll('th').length || 1;
      dtElem.querySelector('tbody').innerHTML = `<tr><td colspan="${columnCount}" class="dataTables_empty" style="text-align: center;">No data available in table</td></tr>`;
    } else {
      dataTableInstance.update();
    }
    return dataTableInstance;
  });
}