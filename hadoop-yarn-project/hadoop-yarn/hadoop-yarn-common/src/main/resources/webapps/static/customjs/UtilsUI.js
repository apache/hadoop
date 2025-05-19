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

function accordionJS(parent, selector) {
  window.addEventListener("DOMContentLoaded", () => {
    window.removeEventListener("DOMContentLoaded", arguments.callee, false);
    if (parent.charAt(0) === "#") {
      parent = parent.substring(1);
    }
    let accordion = new Accordion(
      parent,
      {
        openTab: 1,
        oneOpen: true,
      },
      selector
    );
  });
}

function getProgressbar(percent) {
  return `
    <br title='${percent}'>
    <div class='ui-progressbar ui-widget ui-widget-content ui-corner-all' title='${percent}%'>
      <div class='ui-progressbar-value ui-widget-header ui-corner-left' style='width:${percent}%'> </div>
    </div>
  `;
}

function getTableHeadings(id) {
  let headings = [];
  window.addEventListener("DOMContentLoaded", () => {
    window.removeEventListener("DOMContentLoaded", arguments.callee, false);
    headings = [].slice.call(document.getElementById(id).tHead.rows[0].cells);
    return headings.map((x) => {
      return x.innerText;
    });
  });
}

function DataTableHelper(dtSelector, opts, hasDate, headings) {

  // Ensure opts.data.data is always defined, even if empty
  opts = opts || {};
  opts.data = opts.data || { data: [] };
  if (!Array.isArray(opts.data.data)) {
    opts.data.data = [];
  }

  var tableAPI = {
    grid: null,
    originalData: JSON.parse(JSON.stringify(opts.data.data || [])),
    lengthDropdown: null,
    // Change page size
   changePageSize: function (newPageSize) {
  if (this.grid) {
    try {
      localStorage.setItem(`${dtSelector.replace(/[^a-zA-Z0-9]/g, "")}_pageSize`, newPageSize);

      // Save search state
      const searchInput = document.querySelector(`${dtSelector}_wrapper .gridjs-search input`);
      const searchValue = searchInput ? searchInput.value : '';
      const container = this.grid.config.container;

      let sortedData = [...this.originalData];

      // Apply natural sort on the first column in DESCENDING order
      sortedData.sort(function(a, b) {
        // Get first column values
        const aVal = a[0] || '';
        const bVal = b[0] || '';
        return naturalSort(String(bVal), String(aVal)); // Swap a and b for descending
      });

      // Create Grid.js config
      const newConfig = {
        columns: this.grid.config.columns,
        data: sortedData,
        search: this.grid.config.search,
        sort: true,
        pagination: {
          enabled: true,
          limit: newPageSize,
          summary: true
        },
        className: this.grid.config.className,
        language: this.grid.config.language
      };

      // Destroy and recreate Grid.js
      this.grid.destroy();
      this.grid = new gridjs.Grid(newConfig);
      this.grid.render(container);

      // Restore UI state
      setTimeout(() => {
        // Restore search
        const newSearchInput = document.querySelector(`${dtSelector}_wrapper .gridjs-search input`);
        if (newSearchInput && searchValue) {
          newSearchInput.value = searchValue;
          newSearchInput.dispatchEvent(new Event('input', { bubbles: true }));
        }

        // Recreate dropdown
        const headElement = document.querySelector(`${dtSelector}_wrapper .gridjs-head`);
        if (headElement) {
          const pageLengthContainer = document.createElement("div");
          pageLengthContainer.className = "dataTables_length";
          pageLengthContainer.id = dtSelector.substring(1) + "_length";

          const label = document.createElement("label");
          label.textContent = "Show ";

          const select = document.createElement("select");
          select.name = dtSelector.substring(1) + "_length";
          select.className = "dataTables_selector";

          [20, 40, 60, 80, 100].forEach((val) => {
            const option = document.createElement("option");
            option.value = val;
            option.textContent = val;
            option.selected = val === newPageSize;
            select.appendChild(option);
          });

          select.addEventListener("change", (e) => {
            e.stopPropagation();
            this.changePageSize(parseInt(e.target.value, 10));
          });

          select.addEventListener("click", (e) => e.stopPropagation());

          label.appendChild(select);
          label.appendChild(document.createTextNode(" entries"));
          pageLengthContainer.appendChild(label);

          headElement.insertBefore(pageLengthContainer, headElement.firstChild);
          this.lengthDropdown = pageLengthContainer;
        }

        // Restore other UI enhancements
        if (this.addFilteredInfo) {
          this.addFilteredInfo();
        }
      }, 100);
    } catch (e) {
      console.error("Error changing page size:", e);
    }
  }
  return this;
},
filterByColumn: function(columnIdentifier, value, useRegex = false, smart = true) {
  if (!this.grid) return this;

  // Get current data
  let filteredData = [...this.originalData];

  // Determine column index from name or use directly if it's a number
  let columnIndex = columnIdentifier;

  // If columnIdentifier is a string (column name), find its index
  if (typeof columnIdentifier === 'string') {
    // Get column names from grid.js config
    const columnConfigs = this.grid.config.columns;
    for (let i = 0; i < columnConfigs.length; i++) {
      if (columnConfigs[i].name === columnIdentifier) {
        columnIndex = i;
        break;
      }
    }

    // If column not found, log warning and return
    if (typeof columnIndex === 'string') {
      console.warn(`Column "${columnIdentifier}" not found`);
      return this;
    }
  }

  // Filter the data
  if (value && value.toString().trim() !== '') {
    filteredData = filteredData.filter(row => {
      if (!row || !row[columnIndex]) return false;

      const cellValue = String(row[columnIndex] || '');

      // Handle regex filtering
      if (useRegex) {
        try {
          const regex = new RegExp(value);
          return regex.test(cellValue);
        } catch (e) {
          console.warn('Invalid regex pattern:', value);
          return cellValue.toLowerCase().includes(String(value).toLowerCase());
        }
      } else {
        // Handle normal search
        if (smart) {
          // Smart search - tokenize and match all words
          const tokens = String(value).toLowerCase().split(/\s+/);
          return tokens.every(token => cellValue.toLowerCase().includes(token));
        } else {
          // Simple contains search
          return cellValue.toLowerCase().includes(String(value).toLowerCase());
        }
      }
    });
  }

  // Update grid and render
  this.grid.updateConfig({ data: filteredData });
  this.grid.forceRender();

  // Update filter info if present
  if (this.addFilteredInfo) {
    setTimeout(() => this.addFilteredInfo(), 100);
  }

  return this;
}
  };

  window.tableAPI = tableAPI;

  // Initialize Grid.js table
  function initGrid() {
    try {
      // Check that Grid.js is available
      if (typeof gridjs === "undefined") {
        console.error("Grid.js library is not available. Tables will not render correctly.");
        return tableAPI;
      }

      // Get the container element
      var dtElem = document.querySelector(dtSelector);
      if (!dtElem) {
        console.error("Table element not found:", dtSelector);
        return tableAPI;
      }

      // Prepare the element
      dtElem.classList.add("dataTable");
      dtElem.classList.add("no-footer");
      dtElem.setAttribute("aria-describedby", "apps_info");
      dtElem.style.width = "100%";

      // Create a mapping of column indices to their types
var columnTypes = {};
if (opts && opts.aoColumnDefs) {

  opts.aoColumnDefs.forEach(function(def) {
    if (def.aTargets && Array.isArray(def.aTargets)) {
      def.aTargets.forEach(function(colIndex) {
        columnTypes[colIndex] = {
          sType: def.sType || 'string',
          searchable: def.bSearchable !== false,
          renderer: def.mRender
        };
      });
    }
  });
}
var sortFunctions = {
  // Apply the renderer function for a specific column
  preprocess: function(value, renderer) {
  if (!renderer) return value;

  if (renderer === 'parseHadoopID') {
    return parseHadoopID(value, 'sort');
  } else if (renderer === 'parseHadoopProgress') {
    return parseHadoopProgress(value, 'sort');
  } else if (renderer === 'renderHadoopDate') {
    return renderHadoopDate(value, 'sort');
  } else if (renderer === 'renderHadoopElapsedTime') {
    return renderHadoopElapsedTime(value, 'sort');
  }

  return value;
},

  // Compare two values based on column type
  compare: function(a, b, colType) {
    if (colType === 'natural') {
      return naturalSort(String(a || ''), String(b || ''));
    } else if (colType === 'title-numeric') {
      //title-numeric-asc
      var x = String(a || '').match(/title=["']?(-?\d+\.?\d*)/);
      var y = String(b || '').match(/title=["']?(-?\d+\.?\d*)/);
      x = x ? parseFloat(x[1]) : 0;
      y = y ? parseFloat(y[1]) : 0;
      return ((x < y) ? -1 : ((x > y) ? 1 : 0));
    } else if (colType === 'num-ignore-str') {
      //num-ignore-str-asc
      if (isNaN(a) && isNaN(b)) {
        return String(a || '').localeCompare(String(b || ''));
      }
      if (isNaN(a)) return 1;
      if (isNaN(b)) return -1;

      var x = parseFloat(a);
      var y = parseFloat(b);
      return ((x < y) ? -1 : ((x > y) ? 1 : 0));
    } else if (colType === 'numeric') {
      // Simple numeric comparison
      var numA = parseFloat(a || 0);
      var numB = parseFloat(b || 0);
      if (isNaN(numA)) numA = 0;
      if (isNaN(numB)) numB = 0;
      return numA - numB;
    } else {
      return naturalSort(String(a || ''), String(b || ''));
    }
  }
};

//Pre-sort the data, if needed
if (Array.isArray(opts.data.data) && opts.data.data.length > 0 && opts.aaSorting && opts.aaSorting.length) {

  var sortColIndex = parseInt(opts.aaSorting[0][0], 10);
  var sortDirection = opts.aaSorting[0][1];

  // Get the column type and renderer
  var colType = columnTypes[sortColIndex] ? columnTypes[sortColIndex].sType : 'natural';
  var renderer = columnTypes[sortColIndex] ? columnTypes[sortColIndex].renderer : null;

  // Sort the data using our shared sort functions
  opts.data.data.sort(function(a, b) {
    // Get values from the row data for the sort column
    var aVal = a[sortColIndex];
    var bVal = b[sortColIndex];

    // Preprocess values using the appropriate renderer
    aVal = sortFunctions.preprocess(aVal, renderer);
    bVal = sortFunctions.preprocess(bVal, renderer);

    // Compare the values using the appropriate sort function
    var result = sortFunctions.compare(aVal, bVal, colType);

    // Apply sort direction
    return sortDirection === 'desc' ? -result : result;
  });

}

//Define column functions
function configureColumnSort(columnConfig, index) {
  // Set up the sort configuration
  if (columnTypes[index]) {
    const colType = columnTypes[index].sType;
    const renderer = columnTypes[index].renderer;

    columnConfig.sort = {
      compare: function(a, b) {
        // Use the shared sort functions
        let aVal = sortFunctions.preprocess(a, renderer);
        let bVal = sortFunctions.preprocess(b, renderer);
        return sortFunctions.compare(aVal, bVal, colType);
      }
    };
  } else {
    // Default to natural sort
    columnConfig.sort = {
      compare: function(a, b) {
        return sortFunctions.compare(a, b, 'natural');
      }
    };
  }

  return columnConfig;
}

// STEP 4: Get columns (either from headings or DOM)
var columns = [];

//extract columns from table headers
var headerCells = dtElem.querySelectorAll("th");
if (headerCells && headerCells.length) {
  columns = Array.from(headerCells).map(function (th, index) {
    const headerText = th.textContent.trim();
// Create basic column config
const columnConfig = {
  name: headerText,
  sort: true,
  search: columnTypes[index] ? columnTypes[index].searchable : true,
formatter: (cell) => {
  // Get column configuration
  const colConfig = columnTypes[index];
  if (colConfig && typeof colConfig.renderer === 'function') {
    try {
      if (colConfig.renderer === parseHadoopProgress) {
  const progressValue = parseHadoopProgress(cell, 'sort');
  return gridjs.html(getProgressbar(progressValue));
}
      else if (colConfig.renderer === parseHadoopID) {
        return gridjs.html(parseHadoopID(cell, 'display'));
      }
      else if (colConfig.renderer === renderHadoopDate) {
        return gridjs.html(renderHadoopDate(cell, 'display', true));
      }
      else if (colConfig.renderer === renderHadoopElapsedTime) {
        return gridjs.html(renderHadoopElapsedTime(cell, 'display', true));
      }
      // Default case for any other renderers
      return gridjs.html(colConfig.renderer(cell, 'display', true));
    } catch (e) {
      console.error('Error applying renderer:', e);
      return gridjs.html(cell || "");
    }
  }
  // Special case for History
  if (cell === "History") {
    return gridjs.html(`<a href="#" class="history-link">History</a>`);
  }
  // Default formatting
  return gridjs.html(cell || "");
}
};

    // Apply sort function
    return configureColumnSort(columnConfig, index);
  });
}
// Fallback to generic columns if no headers found
else if (opts.data.data.length > 0) {
  columns = new Array(opts.data.data[0].length).fill(0).map(function (_, i) {
    const columnConfig = {
      name: "Column " + (i + 1),
      sort: true,
    };

    // Apply sort function
    return configureColumnSort(columnConfig, i);
  });
}

      // Determine the default page size
      var pageSize = 10;
      const storageKey = `${dtSelector.replace(/[^a-zA-Z0-9]/g, "")}_pageSize`;
      const storedPageSize = localStorage.getItem(storageKey);
      if (storedPageSize) {
        pageSize = parseInt(storedPageSize, 10);
      } else {
        // Use default logic when no stored value exists
        if (opts.data.data.length <= 25) {
          pageSize = opts.data.data.length || 10; // Show all rows if few, default to 10 if empty
        } else if (opts.data.data.length <= 100) {
          pageSize = 25; // Show 25 rows for medium-sized datasets
        } else {
          pageSize = 50; // Show 50 rows for larger datasets
        }
      }

      var gridConfig = {
        columns: columns,
        data: opts.data.data,
        search: true,
        sort: true,  // Set initial sort
        pagination: {
          enabled: true,
          limit: pageSize,
          summary: true,
        },
        className: {
          table: "dataTable no-footer",
          container: "dataTables_wrapper",
          thead: "",
          tbody: "",
          th: "sorting",
          td: "",
          search: "dataTables_filter",
          footer: "",
          // Add these pagination-related classes
          pagination: "dataTables_paginate paging_simple_numbers",
          paginationButton: "paginate_button",
          paginationButtonNext: "next",
          paginationButtonPrev: "previous",
          paginationButtonCurrent: "current",
        },
        language: {
          search: {
            placeholder: "Search",
          },
          pagination: {
            previous: "Previous",
            next: "Next",
            showing: "Showing",
            results: "entries",
            of: "of",
            to: "to",
          },
          noRecordsFound: "No data available in table",
        },
      };

      // Clear container
      dtElem.innerHTML = "";

      // Create and render Grid.js
      var grid = new gridjs.Grid(gridConfig);
      grid.render(dtElem);


      // Add a class to the container for easier CSS targeting
      dtElem.parentElement.classList.add("dataTables_wrapper");
      dtElem.parentElement.id = dtSelector.substring(1) + "_wrapper";

      // Store grid instance
      tableAPI.grid = grid;

      // Set up the table
      setTimeout(function () {
        // Add DataTables classes
        const container = dtElem.parentElement;

        // Create page length selector
        const headElement = container.querySelector(".gridjs-head");
        if (headElement) {
          const pageLengthContainer = document.createElement("div");
          pageLengthContainer.className = "dataTables_length";
          pageLengthContainer.id = dtSelector.substring(1) + "_length";

          const label = document.createElement("label");
          label.textContent = "Show ";

          const select = document.createElement("select");
          select.name = dtSelector.substring(1) + "_length";
          select.className = "dataTables_selector";

          const perPageOptions = [20, 40, 60, 80, 100];
          perPageOptions.forEach((val) => {
            const option = document.createElement("option");
            option.value = val;
            option.textContent = val;
            option.selected = val === pageSize;
            select.appendChild(option);
          });

          select.addEventListener("change", function (e) {
            e.stopPropagation();
            const newPageSize = parseInt(this.value, 10);
            tableAPI.changePageSize(newPageSize);
          });

          select.addEventListener("click", function (e) {
            e.stopPropagation();
          });

          label.appendChild(select);
          label.appendChild(document.createTextNode(" entries"));
          pageLengthContainer.appendChild(label);

          headElement.insertBefore(pageLengthContainer, headElement.firstChild);

          // Store reference to our dropdown
          tableAPI.lengthDropdown = pageLengthContainer;
        }

        // Ensure DataTables classes are applied
        const searchElement = container.querySelector(".gridjs-search");
        if (searchElement) {
          searchElement.classList.add("dataTables_filter");
        }

        const paginationElement = container.querySelector(".gridjs-pagination");
        if (paginationElement) {
          paginationElement.classList.add("dataTables_paginate");

          // Add pagination button classes to match Hadoop UI style
          const paginationButtons = paginationElement.querySelectorAll("button");
          paginationButtons.forEach((button) => {
            if (button.classList.contains("gridjs-current")) {
              button.classList.add("paginate_button", "current");
            } else if (!button.classList.contains("gridjs-spread")) {
              button.classList.add("paginate_button");
            }
          });
        }

        const infoElement = container.querySelector(".gridjs-summary");
        if (infoElement) {
          infoElement.classList.add("dataTables_info");
        }

        // Apply classes to headers for sorting indicators
        const headers = dtElem.querySelectorAll("th");
        headers.forEach((header) => {
          header.classList.add("sorting");
        });

        // Fix issue with disappearing elements on click
        function stopPropagation(e) {
          e.stopPropagation();
        }

        if (searchElement) {
          searchElement.addEventListener("click", stopPropagation);
          const searchInput = searchElement.querySelector("input");
          if (searchInput) {
            searchInput.addEventListener("click", stopPropagation);
            searchInput.addEventListener("input", stopPropagation);
          }
        }

        if (paginationElement) {
          paginationElement.addEventListener("click", stopPropagation);
          const buttons = paginationElement.querySelectorAll("button");
          buttons.forEach((button) => {
            button.addEventListener("click", stopPropagation);
          });
        }

        // Add pagination button classes to match Hadoop UI style
        const paginationButtons = paginationElement.querySelectorAll("button");
        paginationButtons.forEach((button) => {
          if (button.classList.contains("gridjs-current")) {
            button.classList.add("paginate_button", "current");
          } else if (!button.classList.contains("gridjs-spread")) {
            button.classList.add("paginate_button");
          }
        });

        // Make sure UI elements are visible even when empty
        const footer = dtElem.querySelector(".gridjs-footer");
        if (footer) {
          footer.style.display = "block";
        }

        // Add click handlers to history links
        const historyLinks = dtElem.querySelectorAll(".history-link");
        historyLinks.forEach((link) => {
          link.addEventListener("click", function (e) {
            e.preventDefault();
            const row = this.closest("tr");
            const rowIndex = Array.from(row.parentNode.children).indexOf(row);
          });
        });
        // Emit initialization complete event
      }, 100);
tableAPI.addFilteredInfo = function() {
  let eventsAttached = false;

  // Function to update filter text
  function updateFilterInfo() {
    // Get search input and visible rows
    const searchInput = document.querySelector('.gridjs-search input');
    const isSearching = searchInput && searchInput.value.trim() !== '';

    // If not searching, no need to show filter text
    if (!isSearching) {
      removeFilterText();
      return;
    }

    // Get counts
    const totalEntries = tableAPI.originalData.length;
    const visibleRows = document.querySelectorAll('.gridjs-tbody tr:not(.gridjs-empty)').length;

    // Only show filter text if fewer rows are visible than total
    if (visibleRows < totalEntries) {
      addFilterText(totalEntries);
    } else {
      removeFilterText();
    }
  }
  // Add filter text to summary
  function addFilterText(totalEntries) {
    const summaryElement = document.querySelector('.gridjs-summary');
    if (!summaryElement) return;

    // Remove any existing filter elements first
    removeFilterText();

    // Create and add new filter element
    const filterElement = document.createElement('span');
    filterElement.className = 'dataTables_filtered_info';
    filterElement.style.marginLeft = '5px';
    filterElement.textContent = `(filtered from ${totalEntries} total entries)`;
    summaryElement.appendChild(filterElement);
  }

  // Remove filter text from summary
  function removeFilterText() {
    const filterElements = document.querySelectorAll('.dataTables_filtered_info');
    filterElements.forEach(el => el.remove());
  }

  // Save search to localStorage
  function saveSearch(value) {
    if (value) {
      localStorage.setItem('gridjs_search_' + dtSelector, value);
    } else {
      localStorage.removeItem('gridjs_search_' + dtSelector);
    }
  }

  // Function to set up all event listeners (run only once)
  function setupEventListeners() {
    if (eventsAttached) return;

    // Find required elements
    const searchInput = document.querySelector('.gridjs-search input');
    const gridContainer = document.querySelector('.gridjs-container');

    if (!searchInput || !gridContainer) {
      // Elements not ready yet, try again later
      setTimeout(setupEventListeners, 100);
      return;
    }

    // Listen for search input changes
    searchInput.addEventListener('input', function() {
      saveSearch(this.value.trim());
      setTimeout(updateFilterInfo, 100);
    });

    // Listen for any clicks in the grid container (pagination, etc.)
    gridContainer.addEventListener('click', function(event) {
      if (event.target.closest('.gridjs-pagination')) {
        setTimeout(updateFilterInfo, 100);
      }
    });

    // Mark as attached
    eventsAttached = true;

    // Restore search from localStorage
    const savedSearch = localStorage.getItem('gridjs_search_' + dtSelector);
    if (savedSearch) {
      searchInput.value = savedSearch;
      const event = new Event('input', { bubbles: true });
      searchInput.dispatchEvent(event);
    }
  }

  // Set up event listeners when the table is ready
  setupEventListeners();
  const originalForceRender = tableAPI.grid.forceRender;
  tableAPI.grid.forceRender = function() {
    // Call the original method
    const result = originalForceRender.apply(this, arguments);
    // After Grid.js updates, update our filter text
    setTimeout(updateFilterInfo, 100);
    // Return the original result
    return result;
  };

  // Initial update
  setTimeout(updateFilterInfo, 200);
  return this;
};
tableAPI.addFilteredInfo();
      return tableAPI;
    } catch (e) {
      console.error("Error initializing Grid.js table:", e);
      console.error(e.stack);
      return tableAPI;
    }
  }

  // Start the initialization process
  function initialize() {
  if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", initGrid);
} else {
  initGrid();
}
  }
  // Start initialization
  initialize();

  return tableAPI;
}

// Exposed DataTableHelper globally so it can be accessed before DOMContentLoaded
window.DataTableHelper = DataTableHelper;