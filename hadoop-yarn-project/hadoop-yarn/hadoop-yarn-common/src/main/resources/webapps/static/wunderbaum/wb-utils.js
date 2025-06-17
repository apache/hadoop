/**
 * Parses the HTML structure of a queue element into a nested JSON structure.
 * @param {HTMLElement} element - The root DOM element (e.g., #cs)
 * @param {Object|null} parentQueue - use for recursion of SubQueues
 * @param {Set} seenQueues - Set to track already processed queues
 * @returns {Object|null} - The JSON data of the queues
 */
function parseHtmlToJson(element, parentQueue, seenQueues) {
    const queue = {
        name: '',
        usedQueue: '',
        title: null,
        backgroundColor: null,
        dashedWidth: null,
        fullWidth: null,
        subQueues: []
    };

    const qElement = element.querySelector('a.ui-state-default span.q');
    if (qElement) queue.name = qElement.textContent.trim();
    if (seenQueues.has(queue.name)) return null;
    seenQueues.add(queue.name);

    const aElement = element.querySelector('a.ui-state-default');
    if (aElement) {
        queue.title = aElement.title;
        queue.fullWidth = aElement.style.width;
        const spans = aElement.querySelectorAll('span:not(.q)');
        for (const span of spans) {
            if (span.style.border) queue.dashedWidth = span.style.width;
            const bgColor = span.style.background;
            if (bgColor !== 'none') {
                queue.backgroundColor = bgColor;
                break;
            }
        }
    }

    const qStates = element.querySelector('span.qstats');
    if (qStates) queue.usedQueue = qStates.textContent.trim().replace(/used/i, 'Used');

    const subQueueContainers = element.querySelectorAll('ul#pq, ul#lq');
    subQueueContainers.forEach(ul => {
        ul.querySelectorAll('li').forEach(li => {
            if (li.querySelector('a.ui-state-default')) {
                const subQueue = parseHtmlToJson(li, queue, seenQueues);
                if (subQueue) queue.subQueues.push(subQueue);
            }
        });
    });

    let queueDetails = {};
    const infoWrap = element.querySelector('.info-wrap');
    if (infoWrap) {
        infoWrap.querySelectorAll('tr').forEach(tr => {
            const th = tr.querySelector('th');
            const td = tr.querySelector('td');
            if (th && td) {
                const key = th.textContent.trim().replace(/:$/, '');
                queueDetails[key] = td.textContent.trim();
            }
        });
    }

    let activeUsersInfo = []; // array to hold multiple active users
    const userTable = element?.querySelector('table#userinfo');
    const tbody = userTable?.querySelector('tbody');
    if (userTable && tbody) {
        const headers = Array.from(userTable.querySelectorAll('th')).map(th => th.textContent.trim());
        tbody.querySelectorAll('tr').forEach(tr => {
            const userEntry = { backgroundColor: tr.style.background };
            tr.querySelectorAll('td').forEach((td, i) => {
                const key = headers[i].replace(/\W+/g, ''); // Remove non-alphabet characters (e.g., '-', '_', spaces)",
                userEntry[key] = td.textContent.trim();
            });
            activeUsersInfo.push(userEntry);
        });
    }

    if (queue.subQueues.length === 0) { // Only attach details to current queue if it's a leaf node
        queue.queueDetails = queueDetails;
        queue.activeUsersInfo = activeUsersInfo;

    }

    return queue;
}

/**
 * Converts queue JSON data to Wunderbaum-compatible tree data.
 * @param {Object} queueData - The JSON data of the queue
 * @param {Boolean} isActiveUsersInfoExist - whether the Scheduler Include ActiveUsersInfo or not
 * @returns {Object} - the JSON Wunderbaum Tree data
 */
function transformJsonToWunderbaumTreeData(queueData, isActiveUsersInfoExist) {
    const treeData = {
        title: queueData.name,
        usedQueue: queueData.usedQueue || '',
        nodeTitle: queueData?.title,
        backgroundColor: queueData.backgroundColor,
        dashedWidth: queueData?.dashedWidth,
        fullWidth: queueData?.fullWidth,
        classes: 'queue-header',
        children: []
    };

    if (queueData.queueDetails) {
        treeData.children.push(
            {
                title: '',
                column8: `${queueData.name} Queue Status`,
                classes: 'first-row-header'
            },
            ...Object.entries(queueData.queueDetails).map(([key, value]) => ({
                title: '',
                column3: key, // To center the value
                column4: value,
                classes: 'queue-table-row'
            }))
        );
    }

    if (Array.isArray(queueData.activeUsersInfo) && isActiveUsersInfoExist) {

        treeData.children.push({ title: '', classes: 'space-row' }); // For space between the table data and active users info
        treeData.children.push({ title: 'Active Users Info', classes: 'active-header-info' });

        treeData.children.push({
            title: 'User Name',
            column1: 'Max Resource',
            column2: 'Weight',
            column3: 'Used Resource',
            column4: 'Max AM Resource',
            column5: 'Used AM Resource',
            column6: 'Schedulable Apps',
            column7: 'Non Schedulable Apps',
            classes: 'user-header-info'
        });

        queueData.activeUsersInfo.forEach(user => {
            treeData.children.push({
                title: user.UserName || '', // In case there is no active user, display the title empty",
                column1: user.MaxResource,
                column2: user.Weight,
                column3: user.UsedResource,
                column4: user.MaxAMResource,
                column5: user.UsedAMResource,
                column6: user.SchedulableApps,
                column7: user.NonSchedulableApps,
                backgroundColor: user.backgroundColor,
                classes: 'user-table-row'
            });
        });
    }

    // Recursively process subQueues
    if (queueData?.subQueues.length > 0) {
        queueData.subQueues.forEach(subQueue => {
            treeData.children.push(transformJsonToWunderbaumTreeData(subQueue, isActiveUsersInfoExist));
        });
    }

    return treeData;
}


/**
 * Update data & pagination of the Datable in Application Scheduler page by the selected queue
 * @param {Object} node - The selected header node(queue) of Wunderbaum's Tree
 */
function filterDataTableBySelectedQueue(node){
    const queueHeaderChildren = node.children.filter(child => child.hasClass('queue-header')); // count the 'root...' children only
    const queuePattern = new RegExp(`^${node.title}${queueHeaderChildren.length === 0 ? '$' : '\\.'}`);

    const rows = document.querySelectorAll('#apps tbody tr');
    let visibleCount = 0;

    rows.forEach(row => {
        const queueCell = row.querySelector(`td[data-column-id='queue']`);
        const shouldShow = queuePattern.test(queueCell?.textContent.trim());
        row.style.display = shouldShow ? '' : 'none';
        if (shouldShow) visibleCount++;

    });

    // Get or create the no-data message
    let noDataMessage = document.getElementById('no-data-message');
    if (!noDataMessage){
        noDataMessage = document.createElement('div');
        noDataMessage.id = 'no-data-message';
        noDataMessage.style.textAlign = 'center';
        noDataMessage.style.padding = '12px';
        noDataMessage.innerHTML = 'No data available in table';

        // Insert at the end of Table Body
        const wrapper = document.querySelector('#apps > div > div.gridjs-wrapper > table > tbody');
        wrapper.appendChild(noDataMessage);
    }

    // Toggle noDataMessage visibility
    noDataMessage.style.display = visibleCount === 0 ? 'block' : 'none';

    // Update pagination summary
    const summary = document.querySelector('#apps .gridjs-summary');
    if (summary) {
        if (visibleCount === 0){
            summary.style.display = 'none';
        } else {
            summary.style.display = '';
            summary.innerHTML = `Showing <b>1</b> to <b>${visibleCount}</b> of <b>${visibleCount}</b> entries`;
        }
    }
}
