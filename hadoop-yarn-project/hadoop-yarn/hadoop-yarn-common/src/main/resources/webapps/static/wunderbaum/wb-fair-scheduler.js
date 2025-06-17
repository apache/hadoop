function initFairSchedulerWBTree() {
    const queueJson = {
        queues: [parseHtmlToJson(document.getElementById('cs'), null,  new Set())]
    };

    const wunderbaumTreeData = queueJson.queues.map(
        queueData => transformJsonToWunderbaumTreeData(queueData, false)
    );

    let csDiv = document.getElementById('cs');
    csDiv.innerHTML = `
      <div id='queue-wrapper'>
        <div id='cs-tree'></div>
      </div>
    `;
    let wrapper = document.getElementById('queue-wrapper');
    let legendDiv = document.createElement('div');
    legendDiv.className = 'legend-leaf';
    legendDiv.innerHTML = `
         <span style='font-weight: bold'>Legend:</span>
         <span class='qlegend' style='background:none;border:1px solid #000000' 
               title='The steady fair shares consider all queues, both active (with running applications) and inactive.'
         > Steady Fair Share </span>
         <span class='qlegend' style='background:none;border:1px dashed #000000'
               title='The instantaneous fair shares consider only active queues (with running applications).'
         >Instantaneous Fair Share</span>
         <span class='qlegend' style='background:#5BD75B'>Used</span>
         <span class='qlegend' style='background:#FFA333'>Used (over fair share)</span>
         <span class='qlegend' style='border: 1px solid #d3d3d3; background: #e6e6e6'>Max Capacity</span>
    `;
    wrapper.prepend(legendDiv);

    // Set up the initial queues for URL manipulation
    const urlParams = new URLSearchParams(window.location.search);
    const firstQueue = urlParams.get('openQueues');
    const firstQueues = firstQueue ? [firstQueue] : [];
    const hashFragment = window.location.hash.substring(1); // remove leading '#' by starting from index 1
    const hashQueues = hashFragment ? hashFragment.split('#') : [];
    let openQueues = [...firstQueues, ...hashQueues];

    const wbTree = new mar10.Wunderbaum({
        element: document.getElementById('cs-tree'),
        navigationModeOption: 'row',
        source: wunderbaumTreeData,
        columns: [
            { id: '*', title: 'Property', width: '300px' },
            { id: 'column1', title: 'Column1', width: '300px' },
            { id: 'column2', title: 'Column2', width: '100px' },
            { id: 'column3', title: 'Column3', width: '500px' },
            { id: 'column4', title: 'Column4', width: '400px' },
            { id: 'column5', title: 'Column5', width: '200px' },
            { id: 'column6', title: 'Column6', width: '200px' },
            { id: 'column7', title: 'Column7', width: '300px' },
            { id: 'column8', title: 'Column8', width: '200px' },
        ],
        scrollParent: document.getElementById('queue-wrapper'),
        activate: ({ node }) => {
            // Responsible for displaying the running application when click on specific queue
            if (node.hasClass('queue-header')){
               filterDataTableBySelectedQueue(node);
            }
        },
        expand: ({ node }) => {
            const queueName = node.title;
            if (node.isExpanded()){
                if(!openQueues.includes(queueName)) openQueues.push(queueName);
            } else {
                openQueues = openQueues.filter(q => q !== queueName);
            }

            // Update first queue param
            openQueues.length ? urlParams.set('openQueues', openQueues[0]) : urlParams.delete('openQueues');
            // Update fragment parts
            const hashPart = openQueues.slice(1).join('#');
            const newHash = hashPart ? `#${hashPart}` : '';
            history.replaceState({}, '', `?${urlParams.toString()}${newHash}`);

        },

        render: ({ node, renderColInfosById}) => {
            Object.values(renderColInfosById).forEach(col => {
                if (col.id === 'column7' && node.data.usedQueue){
                    col.elem.textContent = node.data.usedQueue; // To take usedQueue Data display at column7
                } else {
                    col.elem.textContent = node.data[col.id] || ''; // to fill in the value in each column
                }
            });

            if (node.hasClass('queue-header')){ // For the Used Queue Percentage Highlight
                const usedPercentage = node.data.usedQueue.split('%')[0].trim();

                if (usedPercentage > 100){
                    const overfillPercentage = usedPercentage -100;
                    node._rowElem.style.setProperty('--dash-border-radius-top-right', 0);
                    node._rowElem.style.setProperty('--dash-border-radius-bottom-right', 0);
                    node._rowElem.style.setProperty('--overfill-percentage', `${overfillPercentage}%`);
                }
                node._rowElem.title = node.data.nodeTitle;
                node._rowElem.style.setProperty('--full-width', node.data.fullWidth); /* fullWidth Acts as the maximum capacity*/
                node._rowElem.style.setProperty('--fill-color', node.data.backgroundColor);
                node._rowElem.style.setProperty('--fill-percentage', `${usedPercentage}%`);
            }

            if (node.data.dashedWidth != null){ /* Dashed width acts as the Capacity*/
                node._rowElem.style.setProperty('--dash-width', node.data.dashedWidth); /* dashedWidth Acts as the Instantaneous Fair Share*/
                node._rowElem.style.setProperty('--dash-border', '1px dashed #BFBFBF');
                node._rowElem.style.setProperty('--show-dash', '0'); /* Disable the queue-header fill-in background when there is dashed width*/
            } else {
                node._rowElem.style.setProperty('--dash-border', 'none');
                node._rowElem.style.setProperty('--show-dash', '1'); /* Fall back to the queue-header fill-in background when there is no dashed-width*/
            }
        },
    });

    wbTree.visit(node => {
        if (openQueues.includes(node.title)) {
            node.setExpanded(true);
        }
    });

    csDiv.style.display = 'block';
}

document.addEventListener("DOMContentLoaded", initFairSchedulerWBTree);
