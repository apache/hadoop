/*
 * Implementation of Accordion in Vanilla JS
 * Based on the implementation of Jquery-UI Accordion
 */


var Accordion = function (element, options, selector) {
    var el = element;
    if (typeof element === 'string')
        el = document.getElementById(element)

    openTab = options.openTab;
    oneOpen = options.oneOpen || false;
    sel = selector || "h3";
    titleClasses = options.titleClasses || ["ui-accordion-header", "ui-corner-top",
        "ui-state-default", "ui-accordion-icons",
        "ui-accordion-header-collapsed", "ui-corner-all"];
    contentClasses = options.contentClasses || ["ui-accordion-content", "ui-corner-bottom",
        "ui-helper-reset", "ui-widget-content"];
    render();

    function render() {
        el.style.width = "11em";
        el.classList.add("ui-accordion", "ui-widget", "ui-helper-reset");
        [].forEach.call(el.querySelectorAll("." + sel),
            (item, idx) => {
                item.innerHTML = "<span class='ui-accordion-header-icon ui-icon ui-icon-triangle-1-e'></span>" + item.innerHTML;
                item.setAttribute("id", `ui-id-${((idx * 2) + 1)}`)
                item.classList.add(...titleClasses);
                item.setAttribute('role', 'tab');
                item.nextElementSibling.classList.add(...contentClasses);
                item.nextElementSibling.style.height = "100%";
                item.nextElementSibling.setAttribute('id', `ui-id-${(idx + 1) * 2}`)
                item.nextElementSibling.setAttribute('role', 'tabpanel');
                item.addEventListener('click', onClick);
                item.addEventListener('mouseover', onMouseOver);
                item.addEventListener('mouseout', onMouseOut);
            })

        //start with all closed tabs
        closeAll();
        if (openTab) {
            open(openTab);
        }
    }

    function onClick(e) {
        //do nothing if not clickable element
        if (e.target.className.indexOf(titleClasses[0]) === -1) {
            return;
        }

        if (e.target.className.indexOf("ui-state-active") !== -1) {
            return;
        }

        let nextContent = e.target.nextElementSibling;
        e.target.classList.toggle('ui-state-active');
        e.target.children[0].classList.toggle("ui-icon-triangle-1-e");
        e.target.children[0].classList.toggle("ui-icon-triangle-1-s");

        if (nextContent.style.display !== 'none') {
            // toggle current element if open
            e.target.nextElementSibling.style.display = "none";
        }

        if (oneOpen) {
            closeAll();
        }
        toggle(nextContent);
    }

    function onMouseOver(e) {
        if (e.target.classList &&
            e.target.className.indexOf('ui-state-hover') === -1){
                e.target.classList.add('ui-state-hover');
        }
    }

    function onMouseOut(e) {
        if (e.target.classList &&
            e.target.className.indexOf('ui-state-hover') !== -1) {
            e.target.classList.remove('ui-state-hover');
        }
    }

    function closeAll() {
        [].forEach.call(el.querySelectorAll("." + contentClasses[0]),
            (ele) => {
                ele.style.display = 'none';
            });
        [].forEach.call(el.querySelectorAll("." + titleClasses[0]),
            (ele) => {
                if (ele.className.indexOf("ui-state-active") !== -1) {
                    ele.classList.remove("ui-state-active");
                    if (ele.children[0].className.indexOf("ui-icon-triangle-1-e") === -1)
                        ele.children[0].classList.add("ui-icon-triangle-1-e");
                    if (ele.children[0].className.indexOf("ui-icon-triangle-1-s") !== -1){
                        ele.children[0].classList.remove("ui-icon-triangle-1-s");
                    }
                }
            })
    }

    function toggle(el) {
        if (el.style.display === 'none') {
            el.style.display = 'block';
            el.classList.toggle('ui-accordion-content-active');
            el.previousElementSibling.classList.toggle("ui-state-active");
            el.previousElementSibling.children[0].classList.toggle("ui-icon-triangle-1-e");
            el.previousElementSibling.children[0].classList.toggle("ui-icon-triangle-1-s");
        }
        else {
            el.style.display = 'none';
            el.classList.toggle('ui-accordion-content-active');
            el.previousElementSibling.classList.toggle("ui-state-active");
            el.previousElementSibling.children[0].classList.toggle("ui-icon-triangle-1-e");
            el.previousElementSibling.children[0].classList.toggle("ui-icon-triangle-1-s");
        }
    }

    function getTarget(idx) {
        return el.querySelectorAll("." + contentClasses[0])[idx - 1];
    }

    function close(idx) {
        let target = getTarget(idx);

        if (target) {
            target.style.display = 'none';
        }
    }

    function open(idx) {
        let target = getTarget(idx);
        if (target) {
            if (oneOpen) closeAll();
            target.style.display = "block";
            prevElement = target.previousElementSibling;
            if (prevElement.className.indexOf("ui-state-active") === -1){
                prevElement.classList.add('ui-state-active');
            }
            prevElementChild = prevElement.children[0];
            prevElementChild.classList.toggle("ui-icon-triangle-1-e");
            prevElementChild.classList.toggle("ui-icon-triangle-1-s");
        }
    }

}