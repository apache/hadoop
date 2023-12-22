//function accordionJS(selector){
//    const acc = document.getElementsByClassName(selector);
//    const initialHStyles = "ui-accordion-header ui-corner-top ui-state-default ui-accordion-icons ui-accordion-header-collapsed ui-corner-all"
//    const initialULStyles = "ui-accordion-content ui-corner-bottom ui-helper-reset ui-widget-content"
//    for (i=0; i<acc.length; i++) {
//      let siblingUL = acc[i].nextElementSibling;
//      siblingUL.classlist.add(initialULStyles);
//      acc[i].classList.add(initialHStyles);
//      acc[i].addEventListener('click', function () {
//        this.classList.toggle('ui-state-active');
//        siblingUL.classList.toggle('ui-accordion-content-active')
//        siblingUL.style["height"] = "222px";
//      })
//      acc[i].addEventListener('mouseover', function () {
//              this.classList.add('ui-state-hover');
//      })
//      acc[i].addEventListener('mouseout', function () {
//        this.classList.remove('ui-state-hover')
//      })
//      acc[i].innerHTML += "<span class='ui-accordion-header-icon ui-icon ui-icon-triangle-1-e'></span>"
//      let childSpan = acc[i].children;
//      childSpan.addEventListener('click', function(){
//        this.classList.toggle("ui-icon-triangle-1-e");
//        this.classList.toggle("ui-icon-triangle-1-s");
//      })
//    }
//}
//
//function getProgressbar(value){
//    let progressBarHTML = "";
//    progressBarHTML += `<br title="${value}">`
//    progressBarHTML += `<div class="ui-progressbar ui-widget ui-widget-content ui-corner-all" title="${value}">`
//    progressBarHTML += `<div class="ui-progressbar ui-widget ui-widget-content ui-corner-all" style="width:${value}"></div>`
//    progressBarHTML += "</div>"
//    return progressBarHTML
//}

//function accordionJS(selector){
//    const acc = document.getElementsByClassName(selector);
//    const initialHStyles = "ui-accordion-header ui-corner-top ui-state-default ui-accordion-icons ui-accordion-header-collapsed ui-corner-all"
//    const initialULStyles = "ui-accordion-content ui-corner-bottom ui-helper-reset ui-widget-content"
//    for (i=0; i<acc.length; i++) {
//      let siblingUL = acc[i].nextElementSibling;
//      acc[i].classList.add(initialHStyles);
//      acc[i].addEventListener('click', function () {
//        this.classList.toggle('ui-state-active');
//        siblingUL.classList.toggle('ui-accordion-content-active')
//        siblingUL.style["height"] = "222px";
//      })
//      acc[i].addEventListener('mouseover', function () {
//              this.classList.add('ui-state-hover');
//      })
//      acc[i].addEventListener('mouseout', function () {
//        this.classList.remove('ui-state-hover')
//      })
//      acc[i].innerHTML += "<span class='ui-accordion-header-icon ui-icon ui-icon-triangle-1-e'></span>"
//      let childSpan = acc[i].children;
//      siblingUL.classlist.add(initialULStyles);
//      childSpan.addEventListener('click', function(){
//        this.classList.toggle("ui-icon-triangle-1-e");
//        this.classList.toggle("ui-icon-triangle-1-s");
//      })
//    }
//}
//
//function getProgressbar(value){
//    let progressBarHTML = "";
//    progressBarHTML += `<br title="${value}">`
//    progressBarHTML += `<div class="ui-progressbar ui-widget ui-widget-content ui-corner-all" title="${value}">`
//    progressBarHTML += `<div class="ui-progressbar ui-widget ui-widget-content ui-corner-all" style="width:${value}"></div>`
//    progressBarHTML += "</div>"
//    return progressBarHTML
//}


function accordionJS(selector){
    const acc = document.getElementsByClassName(selector);
    const initialHStyles = ["ui-accordion-header", "ui-corner-top",
                            "ui-state-default", "ui-accordion-icons",
                            "ui-accordion-header-collapsed", "ui-corner-all"]
    const initialULStyles = ["ui-accordion-content", "ui-corner-bottom",
                             "ui-helper-reset", "ui-widget-content"]
    for (i=0; i<acc.length; i++) {
      let siblingUL = acc[i].nextElementSibling;
      initialHStyles.forEach((cls) => {acc[i].classList.add(cls)});
      initialULStyles.forEach((cls) => {siblingUL.classlist.add(cls)});
      acc[i].addEventListener('click', function () {
        this.classList.toggle('ui-state-active');
        siblingUL.classList.toggle('ui-accordion-content-active')
        siblingUL.style["height"] = "222px";
      })
      acc[i].addEventListener('mouseover', function () {
              this.classList.add('ui-state-hover');
      })
      acc[i].addEventListener('mouseout', function () {
        this.classList.remove('ui-state-hover')
      })
      acc[i].innerHTML += "<span class='ui-accordion-header-icon ui-icon ui-icon-triangle-1-e'></span>"
      let childSpan = acc[i].children;
      childSpan.addEventListener('click', function(){
        this.classList.toggle("ui-icon-triangle-1-e");
        this.classList.toggle("ui-icon-triangle-1-s");
      })
    }
}

function getProgressbar(value){
    let progressBarHTML = "";
    progressBarHTML += `<br title="${value}">`
    progressBarHTML += `<div class="ui-progressbar ui-widget ui-widget-content ui-corner-all" title="${value}">`
    progressBarHTML += `<div class="ui-progressbar ui-widget ui-widget-content ui-corner-all" style="width:${value}"></div>`
    progressBarHTML += "</div>"
    return progressBarHTML
}