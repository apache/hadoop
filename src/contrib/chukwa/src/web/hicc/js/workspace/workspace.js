/*
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

var portalView = Class.create();
	
function showView (request) {
      	            _currentView.viewObj=request.responseText.evalJSON();
                    _currentView.createView();
	            _currentView.pages[0].pageSelected();
}

portalView.prototype = {

    initialize: function(id) {
	// initialize values
	this.baseURL='/hicc/Workspace';

    	this.id=id;			// id of the view
	this.pagesCount=0;		// total number of pages
	this.currentPage=0;		// current page id
        this.pages=new Array();		// list of pages
	this.description='';		// description of view
	this.modified=0;

	this.columnParentBox=null;	

    	this.error=0;			// error code
    	this.viewObj=false;		// page layout

    	// setup view
    	this.getView();
	// $('page_control_'+this.pages[0].page_tag).style.display='none';
    },

    isModified: function() {
	return this.modified;
    },

    setModified: function(modified) {
	this.modified=modified;
    },

    destroyView: function() {
	// remove all the page;
	for(i=this.pages.length-1;i>=0;i--) {
	    this._deletePageWorker(this.pages[i].page_tag);
	}
    },
    getView: function() {
   	var id=this.id;
   	var json_text='';
   	var myAjax=new Ajax.Request(
            this.baseURL,
	    {
		//asynchronous: false,
		method: 'get',
		parameters: {"_s":(new Date().getTime()),"method":"get_view","id":id},
                onSuccess: showView,
                onException: function(request, error) {
                    alert("Cannot load workspace information.");
                }
	    }
   	);
    },

    getConfig: function() {
	var v=new Object();
	v.description=this.description;
	if (this.viewObj) {
	    v.description=this.viewObj.description;
        }
	v.pages_count=this.pages.length;
	v.pages=new Array();
	for (var i=0;i<v.pages_count;i++) {
	    v.pages[i]=this.pages[i].getConfigObj();
        }
	return Object.toJSON(v);
    },

    saveView: function() {
   	if ((id != '') &&
      	    (id != 'default')) {
	    // TODO. send the json to the server
   	}
    },

    getCurrentPage: function() {
	return this.pages[this.currentPage];
    },

    createView: function() {
   	if (this.viewObj != null) {
	    for (var j=0;j<this.viewObj.pages.length;j++) {
		this.currentPage=j;
	   	// create a div
	   	page=this.addNewPage(this.viewObj.pages[this.currentPage].title, this.viewObj.pages[this.currentPage].columns, this.viewObj.pages[this.currentPage]);
	      	// page.setupPage();
            }
   	}
    },

    _deletePageWorker: function(tag) {
	// remove it from tablist
	var tablistNode=$('href_page_'+tag);
	tablistNode.parentNode.parentNode.removeChild(tablistNode.parentNode);

	// remove it from the content div
	var contentNode=$('page_'+tag);
	contentNode.parentNode.removeChild(contentNode);

	// remove it from the pages array
	this.pages.splice(i,1);
    },

    deletePage: function(tag) {
	this.setModified(1);
	for(i=0;i<this.pages.length;i++) {
	    if (this.pages[i].page_tag==tag) {
		if (this.pages.length==1) {
		    alert("You cannot delete the last page");
		    return;
		} else {
		    this._deletePageWorker(tag);
		}
		this.pages[0].pageSelected();
		return;
	    }
	}
    },

    renamePage: function(tag, new_title) {
	this.setModified(1);
	for(i=0;i<this.pages.length;i++) {
	    if (this.pages[i].page_tag==tag) {
		this.pages[i].title=new_title;
		$('href_page_'+tag).innerHTML=new_title;
	    }
	}
    },

    addNewPage: function(title,columns,layout) {
	this.setModified(1);
	// unique timestamp
	var d=new Date();
	var t=d.getTime();
	var id=this.pages.length;
	t=t*100+id;

	// add the tablist part
	var tabNode=document.createElement("li");
	tabNode.id="li_"+t;
	tabNode.className="tab-off";

	var tabInputSpan=document.createElement("span");
	tabInputSpan.id="page_tab_input_span_"+t;
	tabInputSpan.style.display="none";

	tabPageTabForm=document.createElement("form");
	tabPageTabForm.id='page_rename_form_'+t;
	tabPageTabForm.onsubmit=function() {
	    var id=this.id.substring(17);
	    renamePageForm(id);
	    return false;
        };

	tabPageInputTitle=document.createElement("input");
	tabPageInputTitle.type='text';
	tabPageInputTitle.id='page_rename_'+t;
	tabPageInputTitle.value=title;
	tabPageInputTitle.className='FormInput';
	tabPageInputTitle.onblur=function() {
	    var id=this.id.substring(12);
	    renamePageForm(id);
        };

	tabPageTabForm.appendChild(tabPageInputTitle);
	tabInputSpan.appendChild(tabPageTabForm);

	tabNode.appendChild(tabInputSpan);

	var tabSpan=document.createElement("span");
	tabSpan.id='page_tab_span_'+t;
        tabSpan.onmouseover=function() {
            var id=this.id.substring(14);
            if ($('li_'+id).className!='tab-on') {
                $('li_'+id).className='tab-hover';
            } 
        };
        tabSpan.onmouseout=function() {
            var id=this.id.substring(14);
            if ($('li_'+id).className!='tab-on') {
                $('li_'+id).className='tab-off';
            }
        };

	var tabHref=document.createElement("a");
	tabHref.className="page_tab";
	tabHref.innerHTML=title;
	tabHref.theme='#FFE6E6';
	tabHref.href="#";
	tabHref.id="href_page_"+t;
	tabHref.onclick=clickTab;

	tabSpan.appendChild(tabHref);

	// add page control
	tabPageControl=document.createElement("span");
	tabPageControl.id="page_control_"+t;
	tabPageControl.style.display="none";
	tabPageControl.innerHTML="";
	tabSpan.appendChild(tabPageControl);

	tabNode.appendChild(tabSpan);

	// add config menu
	$("tablist").appendChild(tabNode);

	// add the content div
	var divNode=document.createElement("div");
	divNode.id="page_"+t;
	divNode.className="tabcontent";
	$(contentDiv).appendChild(divNode);

	var portal_page=new portalPage(t,id,title,this,divNode,columns,layout);
	this.pages[this.pages.length]=portal_page;
	return portal_page;
    },

    findPage: function(t) {
	for(i=0;i<this.pages.length;i++) {
	    if (this.pages[i].page_tag==t) {
		return i;
            }
        }
        return 0;
    },

    selectPage: function(t) {
	close_all_popup();
	for(i=0;i<this.pages.length;i++) {
	    if (this.pages[i].page_tag==t) {
		this.currentPage=i;
		this.pages[i].pageSelected();
            } else if (this.pages[i].selected) {
		this.pages[i].pageDeselected();
	    }
	}
    }	
}

function clickTab(evt) {
   evt = (evt)?evt:((window.event)?window.event:"");
   if (evt) {
	var id=this.id.substring(10);
	var page=_currentView.findPage(id);
        _currentView.pages[page].refresh_all();
	if (_currentView.pages[page].selected) {
	    // enable edit 
	    $('page_tab_input_span_'+id).style.display='block';
	    $('page_tab_span_'+id).style.display='none';
        } else {
	    _currentView.selectPage(id);
	}
   }   
   return false;
}

function renamePageForm(id) {
    var page=_currentView.findPage(id);
    if (_currentView.pages[page].selected) {
        // enable edit 
        $('page_tab_input_span_'+id).style.display='none';
        $('page_tab_span_'+id).style.display='block';
	if ($('page_rename_'+id).value != _currentView.pages[page].title) {
	    _currentView.renamePage(id, $('page_rename_'+id).value);
	}
    }
}

var dragDropCounter = -1;	
var autoScrollActive = false;

var dragObject = false;
var dragObjectNextSibling = false;
var dragObjectParent = false;
var destinationObj = false;

var mouse_x;
var mouse_y;
	
var el_x;
var el_y;	
	
var okToMove = true;

var documentHeight = false;
var documentScrollHeight = false;
var dragableAreaWidth = false;

function _isIE() {
    if(navigator.appName == "Microsoft Internet Explorer") {
        return true;
    }
    return false;
}

var portalWidgetParam = Class.create();
portalWidgetParam.protoype = {
    initialize: function(name, value) {
	this.name=name;
	this.value=value;
	this.type="string";
	this.edit=1;
    }
}

var portalWidget = Class.create();

portalWidget.prototype = {
    initialize: function(page,boxIndex,block_obj,columnIndex,heightOfBox,minutesBeforeReload) {  
	this.page=page;
	this.view=this.page.view;
	this.pageid=page.pageid;
	this.block_obj=block_obj;
	this.columnIndex=columnIndex;
	this.heightOfBox=heightOfBox;
	this.minutesBeforeReload=block_obj.refresh;
	this.boxIndex=boxIndex;

   	if(!heightOfBox)heightOfBox = '0';
   	if(!minutesBeforeReload)minutesBeforeReload = '0';
	   
	this.load_javascripts();
   	this.createABox();

	this.page.dragableBoxesArray[this.boxIndex]=this;
   
   	var tmpInterval = false;

   	if(this.minutesBeforeReload && this.minutesBeforeReload>0){
      	    var tmpInterval = setInterval("reloadPageBoxData("+this.pageid+","+ boxIndex + ")",(this.minutesBeforeReload*1000*60));
   	}
   
   	this.intervalObj = tmpInterval;

   	$('dragableBoxHeadertxt'+this.pageid+'_'+boxIndex).innerHTML = '<span>' + block_obj.title + '&nbsp;<\/span>';	// title

   	this.reloadBoxData();
    },

    load_javascripts: function() {
	if (this.block_obj.javascripts) {
	    var js_scripts_array=this.block_obj.javascripts.split(',');	
	    for (var i=0;i<js_scripts_array.length;i++) {
                if (js_scripts_array[i].indexOf("http")==0) {
		    include_once(js_scripts_array[i]);
                } else {
		    include_once('/hicc/js/'+js_scripts_array[i]);
                }
	    }
	}
    },

    getWidgetUrl: function() { 
	return "/hicc/";
    },

    getConfigObj: function() {
        eval("var tmp="+Object.toJSON(this.block_obj));
        var cloneBlockObj=tmp;
	return cloneBlockObj;
    },

    addEditContent: function(parentObj) {
   
   	var editBox = document.createElement('DIV');
	editBox.className='dragableBoxEdit';
   	editBox.style.clear='both';
	editBox.style.cursor='default';
	editBox.onmouseover=null;
	editBox.onmouseout=null;
	editBox.onmousedown=null;
   	editBox.id = 'dragableBoxEdit' + this.pageid+"_"+this.boxIndex;
   	editBox.style.display='none';
   
   	var content='';
	content+='<table cellpadding="0" cellspacing="0" width="100%"><tr><td>';
	content+= '<form id="widgeteditform_'+this.pageid+'_'+this.boxIndex+'" name="widgeteditform_'+this.pageid+'_'+this.boxIndex+'"><table class="widget">';
	// loop through parameters list
	for (iCount=0;iCount<this.block_obj.parameters.length;iCount++) {
	    if (this.block_obj.parameters[iCount].edit!=0) {
		var param_id='param_'+this.pageid+'_'+this.boxIndex + '_'+iCount;
		var param=this.block_obj.parameters[iCount];
		var label=param.name;
	 	if ((param.label) && (param.label!='')) {
		    label=param.label;
		}
		label=label;
	        content+='<tr><td>'+label+':<\/td><td>';
	        if (param.type=='string') {
		    content+='<input type="text" id="'+param_id+'" value="' + param.value + '" size="20" maxlength="255"\/>'
	        } else if (param.type=='checkbox') {
		    content+='<input type="checkbox" id="'+param_id+'" name="'+param_id+'" ' + ((param.value==1 || param.value == "on") ? "checked":"") + ' \/>'
	        } else if (param.type=='radio') {
		    for (var j=0;j<param.options.length;j++) {
		        var option=param.options[j];
		    	content+='<input type="radio" id="'+param_id+'" name="'+param_id+'" '+((param.value==option.value)?'checked':'')+' value="'+option.value+'">'+option.label+'</input>&nbsp;';
		    }
	        } else if (param.type=='select_callback') {
		    // call the backend to get a list
   		    var json_text='';
   		    var myAjax=new Ajax.Request(
	    		param.callback,
	    		{
			    asynchronous: false,
			    method: 'get',
                            onSuccess: function(transport) {
      	                        param_options=transport.responseText.evalJSON();
    		                content+='<select class="formSelect" id="'+param_id+'" name="'+param_id+'" >';
		                for (var j=0;j<param_options.length;j++) {
		    	            var option=param_options[j];
		    	            content+='<option '+((param.value==option.value)?'selected':'')+' value="'+option.value+'">'+option.label+'</option>';
		                }
		                content+='</select>';
                            }
	    		}
   		    );

                } else if (param.type=='select_multiple_callback') {
                    // call the backend to get a list
                    var json_text='';
                    var myAjax=new Ajax.Request(
                        param.callback,
                        {                             asynchronous: false,
                            method: 'get'
                        }
                    );
                    if (myAjax.success()) {
                        json_text=myAjax.transport.responseText;
                    }
                    param_options=json_text.parseJSON();
		    content+='<table cellpadding="0" cellspacing="0" style="font-size:10px;"><tr><td>';
                    content+='Available:<br/><select multiple size="10" class="formSelect" id="available_'+param_id+'" name="available_'+param_id+'" >';
                    for (var j=0;j<param_options.length;j++) {
                        var option=param_options[j];
                        var selected="";
                        for (var k=0;k<param.value.length;k++) {
                           if (param.value[k] == option.value) {
                                selected="selected";
                           }
                        }
			if (selected != 'selected') {
                            content+='<option '+selected+' value="'+option.value+'">'+option.label+'</option>';
			}
                    }
		    content+='</select>';
                    content+='</td><td valign="middle">';
		    content+='<table cellspacing="2" cellpadding="0"><tr><td><input class=formButton type=button name="add" value=" >> " onclick="moveItem(\'available_'+param_id+'\',\''+param_id+'\');" /></td></tr><tr><td><input class=formButton type=button name="delete" value=" << " onclick="moveItem(\''+param_id+'\',\'available_'+param_id+'\');" /></td></tr></table>';
                    content+='</td><td>';
                    content+='Selected:<br/><select multiple size="10" class="formSelect" id="'+param_id+'" name="'+param_id+'" >';
                    for (var k=0;k<param.value.length;k++) {
                        for (var j=0;j<param_options.length;j++) {
                           var option=param_options[j];
                           if (param.value[k] == option.value) {
                               content+='<option value="'+option.value+'">'+option.label+'</option>';
                           }
                        }
                    }
		    content+='</select>';
                    content+='</td><td valign="middle">';
		    content+='<table cellspacing="2" cellpadding="0"><tr><td>';
		    content+='<input class=formButton border=0 type=image src="/hicc/images/u.gif" name="up" value="up" onclick="moveUpList(\''+param_id+'\');return false;"></td></tr>';
		    content+='<tr><td>';
		    content+='<input class=formButton border=0 type=image src="/hicc/images/d.gif" name="down" value="down" onclick="moveDownList(\''+param_id+'\');return false;">';
		    content+='</td></tr>';
		    content+='</table>';
                    content+='</td></tr>';
		    content+='</table>';
	        } else if (param.type=='select') {
    		    content+='<select class="formSelect" id="'+param_id+'" name="'+param_id+'" >';
		    for (var j=0;j<param.options.length;j++) {
		    	var option=param.options[j];
		    	content+='<option '+((param.value==option.value)?'selected':'')+' value="'+option.value+'">'+option.label+'</option>';
		    }
		    content+='</select>';
	        } else if (param.type=='select_multiple') {
    		    content+='<select class="formSelect" id="'+param_id+'" name="'+param_id+'" multiple>';
		    for (var j=0;j<param.options.length;j++) {
		    	var option=param.options[j];
                        var param_selected=false;
                        for(var k=0;k<param.value.length;k++) {
                            if(param.value[k] == option.value) {
                                param_selected=true;
                            }
                        }
		    	content+='<option '+(param_selected ?'selected':'')+' value="'+option.value+'">'+option.label+'</option>';
		    }
		    content+='</select>';
		}
		content+='<\/td><\/tr>'
	    }
        }

	content+='<tr><td>'+'Refresh (min)'+':<\/td><td>';
	content+='<input type="text" id="'+this.pageid+'_'+this.boxIndex + '_refreshrate" value="' + this.block_obj.refresh + '" size="20" maxlength="255"\/><\/td><\/tr>'

      	content+='<tr><td colspan="2"><input class="formButton" type="button" onclick="saveParameters(\'' + this.pageid+'_'+this.boxIndex + '\');saveView();" value="'+'Save'+'">&nbsp;<input class="formButton" type="button" onclick="resetParameters(\'' + this.pageid+'_'+this.boxIndex + '\');" value="'+'Reset'+'"><\/td><\/tr><\/table><\/form>';
	content+='<\/td><\/tr><\/table>';
	content+='<br/>';

   	editBox.innerHTML = content;
   
   	parentObj.appendChild(editBox);		
    },

    saveParameters: function() {
	for (iCount=0;iCount<this.block_obj.parameters.length;iCount++) {
	    var param_id='param_'+this.pageid+'_'+this.boxIndex + '_'+iCount;
	    var param=this.block_obj.parameters[iCount];
	    if (param.edit!=0) {
		if (param.type=='radio') {
		    for (var j=0;j<param.options.length;j++) {
			if (eval("document.widgeteditform_"+this.pageid+"_"+this.boxIndex+"."+param_id+"["+j+"].checked")==true) {
	            	    param.value=param.options[j].value;
			}
		    }
                } else if (param.type=='select_multiple_callback') {
		    selectAll(param_id);
	            param.value=$F(param_id);
		} else {
	            param.value=$F(param_id);
		}
	    }
        }
	if (this.block_obj.refresh != $(this.pageid+'_'+this.boxIndex + '_refreshrate').value) {
	    this.block_obj.refresh=$(this.pageid+'_'+this.boxIndex + '_refreshrate').value;	
	    if (this.intervalObj) {
	        clearInterval(this.intervalObj);
            }
	    if (this.block_obj.refresh==0) {
		this.intervalObj=false;
            } else {
      	        this.intervalObj = setInterval("reloadPageBoxData("+this.pageid+","+ this.boxIndex + ")",(this.block_obj.refresh*1000*60));
	    }
	}

	this.view.setModified(1);
	this.reloadBoxData();

	// close the edit box
   	var editBox= $('dragableBoxEdit' + this.pageid+"_"+this.boxIndex);
      	editBox.style.display='none';
        $('dragableBoxHeader' + this.pageid+"_"+this.boxIndex).style.height = '20px';
    },

    resetParameters: function() {
	for (iCount=0;iCount<this.block_obj.parameters.length;iCount++) {
	    var param_id='param_'+this.pageid+'_'+this.boxIndex + '_'+iCount;
	    var param=this.block_obj.parameters[iCount];

            if ($(param_id) == null) {
		continue;
            }
	    if (param.type=='string') {
		$(param_id).value=param.value;
	    } else if (param.type=='checkbox') {
		$(param_id).checked=((param.value==1 || param.value == "on") ? 1:0);
	    } else if (param.type=='radio') {
		if ($(param_id).length == undefined) {
		    if ($(param_id).value == param.value) {
			$(param_id).checked=true;
		    } else {
			$(param_id).checked=false;
		    }
		} else {
		    for (var j=0;j<param.options.length;j++) {
		        if ($(param_id)[j].value == param.value) {
			    $(param_id)[j].checked=1;
			}
		    }
		}
	    } else if (param.type=='select_callback') {
		// call the backend to get a list
		for (var j=0;j<$(param_id).options.length;j++) {
		    if ($(param_id).options[j].value==param.value) {
			$(param_id).selectedIndex=j;
		    }
		}
            } else if (param.type=='select_multiple_callback') {
                // call the backend to get a list
                var json_text='';
                var myAjax=new Ajax.Request(
                    param.callback,
                    {                             asynchronous: false,
                        method: 'get'
                    }
                );
                if (myAjax.success()) {
                    json_text=myAjax.transport.responseText;
                }
                param_options=json_text.parseJSON();
		$('available_'+param_id).options.length=0;
		$(param_id).options.length=0;
                for (var j=0;j<param_options.length;j++) {
                    var option=param_options[j];
                    var selected="";
                    for (var k=0;k<param.value.length;k++) {
                       if (param.value[k] == option.value) {
                            selected="selected";
                       }
                    }
		    if (selected != 'selected') {
			var control=$('available_'+param_id);
			control.options[control.options.length]=new Option(option.label, option.value);
		    } else {
			var control=$(param_id);
			control.options[control.options.length]=new Option(option.label, option.value);
		    }
                }
	    } else if (param.type=='select') {
		for (var j=0;j<$(param_id).options.length;j++) {
		    if ($(param_id).options[j].value==param.value) {
			$(param_id).selectedIndex=j;
		    }
		}
	    } else if (param.type=='select_multiple') {
		for (var j=0;j<$(param_id).options.length;j++) {
		    if ($(param_id).options[j].value==param.value) {
			$(param_id).selectedIndex=j;
		    }
		}
	    }
        }
	$(this.pageid+'_'+this.boxIndex + '_refreshrate').value=this.block_obj.refresh;

	this.view.setModified(1);
	this.reloadBoxData();

	// close the edit box
   	// var editBox= $('dragableBoxEdit' + this.pageid+"_"+this.boxIndex);
      	// editBox.style.display='none';
        // $('dragableBoxHeader' + this.pageid+"_"+this.boxIndex).style.height = '20px';
    },

    createABox: function() {
   	var div = document.createElement('DIV');
   	div.className = 'dragableBox';
   	div.id = 'dragableBox' + this.pageid+'_'+this.boxIndex;
   	this.addBoxHeader(div);
   	this.addEditContent(div)
   	this.addBoxContentContainer(div,this.heightOfBox);
   	this.addBoxStatusBar(div);   

   	var obj = $('_dragColumn' + this.pageid+"_"+this.columnIndex);		
   	var subs = obj.getElementsByTagName('DIV');
   	if(subs.length>0){
      	    obj.insertBefore(div,subs[0]);
   	}else{
      	    obj.appendChild(div);
   	}
	this.obj=div;
	this.parentObj=div.parentNode;
    },

    addBoxHeader: function(parentObj) {
   	var div = document.createElement('DIV');
   	div.className = 'dragableBoxHeader';
   	div.style.cursor = 'move';
   	div.id = 'dragableBoxHeader' + this.pageid+'_'+this.boxIndex;
   	div.onmouseover = mouseoverBoxHeader;
   	div.onmouseout = mouseoutBoxHeader;
   	div.onmousedown = initDragDropBox;
   
   	var image = document.createElement('IMG');
   	image.id = 'dragableBoxExpand' + this.pageid+"_"+this.boxIndex;
   	image.src = rightArrowImage;
   	image.style.visibility = 'hidden';	
   	image.style.cursor = 'pointer';
   	image.onmousedown = showHideBoxContent;	
      	image.setAttribute('alt','Expand/Collapse Widget');
   	div.appendChild(image);
   
   	var textSpan = document.createElement('SPAN');
   	textSpan.id = 'dragableBoxHeadertxt' + this.pageid+"_"+this.boxIndex;
   	div.appendChild(textSpan);
   
   	parentObj.appendChild(div);	
   
	/*
   	var closeLink = document.createElement('A');
   	closeLink.style.cssText = 'float:right';
   	closeLink.style.styleFloat = 'right';
   	closeLink.id = 'dragableBoxCloseLink' + this.pageid+"_"+this.boxIndex;
   	closeLink.innerHTML = 'x';
   	closeLink.className = 'closeButton';
   	closeLink.onmouseover = mouseover_CloseButton;
   	closeLink.onmouseout = mouseout_CloseButton;
   	closeLink.style.cursor = 'pointer';
   	closeLink.style.visibility = 'hidden';
   	closeLink.onmousedown = closeDragableBox;
   	div.appendChild(closeLink);
	*/

   	var ControlSpan = document.createElement('SPAN');
   	ControlSpan.style.cssText = 'float:right';
   	ControlSpan.style.styleFloat = 'right';

   	parentObj.appendChild(div);	

   	var statusBar = document.createElement('SPAN');
   	statusBar.style.visibility = 'hidden';
   	statusBar.id = 'dragableBoxStatusBar' + this.pageid+"_"+this.page.boxIndex;
        ControlSpan.appendChild(statusBar);

   	image = document.createElement('IMG');
   	image.src = editImage;
   	image.id = 'dragableBoxEditSource' + this.pageid+"_"+this.boxIndex;
   	image.style.border = '0';
   	image.style.visibility = 'hidden';
   	image.onclick = editContent;
   	image.style.cursor = 'pointer';
      	image.setAttribute('alt','Edit Widget');
   	ControlSpan.appendChild(image);

   	textSpan = document.createElement('SPAN');
	textSpan.innerHTML='&nbsp;';
   	ControlSpan.appendChild(textSpan);
   
   	image = document.createElement('IMG');
   	image.src = refreshImage;
   	image.id = 'dragableBoxRefreshSource' + this.pageid+"_"+this.boxIndex;
   	image.style.visibility = 'hidden';
   	image.onclick = refreshContent;
   	image.style.cursor = 'pointer';
      	image.setAttribute('alt','Refresh Widget');
   	ControlSpan.appendChild(image);

   	textSpan = document.createElement('SPAN');
	textSpan.innerHTML='&nbsp;';
   	ControlSpan.appendChild(textSpan);
   
   	var image = document.createElement('IMG');
   	image.src = closeImage;
   	image.id = 'dragableBoxCloseSource' + this.pageid+"_"+this.boxIndex;
   	image.style.visibility = 'hidden';
   	image.onclick = closeDragableBox;
      	image.setAttribute('alt','Remove Widget');
   	image.style.cursor = 'pointer';
   	ControlSpan.appendChild(image);

   	div.appendChild(ControlSpan);
    },

    addBoxStatusBar: function(parentObj) {

    },
	

    addBoxContentContainer: function(parentObj){
   	var div = document.createElement('DIV');
   	div.className = 'dragableBoxContent';
	if (_isIE()) {
	    div.style.width="100%";
	}
   	if(opera)div.style.clear='none';
   	div.id = 'dragableBoxContent' + this.pageid+'_'+this.boxIndex;
   	parentObj.appendChild(div);			
   	if(this.heightOfBox && this.heightOfBox/1>40){
      	    div.style.height = this.heightOfBox + 'px';
      	    div.setAttribute('heightOfBox',this.heightOfBox);
      	    div.heightOfBox = this.heightOfBox;	
      	    if(document.all)div.style.overflowY = 'auto';else div.style.overflow='-moz-scrollbars-vertical;';
      	    if(opera)div.style.overflow='auto';
   	}		
   	// div.style.overflowX='auto';
   	div.style.overflow='hidden';
    },

    getParameter: function(p) {
	for (iCount=0;iCount<this.block_obj.parameters.length;iCount++) {
	    if (this.block_obj.parameters[iCount].name==p) {
		return this.block_obj.parameters[iCount].value;
            }
        }
	return "";
    },

    setParameter: function(name,value) {
	var setted=0;
	for (iCount=0;iCount<this.block_obj.parameters.length;iCount++) {
	    if (this.block_obj.parameters[iCount].name==name) {
		this.block_obj.parameters[iCount].value=value;
		setted=1;
            }
        }
	if (setted==0) {
	    var count=this.block_obj.parameters.length;
	    var newParam=new portalWidgetParam(name,value);
	    this.block_obj.parameters[count]=newParam;		    
	}
    },

    getParametersString: function() {
	var module="Workspace";
	if (this.block_obj.module) {
	    module=this.block_obj.module;
	}
	var string=module+"/";
	string+='&page='+this.pageid+'&boxIndex='+this.boxIndex+'&boxId='+this.pageid+'_'+this.boxIndex;

	for (iCount=0;iCount<this.block_obj.parameters.length;iCount++) {
	    string+="&";
	    string+=this.block_obj.parameters[iCount].name+"="+escape(this.block_obj.parameters[iCount].value);
        }
	return string;
    },

    showStatusBarMessage: function(message) {
    	$('dragableBoxStatusBar' + this.pageid+"_"+this.boxIndex).innerHTML = message;   
    },

    reloadBoxData: function() {

   	this.showStatusBarMessage('<img src="/hicc/images/loading.gif">');
        showHeader(this.pageid+"_"+this.boxIndex, 'visible');
        var url=this.getWidgetUrl();
        var parameters="";
	var d=new Date();
        if(this.block_obj.module) {
            if(this.block_obj.module.match(/^http:\/\//)) {
                url=this.block_obj.module;
            } else {
                url=this.getWidgetUrl()+this.block_obj.module;
                parameters=this.getParametersString()+"&_s="+(d.getTime());
            }
        }
	var myAjax = new Ajax.Request(
         	url,
         	{ method: 'get', 
		parameters: parameters,
		onSuccess: function(request) { loadContentComplete(request); }
		}		
      		);   
    }
}

function showHeader(id, state) {
   $('dragableBoxExpand' + id).style.visibility = state;		
   $('dragableBoxRefreshSource' + id).style.visibility = state;
   $('dragableBoxEditSource' + id).style.visibility = state;
   $('dragableBoxCloseSource' + id).style.visibility = state;
   $('dragableBoxStatusBar' + id).style.visibility = state;
}

function mouseoverBoxHeader() {
   if(dragDropCounter==10)return;
   var id = this.id.replace(/[^0-9|_]/g,'');
   showHeader(id, 'visible');
}

function mouseoutBoxHeader(e,obj) {
   if(!obj)obj=this;
   
   var id = obj.id.replace(/[^0-9|_]/g,'');
   showHeader(id, 'hidden');
}
	
function initDragDropBox(e) {
		
   dragDropCounter = 1;
   if(document.all)e = event;
   
   if (e.target) source = e.target;
   else if (e.srcElement) source = e.srcElement;
   if (source.nodeType == 3) {
	// defeat Safari bug
        source = source.parentNode;
    }
   
   if(source.tagName.toLowerCase()=='img' || source.tagName.toLowerCase()=='a' || source.tagName.toLowerCase()=='input' || source.tagName.toLowerCase()=='td' || source.tagName.toLowerCase()=='tr' || source.tagName.toLowerCase()=='table')return;
   
   mouse_x = e.clientX;
   mouse_y = e.clientY;	
   var numericId = this.id.replace(/[^0-9|_]/g,'');
   var idsplit=numericId.split("_");
   numericId=idsplit[1];
   el_x = getLeftPos(this.parentNode)/1;
 var scrollTop=getScrollTop();

   el_y = getTopPos(this.parentNode)/1 - scrollTop;
   
   dragObject = this.parentNode;
   
   documentScrollHeight = document.documentElement.scrollHeight + 100 + dragObject.offsetHeight;
      
   if(dragObject.nextSibling){
      dragObjectNextSibling = dragObject.nextSibling;
      if(dragObjectNextSibling.tagName!='DIV')dragObjectNextSibling = dragObjectNextSibling.nextSibling;
   }
   if(dragableBoxesArray[numericId]) {
       dragObjectParent = dragableBoxesArray[numericId].parentObj;   
   }
   dragDropCounter = 0;
   initDragDropBoxTimer();	
   
   return false;
}
		
function initDragDropBoxTimer() {
   if(dragDropCounter>=0 && dragDropCounter<10){
      dragDropCounter++;
      setTimeout('initDragDropBoxTimer()',10);
      return;
   }
   if(dragDropCounter==10){
      mouseoutBoxHeader(false,dragObject);
   }   
}

function showHideBoxContentHelper(id, forceExpand) {
   var obj = $('dragableBoxContent' + id);
   var objImg = $('dragableBoxExpand' + id);
   
   if (forceExpand) {
       obj.style.display = (forceExpand == 'true')?'block':'none';
       objImg.src = (forceExpand == 'true')?rightArrowImage:downArrowImage;
   } else {
       obj.style.display = objImg.src.indexOf(rightArrowImage)>=0?'none':'block';
       objImg.src = objImg.src.indexOf(rightArrowImage)>=0?downArrowImage:rightArrowImage;
   }

   setTimeout('dragDropCounter=-5',5);
}

function showHideBoxContent()
{
   var numericId = this.id.replace(/[^0-9|_]/g,'');
   showHideBoxContentHelper(numericId);
}

function mouseover_CloseButton()
{
   this.className = 'closeButton_over';	
   setTimeout('dragDropCounter=-5',5);
}

function highlightCloseButton()
{
   this.className = 'closeButton_over';	
}

function mouseout_CloseButton()
{
   this.className = 'closeButton';	
}

function closeDragableBox()
{
   if (confirm("Are you sure that you want to remove the widget?")) {
       var numericId = this.id.replace(/[^0-9|_]/g,'');
       $('dragableBox' + numericId).style.display='none';	
       var idsplit=numericId.split("_");
       var widget=findWidget(idsplit[0],idsplit[1]);
       if (widget!=null) {
          widget.view.setModified(1);
          // saveView(_currentViewId);
       }
   
       setTimeout('dragDropCounter=-5',5);
    }	
}

function selectAll(c1) {
    if (document.all) { s2=document.all[c1];} else if (document.getElementById) { s2=document.getElementById(c1); }
    for (index=0;index<s2.options.length;index++) { s2.options[index].selected=true; }
    return true;
}

function moveItem(c1,c2) {
    var s1=document.getElementById(c1);
    var s2=document.getElementById(c2);
    for (index=0;index<s1.options.length;index++) {
       if (s1.options[index].selected==true) {
	    if (!itemExist(s2,s1.options[index])) {
	        s2.options[s2.options.length]=new Option(s1.options[index].text, s1.options[index].value, false, false);
            }
        }
    }
    for (index=(s1.options.length-1);index>=0;index--) {
       if (s1.options[index].selected==true) {
	    s1.options[index]=null;
        }
    }
}

function itemExist(lst,itm) {
	for (var i = 0; i < lst.options.length; i++)
		if (lst.options[i].text == itm.text && lst.options[i].value == itm.value) return true;
	return;
}

function moveUpList(c1) {
    if (document.all) { listField=document.all[c1];} else if (document.getElementById) { listField=document.getElementById(c1); }

   if ( listField.length == -1) { alert("There are no values which can be moved!"); } else {
      var selected = listField.selectedIndex;
      if (selected == -1) { alert("You must select an entry to be moved!"); }
      else {  // Something is selected
         if ( listField.length == 0 ) {  // If there's only one in the list
            alert("There is only one entry!\nThe one entry will remain in place.");
         } else {  // There's more than one in the list, rearrange the list order
            if ( selected == 0 ) {
               alert("The first entry in the list cannot be moved up.");
            } else {
               // Get the text/value of the one directly above the hightlighted entry as
               // well as the highlighted entry; then flip them
               var moveText1 = listField[selected-1].text; var moveText2 = listField[selected].text;
               var moveValue1 = listField[selected-1].value; var moveValue2 = listField[selected].value;
               listField[selected].text = moveText1; listField[selected].value = moveValue1;
               listField[selected-1].text = moveText2; listField[selected-1].value = moveValue2;
               listField.selectedIndex = selected-1; // Select the one that was selected before
            }  // Ends the check for selecting one which can be moved
         }  // Ends the check for there only being one in the list to begin with
      }  // Ends the check for there being something selected
   }  // Ends the check for there being none in the list
}

function moveDownList(c1) {
    if (document.all) { listField=document.all[c1];} else if (document.getElementById) { listField=document.getElementById(c1); }

   if ( listField.length == -1) { alert("There are no values which can be moved!");
   } else {
      var selected = listField.selectedIndex;
      if (selected == -1) { alert("You must select an entry to be moved!"); }
      else {  // Something is selected
         if ( listField.length == 0 ) {  // If there's only one in the list
            alert("There is only one entry!\nThe one entry will remain in place.");
         } else {  // There's more than one in the list, rearrange the list order
            if ( selected == listField.length-1 ) {
               alert("The last entry in the list cannot be moved down.");
            } else {
               // Get the text/value of the one directly below the hightlighted entry as
               // well as the highlighted entry; then flip them
               var moveText1 = listField[selected+1].text;  var moveText2 = listField[selected].text;
               var moveValue1 = listField[selected+1].value; var moveValue2 = listField[selected].value;
               listField[selected].text = moveText1; listField[selected].value = moveValue1;
               listField[selected+1].text = moveText2; listField[selected+1].value = moveValue2;
               listField.selectedIndex = selected+1; // Select the one that was selected before
            }  // Ends the check for selecting one which can be moved
         }  // Ends the check for there only being one in the list to begin with
      }  // Ends the check for there being something selected
   }  // Ends the check for there being none in the list
}

		
function editContent() {
   var numericId = this.id.replace(/[^0-9|_]/g,'');
   var obj = $('dragableBoxEdit' + numericId);
   if(obj.style.display=='none'){
      obj.style.display='block';
   }else{
      obj.style.display='none';
   }
   setTimeout('dragDropCounter=-5',5);
}

function refreshContent()
{
   var id=this.id.replace(/[^0-9|_]/g,'');
   var idsplit=id.split("_");
   reloadPageBoxData(idsplit[0],idsplit[1]);
   setTimeout('dragDropCounter=-5',5);
}

function loadContentComplete(request) {
   var boxId=request.getResponseHeader('boxId');
   if ((boxId!=null) && (boxId != '')) {
   	var status=request.getResponseHeader('return_status');	
	if ((status!=null) && (status != '')) {
       	    $('dragableBoxStatusBar'+boxId).innerHTML=status+'&nbsp;';
	} else {
       	    var d=new Date();
            $msg="<font style='font-size:9px;'>Updated: "+d.formatDate("H:i:s")+"&nbsp;&nbsp;</font>";
       	    $('dragableBoxStatusBar'+boxId).innerHTML=$msg;
            if($('dragableBoxRefreshSource'+boxId)) {
            $('dragableBoxRefreshSource'+boxId).setAttribute('alt','Refresh Widget. '+$msg);
            }
            if($('dragableBoxContent'+boxId)) {
                $('dragableBoxContent'+boxId).innerHTML=request.responseText;
            }
            var s_index = request.responseText.indexOf("<script");
            var e_index = request.responseText.indexOf("</script>");
            var jscript = request.responseText.substring(s_index,e_index);
            s_index = jscript.indexOf(">");
            jscript = jscript.substring(s_index+1,jscript.length);
            try {
                eval(jscript);
            } catch(e) {
                alert(e);
            }
	}
        showHeader(boxId, 'hidden');
   }
}

function resetParameters(id) {
   var idsplit=id.split("_");
	_currentView.pages[idsplit[0]].dragableBoxesArray[idsplit[1]].resetParameters();
}

function saveParameters(id) {
   var idsplit=id.split("_");
	_currentView.pages[idsplit[0]].dragableBoxesArray[idsplit[1]].saveParameters();
}

Array.prototype.exists = function (x) {
    for (var i = 0; i < this.length; i++) {
        if (this[i] == x) return true;
    }
    return false;
}

Date.prototype.formatDate = function (input,time) {
    // formatDate :
    // a PHP date like function, for formatting date strings
    // See: http://www.php.net/date
    //
    // input : format string
    // time : epoch time (seconds, and optional)
    //
    // if time is not passed, formatting is based on 
    // the current "this" date object's set time.
    //
    // supported:
    // a, A, B, d, D, F, g, G, h, H, i, j, l (lowercase L), L, 
    // m, M, n, O, r, s, S, t, U, w, W, y, Y, z
    //
    // unsupported:
    // I (capital i), T, Z    

    var switches =    ["a", "A", "B", "d", "D", "F", "g", "G", "h", "H", 
                       "i", "j", "l", "L", "m", "M", "n", "O", "r", "s", 
                       "S", "t", "U", "w", "W", "y", "Y", "z"];
    var daysLong =    ["Sunday", "Monday", "Tuesday", "Wednesday", 
                       "Thursday", "Friday", "Saturday"];
    var daysShort =   ["Sun", "Mon", "Tue", "Wed", 
                       "Thu", "Fri", "Sat"];
    var monthsShort = ["Jan", "Feb", "Mar", "Apr",
                       "May", "Jun", "Jul", "Aug", "Sep",
                       "Oct", "Nov", "Dec"];
    var monthsLong =  ["January", "February", "March", "April",
                       "May", "June", "July", "August", "September",
                       "October", "November", "December"];
    var daysSuffix = ["st", "nd", "rd", "th", "th", "th", "th", // 1st - 7th
                      "th", "th", "th", "th", "th", "th", "th", // 8th - 14th
                      "th", "th", "th", "th", "th", "th", "st", // 15th - 21st
                      "nd", "rd", "th", "th", "th", "th", "th", // 22nd - 28th
                      "th", "th", "st"];                        // 29th - 31st

    function a() {
        // Lowercase Ante meridiem and Post meridiem
        return self.getHours() > 11? "pm" : "am";
    }
    function A() {
        // Uppercase Ante meridiem and Post meridiem
        return self.getHours() > 11? "PM" : "AM";
    }

    function B(){
        // Swatch internet time. code simply grabbed from ppk,
        // since I was feeling lazy:
        // http://www.xs4all.nl/~ppk/js/beat.html
        var off = (self.getTimezoneOffset() + 60)*60;
        var theSeconds = (self.getHours() * 3600) + 
                         (self.getMinutes() * 60) + 
                          self.getSeconds() + off;
        var beat = Math.floor(theSeconds/86.4);
        if (beat > 1000) beat -= 1000;
        if (beat < 0) beat += 1000;
        if ((""+beat).length == 1) beat = "00"+beat;
        if ((""+beat).length == 2) beat = "0"+beat;
        return beat;
    }
    
    function d() {
        // Day of the month, 2 digits with leading zeros
        return new String(self.getDate()).length == 1?
        "0"+self.getDate() : self.getDate();
    }
    function D() {
        // A textual representation of a day, three letters
        return daysShort[self.getDay()];
    }
    function F() {
        // A full textual representation of a month
        return monthsLong[self.getMonth()];
    }
    function g() {
        // 12-hour format of an hour without leading zeros
        return self.getHours() > 12? self.getHours()-12 : self.getHours();
    }
    function G() {
        // 24-hour format of an hour without leading zeros
        return self.getHours();
    }
    function h() {
        // 12-hour format of an hour with leading zeros
        if (self.getHours() > 12) {
          var s = new String(self.getHours()-12);
          return s.length == 1?
          "0"+ (self.getHours()-12) : self.getHours()-12;
        } else { 
          var s = new String(self.getHours());
          return s.length == 1?
          "0"+self.getHours() : self.getHours();
        }  
    }
    function H() {
        // 24-hour format of an hour with leading zeros
        return new String(self.getHours()).length == 1?
        "0"+self.getHours() : self.getHours();
    }
    function i() {
        // Minutes with leading zeros
        return new String(self.getMinutes()).length == 1? 
        "0"+self.getMinutes() : self.getMinutes(); 
    }
    function j() {
        // Day of the month without leading zeros
        return self.getDate();
    }    
    function l() {
        // A full textual representation of the day of the week
        return daysLong[self.getDay()];
    }
    function L() {
        // leap year or not. 1 if leap year, 0 if not.
        // the logic should match iso's 8601 standard.
        var y_ = Y();
        if (         
            (y_ % 4 == 0 && y_ % 100 != 0) ||
            (y_ % 4 == 0 && y_ % 100 == 0 && y_ % 400 == 0)
            ) {
            return 1;
        } else {
            return 0;
        }
    }
    function m() {
        // Numeric representation of a month, with leading zeros
        return self.getMonth() < 9?
        "0"+(self.getMonth()+1) : 
        self.getMonth()+1;
    }
    function M() {
        // A short textual representation of a month, three letters
        return monthsShort[self.getMonth()];
    }
    function n() {
        // Numeric representation of a month, without leading zeros
        return self.getMonth()+1;
    }
    function O() {
        // Difference to Greenwich time (GMT) in hours
        var os = Math.abs(self.getTimezoneOffset());
        var h = ""+Math.floor(os/60);
        var m = ""+(os%60);
        h.length == 1? h = "0"+h:1;
        m.length == 1? m = "0"+m:1;
        return self.getTimezoneOffset() < 0 ? "+"+h+m : "-"+h+m;
    }
    function r() {
        // RFC 822 formatted date
        var r; // result
        //  Thu    ,     21          Dec         2000
        r = D() + ", " + j() + " " + M() + " " + Y() +
        //        16     :    01     :    07          +0200
            " " + H() + ":" + i() + ":" + s() + " " + O();
        return r;
    }
    function S() {
        // English ordinal suffix for the day of the month, 2 characters
        return daysSuffix[self.getDate()-1];
    }
    function s() {
        // Seconds, with leading zeros
        return new String(self.getSeconds()).length == 1?
        "0"+self.getSeconds() : self.getSeconds();
    }
    function t() {

        // thanks to Matt Bannon for some much needed code-fixes here!
        var daysinmonths = [null,31,28,31,30,31,30,31,31,30,31,30,31];
        if (L()==1 && n()==2) return 29; // leap day
        return daysinmonths[n()];
    }
    function U() {
        // Seconds since the Unix Epoch (January 1 1970 00:00:00 GMT)
        return Math.round(self.getTime()/1000);
    }
    function W() {
        // Weeknumber, as per ISO specification:
        // http://www.cl.cam.ac.uk/~mgk25/iso-time.html
        
        // if the day is three days before newyears eve,
        // there's a chance it's "week 1" of next year.
        // here we check for that.
        var beforeNY = 364+L() - z();
        var afterNY  = z();
        var weekday = w()!=0?w()-1:6; // makes sunday (0), into 6.
        if (beforeNY <= 2 && weekday <= 2-beforeNY) {
            return 1;
        }
        // similarly, if the day is within threedays of newyears
        // there's a chance it belongs in the old year.
        var ny = new Date("January 1 " + Y() + " 00:00:00");
        var nyDay = ny.getDay()!=0?ny.getDay()-1:6;
        if (
            (afterNY <= 2) && 
            (nyDay >=4)  && 
            (afterNY >= (6-nyDay))
            ) {
            // Since I'm not sure we can just always return 53,
            // i call the function here again, using the last day
            // of the previous year, as the date, and then just
            // return that week.
            var prevNY = new Date("December 31 " + (Y()-1) + " 00:00:00");
            return prevNY.formatDate("W");
        }
        
        // week 1, is the week that has the first thursday in it.
        // note that this value is not zero index.
        if (nyDay <= 3) {
            // first day of the year fell on a thursday, or earlier.
            return 1 + Math.floor( ( z() + nyDay ) / 7 );
        } else {
            // first day of the year fell on a friday, or later.
            return 1 + Math.floor( ( z() - ( 7 - nyDay ) ) / 7 );
        }
    }
    function w() {
        // Numeric representation of the day of the week
        return self.getDay();
    }
    
    function Y() {
        // A full numeric representation of a year, 4 digits

        // we first check, if getFullYear is supported. if it
        // is, we just use that. ppks code is nice, but wont
        // work with dates outside 1900-2038, or something like that
        if (self.getFullYear) {
            var newDate = new Date("January 1 2001 00:00:00 +0000");
            var x = newDate .getFullYear();
            if (x == 2001) {              
                // i trust the method now
                return self.getFullYear();
            }
        }
        // else, do this:
        // codes thanks to ppk:
        // http://www.xs4all.nl/~ppk/js/introdate.html
        var x = self.getYear();
        var y = x % 100;
        y += (y < 38) ? 2000 : 1900;
        return y;
    }
    function y() {
        // A two-digit representation of a year
        var y = Y()+"";
        return y.substring(y.length-2,y.length);
    }
    function z() {
        // The day of the year, zero indexed! 0 through 366
        var t = new Date("January 1 " + Y() + " 00:00:00");
        var diff = self.getTime() - t.getTime();
        return Math.floor(diff/1000/60/60/24);
    }
        
    var self = this;
    if (time) {
        // save time
        var prevTime = self.getTime();
        self.setTime(time);
    }
    
    var ia = input.split("");
    var ij = 0;
    while (ia[ij]) {
        if (ia[ij] == "\\") {
            // this is our way of allowing users to escape stuff
            ia.splice(ij,1);
        } else {
            if (switches.exists(ia[ij])) {
                ia[ij] = eval(ia[ij] + "()");
            }
        }
        ij++;
    }
    // reset time, back to what it was
    if (prevTime) {
        self.setTime(prevTime);
    }
    return ia.join("");
}

var dragableBoxesArray;

var portalPage = Class.create();

portalPage.prototype = {
    initialize: function(tag,id,title,view,div,columns,layout) {
	this.page_tag=tag;
	this.selected=false;
	this.pageid=id;
	this.view=view;	
	this.title=title;
	this.div=div;

	if ((layout) && (layout != null) && (typeof layout != "undefined")) {
	    this.columns=columns;
	    this.pageLayout=layout;
        } else {
	    this.columns=0;
	    var tmpLayout=new Object;
    	    tmpLayout.layout=new Array();
    	    tmpLayout.colSize=new Array();
	    this.pageLayout=tmpLayout;
        }
	this.dragableBoxesArray=new Array();
	this.boxIndex=0;
	this.setup=false;
	this.initEvents();
	this.dragableRuler=new Object;
    },

    setupPage: function() {
	this.createColumns();
	this.createBoxes();
	dragableBoxesArray=this.dragableBoxesArray;
    },

    getConfigObj: function() {
    	var p=new Object();
	if (!this.setup) {
	    p=this.pageLayout;
     	    p.title=this.title;
    	    p.columns=this.columns;
        } else {
    	    p.title=this.title;
    	    p.columns=this.columns;
    	    p.layout=new Array();
    	    p.colSize=new Array();

	    var colIndex=0;
    	    var cols=this.div.childNodes;

    	    for (var j=0;j<cols.length;j++) {
	        if (cols[j].id.indexOf('_dragColumn')!=(-1)) {
    	            var c=new Array;
    	            var widgets=cols[j].childNodes;
	            var count=0;
    	            for (var k=0;k<widgets.length;k++) {	
		        if ((widgets[k].id != '') && (widgets[k].style.display!='none')) {
		            // c[k]=widgets[k].id;
		            var numericId = widgets[k].id.replace(/[^0-9|_]/g,'');
   		            var idsplit=numericId.split("_");
		            var widget=findWidget(idsplit[0],idsplit[1]);
		            if (widget!=null) {
		    	        c[count]=widget.getConfigObj();
			        count++;
		            }
		        }
    	            }
	    	    p.layout[colIndex]=c;
		    p.colSize[colIndex]=getWidth(cols[j]);
                    colIndex++;
	        }
	    }
        }
    	return p;
    },
	
    pageSelected: function() {
	if (!this.setup) {
	    this.setupPage();
	    this.setup=true;	
	}
	this.initEvents();
	this.view.currentPage=this.view.findPage(this.page_tag);
	$('li_'+this.page_tag).className='tab-on';
	$('page_'+this.page_tag).style.display='block';
	$('page_control_'+this.page_tag).style.display='block';
	$('page_tab_input_span_'+this.page_tag).style.display='none';
	$('page_tab_span_'+this.page_tag).style.display='block';
	this.selected=true;
	dragableBoxesArray=this.dragableBoxesArray;
    },

    pageDeselected: function() {
	this.selected=false;
	$('li_'+this.page_tag).className='tab-off';
	$('page_'+this.page_tag).style.display='none';
	$('page_control_'+this.page_tag).style.display='none';
	$('page_tab_input_span_'+this.page_tag).style.display='none';
	$('page_tab_span_'+this.page_tag).style.display='block';
    },

    initEvents: function() {
   	document.body.onmousemove = moveDragableElement;
   	document.body.onmouseup = stop_dragDropElement;
   	document.body.onselectstart = cancelSelectionEvent;   
   	document.body.ondragstart = cancelEvent;	   
	// BUGBUG
   	documentHeight = getClientHeight();		
    },

    deleteColumn: function() {
   	if(!this.div){
      	    alert('Parent div is not defined.');
            return;
   	}

	this.view.setModified(1);

	// first disable all the object in the column first
   	var cols=this.div.childNodes;  
   	if (cols.length > 2) {
	    var lastCol=cols[cols.length-2];
	    var lastColChildNodes=lastCol.childNodes;
	    for (var no=0;no<lastColChildNodes.length;no++) {
	    	if (lastColChildNodes[no].id != '') {
		    $(lastColChildNodes[no].id).style.display='none';
            	}
            }
   	}

   	this.columns--;
   	var columnWidth = Math.floor(100/this.columns);
   	var sumWidth = 0;

   	// remove the last column
   	var lastCol=this.div.removeChild(cols[cols.length-2]);

	var lastColWidth=getWidth(lastCol);
	var extraWidth=Math.round(lastColWidth/this.columns);

	if (cols[cols.length-2].id.indexOf('_dragColRuler')!=(-1)) {
	    // remove the draggable as well
	    this.dragableRuler[cols[cols.length-2].id].destroy();
	    this.dragableRuler[cols[cols.length-2].id]=null;
	    this.div.removeChild(cols[cols.length-2]);	    
	}

	cols=this.div.childNodes;
	currentColumn=0;
   	for(var no=0;no<cols.length;no++){
	    // for existing columns, add the extra percentage
            var div = cols[no];
	    if (div.id.indexOf('_dragColumn')!=(-1)) {
                if(currentColumn==(this.columns-1)) {
		    columnWidth = 99 - sumWidth;
                } else {
		    columnWidth=getWidth(div)+extraWidth;
		}
                sumWidth = sumWidth + columnWidth;
                div.style.cssText = 'float:left;width:'+columnWidth+'%;padding:0px;margin:0px;';
                div.style.width = columnWidth + '%';
		currentColumn++;
	    }
      	}
    },

    addColumn: function() {
   	if(!this.div){
      	    alert('Parent div is not defined.');
      	    return;
   	}
	
	this.view.setModified(1);
        if(this.columns>=6) {
            alert('Unable to add more columns.');
            return;
        }
	this.columns++;
        var suggestedColumnRatio=1;
        if(this.columns==2) {
            suggestedColumnRatio=0.5;
        } else if(this.columns==3) {
            suggestedColumnRatio=0.7;
        } else if(this.columns==4) {
            suggestedColumnRatio=0.75;
        } else if(this.columns==5) {
            suggestedColumnRatio=0.8;
        } else if(this.columns==6) {
            suggestedColumnRatio=0.84;
        }
   	//var suggestedColumnWidth = Math.floor(100/this.columns);
   	var sumWidth = 0;

   	// remove the clearingDiv
   	var cols=this.div.childNodes;
   	var clearingDiv=null;

   	if (cols.length > 1) {
       	    clearingDiv=this.div.removeChild(cols[cols.length-1]);
   	} else {
       	    clearingDiv = document.createElement('DIV');
   	}

	var tmp_cols=this.div.childNodes;
	cols=new Array();
	for (var no=0;no<tmp_cols.length;no++) {
	    if (tmp_cols[no].id.indexOf('_dragColumn')>=0) {
		cols.push(tmp_cols[no]);
	    }
        }
	var cols_length=cols.length;

	for(var no=0;no<cols_length;no++){
	    if (cols[no].id.indexOf('_dragColumn')!=(-1)) {
	  	// existing columns
          	var div = cols[no];
	        var oldwidth=getWidth(div);
		var columnWidth=Math.round(oldwidth*suggestedColumnRatio);
          	sumWidth = sumWidth + columnWidth;
          	div.style.cssText = 'float:left;width:'+columnWidth+'%;padding:0px;margin:0px;';
          	div.style.width = columnWidth + '%';

      	        if (no == (cols_length-1)) {

		    // add a ruler first
          	    var ruler=this.createDragPoint(no);
          	    this.div.appendChild(ruler);

	  	    this.dragableRuler[ruler.id]=new Draggable(ruler.id,{constraint:'horizontal',revert:moveRuler,
      			reverteffect: function(element, top_offset, left_offset) {
        		element._revert = new Effect.Move(element, { x: -left_offset, y: -top_offset, duration: 0.001});
      		    }});
            	    var div = document.createElement('DIV');
                    columnWidth = 99 - sumWidth;
          	    div.style.cssText = 'float:left;width:'+(columnWidth)+'%;padding:0px;margin:0px;';
          	    div.style.height='100%';
		    div.style.position='relative';
          	    div.style.styleFloat='left';
          	    div.style.width = (columnWidth) + '%';
          	    div.style.padding = '0px';
          	    div.style.margin = '0px';
		    div.style.background = '#ffffff';
      
          	    div.id = '_dragColumn' + this.pageid+"_"+(no+2);
          	    this.div.appendChild(div);
      
          	    var clearObj = document.createElement('HR');	
          	    clearObj.style.clear = 'both';
          	    clearObj.style.visibility = 'hidden';
          	    div.appendChild(clearObj);
                }
           }
       }       
       this.div.appendChild(clearingDiv);
    },

    createDragPoint: function(no) {
	    var ruler=document.createElement('DIV');
            var height = (document.height!==undefined) ? document.height : document.body.offsetHeight;
      	    ruler.style.cssText = 'float:left;width:5px;padding:0px;margin:0px;';
      	    ruler.style.height=height+'px';
      	    ruler.style.styleFloat='left';
      	    ruler.style.width = '5px';
      	    ruler.style.padding = '0px';
      	    ruler.style.margin = '0px';
            ruler.style.backgroundColor ='#ffffff' ;
   	    // ruler.style.visibility='hidden';	
	    ruler.onmouseover=mouseOverRuler;
	    ruler.onmouseout=mouseOutRuler;
      
            ruler.id = '_dragColRuler' + this.pageid+"_"+(no+1);
            ruler.innerHTML="&nbsp;";

      	    var clearObj = document.createElement('HR');	
      	    clearObj.style.clear = 'both';
      	    clearObj.style.visibility = 'hidden';
      	    ruler.appendChild(clearObj);

	    return ruler;
    },

    createColumns: function() {
	if(!this.div){
      	    alert('Parent div is not defined.');
      	    return;
   	}

   	var columnWidth = Math.floor(100/this.columns);
   	var sumWidth = 0;
   	for(var no=0;no<this.columns;no++){
      	    var div = document.createElement('DIV');	
	    if ((this.pageLayout.colSize) && (this.pageLayout.colSize[no])) {
	        columnWidth=this.pageLayout.colSize[no];
	    }
      	    if(no==(this.columns-1))columnWidth = 99 - sumWidth;
      	    sumWidth = sumWidth + columnWidth;
      	    div.style.cssText = 'float:left;width:'+(columnWidth-1)+'%;padding:0px;margin:0px;';
      	    div.style.height='100%';
		    div.style.position='relative';
      	    div.style.styleFloat='left';
      	    div.style.width = (columnWidth) + '%';
      	    div.style.padding = '0px';
      	    div.style.margin = '0px';
            div.style.background = '#ffffff';
      
      	    div.id = '_dragColumn' + this.pageid+"_"+(no+1);
      	    this.div.appendChild(div);
      
      	    var clearObj = document.createElement('HR');	
      	    clearObj.style.clear = 'both';
      	    clearObj.style.visibility = 'hidden';
      	    div.appendChild(clearObj);

	    if (no!=(this.columns-1)) {
                var ruler=this.createDragPoint(no);
                this.div.appendChild(ruler);

	  	this.dragableRuler[ruler.id]=new Draggable(ruler.id,{constraint:'horizontal',revert:moveRuler,
      		    reverteffect: function(element, top_offset, left_offset) {
        	    element._revert = new Effect.Move(element, { x: -left_offset, y: -top_offset, duration: 0.001});
      		}});
	    }
        }
       
   	var clearingDiv = document.createElement('DIV');
   	this.div.appendChild(clearingDiv);
   	clearingDiv.style.clear='both';   

    },

    createBoxes: function() {
    	for (var i=0;i<this.pageLayout.layout.length;i++) {
	    for (var j=this.pageLayout.layout[i].length-1;j>=0;j--) {
		this.addWidget(this.pageLayout.layout[i][j], i+1,false);
            }
    	}
    },

    addWidget: function(block_obj, columnIndex, heightOfBox) {
	this.boxIndex++;
	this.dragableBoxesArray[this.boxIndex]=new portalWidget(this, this.boxIndex, block_obj, columnIndex, heightOfBox);

    },

    collapse_all: function () {
	close_all_popup();
    	for(i=1;i<=this.boxIndex;i++) {
            id='dragableBox'+this.pageid+'_'+i;
   	    if ($(id).style.display!='none') {
	       showHideBoxContentHelper(this.pageid+'_'+i,'false');
            }
        }
    },

    expand_all: function() {
	close_all_popup();
    	for(i=1;i<=this.boxIndex;i++) {
            id='dragableBox'+this.pageid+'_'+i;
   	    if ($(id).style.display!='none') {
	       showHideBoxContentHelper(this.pageid+'_'+i,'true');
            }
        }
    },

    refresh_all: function() {
	close_all_popup();
    	for(var i=1;i<=this.boxIndex;i++) {
            id='dragableBox'+this.pageid+'_'+i;
            if($(id)!=null) {
   	        if ($(id).style.display!='none') {
	            this.dragableBoxesArray[i].reloadBoxData();
                }
            }
        }
    }
}

function moveDragableElement(e) {
   if(document.all)e = event;
   if(dragDropCounter<10)return;
   
   if(document.body!=dragObject.parentNode){
      dragObject.style.width = (dragObject.offsetWidth - (dragObjectBorderWidth*2)) + 'px';
      dragObject.style.position = 'absolute';	
      dragObject.style.textAlign = 'left';
      if(transparencyWhenDragging){	
	 dragObject.style.filter = 'alpha(opacity=70)';
	 dragObject.style.opacity = '0.7';
      }	
      dragObject.parentNode.insertBefore(rectangleDiv,dragObject);
      rectangleDiv.style.display='block';
      document.body.appendChild(dragObject);
      
      rectangleDiv.style.width = dragObject.style.width;
      rectangleDiv.style.height = (dragObject.offsetHeight - (dragObjectBorderWidth*2)) + 'px';       
   }
		
   if(e.clientY<50 || e.clientY>(documentHeight-50)){
      if(e.clientY<50 && !autoScrollActive){
	 autoScrollActive = true;
	 autoScroll((autoScrollSpeed*-1),e.clientY);
      }
      
      if(e.clientY>(documentHeight-50) && document.documentElement.scrollHeight<=documentScrollHeight && !autoScrollActive){
	 autoScrollActive = true;
	 autoScroll(autoScrollSpeed,e.clientY);
      }
   }else{
      autoScrollActive = false;
   }		
   
   var leftPos = e.clientX;
   var topPos = e.clientY + getScrollTop();
   
   dragObject.style.left = (e.clientX - mouse_x + el_x) + 'px';
   dragObject.style.top = (el_y - mouse_y + e.clientY + getScrollTop()) + 'px';
   
   if(!okToMove)return;
   okToMove = false;
   
   destinationObj = false;	
   rectangleDiv.style.display = 'none'; 
   
   var objFound = false;
   var tmpParentArray = new Array();
   
   if(!objFound){
      for(var no=1;no<dragableBoxesArray.length;no++){
	 if(dragableBoxesArray[no].obj==dragObject)continue;
	 tmpParentArray[dragableBoxesArray[no].obj.parentNode.id] = true;
	 if(!objFound){
	    var tmpX = getLeftPos(dragableBoxesArray[no].obj);
	    var tmpY = getTopPos(dragableBoxesArray[no].obj);
	    
	    if(leftPos>tmpX && leftPos<(tmpX + dragableBoxesArray[no].obj.offsetWidth) && topPos>(tmpY-20) && topPos<(tmpY + (dragableBoxesArray[no].obj.offsetHeight/2))){
	       destinationObj = dragableBoxesArray[no].obj;
	       destinationObj.parentNode.insertBefore(rectangleDiv,dragableBoxesArray[no].obj);
	       rectangleDiv.style.display = 'block';
	       objFound = true;
	       break;	       
	    }
	    
	    if(leftPos>tmpX && leftPos<(tmpX + dragableBoxesArray[no].obj.offsetWidth) && topPos>=(tmpY + (dragableBoxesArray[no].obj.offsetHeight/2)) && topPos<(tmpY + dragableBoxesArray[no].obj.offsetHeight)){
	       objFound = true;
	       if(dragableBoxesArray[no].obj.nextSibling){		  
		  destinationObj = dragableBoxesArray[no].obj.nextSibling;
		  if(!destinationObj.tagName)destinationObj = destinationObj.nextSibling;
		  if(destinationObj!=rectangleDiv)destinationObj.parentNode.insertBefore(rectangleDiv,destinationObj);
	       }else{
		  destinationObj = dragableBoxesArray[no].obj.parentNode;
		  dragableBoxesArray[no].obj.parentNode.appendChild(rectangleDiv);
	       }
	       rectangleDiv.style.display = 'block';
	       break;					
	    }
	    	    
	    if(!dragableBoxesArray[no].obj.nextSibling && leftPos>tmpX && leftPos<(tmpX + dragableBoxesArray[no].obj.offsetWidth)
	       && topPos>topPos>(tmpY + (dragableBoxesArray[no].obj.offsetHeight))){
	       destinationObj = dragableBoxesArray[no].obj.parentNode;
	       dragableBoxesArray[no].obj.parentNode.appendChild(rectangleDiv);	
	       rectangleDiv.style.display = 'block';	
	       objFound = true;					       
	    }
	 }	 
      }
   }
   
   if(!objFound){      
      for(var no=1;no<=_currentView.pages[_currentView.currentPage].columns;no++){
	 if(!objFound){

	    var obj = $('_dragColumn' + _currentView.currentPage+"_"+no);	    
	    var left = getLeftPos(obj)/1;			    
	    var width = obj.offsetWidth;
	    if(leftPos>left && leftPos<(left+width)){
	       destinationObj = obj;
	       obj.appendChild(rectangleDiv);
	       rectangleDiv.style.display='block';
	       objFound=true;			       
	    }				
	 }
      }					
   }
	
   setTimeout('okToMove=true',5);		
}
	
function stop_dragDropElement() {		
   if(dragDropCounter<10){
      dragDropCounter = -1
      return;
   }
   dragDropCounter = -1;
   if(transparencyWhenDragging){
      dragObject.style.filter = null;
      dragObject.style.opacity = null;
   }		
   dragObject.style.position = 'static';
   dragObject.style.width = null;
   var numericId = dragObject.id.replace(/[^0-9|_]/g,'');
   var idsplit=numericId.split("_");
   numericId=idsplit[1];
   if(destinationObj && destinationObj.id!=dragObject.id){      
      if(destinationObj.id.indexOf('_dragColumn')>=0){
	 destinationObj.appendChild(dragObject);
	 dragableBoxesArray[numericId].parentObj = destinationObj;
      }else{
	 destinationObj.parentNode.insertBefore(dragObject,destinationObj);
	 dragableBoxesArray[numericId].parentObj = destinationObj.parentNode;
      }
   }else{
      if(dragObjectNextSibling){
	dragObjectParent.insertBefore(dragObject,dragObjectNextSibling);
      }else{
	 dragObjectParent.appendChild(dragObject);
      }				
   }
	
   autoScrollActive = false;
   rectangleDiv.style.display = 'none'; 
   dragObject = false;
   dragObjectNextSibling = false;
   destinationObj = false;

   _currentView.setModified(1);   
   documentHeight = getClientHeight();	
}

function cancelEvent() {
   return false;
}

function cancelSelectionEvent(e) {
   if(document.all)e = event;
   
   if (e.target) source = e.target;
   else if (e.srcElement) source = e.srcElement;
   if (source.nodeType == 3) // defeat Safari bug
      source = source.parentNode;
   if(source.tagName.toLowerCase()=='input')return true;   
   if(dragDropCounter>=0)return false; else return true;   
}

function moveRuler(el) {
   // debug("end move ruler:"+el.id);
   // resize column
   // find left and right
   var numericId = el.id.substr(13);
   var idsplit=numericId.split("_");
   var page=idsplit[0];
   var ruler_index=idsplit[1];

   var leftdiv = $('_dragColumn'+page+'_'+ruler_index);
   var rightdiv = $('_dragColumn'+page+'_'+(parseInt(ruler_index)+1));

   var leftoldwidth=getWidth(leftdiv);
   var rightoldwidth=getWidth(rightdiv);
   var min_width=2;
   if (el.offsetLeft < leftdiv.offsetLeft) {
      columnWidth=min_width;
      leftdiv.style.cssText = 'float:left;width:'+columnWidth+'%;padding:0px;margin:0px;';
      leftdiv.style.width = columnWidth + '%';

      columnWidth=(rightoldwidth+leftoldwidth-min_width);
      rightdiv.style.cssText = 'float:left;width:'+columnWidth+'%;padding:0px;margin:0px;';
      rightdiv.style.width = columnWidth + '%';

      _currentView.setModified(1);

   } else if (el.offsetLeft > (rightdiv.offsetLeft+rightdiv.offsetWidth)) {
      columnWidth=min_width;
      rightdiv.style.cssText = 'float:left;width:'+columnWidth+'%;padding:0px;margin:0px;';
      rightdiv.style.width = columnWidth + '%';

      columnWidth=(leftoldwidth+rightoldwidth-min_width);
      leftdiv.style.cssText = 'float:left;width:'+columnWidth+'%;padding:0px;margin:0px;';
      leftdiv.style.width = columnWidth + '%';

      _currentView.setModified(1);

   } else {
      leftWidth = leftdiv.style.width;
      leftWidth = parseFloat(leftWidth.substring(0,leftWidth.length-1));
      rightWidth = rightdiv.style.width;
      rightWidth = parseFloat(rightWidth.substring(0,rightWidth.length-1));
      var total=rightdiv.offsetWidth+rightdiv.offsetLeft-leftdiv.offsetLeft;
      var line=el.offsetLeft-leftdiv.offsetLeft;
      columnWidth=(leftWidth+rightWidth)*line/total;
      var rightcolumnWidth=(leftWidth+rightWidth)-columnWidth;
      // resize small column first to prevent wraping
      if(leftWidth>rightWidth) {
          leftdiv.style.cssText = 'float:left;width:'+columnWidth+'%;padding:0px;margin:0px;';
          leftdiv.style.width = columnWidth + '%';
          rightdiv.style.cssText = 'float:left;width:'+rightcolumnWidth+'%;padding:0px;margin:0px;';
          rightdiv.style.width = rightcolumnWidth + '%';
      } else {
          rightdiv.style.cssText = 'float:left;width:'+rightcolumnWidth+'%;padding:0px;margin:0px;';
          rightdiv.style.width = rightcolumnWidth + '%';
          leftdiv.style.cssText = 'float:left;width:'+columnWidth+'%;padding:0px;margin:0px;';
          leftdiv.style.width = columnWidth + '%';
      }
      _currentView.setModified(1);
   }
   return true;
}

function mouseOverRuler() {
   // debug("ruler "+this.id+" visible");
   $(this.id).style.cursor='e-resize';
}

function mouseOutRuler() {
   // debug("ruler "+this.id+" hidden");
   $(this.id).style.cursor='auto';
}

function getWidth(col) {
    var colWidth=col.style.width;
    if (colWidth.lastIndexOf('%')!=-1) {
	colWidth=parseInt(colWidth.substr(0,colWidth.lastIndexOf('%')));
    }
    return colWidth;
}

/*****************************************************
Manage Menu functions
*****************************************************/

var _tree=null;
var _plugin_hash=null;

/***************************************************
   view management
***************************************************/

function addColumn() {
    need_save=1;
    _currentView.getCurrentPage().addColumn();
}

function deleteColumn() {
    need_save=1;
    if (_currentView.getCurrentPage().columns <=1) {
	alert("You cannot delete the last column from the page.");
	return;
    }

    if (confirm("Are you sure you want to delete the last column in the current page?")) {
        _currentView.getCurrentPage().deleteColumn();
    }
}

function deleteView(name) {
    var myAjax=new Ajax.Request(	
	'/hicc/Workspace',
	{
	    asynchronous: false,
	    method: 'post',
	    parameters: "_s="+(new Date().getTime())+"&"+'&method=delete_view&name='+name
	}
    );
    if (myAjax.success()) {
      	var return_text=myAjax.transport.responseText;
	// alert(return_text);
    } 
    update_views_list();
}

function setDefaultView(name) {
    var myAjax=new Ajax.Request(	
	'/admin/',
	{
	    asynchronous: false,
	    method: 'post',
	    parameters: "_s="+(new Date().getTime())+"&"+'module=Workspace&method=set_default_view&name='+name
	}
    );
    if (myAjax.success()) {
      	var return_text=myAjax.transport.responseText;
	// alert(return_text);
    	update_views_list();
    } 
}

//
// Saving the current view
//
function saveView() {
    need_save=0;
    close_all_popup();
    var config=_currentView.getConfig();
    var myAjax=new Ajax.Request(	
	'/hicc/Workspace',
	{
	    asynchronous: false,
	    method: 'post',
	    parameters: "_s="+(new Date().getTime())+"&"+'method=save_view&force=1&name='+_currentView.id+'&config='+escape(config)
	}
   );
   if (myAjax.success()) {
      	var return_text=myAjax.transport.responseText;
	// alert(return_text);
   } 
}

//
// Clone the current view
//
function cloneView(id) {
    var myAjax=new Ajax.Request(	
	'/hicc/Workspace',
	{
	    asynchronous: false,
	    method: 'post',
	    parameters: "&method=clone_view&name="+id+"&clone_name="+id+".view&_s="+(new Date().getTime())
	}
    );
    if (myAjax.success()) {
      	var return_text=myAjax.transport.responseText;
	// alert(return_text);
    } 
    update_views_list();
}

//
// restore the default view
//
function restoreView() {
//    var myAjax=new Ajax.Request(	
//	'/admin/',
//	{
//	    asynchronous: false,
//	    method: 'post',
//	    parameters: "_s="+(new Date().getTime())+"&"+'module=Workspace&method=restore_view'
//	}
//    );
alert("TODO: implement this");
//    if (myAjax.success()) {
//      	var return_text=myAjax.transport.responseText;
	// alert(return_text);
//    } 
    update_views_list();
    initScript('default');
    set_current_view('default');
}

function createNewView() {
    var myAjax=new Ajax.Request(	
	'/hicc/Workspace',
	{
	    asynchronous: false,
	    method: 'post',
	    parameters: "&method=clone_view&name=NewView&clone_name=newview.tpl&_s="+(new Date().getTime())
	}
    );
    if (myAjax.success()) {
      	var return_text=myAjax.transport.responseText;
	// alert(return_text);
    } 
    update_views_list();
}

function changeViewName(id, save) {
    if ($("changeViewNameButtonBlock"+id).style.display=="block") {
	$("displayViewPermissionBlock"+id).style.display="none";
	$("editViewPermissionBlock"+id).style.display="block";
	$("changeViewNameButtonBlock"+id).style.display="none";
	$("cancelChangeViewNameBlock"+id).style.display="block";
	$("displayViewNameBlock"+id).style.display="none";
	$("changeViewNameBlock"+id).style.display="block";
    } else {
	// save it
	if (save) {	
	    // make sure the description name cannot contain '
            if ($("changeViewNameField"+id).value.indexOf("'")>=0) {
		alert("Name cannot contain '.");
		return false;
    	    }

	    // make sure that we have at least one permission

	    var tbl=$('permissionTable'+id);
	    var trTags=$A(tbl.getElementsByTagName('tr'));
	    if (trTags.length <= 0) {
		alert("At least one permissions level must be enabled");
		return false;
	    }

	    var okay=0;
	    trTags.each(function(node) {
	        var selectObjArray=node.getElementsByTagName('select');
		var read_permission=0;
		var modify_permission=0;
	        if (selectObjArray.length > 0) {
		    var obj=selectObjArray[0];
	            var userid=obj.options[obj.selectedIndex].value;
		    var inputArray=node.getElementsByTagName('input');
		    for (var i=0;i<inputArray.length;i++) {
		        obj=inputArray[i];
		        if (obj.id == 'read_permission') {	
	    		    read_permission=(obj.checked)?1:0;
		        }
		        if (obj.id == 'modify_permission') {	
	    		    modify_permission=(obj.checked)?1:0;
		        }
		    }
		    if ((userid != '') && (read_permission==1) && (modify_permission==1)) {
			okay=1;
		    }
	        }
            });
	    //if (okay==0) {
		//alert("You must have at least one user with read and modify permissions enabled");
		//return false;
	    //}


	    var permissionObj=new permissionRowObj(id);
	    var json=permissionObj.getJson();

	    var myAjax=new Ajax.Request(	
		'/hicc/Workspace',
		{
	    	    asynchronous: false,
	    	    method: 'post',
	    	    parameters: "_s="+(new Date().getTime())+"&method=change_view_info&name="+id+'&config='+json
		}
    	    );
    	    if (myAjax.success()) {
      		var return_text=myAjax.transport.responseText;
		alert(return_text);
	    }

    	    update_views_list();
	}
	$("displayViewPermissionBlock"+id).style.display="block";
	$("editViewPermissionBlock"+id).style.display="none";
	$("changeViewNameButtonBlock"+id).style.display="block";
	$("cancelChangeViewNameBlock"+id).style.display="none";
	$("displayViewNameBlock"+id).style.display="block";
	$("changeViewNameBlock"+id).style.display="none";
    }
}

function update_views_list() {
    var viewListUpdater = new Ajax.Updater(
	"views_list_div",
	"/hicc/jsp/workspace/manage_view.jsp", {
	parameters: { "_s" : (new Date().getTime()),
                      "method" : "get_views_list" },
	onException: function(resp) {
	    alert("Cannot load view.");
	}
    });
    var param="_s="+(new Date().getTime())+"&method=get_views_list&format=dropdown";
    var myAjax=new Ajax.Request(	
        '/hicc/Workspace',
        {
    	    asynchronous: false,
	    method: 'post',
	    parameters:  { "_s" : (new Date().getTime()),
                           "method" : "get_views_list",
                           "format" : "dropdown"
                         },
            onSuccess : function(transport) {
      	                    var view_array=transport.responseText.evalJSON();
	                    $('currentpage').options.length=0;
	                    for (i=0;i<view_array.length;i++) {
		                $('currentpage').options[i]=new Option(view_array[i].description,view_array[i].key);
		                if (view_array[i].key == _currentViewId) {
		                    $('currentpage').selectedIndex=i;
		                }
	                    }
            },
            onException : function(request, error) {
                              alert(error);
            }
	}
    );
}

function set_current_view(id) {
	var control=$('currentpage');
	for (i=0;i<control.options.length;i++) {
	    if (control.options[i].value == id) {
		control.selectedIndex=i;
	    }
	}
}

function changeView(obj) {
    close_all_popup();
    var i=obj.selectedIndex;
    document.location="/hicc/?view="+obj.options[i].value;
}

function addPermissionRow(tableid,userid,read,modify) {
    var tbl=$("permissionTable"+tableid);
    var row=document.createElement("tr");
    var td1=document.createElement("td");
    var el=document.createElement('select');
    el.setAttribute('class','formSelect');
    el.setAttribute('id','userid_permission');
    for (i=0;i<_users_list.length;i++) {
	el.options[i]=new Option(_users_list[i].value, _users_list[i].key);
    }
    td1.appendChild(el);

    var td2=document.createElement("td");
    td2.setAttribute('align','middle');
    el=document.createElement('input');
    el.setAttribute('id','read_permission');
    el.setAttribute('type','checkbox');    
    el.setAttribute('checked',1);
    td2.appendChild(el);

    var td3=document.createElement("td");
    td3.setAttribute('align','middle');
    el=document.createElement('input');
    el.setAttribute('id','modify_permission');
    el.setAttribute('type','checkbox');
    el.setAttribute('checked',1);
    td3.appendChild(el);

    var td4=document.createElement("td");
    el=document.createElement('img');
    el.setAttribute('src','/hicc/images/close.png');
    el.onclick=function() {deleteCurrentRow(tableid, this);};
    td4.appendChild(el);

    row.appendChild(td1);
    row.appendChild(td2);
    row.appendChild(td3);
    row.appendChild(td4);

    tbl.getElementsByTagName("tbody")[0].appendChild(row);
}

function deleteCurrentRow(tableid, obj) {
    var delRow=obj.parentNode.parentNode;
    var rIndex=delRow.sectionRowIndex;
    $("permissionTable"+tableid).getElementsByTagName("tbody")[0].deleteRow(rIndex);
}

/***************************************************
   add/delete/rename page
***************************************************/
function addNewPage() {
   var pageName=prompt("Please type in the new page title.");
   if ((pageName == null ) || (pageName == '')) {
	// alert("Page title is empty. New page is not created.");
   } else { 
	var page=_currentView.addNewPage(pageName);
	// create 1 column
	page.addColumn();
	_currentView.selectPage(page.page_tag);
   }
}

function addPageButtonClick() {
    if ($F('add_page_title') == '') {
	alert("Page title cannot be empty.");
    } else {
	var page=_currentView.addNewPage($F('add_page_title'));
	// create 1 column
	page.addColumn();
	_currentView.selectPage(page.page_tag);
    }
}

function deletePageButtonClick(page_id) {
    close_all_popup();
    if (confirm("Are you sure you want to delete the selected page?")) {
        _currentView.deletePage(page_id);
    }
    return false;
}

function deleteCurrentPage() {
    close_all_popup();
    if (confirm("Are you sure you want to delete the selected page?")) {
        _currentView.deletePage(_currentView.getCurrentPage().page_tag);
    }
    return false;
}

function renamePageButtonClick() {
    if ($F('new_page_name') == '') {
	alert('New title cannot be empty.');
	return false;
    }
    _currentView.renamePage(_configPageTag, $F('new_page_name'));
}

function _addTextNode(node, thisId, title) {
	var data = {	
		id: thisId,
		label: title,
		href: "javascript:onLabelClick('" + thisId + "')"
	}
	new YAHOO.widget.TextNode(data, node, false);
}

function _findNode(id) {
    for (var i=0;i<_plugin_hash.detail.length;i++) {
	if (_plugin_hash.detail[i].id==id) {
	    return _plugin_hash.detail[i];
	}
    }
    return null;
}

function _addNode(parent, node) {
    if (node != null) {	
	for (x in node) {
	    if (x.indexOf('node:')==0) {
	    	var newNode=new YAHOO.widget.TextNode(x.substring(5), parent, false);
		_addNode(newNode, node[x]);
	    } else if (x.indexOf('leaf:')==0) {
		tmpNode=_findNode(node[x]);
		_addTextNode(parent, node[x], tmpNode.title)
	    } 
	}
    }
}

function onLabelClick(id) {
    // create detail panel

    var node=_findNode(id);
    var boxId="widget"+new Date().getTime();
    var detail='';
    detail+="<table class='configurationTableContent' width='95%'><tr><td valign='top' width='40%'><table class='configurationTableContent'>";
    detail+="<tr><td><b>"+'Name'+":</b>&nbsp;</td><td>";
    detail+=node.title;
    detail+="</td></tr>";
    detail+="<tr><td><b>"+'Version'+":&nbsp;</b></td><td>";
    detail+=node.version;
    detail+="</td></tr>";
    detail+="<tr><td valign='top'><b>"+'Description'+":&nbsp;</b></td><td>";
    detail+=node.description;
    detail+='</td></tr><tr><td><input class="formButton" type="button" name="addwidget" value="Add Widget" onClick="add_widget(\''+id+'\');"/>';
    detail+="</td></tr>";
    detail+="</table></td>";	
    detail+="<td width='55%'><div id='_"+boxId+"' style='width:100%;overflow:hidden;'>";
    detail+="<div id='"+boxId+"' class='dragableBoxContent' style='width:100%;height:200px;overflow:hidden;'><img src='/hicc/images/loading.gif'></div></div></td></tr></table>";

    $('widget_detail').innerHTML=detail;
    var url="";
    var parameters="";
    if(node.module) {
        if(node.module.match(/^http:\/\//)) {
            url=node.module;
        } else {
            url=node.module;
	    var string=node.module+"?";
            string+="&boxId="+boxId;
	    for (iCount=0;iCount<node.parameters.length;iCount++) {
	        string+="&";
	        string+=node.parameters[iCount].name+"="+escape(node.parameters[iCount].value);
            }
            parameters=string;
        }
    }
    var myAjax = new Ajax.Updater(
            boxId,
            url,
            { 
                method: 'get',
                parameters: parameters,
                onComplete: renderWidget
            }
            );
}

function renderWidget(request) {
    var boxId=request.getResponseHeader('boxId');
    var obj = request.responseText;
    var s_index = request.responseText.indexOf("<script");
    var e_index = request.responseText.indexOf("</script>");
    var jscript = obj.substring(s_index,e_index);
    s_index = jscript.indexOf(">");
    jscript = jscript.substring(s_index+1,jscript.length);
    eval(jscript);
}

function initPreviewTimer() {
   if(dragDropCounter>=0 && dragDropCounter<10){
      dragDropCounter++;
      setTimeout('initPreviewTimer()',10);
      return;
   }
}

function dragWidget(e) {
   dragDropCounter = 1;
   if(document.all)e = event;
   
   if (e.target) source = e.target;
   else if (e.srcElement) source = e.srcElement;
   if (source.nodeType == 3) {
	// defeat Safari bug
        source = source.parentNode;
    }
   
   if(source.tagName.toLowerCase()=='img' || source.tagName.toLowerCase()=='a' || source.tagName.toLowerCase()=='input' || source.tagName.toLowerCase()=='td' || source.tagName.toLowerCase()=='tr' || source.tagName.toLowerCase()=='table')return;
   
   mouse_x = e.clientX;
   mouse_y = e.clientY;	
   var numericId = this.id.replace(/[^0-9|_]/g,'');
   var idsplit=numericId.split("_");
   numericId=idsplit[1];
   el_x = getLeftPos(this.parentNode)/1;
 var scrollTop=getScrollTop();

   el_y = getTopPos(this.parentNode)/1 - scrollTop;
   
   dragObject = this.parentNode;
   
   documentScrollHeight = document.documentElement.scrollHeight + 100 + dragObject.offsetHeight;
      
   if(dragObject.nextSibling){
      dragObjectNextSibling = dragObject.nextSibling;
      if(dragObjectNextSibling.tagName!='DIV')dragObjectNextSibling = dragObjectNextSibling.nextSibling;
   }
   dragDropCounter = 0;
   initPreviewTimer();	
   return false;
}

function dropWidget(e) {
   if(dragDropCounter<10){
      dragDropCounter = -1
      return;
   }
   dragDropCounter = -1;
   if(transparencyWhenDragging){
      dragObject.style.filter = null;
      dragObject.style.opacity = null;
   }
   dragObject.style.position = 'static';
   dragObject.style.width = null;
   dragObject = false;
   if(document.all)e = event;
   
   if (e.target) source = e.target;
   else if (e.srcElement) source = e.srcElement;
   if (source.nodeType == 3) {
	// defeat Safari bug
        source = source.parentNode;
    }
   source = source.parentNode;
   source.parentNode.removeChild(source);
}

// find x-y position
function findPosX(obj)
{
	var curleft = 0;
	if (obj.offsetParent)
	{
		while (obj.offsetParent)
		{
			curleft += obj.offsetLeft
			obj = obj.offsetParent;
		}
	}
	else if (obj.x)
		curleft += obj.x;
	return curleft;
}

function findPosY(obj)
{
	var curtop = 0;
	if (obj.offsetParent)
	{
		while (obj.offsetParent)
		{
			curtop += obj.offsetTop
			obj = obj.offsetParent;
		}
	}
	else if (obj.y)
		curtop += obj.y;
	return curtop;
}

var _configPageOpen=false;
var _configPageTag='';

function configPage(id) {
	if ((_configPageOpen) || (id=='')) {
	    _configPageTag='';
	    _configPageOpen=false;
//	    $('page_config_menu').style.display='none';
	} else {
	    close_all_popup();
	    _configPageTag=id;
	    _configPageOpen=true;
	    var newX=findPosX($("href_page_"+id));
	    var newY=findPosY($("href_page_"+id));
	    newY+=25;
//	    $('page_config_menu').style.top=newY+'px';
//	    $('page_config_menu').style.left=newX+'px';
//	    $('page_config_menu').style.display='block';
        }
}

/***************************************************
   add widget
***************************************************/
function add_widget(id) {
    var node=_findNode(id);
    eval("var tmp ="+ Object.toJSON(node));
    need_save=1;
    _currentView.getCurrentPage().addWidget(tmp,1,false);
}



/***************************************************
 Loading content page
***************************************************/
function refresh_all() {
    _currentView.getCurrentPage().refresh_all();
}

var _menu_open=false;

function manage_content(close) {
    if ((_menu_open) || (close)) {
	//$('manage_button').value='Workspace Builder >>';
	if(_menu_open) {
            new Effect.BlindUp($('manage_view'));
        }
    	//$('manage_view').style.display="none";
	_menu_open=false;
    } else {
	close_all_popup();
	//$('manage_button').value='Workspace Builder <<';
        $('manage_view').style.zIndex='20';
	new Effect.BlindDown($('manage_view'));
    	//$('manage_view').style.display="block";
	_menu_open=true;
    }
    return false;
}

var _widget_menu_open=false;

var TreeBuilder = {
     buildTree:function (treeNodes){
         tree = new YAHOO.widget.TreeView("myWidgetContainer");
         _addNode(tree.getRoot(), treeNodes);
         tree.subscribe("labelClick", function(node) {
              onLabelClick(node);
         });

         tree.draw();
     }
}

function add_widget_menu(close) {
	// manage widget
	if (_plugin_hash == null) {
   	    var json_text='';
   	    var myAjax=new Ajax.Request(
	        '/hicc/Workspace',
	        {
		    asynchronous: false,
		    method: 'get',
		    parameters: "_s="+(new Date().getTime())+"&"+
			'method=get_widget_list&id='+'userid',
                    onSuccess : function(transport) {
      	                           _plugin_hash=transport.responseText.evalJSON();
                                   TreeBuilder.buildTree(_plugin_hash.children);
                                }
	        }
   	    );
	}
    if ((_widget_menu_open) || (close)) {
        if(_widget_menu_open) {
	    new Effect.BlindUp($('widget_view'));
        }
	_widget_menu_open=false;
    } else {
	close_all_popup();
        $('widget_view').style.zIndex='15';
	new Effect.BlindDown($('widget_view'));
	_widget_menu_open=true;
    }
    return false;
}

function close_all_popup() {
	manage_content('close');
	add_widget_menu('close');
	configPage('');
}

var permissionRowObj = Class.create();

permissionRowObj.prototype = {
    initialize: function(id) {
        this.description=$('changeViewNameField'+id).value;
        var tbl=$('permissionTable'+id);
        // get each row and put the value into individual array
        var permissionRow=new Object;
        var trTags=$A(tbl.getElementsByTagName('tr'));
        trTags.each(function(node) {
            var selectObjArray=node.getElementsByTagName('select');
            var read_permission=0;
            var modify_permission=0;

            if (selectObjArray.length > 0) {
                var obj=selectObjArray[0];
                var userid=obj.options[obj.selectedIndex].value;
                var inputArray=node.getElementsByTagName('input');
                for (var i=0;i<inputArray.length;i++) {
                    obj=inputArray[i];
                    if (obj.id == 'read_permission') {
                        read_permission=(obj.checked)?1:0;
                    }
                    if (obj.id == 'modify_permission') {
                        modify_permission=(obj.checked)?1:0;
                    }
                }
                var permissionRecord=new Object;
                permissionRecord['read']=read_permission;
                permissionRecord['modify']=modify_permission;
                permissionRow[userid]=permissionRecord;
            }
        });
        this.permission=permissionRow;
    },

    getJson: function() {
        var v=new Object();
        v.description=this.description;
        v.permission=this.permission;
        return Object.toJSON(v);
    }
};

/************************************************************************************************************
This is a modified version of the dynamic portal script from www.dhtmlgoodies.com. Here is the original copyright. 
*************************************************************************************************************
(C) www.dhtmlgoodies.com, January 2006
	
This is a script from www.dhtmlgoodies.com. You will find this and a lot of other scripts at our website.	
	
Version:	1.0	: January 16th - 2006
	
Terms of use:
You are free to use this script as long as the copyright message is kept intact. 
However, you may not redistribute, sell or repost it without our permission.
	
Thank you!
	
www.dhtmlgoodies.com
Alf Magne Kalleland
************************************************************************************************************/
	
// default behaviors
var contentDiv="workspaceContainer";		// ID for the content div
var transparencyWhenDragging = true;
var txtEditLink='Edit';				// text for edit start
var txtEditEndLink='End Edit';			// text for edit end
var autoScrollSpeed=4;				// higher = faster
var dragObjectBorderWidth=1;			// Border size of the RSS box

// images
var rightArrowImage='/hicc/images/arrow_right.gif';	// for window open
var downArrowImage='/hicc/images/arrow_down.gif';	// for window collpase
var refreshImage='/hicc/images/refresh.png';		// refresh window
var editImage='/hicc/images/info.png';		// edit window
var closeImage='/hicc/images/close.png';		// close window
var smallRightArrow='/hicc/images/small_arrow.gif';	// for individual item inside window

//-----------------------------------------------------------------
var _currentView=null;
var _currentViewId='default';
var _dragableBoxesObj;
var rectangleDiv;

// ping server every 5 minutes
var opera = navigator.userAgent.toLowerCase().indexOf('opera')>=0?true:false;
var _stayLogin=setInterval("ping_server()",1000*60*5);

function ping_server() {
    var myAjax=new Ajax.Request(	
	'/admin/',
	{
	    asynchronous: false,
	    method: 'post',
	    parameters: "_s="+(new Date().getTime())+"&"+'module=Workspace&method=no_op'
	}
    );
}
	
// initialize the window
function initScript(view_id) {
   resetAll();
   if (view_id == '') {
      view_id='default';
   }

   //create view
   _currentViewId=view_id;
   _currentView=new portalView(view_id);
   rectangleDiv=$('rectangleDiv');	
}

// reset all the internal parameters
function resetAll() {
   if (_currentView != null) {
	_currentView.destroyView();
   }

   // remove all the components under contentDiv
   el=$(contentDiv);
   if (el!=null) {
      while(el.hasChildNodes()) {
	 el.removeChild(el.lastChild);
      }
   }

   el=$('tablist');
   if (el!=null) {
      while(el.hasChildNodes()) {
	 el.removeChild(el.lastChild);
      }
   }

   // reset to default images
   //rightArrowImage='/hicc/images/arrow_right.gif';
   //downArrowImage='/hicc/images/arrow_down.gif';	
   //refreshImage='/hicc/images/refresh.gif';		
   //smallRightArrow='/hicc/images/small_arrow.gif';	
}

function getTopPos(inputObj)
{		
   var returnValue = inputObj.offsetTop;
   while((inputObj = inputObj.offsetParent) != null){
      if(inputObj.tagName!='HTML')returnValue += inputObj.offsetTop;
   }
   return returnValue;
}

function getScrollTop() {
if (document.documentElement && document.documentElement.scrollTop)
	scrollTop = document.documentElement.scrollTop;
else if (document.body)
	scrollTop = document.body.scrollTop;
else
	scrollTop = document.body.scrollTop;
return scrollTop;
}

function getClientHeight() {
    return (document.height!==undefined) ? document.height : document.body.offsetHeight;
}

function getLeftPos(inputObj)
{
   var returnValue = inputObj.offsetLeft;
   while((inputObj = inputObj.offsetParent) != null){
      if(inputObj.tagName!='HTML')returnValue += inputObj.offsetLeft;
   }
   return returnValue;
}
		
function reloadPageBoxData(page,index) {
   _currentView.pages[page].dragableBoxesArray[index].reloadBoxData();
}
	
function autoScroll(direction,yPos) {
   return; // disable auto scroll
   if(document.documentElement.scrollHeight>documentScrollHeight && direction>0)return;
   if(opera)return;
   window.scrollBy(0,direction);
   
   if(direction<0){
      if(document.documentElement.scrollTop>0){
	 dragObject.style.top = (el_y - mouse_y + yPos + document.documentElement.scrollTop) + 'px';		
      }else{
	 autoScrollActive = false;
      }
   }else{
      if(yPos>(documentHeight-50)){	
	if (dragObject) {
	 dragObject.style.top = (el_y - mouse_y + yPos + document.documentElement.scrollTop) + 'px';
	}
      }else{
	 autoScrollActive = false;
      }
   }
   if(autoScrollActive)setTimeout('autoScroll('+direction+',' + yPos + ')',5);
}
	
function showRSSData(boxIndex) {
   var rssContent='';
   var myAjax=new Ajax.Request(
        dragableBoxesArray[boxIndex].getParameter('url'),
	{
	asynchronous: false,
	method: 'get',
	parameters: "_s="+(new Date().getTime())+"&"+dragableBoxesArray[boxIndex].getParametersString()
	}
   );
   if (myAjax.success()) {
      rssContent=myAjax.transport.responseText;
   }   
   tokens = rssContent.split(/\n\n/g);   
   
   var headerTokens = tokens[0].split(/\n/g);
   if(headerTokens[0]=='0'){
      headerTokens[1] = '';
      headerTokens[0] = 'Invalid source';
   }
   $('dragableBoxHeadertxt' + boxIndex).innerHTML = '<span>' + headerTokens[0] + '&nbsp;<\/span><span class="rssNumberOfItems">(' + headerTokens[1] + ')<\/span>';	// title
   
   var string = '<table cellpadding="1" cellspacing="0">';
   for(var no=1;no<tokens.length;no++){	// Looping through RSS items
      var itemTokens = tokens[no].split(/##/g);			
      string = string + '<tr><td><img src="' + smallRightArrow + '"><td><p class=\"boxItemHeader\"><a class=\"boxItemHeader\" href="' + itemTokens[3] + '" onclick="var w = window.open(this.href);return false">' + itemTokens[0] + '<\/a><\/p><\/td><\/tr>';		
   }
   string = string + '<\/table>';
   $('dragableBoxContent' + boxIndex).innerHTML = string;
   dragableBoxesArray[boxIndex].showStatusBarMessage('');
}

function findWidget(page,index) {
    for (var i=0;i<_currentView.pages.length;i++) {
	if (_currentView.pages[i].pageid==page) {
	    return _currentView.pages[i].dragableBoxesArray[index];
	}
    }
    return null;
}

function debug(str) {
   var debug_div=$('debug');
   debug_div.innerHTML=str;
   // alert(str);
}

function block_object() {
   this.url=''; 
   this.parameters='';
   this.title='';
   this.type='html';
   this.refresh=5;
}


// include js scripts on demand

function include_dom(script_filename) {
	var html_doc = document.getElementsByTagName('head').item(0);
        var js = document.createElement('script');
        js.setAttribute('language', 'javascript');
        js.setAttribute('type', 'text/javascript');
        js.setAttribute('src', script_filename);
        html_doc.appendChild(js);
        //return false;
}

var _included_js_files = new Array();

function include_once(script_filename) {
	if (!in_array(script_filename, _included_js_files)) {
        	_included_js_files[_included_js_files.length] = script_filename;
                include_dom(script_filename);
        }
}

function in_array(needle, haystack) {
        for (var i = 0; i < haystack.length; i++) {
        	if (haystack[i] == needle) {
                    return true;
                }
        }
        return false;
}

function check_save() {
        if(need_save) {
                return "Discard workspace changes?";
        }
        return;
}

function Get_Cookie( check_name ) {
	// first we'll split this cookie up into name/value pairs
	// note: document.cookie only returns name=value, not the other components
	var a_all_cookies = document.cookie.split( ';' );
	var a_temp_cookie = '';
	var cookie_name = '';
	var cookie_value = '';
	var b_cookie_found = false; // set boolean t/f default f
	
	for ( i = 0; i < a_all_cookies.length; i++ )
	{
		// now we'll split apart each name=value pair
		a_temp_cookie = a_all_cookies[i].split( '=' );
		
		
		// and trim left/right whitespace while we're at it
		cookie_name = a_temp_cookie[0].replace(/^\s+|\s+$/g, '');
	
		// if the extracted name matches passed check_name
		if ( cookie_name == check_name )
		{
			b_cookie_found = true;
			// we need to handle case where cookie has no value but exists (no = sign, that is):
			if ( a_temp_cookie.length > 1 )
			{
				cookie_value = unescape( a_temp_cookie[1].replace(/^\s+|\s+$/g, '') );
			}
			// note that in cases where cookie is initialized but no value, null is returned
			return cookie_value;
			break;
		}
		a_temp_cookie = null;
		cookie_name = '';
	}
	if ( !b_cookie_found )
	{
		return null;
	}
}

function Set_Cookie( name, value, expires, path, domain, secure ) 
{
// set time, it's in milliseconds
var today = new Date();
today.setTime( today.getTime() );

/*
if the expires variable is set, make the correct 
expires time, the current script below will set 
it for x number of days, to make it for hours, 
delete * 24, for minutes, delete * 60 * 24
*/
if ( expires )
{
expires = expires * 1000 * 60 * 60 * 24;
}
var expires_date = new Date( today.getTime() + (expires) );

document.cookie = name + "=" +escape( value ) +
( ( expires ) ? ";expires=" + expires_date.toGMTString() : "" ) + 
( ( path ) ? ";path=" + path : "" ) + 
( ( domain ) ? ";domain=" + domain : "" ) +
( ( secure ) ? ";secure" : "" );
}

function save_cluster(boxId) {
    obj = document.getElementById(boxId+"cluster");
    for(var i=0;i < obj.options.length;i++) {
        if(obj.options[i].selected) {
            cluster=obj.options[i].value;
        }
    }
    var myAjax=new Ajax.Request(
        "/hicc/jsp/session.jsp",
        {
            asynchronous: false,
            method: 'get',
            parameters: "cluster="+cluster+"&_delete=machine_names"
        }
    );
    if (myAjax.success()) {
        _currentView.getCurrentPage().refresh_all();
    }
}

function save_host(boxId) {
    var obj = document.getElementById(boxId+"group_items");
    var cookie = "";
    var cluster = "";
    for(var i=0;i< obj.options.length;i++) {
        if(obj.options[i].selected) {
            if(i!=0) {
                cookie = cookie + "," + obj.options[i].value;
            } else {
                cookie = obj.options[i].value;
            }
        }
    }
    var myAjax=new Ajax.Request(
        "/hicc/jsp/session.jsp",
        {
            asynchronous: false,
            method: 'get',
            parameters: "hosts="+cookie
        }
    );
    if (myAjax.success()) {
        _currentView.getCurrentPage().refresh_all();
    }

}

function save_hod(HodID) {
    var myAjax=new Ajax.Request(
        "/hicc/jsp/session.jsp",
        {
            asynchronous: false,
            method: 'get',
            parameters: "HodID="+HodID
        }
    );
    if (myAjax.success()) {
        _currentView.getCurrentPage().refresh_all();
    }
}

function filter_event_viewer(boxId) {
    var obj = document.getElementById(boxId+'filter').value;
    var myAjax=new Ajax.Request(
        "/hicc/jsp/session.jsp",
        {
            asynchronous: false,
            method: 'get',
            parameters: "filter="+obj
        }
    );
    if (myAjax.success()) {
        var idsplit=boxId.split("_");
        _currentView.getCurrentPage().dragableBoxesArray[idsplit[1]].reloadBoxData();
    }
}

