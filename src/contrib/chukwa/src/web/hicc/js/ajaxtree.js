/*
Title: Ajax Tree
	Created by Colin Mollenhour
	
	Description:
	This tree class can be used with or without Ajax. You simply define node types and supply options,
	hooks, handlers, etc. for each type and start creating nodes. All node types can be created using
	the same constructor by passing it the type as a string. The way it handles data and reacts to user input
	is completely customizable for each individual node type.

	LIVE DEMO:
	<http://colin.mollenhour.com/ajaxtree/ajaxtreetest.html>
	
Section: Usage
	Usage specifications and examples.
	
	Requirements:
	Prototype 1.5.0 rc1 <http://prototype.conio.net/>
	
	LIVE DEMO:
	<http://colin.mollenhour.com/ajaxtree/ajaxtreetest.html>
	
	Source Code:
	<http://colin.mollenhour.com/ajaxtree/ajaxtree.js>

Group: Using the Javascript
	Ajax.Tree is designed to be used in two steps::
	- Extend the base class with your customizations. See <Ajax.Tree.Usage>
	- Instantiate nodes of your new class. See <Ajax.Tree.Base.Usage>
	
Group: Handling Server Requests
	The default format for server requests, which can be customized via the <callback> hook:
	
	getContents - 'action=getContents&id='this.element.id
	
	Now just handle those post variables however you like and use correct format in your <Server Response>.
	
Group: Server Response
	The server response is expected to be in JSON format. It can be sent either in the responseText, or an X-JSON header.
	The X-JSON header evaluated result is check for the presence of a *nodes* key, which is expected to be an Array.
	If the X-JSON header does not contain a *nodes* key, the responseText is evaluated. If the evaluated responseText
	does not have a *nodes* key, no nodes are built and this is *not* an error. Any related hooks will still be called
	and passed the server response variables as usual.

Topic: Specifications
	- Must contain a "nodes" array of nodes.
	- If the "nodes" array does not exist, no nodes will be created. However,
		the <onContentLoaded> and <onGetContentsComplete> hooks will still be called.
	- Each element of "nodes" must contain:
		id - The new node's element id. See <prependParentId>.
		type - The new node's <type> keyword, as defined in the structure passed to <Ajax.Tree.create>.
		data - Either a string (for default <insertion>) or some other object to be passed to <insertion>.
	- Each node can contain nested nodes by adding a "nodes" array to a node.
	- Other attributes can be scattered throughout the response data. It can be accessed by the
		<onContentLoaded> and <getContentsComplete> hooks or for each individual node through the <insertion> option.
	
Topic: Example
	This response example coincides with the <Ajax.Tree.Usage> example:
	(start code)
	{
		nodes: [
			{
				id: 'dir-1',
				type: 'directory',
				data: 'Work',
				nodes: [
					{
						id: 'dir-1_subdir-1',
						type: 'directory',
						data: 'Accounting'	
					}
				]
			},
			{
				id: 'file-1',
				type: 'file',
				data: {
					name: 'Presentation.ppt',
					size: '30.2KB',
					fileid: 53255596
				},
			}
		]
	}
	(end)

Class: Ajax.Tree
	Ajax.Tree is the utility class for <Ajax.Tree.Base>. Using <create>, you can
	create a new class that is an extension of <Ajax.Tree.Base>. Using this new
	constructor, you can build a tree dynamically using multiple node types
	without the need for separate constructors. The node types are defined in a hash
	passed to <create> which also defines settings and handlers for each type.

Group: Usage
	Use Ajax.Tree.create to create a customized tree class.
	
	This example creates a simple file browser tree. It also demonstrates the use of an overridden
	<insertion>, and the <onClick> hook.
	
	See <Ajax.Tree.Base.Usage> for information on using the constructor produced by this example.
	
	For more detail on defining your own types and handling server responses, see <Options> and <Ajax.Tree.Base>.
	(start code)
	Ajax.FileBrowser = Ajax.Tree.create({
		types: {
			directory: {
				page: 'filebrowser.php'
			},
			file: {
				leafNode: true,
				insertion: function(element,data){
					Element.update(element, data.name+' Size: '+data.size);
					this.dlLink = Builder.node('span',{
						id: data.fileid,
						className: 'download'
					},['download']);
					this.events.observe(this.dlLink, 'click', this.options.download.bindAsEventListener(this));
				},
				onClick: function(event){
					showThumbnail(this.element.id);
				},
				download: function(event){
					window.location.href = 'getfile.php?fileid='+Event.element(event).id;
				}
			}
		}
	});
	(end)
	
Group: Functions
*/
Ajax.Tree = {
	/* Function: create
	Returns a constructor for a new class that is specific to the structure passed.
	This new class is an extension of <Ajax.Tree.Base>

	structure - The structure that defines node types and their options and hooks.
	*/
	create: function(structure){
		if( !structure.types._root ){ structure.types._root = {}; }
		for(var type in structure.types){
			/* Group: Options
			All options are unique per type, and can be accessed inside class functions by "this.options.<option>". */
			var options = {
				/* Option: className
				The className for the newly created element div and span elements, defaults to the node type */
				className: type,
				/* Option: draggable
				Not yet implimented. */
				draggable: false,
				/* Option: leafNode
				If true, the <mark> will get the className 'leaf' and clicks will not fire a <toggleChildren>. */
				leafNode: false,
				/* Option: page
				If specified, <getContents> will be called on clicking the <mark>. */
				page: null,
				/* Option: prependParentId
				If not false, the new element id will be prepended with it's parent's id and this option's value as a separator.
				
				Example::
				|prependParentId: '_', parent.id: 'one-4', id: 'two-3'
				|newid = 'one-4_two-3' */
				prependParentId: false,
				/* Option: insertion
				The insertion function used to handle the node "data".
				
				See <Ajax.Tree.Base.insertion> */
				insertion: Element.update
			};
			structure.types[type] = Object.extend(options,structure.types[type]);
		}
		var newTreeClass = Class.create();
		Object.extend(newTreeClass.prototype,Object.extend(Ajax.Tree.Base.prototype,structure));
		newTreeClass.prototype.constructor = newTreeClass;
		return newTreeClass;
	},
	error: {
		ajax: function(transport){
			var msg = 'There was an error communicating with the server:\n'+transport.statusText;
			Ajax.Tree.showError(msg);
		}
	},
	showError: function(message){
		alert(message);
	}
};

/*
Class: Ajax.Tree.Base
	Ajax.Tree.Base is designed to be extended using <Ajax.Tree.create>. The extension
	of this base class lets you add nodes to a tree in any way you like.

Group: Usage
	Use this class as the base for your own customized Ajax.Tree class.
	The Ajax.Tree.Base class is not intended to be used directly. Instead, create your own extension
	of this class with <Ajax.Tree.create>. With the extended class, you may now start creating nodes
	using a typical javascript constructor.

Topic: constructor
	The class returned from <Ajax.Tree.create> can be instantied with the following arguments:

Arguments:
	parent - The id or element of the new node's parent
	id - The new node's id
	type - The new node's type, corresponding to one of the keys of the *types* hash passed to <Ajax.Tree.create>
	data - A string or other object containing data to be processed by the type's <insertion> function

Example code:
	See <Ajax.Tree.Usage> for the corresponding, more detailed usage example of <Ajax.Tree.create>
	(start code)
	<div id="file_browser"></div>
	<script type="text/javascript">
		Ajax.FileBrowser = Ajax.Tree.create({...});
		new Ajax.FileBrowser('file_browser','root','directory','My Files');
	</script>
	(end)

Group: DOM Elements
The DOM elements created on instantiation of a new node.
All DOM elements are accessible using *this.<element>*.

	parent - (div) - the node's parent element
	element - (div) - the primary div, contains mark, span and children. has className: 'treenode' and this.options.className || this.type
	mark - (span) - the expanded/collapsed mark. has className: 'mark' and one of 'expanded', 'collapsed', 'leaf'
	span - (span) - the primary container for node data. has className: 'treedata' and this.options.className || this.type

Group: Hooks
	Hooks are provided for fine control over interactivity. Implement hooks separately for each node type.

NOTE:
	For all hooks, *this* is a reference to the node's class instance.

Example:
	Alerts the user as to how many child nodes are loaded.
	(start code)
	TreeOfNodes = Ajax.Tree.create({
		types: {
			node:  {
				page: 'nodes.php',
				onContentLoaded: function(xhr,json){
					alert('Got '+$H(json.nodes).keys().length+' new nodes');
				}
			}
		}
	});
	(end code)

	Hook: callback
		Called to build query parameters for the Ajax.Request. Should return a query string.
		
		id - The id of the node's element.
	
	Hook: onClearContents
		Called after a node's children have been cleared.
	
	Hook: onClick
		Called on user clicking the mark. If this function returns false, the click is effectively cancelled.
	
		event - The click event object
	
	Hook: onContentLoaded
		Called after all new nodes have been constructed.
		
		xhr - The XMLHttpRequest transport
		json - The evaluated X-JOSN header object
	
	Hook: dispose
		Called during a node disposal.
	
	Hook: onGetContents
		Called immediately after the Ajax.Request is sent.
		
		request - The Ajax.Request object
	
	Hook: onGetContentsComplete
		Called after the Ajax.Request onComplete
		
		xhr - The XMLHttpRequest transport
		json - The evaluated X-JOSN header object
	
	Hook: insertion
		Called by the constructor after the basic node element has been built.
		
		element - The tree node's *span* DOM element
		data - The *data* value from the <getContents> response
		
		Default:
		|Element.update(element,data)

Group: Flags
All flags listed here are accessed by *this.<flag>* and initialized to the values on the left.

	loaded - false - The data below this node has been loaded (<loadContents> sets to true, <getContents> sets to false)
	opening - false - Ajax.Request is in progress (<getContents> sets to true, <getContentsComplete> sets to false)

Group: Functions
	clearContents - clears the node's children and sets loaded to false
	deleteChildNode - deletes the given child node from the tree, performing all necessary cleanup
	deleteSelf - deletes this node from the tree, performing all necessary cleanup
	dispose - calls clearContents and cleans up the node's events, references, etc..
	getContents - triggers the Ajax.Request if loaded is true, otherwise, calls clearContents
	hide - hides the node's element
	hideChildren - hides all of the node's children's elements and sets the mark's class to 'collapsed'
	show - shows the node's element
	showChildren  - shows all of the node's children's elements and sets the mark's class to 'expanded'
	toggle - calls show/hide as appropriate
	toggleChildren - calls showChildren/hideChildren as appropriate
	
Group: Options

See <Ajax.Tree.Options>

Group: Properties
All properties are accessed by *this.<property>*.

	children - An Array (with Prototype extensions) of all children of the current node
	element.treeNode - Reference to the tree node (*this*)
	parent.treeNode - The node's parent's tree node (if exists, else undefined)
	type - The tree node's type (string)
	
*/

Ajax.Tree.Base = {};
Ajax.Tree.Base.prototype = {
	initialize: function(parent,id,type,data){
		this.type = type || '_root';
		this.options = this.types[this.type];
		this.children = [];
		this.loaded = this.opening = this.root = false;
		this.disposables = [(this.events = new EventCache())];
				
		/* create special purpose root node if parent == null */
		if(parent == null){
			this.element = $(id);
			this.element.addClassName(this.type);
			this.element.treeNode = this;
			this.parent = this.element.parentNode || document.body;
			this.root = true;
			if(data){
				if(this.options){ this.options.insertion.call(this, this.element,data.data || data); }
				if(data.nodes){ this.createNodes(data.nodes); }
			}
			return;
		}		
		this.parent = $(parent);
		this.id = id;
		this.createNode();
		this.options.insertion.call(this, this.span,data.data || data);
		
		
		/* if this node's parent doesn't have a tree node, create a special purpose one */
		if(!this.parent.treeNode){ var newNode = new this.constructor(null,this.parent); }
		this.parent.treeNode.children.push(this);
		if(data.nodes){ this.createNodes(data.nodes); }
	},
	clearContents: function(){
		while(this.children.length){
			var node = this.children.shift();
			node.dispose();
		}
		this.loaded = false;
		this.hideChildren();
		if(this.options.onClearContents){ this.options.onClearContents.call(this); }
	},
	createNode: function(){
		var linkType = (this.options.leafNode ? 'leaf':'collapsed');
		var newID = (this.options.prependParentId !== false ? this.parent.id+this.options.prependParentId:'')+this.id;
		this.mark = Builder.node('span',{className:'mark '+linkType});
		this.span = Builder.node('span',{className:this.options.className+' treedata'});
		this.element = Builder.node('div',{id:newID,className:this.options.className+' treenode'},[
			this.mark,this.span
		]);
		this.events.observe(this.mark, 'click', this.onClick.bindAsEventListener(this));
		if(this.options.mouseOver){
			this.events.observe(this.span, 'mouseover', this.options.mouseOver.bindAsEventListener(this));
		}
		if(this.options.mouseOut){
			this.events.observe(this.span, 'mouseout', this.options.mouseOut.bindAsEventListener(this));
		}
		this.element.treeNode = this;
		this.parent.appendChild(this.element);
	},
	createNodes: function(nodes){
		this.showChildren();
		this.loaded = true;
		for(var i=0; i < nodes.length; i++){
			var newNode = new this.constructor(this.element,nodes[i].id,nodes[i].type,nodes[i]);
		}
		if(nodes.length && this.options.sortable){ this.createSortable(); }
	},
	createSortable: function(){
		//if(!this.options.sortable) return;
		Sortable.create(this.element,{
			tag: 'div',
			only: 'treenode'
		});
	},
	deleteChildNode: function(node){
		this.children = this.children.without(node);
		node.dispose();
	},
	deleteSelf: function(){
		if(this.parent.treeNode){ this.parent.treeNode.deleteChildNode(this); }
		else{ this.dispose(); }
		if(this.options.onDeleteSelf){ this.options.onDeleteSelf.call(this); }
	},
	dispose: function(){
		//if(this.options.sortable){ Sortable.destroy(this.sortable); }
		this.clearContents();
		if(this.options.dispose){ this.options.dispose.call(this); }
		while(this.disposables.length){ this.disposables.shift().dispose(); }
		this.element.treeNode = null;
		this.parent.removeChild(this.element);
	},
	getContents: function(onSuccess){
		if(this.opening || !this.options.page){ return; }
		this.opening = true;
		var params = 'action=getContents&' + ((this.options.callback) ? 
			this.options.callback.call(this, this.element.id) : 'id='+this.element.id);
		var request = new Ajax.Request(this.options.page,{
			parameters: params,
			onComplete: this.getContentsComplete.bind(this),
			onSuccess: function(xhr,json){
				if(json && json.error){ Ajax.Tree.showError(json.error); return; }
				if(!xhr.responseText){ Ajax.Tree.showError('Error, empty response from server'); return; }
				var data = xhr.responseText.evalJSON();
				this.clearContents();
				this.showChildren();
				this.loadContents(data,json);
				if(onSuccess) onSuccess();
			}.bind(this),
			onFailure: Ajax.Tree.error.ajax
		});
		if(this.options.onGetContents){ this.options.onGetContents.call(this, request); }
	},
	getContentsComplete: function(xhr,json){
		this.opening = false;
		if(this.options.onGetContentsComplete){ this.options.onGetContentsComplete.call(this, xhr, json); }
	},
	hide: function(el){
		Element.hide((el || this).element);
	},
	hideChildren: function(){
		this.children.each(this.hide);
		Element.removeClassName(this.mark,'expanded');
		Element.addClassName(this.mark,'collapsed');
	},
	loadContents: function(data,json){
		if(this.options.onLoadContent){ this.options.onLoadContent.call(this, data, json); }
		if(data.nodes){
			this.createNodes(data.nodes);
			if(this.options.onContentLoaded){ this.options.onContentLoaded.call(this, data, json); }
		}
	},
	onClick: function(event){
		if(this.options.onClick){
			if(this.options.onClick.call(this, event) === false) return;
		}
		if(this.options.page){
			if(this.loaded){ this.clearContents(); }
			else{ this.getContents(); }
		}
		else if(!this.options.leafNode){ this.toggleChildren(); }
	},
	show: function(el){
		Element.show((el || this).element);
	},
	showChildren: function(){
		this.children.each(this.show);
		Element.removeClassName(this.mark,'collapsed');
		Element.addClassName(this.mark,'expanded');
	},
	toggle: function(){
		this.element.visible() ? this.hide() : this.show();
	},
	toggleChildren: function(){
		this.isExpanded() ? this.hideChildren() : this.showChildren();
	},
	isExpanded: function(){ return this.mark.hasClassName('expanded'); }
};