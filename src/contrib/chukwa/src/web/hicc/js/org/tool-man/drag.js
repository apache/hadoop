/* Copyright (c) 2005 Tim Taylor Consulting (see LICENSE.txt) */

ToolMan._dragFactory = {
	createSimpleGroup : function(element, handle) {
		handle = handle ? handle : element
		var group = this.createGroup(element)
		group.setHandle(handle)
		group.transparentDrag()
		group.onTopWhileDragging()
		return group
	},

	createGroup : function(element) {
		var group = new _ToolManDragGroup(this, element)

		var position = ToolMan.css().readStyle(element, 'position')
		if (position == 'static') {
			element.style["position"] = 'relative'
		} else if (position == 'absolute') {
			/* for Safari 1.2 */
			ToolMan.coordinates().topLeftOffset(element).reposition(element)
		}

		// TODO: only if ToolMan.isDebugging()
		group.register('draginit', this._showDragEventStatus)
		group.register('dragmove', this._showDragEventStatus)
		group.register('dragend', this._showDragEventStatus)

		return group
	},

	_showDragEventStatus : function(dragEvent) {
		window.status = dragEvent.toString()
	},

	constraints : function() {
		return this._constraintFactory
	},

	_createEvent : function(type, event, group) {
		return new _ToolManDragEvent(type, event, group)
	}
}

function _ToolManDragGroup(factory, element) {
	this.factory = factory
	this.element = element
	this._handle = null
	this._thresholdDistance = 0
	this._transforms = new Array()
	// TODO: refactor into a helper object, move into events.js
	this._listeners = new Array()
	this._listeners['draginit'] = new Array()
	this._listeners['dragstart'] = new Array()
	this._listeners['dragmove'] = new Array()
	this._listeners['dragend'] = new Array()
}

_ToolManDragGroup.prototype = {
	/*
	 * TODO:
	 *   - unregister(type, func)
	 *   - move custom event listener stuff into Event library
	 *   - keyboard nudging of "selected" group
	 */

	setHandle : function(handle) {
		var events = ToolMan.events()

		handle.toolManDragGroup = this
		events.register(handle, 'mousedown', this._dragInit)
		handle.onmousedown = function() { return false }

		if (this.element != handle)
			events.unregister(this.element, 'mousedown', this._dragInit)
	},

	register : function(type, func) {
		this._listeners[type].push(func)
	},

	addTransform : function(transformFunc) {
		this._transforms.push(transformFunc)
	},

	verticalOnly : function() {
		this.addTransform(this.factory.constraints().vertical())
	},

	horizontalOnly : function() {
		this.addTransform(this.factory.constraints().horizontal())
	},

	setThreshold : function(thresholdDistance) {
		this._thresholdDistance = thresholdDistance
	},

	transparentDrag : function(opacity) {
		var opacity = typeof(opacity) != "undefined" ? opacity : 0.75;
		var originalOpacity = ToolMan.css().readStyle(this.element, "opacity")

		this.register('dragstart', function(dragEvent) {
			var element = dragEvent.group.element
			element.style.opacity = opacity
			element.style.filter = 'alpha(opacity=' + (opacity * 100) + ')'
		})
		this.register('dragend', function(dragEvent) {
			var element = dragEvent.group.element
			element.style.opacity = originalOpacity
			element.style.filter = 'alpha(opacity=100)'
		})
	},

	onTopWhileDragging : function(zIndex) {
		var zIndex = typeof(zIndex) != "undefined" ? zIndex : 100000;
		var originalZIndex = ToolMan.css().readStyle(this.element, "z-index")

		this.register('dragstart', function(dragEvent) {
			dragEvent.group.element.style.zIndex = zIndex
		})
		this.register('dragend', function(dragEvent) {
			dragEvent.group.element.style.zIndex = originalZIndex
		})
	},

	_dragInit : function(event) {
		event = ToolMan.events().fix(event)
		var group = document.toolManDragGroup = this.toolManDragGroup
		var dragEvent = group.factory._createEvent('draginit', event, group)

		group._isThresholdExceeded = false
		group._initialMouseOffset = dragEvent.mouseOffset
		group._grabOffset = dragEvent.mouseOffset.minus(dragEvent.topLeftOffset)
		ToolMan.events().register(document, 'mousemove', group._drag)
		document.onmousemove = function() { return false }
		ToolMan.events().register(document, 'mouseup', group._dragEnd)

		group._notifyListeners(dragEvent)
	},

	_drag : function(event) {
		event = ToolMan.events().fix(event)
		var coordinates = ToolMan.coordinates()
		var group = this.toolManDragGroup
		if (!group) return
		var dragEvent = group.factory._createEvent('dragmove', event, group)

		var newTopLeftOffset = dragEvent.mouseOffset.minus(group._grabOffset)

		// TODO: replace with DragThreshold object
		if (!group._isThresholdExceeded) {
			var distance = 
					dragEvent.mouseOffset.distance(group._initialMouseOffset)
			if (distance < group._thresholdDistance) return
			group._isThresholdExceeded = true
			group._notifyListeners(
					group.factory._createEvent('dragstart', event, group))
		}

		for (i in group._transforms) {
			var transform = group._transforms[i]
			newTopLeftOffset = transform(newTopLeftOffset, dragEvent)
		}

		var dragDelta = newTopLeftOffset.minus(dragEvent.topLeftOffset)
		var newTopLeftPosition = dragEvent.topLeftPosition.plus(dragDelta)
		newTopLeftPosition.reposition(group.element)
		dragEvent.transformedMouseOffset = newTopLeftOffset.plus(group._grabOffset)

		group._notifyListeners(dragEvent)

		var errorDelta = newTopLeftOffset.minus(coordinates.topLeftOffset(group.element))
		if (errorDelta.x != 0 || errorDelta.y != 0) {
			coordinates.topLeftPosition(group.element).plus(errorDelta).reposition(group.element)
		}
	},

	_dragEnd : function(event) {
		event = ToolMan.events().fix(event)
		var group = this.toolManDragGroup
		var dragEvent = group.factory._createEvent('dragend', event, group)

		group._notifyListeners(dragEvent)

		this.toolManDragGroup = null
		ToolMan.events().unregister(document, 'mousemove', group._drag)
		document.onmousemove = null
		ToolMan.events().unregister(document, 'mouseup', group._dragEnd)
	},

	_notifyListeners : function(dragEvent) {
		var listeners = this._listeners[dragEvent.type]
		for (i in listeners) {
			listeners[i](dragEvent)
		}
	}
}

function _ToolManDragEvent(type, event, group) {
	this.type = type
	this.group = group
	this.mousePosition = ToolMan.coordinates().mousePosition(event)
	this.mouseOffset = ToolMan.coordinates().mouseOffset(event)
	this.transformedMouseOffset = this.mouseOffset
	this.topLeftPosition = ToolMan.coordinates().topLeftPosition(group.element)
	this.topLeftOffset = ToolMan.coordinates().topLeftOffset(group.element)
}

_ToolManDragEvent.prototype = {
	toString : function() {
		return "mouse: " + this.mousePosition + this.mouseOffset + "    " +
				"xmouse: " + this.transformedMouseOffset + "    " +
				"left,top: " + this.topLeftPosition + this.topLeftOffset
	}
}

ToolMan._dragFactory._constraintFactory = {
	vertical : function() {
		return function(coordinate, dragEvent) {
			var x = dragEvent.topLeftOffset.x
			return coordinate.x != x
					? coordinate.factory.create(x, coordinate.y) 
					: coordinate
		}
	},

	horizontal : function() {
		return function(coordinate, dragEvent) {
			var y = dragEvent.topLeftOffset.y
			return coordinate.y != y
					? coordinate.factory.create(coordinate.x, y) 
					: coordinate
		}
	}
}
