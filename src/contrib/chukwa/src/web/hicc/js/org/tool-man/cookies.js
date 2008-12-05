/* Copyright (c) 2005 Tim Taylor Consulting (see LICENSE.txt)

based on http://www.quirksmode.org/js/cookies.html
*/

ToolMan._cookieOven = {

	set : function(name, value, expirationInDays) {
		if (expirationInDays) {
			var date = new Date()
			date.setTime(date.getTime() + (expirationInDays * 24 * 60 * 60 * 1000))
			var expires = "; expires=" + date.toGMTString()
		} else {
			var expires = ""
		}
		document.cookie = name + "=" + value + expires + "; path=/"
	},

	get : function(name) {
		var namePattern = name + "="
		var cookies = document.cookie.split(';')
		for(var i = 0, n = cookies.length; i < n; i++) {
			var c = cookies[i]
			while (c.charAt(0) == ' ') c = c.substring(1, c.length)
			if (c.indexOf(namePattern) == 0)
				return c.substring(namePattern.length, c.length)
		}
		return null
	},

	eraseCookie : function(name) {
		createCookie(name, "", -1)
	}
}
