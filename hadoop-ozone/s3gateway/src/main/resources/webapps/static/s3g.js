window.onload = function () {
    var safeurl = window.location.protocol + "//" + window.location.host + window.location.pathname;
    safeurl = safeurl.replace("static/", "");
    document.getElementById('s3gurl').innerHTML = safeurl;
};