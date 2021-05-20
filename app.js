chrome.tabs.query({active: true, lastFocusedWindow: true}, tabs => {
    var tab = tabs[0];
    var url = new URL(tab.url)
    var domain = url.hostname
    var div = document.getElementById('put_link');
    div.innerHTML += domain;
    // use `url` here inside the callback because it's asynchronous!
});
