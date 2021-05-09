chrome.tabs.query({active: true, lastFocusedWindow: true}, tabs => {
    let url = tabs[0].url;
    var div = document.getElementById('put_link');

    div.innerHTML += url;
    // use `url` here inside the callback because it's asynchronous!
});
