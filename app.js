
chrome.tabs.query({active: true, lastFocusedWindow: true}, tabs => {
    var tab = tabs[0];
    var url = new URL(tab.url)
    var domain = url.hostname
    var div = document.getElementById('put_link');
    div.innerHTML += domain;
    site = tabs[0].url.split("/")[2];
    sit = site.split(".");
    let result = '';
    let result_1 = ''
    if(sit.length ===3){
        result=sit[1]
        result_1 = sit[1] + '.' + sit[2]
    }else{
        result=sit[0]
        result_1 = site
    }
    var div_2 = document.getElementById('put_link_3');
    div_2.innerHTML = "<span>" + result + "</span>";
    fetch("http://51.159.52.80:5000/med/" + result)
        .then(response => response.json())
        .then(function(response) { 
            news_data = response["w.summary"];
            var div_1 = document.getElementById("put_link_2");
            div_1.innerHTML += news_data;
        })
        .catch(error => alert("Erreur : " + error));
        function display_news(response) {
            // alert("Résultat de la requête : " + JSON.stringify(response));
            var news_data = JSON.stringify(response);
            
            
        }
        
        fetch("http://51.159.52.80:5000/med_1/" + result_1)
            .then(response => response.json())
            .then(function(response) { 
                news_data_1 = response[0]["e2.name"] + ' - ' + response[1]["e2.name"] + ' - ' + response[2]["e2.name"] +
                             ' - ' + response[3]["e2.name"] + ' - ' + response[4]["e2.name"];
                var div_4 = document.getElementById("put_link_4");
                div_4.innerHTML += news_data_1;
            })
            .catch(error => alert("Erreur : " + error));
            function display_news(response) {
                // alert("Résultat de la requête : " + JSON.stringify(response));
                var news_data = JSON.stringify(response);
                
                
            }
    
    // use `url` here inside the callback because it's asynchronous!
});
