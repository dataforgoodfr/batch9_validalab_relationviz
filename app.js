
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
        result=sit[1] + '.' + sit[2];
        result_1 = sit[1] + '.' + sit[2]
    }else{
        result=site
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
        .catch(error => alert("Erreur : " + "Votre media n'est pas reference dans notre base.Contactez Validalab"));
        
        
    fetch("http://51.159.52.80:5000/med_1/" + result_1)
        .then(response => response.json())

        .then(function(response) { 
            var news_data_1 = ''
            for (let i = 0; i < response.length; i++) {
                news_data_1 += response[i]["e2.name"] + ' - ' 
                }

            
            var div_4 = document.getElementById("put_link_4");
            div_4.innerHTML += news_data_1;
        })
            .catch(error => alert("Erreur : " + "Votre media n'est pas reference dans notre base.Contactez Validalab"));
    fetch("http://51.159.52.80:5000/med_2/" + result)
        .then(response => response.json())

        .then(function(response) { 
            var news_data_2 = '';
            for (let i = 0; i < response.length; i++) {
                if(i<response.length-1){
                    news_data_2 += response[i]["e.name"] + ' <== '  ;
                }else{
                    news_data_2 += response[i]["e.name"]   ; 
                    }
                }

            
            var div_5 = document.getElementById("put_link_5");
            div_5.innerHTML += news_data_2;
        })
            .catch(error => alert("Erreur : " + "Votre media n'est pas reference dans notre base.Contactez Validalab"));
        
    fetch("http://51.159.52.80:5000/med_3/" + result)
        .then(response => response.json())

        .then(function(response) { 
            var news_data_5 = '';
            if (response[0]["wiki.genre"] == null){
                news_data_5 = "Type: pas disponible";
            }else{
                news_data_5 = "Type: " + response[0]["wiki.genre"] ;
                news_data_5 = news_data_5.replace("[[", "").replace("]]", "").replace("|", ", ").replace("[", "").replace("]", "");
            }
            if (response[0]["wiki.categories[0]"] == null){
                news_data_6 = "Diffusion: pas disponible" ;
            }else{
                news_data_6 = "Diffusion: " + response[0]["wiki.categories[0]"] ;
            }
            // news_data_5 = "Type: " + response[0]["wiki.genre"] ;
            // news_data_6 = "Diffusion: " + response[0]["wiki.categories[0]"] ;
            
            var div_6 = document.getElementById("put_link_6");
            var div_7 = document.getElementById("put_link_7");
            div_6.innerHTML += news_data_5;
            div_7.innerHTML += news_data_6.replace("|", "");
        })
            .catch(error => alert("Erreur : " + "Votre media n'est pas reference dans notre base.Contactez Validalab"));
    
    fetch("http://51.159.52.80:5000/med_4/" + result)
        .then(response => response.json())

        .then(function(response) { 
            var news_data_5 = '';
            if (response[0]["gR"] == null){
                news_data_5 = "";
            }else{
                var vis = Math.round(response[0]["gV"] / 1000000) + ' M';
                news_data_5 = response[0]["gR"] +" ieme site grand public de France   ,   " + vis +" visites/mois";
            }
            if (response[0]["pR"] == null){
                news_data_6 = "" ;
            }else{
                var vis = Math.round(response[0]["pV"] / 1000000) + ' M';
                news_data_6 = response[0]["pR"] + " ieme site pro de France     ,  " + vis +" visites/mois";
            }
            var div_8 = document.getElementById("put_link_8");
            var div_9 = document.getElementById("put_link_9");
            div_8.innerHTML += news_data_5;
            div_9.innerHTML += news_data_6.replace("|", "");
        })
            .catch(error => alert("Erreur : " + "Votre media n'est pas reference dans notre base.Contactez Validalab"));
    fetch("http://51.159.52.80:5000/med_5/" + result)
            .then(response => response.json())
    
            .then(function(response) { 
                var news_data_5 = '';
                if (response[0] == null){
                    news_data_5 = " ";
                }else{
                    var vis = Math.round(response[0]["tw.followers_count"] / 1000) + ' K';
                    var alink = document.createElement("a");
                    alink.href = "https://twitter.com/" + response[0]["tw.user_name"];
                    alink.text = "@" + response[0]["tw.user_name"];
                    alink.target = "_blank"
                    news_data_5 = " - " + vis +"  followers";
                    document.getElementById('where_to_insert').appendChild(alink);
                }
                
                var div_10 = document.getElementById("element4");
                div_10.innerHTML += news_data_5;
            })
                .catch(error => alert("Erreur : " + "Votre media n'est pas reference dans notre base.Contactez Validalab"));
    fetch("http://51.159.52.80:5000/med_6/" + result_1)
    .then(response => response.json())

    .then(function(response) { 
        var news_data_5 = '';
        // alert(JSON.stringify(response));
        if (response[0] == null){
            news_data_5 = "";
        }else{
            var vis = Math.round(response[0]["yt.pro_subscriberCount"] / 1000) + ' K';
            var alink = document.createElement("a");
            alink.href = response[0]["yt.url"];
            alink.text = "Chaine Youtube: " + response[0]["yt.user_name"];
            alink.target = "_blank"
            news_data_5 = " - " + vis +"  Subscribers";
            document.getElementById('where_to_insert_1').appendChild(alink);
        }
        
        var div_11 = document.getElementById("element5");
        div_11.innerHTML += news_data_5;
    })
        .catch(error => alert("Erreur : " + "Votre media n'est pas reference dans notre base.Contactez Validalab"));
    fetch("http://51.159.52.80:5000/med_7/" + result_1)
        .then(response => response.json())
    
        .then(function(response) { 
            var news_data_5 = '';
            // alert(JSON.stringify(response));
            if (response[0] == null){
                news_data_5 = " ";
            }else{
                var alink = document.createElement("a");
                alink.href = "https://www.facebook.com/" + response[0]["fb.user_name"];
                alink.text = "@" + response[0]["fb.user_name"];
                alink.target = "_blank"
                news_data_5 = "  " ;
                document.getElementById('where_to_insert_2').appendChild(alink);
            }
            
            var div_11 = document.getElementById("element5");
            div_11.innerHTML += news_data_5;
        })
            .catch(error => alert("Erreur : " + "Votre media n'est pas reference dans notre base.Contactez Validalab"));
        
        
    fetch("http://51.159.52.80:5000/med_1/" + result_1)
        .then(response => response.json())

        .then(function(response) { 
            var news_data_1 = ''
            for (let i = 0; i < response.length; i++) {
                news_data_1 += response[i]["e2.name"] + ' - ' 
                }

            
            var div_4 = document.getElementById("put_link_4");
            div_4.innerHTML += news_data_1;
        })
            .catch(error => alert("Erreur : " + error));
        fetch("http://51.159.52.80:5000/med_2/" + result)
            .then(response => response.json())
    
            .then(function(response) { 
                var news_data_2 = '';
                for (let i = 0; i < response.length; i++) {
                    if(i<response.length-1){
                        news_data_2 += response[i]["e.name"] + ' <-- ' ;
                    }else{
                        news_data_2 += response[i]["e.name"]   ; 
                        }
                    }
    
                
                var div_5 = document.getElementById("put_link_5");
                div_5.innerHTML += news_data_2;
            })
                .catch(error => alert("Erreur : " + error));
    
    // use `url` here inside the callback because it's asynchronous!
});
