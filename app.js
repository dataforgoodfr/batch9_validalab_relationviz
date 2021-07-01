var api_url = "http://app.validalab.fr"
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
    var div_2 = document.getElementById('put_link')
    div_2.style.display='block'
    div_2.innerHTML = "<span>" + result + "</span>";
    fetch(api_url + "/med/" + result)
        .then(response => response.json())
        .then(function(response) { 
            console.log('test',response)
            if(response.length==0){
            news_data="Information indisponible"
            }
            else{
                news_data = response[0]["w.summary"];
            }
            
            var div_1 = document.getElementById("put_link_2");
            var content=document.getElementById("summary_content");

           div_1.removeChild(content)
            div_1.innerHTML+="<p id='summary_content'>"+news_data+"</p>";
        })
        .catch(error => console.log("Erreur : " + "Nous n'avons pas de description pour ce site.Contactez nous si vous en avez!"));
        
        
    fetch(api_url + "/med_1/" + result_1)
        .then(response => response.json())

        .then(function(response) { 
            var news_data_1 = ''
            if(response.length==0){
                news_data_1="Information indisponible"
                }
                else{
                    for (let i = 0; i < response.length; i++) {
                        news_data_1 += "<span class='badge badge-info friends-badge'>"+response[i]["e2.name"] +"</span>" + ' '  
                        }
                }
            

                
            var div_4 = document.getElementById("put_link_4");
            var content=document.getElementById("owner_content");

            div_4.removeChild(content)
            div_4.innerHTML+="<p id='owner_content'>"+news_data_1+"</p>";
        })
            .catch(error => console.log("Erreur : " + "Nous ne savons pas à qui appartient ce site pour l'instant. Contactez nous si vous en savez plus!"));
    fetch(api_url + "/med_2/" + result)
        .then(response => response.json())

        .then(function(response) { 
            var news_data_2 = '';
            if(response.length==0){
                news_data_2="Information indisponible"
                }
                else{
                    for (let i = 0; i < response.length; i++) {
                        if(i<response.length-1){
                            news_data_2 += "<span class='badge badge-info friends-badge'>"+response[i]["e.name"] +"</span>" + ' ';
                        }else{
                            news_data_2 += "<span class='badge badge-info friends-badge'>"+response[i]["e.name"] +"</span>"  ; 
                            }
                        }
                }
            
                //news_data_2 += response[response.length-1]["e.name"]  
            
            var div_5 = document.getElementById("put_link_5");
            var content=document.getElementById("links_content");

                div_5.removeChild(content)
                div_5.innerHTML+="<p id='links_content'>"+news_data_2+"</p>";
        })
            .catch(error => console.log("Erreur : " + "Nous ne savons pas à qui appartient ce site pour l'instant. Contactez nous si vous en savez plus!"));
        
    fetch(api_url + "/med_3/" + result)
        .then(response => response.json())

        .then(function(response) { 
            var div_6 = document.getElementById("put_link_6");
            var div_7 = document.getElementById("put_link_7");
            var news_data_5 = '';
            if (response.length ==0 || response[0]["wiki.genre"]==null){
                //div_6.style.display='none'
                news_data_5 = "Type: Information indisponible";
            }else{
                news_data_5 = "Type: " + response[0]["wiki.genre"] ;
                news_data_5 = news_data_5.replace("[[", "").replace("]]", "").replace("|", ", ").replace("[", "").replace("]", "");
                //div_7.style.display='block'
            }
            if (response.length ==0 || response[0]["wiki.genre"]==null){
                news_data_6 = "Diffusion: Information indisponible" ;
               // div_7.style.display='none'
            }else{
                news_data_6 = "Diffusion: " + response[0]["wiki.categories[0]"] ;
               // div_7.style.display='block'
            }
            // news_data_5 = "Type: " + response[0]["wiki.genre"] ;
            // news_data_6 = "Diffusion: " + response[0]["wiki.categories[0]"] ;
            
            
            div_6.innerHTML = news_data_5;
            div_7.innerHTML = news_data_6.replace("|", "");
        })
            .catch(error => console.log("Erreur : " + "Nous n'avons pas de catégorie précise à attribuer à ce site.Contactez nous si vous en savez plus!"));
    
    fetch(api_url + "/med_4/" + result)
        .then(response => response.json())

        .then(function(response) { 
            var div_8 = document.getElementById("put_link_8");
            var div_9 = document.getElementById("put_link_9");

            var news_data_5 = '';
            if (response[0]["gR"] == null){
                news_data_5 = "";
                div_8.style.display='none'
            }else{
                var vis = Math.round(response[0]["gV"] / 1000000) + ' M';
                news_data_5 = " <b>"+response[0]["gR"] +"<sup>ième<sup></b> site grand public de France   <br>  <b>" + vis +"</b> visites/mois";
                div_8.style.display='block'
            }
            if (response[0]["pR"] == null){
                news_data_6 = "" ;
                div_9.style.display='none'
            }else{
                var vis = Math.round(response[0]["pV"] / 1000000) + ' M';
                news_data_6 = " <b>"+response[0]["pR"] + "<sup>ième<sup></b> site pro de France     <br>   <b>" + vis +"</b> visites/mois";
                div_9.style.display='block'
            }
            
            var content=document.getElementById("stats_content");

            div_8.removeChild(content)
            div_8.innerHTML+="<p id='stats_content'>"+news_data_5+"</p>";

            
            var content=document.getElementById("stats_pro_content");

            div_9.removeChild(content)
            div_9.innerHTML+="<p id='stats_pro_content'>"+news_data_6.replace("|", "")+"</p>";


            
        })
            .catch(error => console.log("Erreur : " + "Nous n'avons pas de statistiques fiables sur ce site.Contactez nous si vous en avez!"));
            fetch(api_url + "/med_5/" + result)
            .then(response => response.json())
    
            .then(function(response) { 
                var news_data_tt = '';
                var div_10 = document.getElementById("element4");
                if (response[0] == null){
                    news_data_tt = " ";
                    div_10.style.display='none'
                }else{
                    div_10.style.display='block'
                    var vis = Math.round(response[0]["tw.followers_count"] / 1000) + ' K';
                    var alink = document.createElement("a");
                    alink.href = "https://twitter.com/" + response[0]["tw.user_name"];
                    alink.text = "@" + response[0]["tw.user_name"];
                    alink.target = "_blank"
                    news_data_tt = " - " + vis +"  followers";
                    
                }
                console.log('Twitter',alink)
                
                
                div_10.innerHTML = "<span id='put_1'><i class='fab fa-twitter fa-lg'> </i></span> <span id='where_to_insert'></span>";
                document.getElementById('where_to_insert').appendChild(alink);
                document.getElementById('where_to_insert').innerHTML+=news_data_tt
            })
                .catch(error => console.log("Erreur : " + "Votre media n'est pas reference dans notre base.Contactez Validalab"));
    fetch(api_url + "/med_6/" + result_1)
    .then(response => response.json())

    .then(function(response) { 
        var news_data_yt = '';
        // alert(JSON.stringify(response));
        if (response[0] == null){
            news_data_yt = "";
        }else{
            var vis = Math.round(response[0]["yt.pro_subscriberCount"] / 1000) + ' K';
            var alink = document.createElement("a");
            alink.href = response[0]["yt.url"];
            alink.text = "@" + response[0]["yt.user_name"];
            alink.target = "_blank"
            news_data_yt = " - " + vis +"  Subscribers";
            document.getElementById('where_to_insert_1').appendChild(alink);
        }
        console.log('Youtube',news_data_yt)
        var div_11 = document.getElementById("element5");
        //div_11.innerHTML = news_data_yt;

        div_11.innerHTML = "<span id='put_2'><i class='fab fa-youtube fa-lg'> </i></span> <span id='where_to_insert_1'></span>";
        document.getElementById('where_to_insert_1').appendChild(alink);
        document.getElementById('where_to_insert_1').innerHTML+=news_data_yt
    })
        .catch(error => console.log("Erreur : " + "Votre media n'est pas reference dans notre base.Contactez Validalab"));
    fetch(api_url + "/med_7/" + result_1)
        .then(response => response.json())
    
        .then(function(response) { 
            var news_data_fb = '';
            // alert(JSON.stringify(response));
            if (response[0] == null){
                news_data_fb = " ";
            }else{
                var alink = document.createElement("a");
                alink.href = "https://www.facebook.com/" + response[0]["fb.user_name"];
                alink.text = "@" + response[0]["fb.user_name"];
                alink.target = "_blank"
                news_data_fb = "  " ;
                document.getElementById('where_to_insert_2').appendChild(alink);
            }
            console.log('Facebook',news_data_fb)
            var div_11 = document.getElementById("element6");

            div_11.innerHTML = "<span id='put_3'><i class='fab fa-facebook fa-lg'> </i></span> <span id='where_to_insert_2'></span>";
            document.getElementById('where_to_insert_2').appendChild(alink);
            document.getElementById('where_to_insert_2').innerHTML+=news_data_fb
        })
            .catch(error => console.log("Erreur : " + "Votre media n'est pas reference dans notre base.Contactez Validalab"));
        
        
            fetch(api_url + "/med_8/" + result_1)
            .then(response => response.json())
    
            .then(function(response) { 
                var news_data_5 = '';
                
                // alert(JSON.stringify(response));
                if (response[0] == null){
                    news_data_5 = "Information indisponible";
                }else{
                    var l_1 = response.length
                    if (l_1>5){
                        for (let i = 0; i < 5; i++) {
                            news_data_5 += "<span class='badge badge-info friends-badge'>"+response[i]["w2.name"] +"</span>" + ' ' 
                            }
                    }else{
                        for (let i = 0; i < l_1; i++) {
                            news_data_5 += "<span class='badge badge-info friends-badge'>"+response[i]["w2.name"]+"</span>" + " " 
                            }
                             
                    }
                   
                    
                }
                
                var div_11 = document.getElementById("put_link_11");
                
                var content=document.getElementById("friends_content");

                div_11.removeChild(content)
                div_11.innerHTML+="<p id='friends_content'>"+news_data_5.replace("|", "")+"</p>";
            })
                .catch(error => console.log("Erreur : " + "Votre media n'est pas reference dans notre base.Contactez Validalab"));
            fetch(api_url + "/med_9/" + result_1)
            .then(response => response.json())
    
            .then(function(response) { 
                var news_data_5 = '';
                var news_data_6 = '';
                var news_data_7 = '';
                var pos_display='none';
                var neg_display='none';
                var posneg_display='none;'
                var none_display='none;'
                
                const weights = [...new Set(response.map(item => item.weight))]
                min_weight=Math.min(...weights)
                // alert(JSON.stringify(response));
                if (response[0] == null){
                    news_data_5 = "";
                    news_data_6 = '';
                    news_data_7 = '';
                    pos_display='none';
                    neg_display='none';
                    posneg_display='none';
                    none_display='none;'
            
                    
                }else{
                    var positive_reco=response.filter(x=> x.weight>=0)
                    var positive_length=positive_reco.length
                    var negative_reco=response.filter(x=> x.weight<-0.5)
                    var negative_length=negative_reco.length
                    var posneg_reco=response.filter(x=> x.weight<0 & x.weight>=-0.5  )
                    console.log()
                    var posneg_length=posneg_reco.length
                    var content=document.getElementById("put_link_14")
                    if (weights.includes(1)){
                        pos_display='block'
                    }
                    if (weights.includes(-0.5)){
                        posneg_display='block'
                    }
                    if (weights.includes(-1)){
                        neg_display='block'
                    }
                    if (weights.includes(null)){
                        none_display='block'
                    }
                    if (min_weight==1){
                        
                        var div_2 = document.getElementById('put_link')
                        div_2.style.backgroundColor='#3c9951'
                        div_2.style.borderColor='#3c9951'
                        div_2.style.color='#fff'
                        content.innerHTML="<b>Plutôt fiable :</b>  site recommandé par d’autres sites comme fiable.\nCherchez d’autres points de vue pour confirmer les informations que vous trouverez sur ce site";
                        content.style.borderStyle='solid'
                        content.style.borderRadius='5px'
                        content.style.borderWidth='2px'
                        content.style.borderColor='#3c9951'
                        content.style.padding='3px'
                        content.style.fontSize='small'
                    }
                    if(min_weight==-0.5) {
                        
                        var div_2 = document.getElementById('put_link')
                        div_2.style.backgroundColor='#e89613'
                        div_2.style.borderColor='#e89613'
                        div_2.style.color='#fff'
                        content.innerHTML="<b>Peu fiable :</b> site jugé peu fiable par au moins un autre site\nAttention, il vaut mieux confirmer ce que vous y lisez sur d’autres sites ";
                        content.style.borderStyle='solid'
                        content.style.borderRadius='5px'
                        content.style.borderWidth='2px'
                        content.style.borderColor='#e89613'
                        content.style.padding='3px'
                        content.style.fontSize='small'
                        
                    }
                    if(min_weight==-1){
                        
                        var div_2 = document.getElementById('put_link')
                        div_2.style.backgroundColor='#b50d1e'
                        div_2.style.borderColor='#b50d1e'
                        div_2.style.color='#fff'
                        content.innerHTML="<b>Pas fiable :</b>  ce site est connu pour diffuser de fausses informations\nAttention, soyez vigilants et cherchez d’autres sources d’information";
                        content.style.borderStyle='solid'
                        content.style.borderRadius='5px'
                        content.style.borderWidth='2px'
                        content.style.borderColor='#b50d1e'
                        content.style.padding='3px'
                        content.style.fontSize='small'
                    }
                    if(min_weight==null) {
                        var div_2 = document.getElementById('put_link')
                        div_2.style.backgroundColor='#a6a5a4'
                        div_2.style.borderColor='#a6a5a4'
                        div_2.style.color='#fff'
                        content.innerHTML="<b>Pas d’information :</b>  nous ne disposons pas assez d’information pour recommander ce site\nDans tous les cas, cherchez d’autres points de vue permet de confirmer ce que vous lisez sur ce site";
                        content.style.borderStyle='solid'
                        content.style.borderRadius='5px'
                        content.style.borderWidth='2px'
                        content.style.borderColor='#a6a5a4'
                        content.style.padding='3px'
                        content.style.fontSize='small'
                    }
                /*     if (negative_length==0){
                        neg_display='none'
                        var div_2 = document.getElementById('put_link')
                        div_2.style.backgroundColor='#3c9951'
                        div_2.style.borderColor='#3c9951'
                        div_2.style.color='#fff'
                    }
                    else{
                        neg_display='block'
                        var div_2 = document.getElementById('put_link')
                        div_2.style.backgroundColor='#b50d1e'
                        div_2.style.borderColor='#b50d1e'
                        div_2.style.color='#fff'
                        
                    } */
                  
                    for (let i = 0; i < positive_length; i++) {
                        news_data_5 += "<tr><td> <span class='badge badge-reco friends-badge'>"+positive_reco[i]['recommender']+"</span> </td>"+
                        "<td><span class='badge badge-reco friends-badge'>"+positive_reco[i]['meaning'] +" </span> </td>"+
                        "<td><span class='badge badge-reco friends-badge'><a target='_blank' href='"+positive_reco[i]["sourceURL"]+"'>Source</a></span>  </td>"+
                        "</tr>"
                        
                        }
                    for (let i = 0; i < negative_length; i++) {
                        news_data_6 += "<tr><td> <span class='badge badge-negative friends-badge'>"+negative_reco[i]['recommender']+"</span> </td>"+
                        "<td><span class='badge badge-negative friends-badge'>"+negative_reco[i]['meaning'] +" </span> </td>"+
                        "<td><span class='badge badge-negative friends-badge'><a target='_blank'  href='"+negative_reco[i]["sourceURL"]+"'>Source</a></span>  </td>"+
                        "</tr>"
                        
                        }
                    for (let i = 0; i < posneg_length; i++) {
                            news_data_7 += "<tr><td> <span class='badge badge-snegative friends-badge'>"+posneg_reco[i]['recommender']+"</span> </td>"+
                            "<td><span class='badge badge-snegative friends-badge'>"+posneg_reco[i]['meaning'] +" </span> </td>"+
                            "<td><span class='badge badge-snegative friends-badge'><a target='_blank'  href='"+posneg_reco[i]["sourceURL"]+"'>Source</a></span>  </td>"+
                            "</tr>"
                            
                            }
                    }
                /* var div_10 = document.getElementById("put_link_14");
                div_10.innerHTML+=text */
                var div_11 = document.getElementById("put_link_12");
                var pos_content=document.getElementById("pos_reco_content");
                var pos_table=document.getElementById("pos_reco_table");

                div_11.removeChild(pos_content)
                div_11.removeChild(pos_table)
                div_11.innerHTML+="<p style='display:"+pos_display+"' id='pos_reco_content'>"+  result_1 + " est recommandé par "  +  positive_length + " labels</p>";
                div_11.innerHTML+= "<table style='display:"+pos_display+"' class='table table-borderless' id='pos_reco_table'>"+ news_data_5+"</table>";
                    
                console.log(response)
                var div_12 = document.getElementById("put_link_13");
                var neg_content=document.getElementById("neg_reco_content");
                var neg_table=document.getElementById("neg_reco_table");
                div_12.removeChild(neg_content)
                div_12.removeChild(neg_table)
                div_12.innerHTML+="<p style='display:"+neg_display+"' id='neg_reco_content'>"  +  negative_length + " labels mettent le public en garde sur" +result_1+" </p>";
                div_12.innerHTML+= "<table style='display:"+neg_display+"' class='table table-borderless' id='neg_reco_table'>"+ news_data_6+"</table>";

                var div_13 = document.getElementById("put_link_15");
                var posneg_content=document.getElementById("posneg_reco_content");
                var posneg_table=document.getElementById("posneg_reco_table");
                div_13.removeChild(posneg_content)
                div_13.removeChild(posneg_table)
                div_13.innerHTML+="<p style='display:"+posneg_display+"' id='posneg_reco_content'>"  +  posneg_length + " labels mettent le public en garde sur" +result_1+" </p>";
                div_13.innerHTML+= "<table style='display:"+posneg_display+"' class='table table-borderless' id='posneg_reco_table'>"+ news_data_7+"</table>";
                //div_11.innerHTML += "<span style='background-color:blueviolet;'>" +  result_1 + " est recommande par " + "</span>" + "<br>" + news_data_5;
            })
                .catch(error => console.log("Erreur : " + "Votre media n'est pas reference dans notre base.Contactez Validalab"));

    // use `url` here inside the callback because it's asynchronous!
});
