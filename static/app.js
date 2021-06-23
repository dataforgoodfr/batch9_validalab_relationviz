
function get_info(sitename){
    var result=sitename
    var result_1=sitename
    //document.getElementById("input-search").value
       /*  console.log(result)
        var div_2 = document.getElementById('put_link_3');
        div_2.innerHTML = "<span>" + result + "</span>"; */
        fetch("http://163.172.110.238:5000/med/" + result)
            .then(response => response.json())
            .then(function(response) { 
                news_data = response["w.summary"];
                var div_1 = document.getElementById("put_link_2");
                var content=document.getElementById("summary_content");

               div_1.removeChild(content)
                div_1.innerHTML+="<p id='summary_content'>"+news_data+"</p>";
            })
            .catch(error => alert("Erreur : " + "Nous n'avons pas de description pour ce site.Contactez nous si vous en avez!"));
            
            
        fetch("http://163.172.110.238:5000/med_1/" + result_1)
            .then(response => response.json())
    
            .then(function(response) { 
                var news_data_1 = ''
                for (let i = 0; i < response.length; i++) {
                    news_data_1 += response[i]["e2.name"] + ' - ' 
                    }
    
                    
                var div_4 = document.getElementById("put_link_4");
                var content=document.getElementById("owner_content");

                div_4.removeChild(content)
                div_4.innerHTML+="<p id='owner_content'>"+news_data_1+"</p>";
            })
                .catch(error => alert("Erreur : " + "Nous ne savons pas à qui appartient ce site pour l'instant. Contactez nous si vous en savez plus!"));
        fetch("http://163.172.110.238:5000/med_2/" + result)
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
                var content=document.getElementById("links_content");

                    div_5.removeChild(content)
                    div_5.innerHTML+="<p id='links_content'>"+news_data_2+"</p>";
            })
                .catch(error => alert("Erreur : " + "Nous ne savons pas à qui appartient ce site pour l'instant. Contactez nous si vous en savez plus!"));
            
        fetch("http://163.172.110.238:5000/med_3/" + result)
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
                div_6.innerHTML = news_data_5;
                div_7.innerHTML = news_data_6.replace("|", "");
            })
                .catch(error => alert("Erreur : " + "Nous n'avons pas de catégorie précise à attribuer à ce site.Contactez nous si vous en savez plus!"));
        
        fetch("http://163.172.110.238:5000/med_4/" + result)
            .then(response => response.json())
    
            .then(function(response) { 
                var news_data_5 = '';
                if (response[0]["gR"] == null){
                    news_data_5 = "";
                }else{
                    var vis = Math.round(response[0]["gV"] / 1000000) + ' M';
                    news_data_5 = " <b>"+response[0]["gR"] +"<sup>ième<sup></b> site grand public de France   <br>  <b>" + vis +"</b> visites/mois";
                }
                if (response[0]["pR"] == null){
                    news_data_6 = "" ;
                }else{
                    var vis = Math.round(response[0]["pV"] / 1000000) + ' M';
                    news_data_6 = " <b>"+response[0]["pR"] + "<sup>ième<sup></b> site pro de France     <br>   <b>" + vis +"</b> visites/mois";
                }
                var div_8 = document.getElementById("put_link_8");
                var content=document.getElementById("stats_content");

                div_8.removeChild(content)
                div_8.innerHTML+="<p id='stats_content'>"+news_data_5+"</p>";

                var div_9 = document.getElementById("put_link_9");
                var content=document.getElementById("stats_pro_content");

                div_9.removeChild(content)
                div_9.innerHTML+="<p id='stats_pro_content'>"+news_data_6.replace("|", "")+"</p>";


                
            })
                .catch(error => alert("Erreur : " + "Nous n'avons pas de statistiques fiables sur ce site.Contactez nous si vous en avez!"));
                fetch("http://163.172.110.238:5000/med_5/" + result)
                .then(response => response.json())
        
                .then(function(response) { 
                    var news_data_tt = '';
                    if (response[0] == null){
                        news_data_tt = " ";
                    }else{
                        var vis = Math.round(response[0]["tw.followers_count"] / 1000) + ' K';
                        var alink = document.createElement("a");
                        alink.href = "https://twitter.com/" + response[0]["tw.user_name"];
                        alink.text = "@" + response[0]["tw.user_name"];
                        alink.target = "_blank"
                        news_data_tt = " - " + vis +"  followers";
                        
                    }
                    console.log('Twitter',alink)
                    var div_10 = document.getElementById("element4");
                    
                    div_10.innerHTML = "<span id='put_1'><i class='fab fa-twitter fa-lg'> </i></span> <span id='where_to_insert'></span>";
                    document.getElementById('where_to_insert').appendChild(alink);
                    document.getElementById('where_to_insert').innerHTML+=news_data_tt
                })
                    .catch(error => console.log("Erreur : " + "Votre media n'est pas reference dans notre base.Contactez Validalab"));
        fetch("http://163.172.110.238:5000/med_6/" + result_1)
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
                alink.text = "Chaine Youtube: " + response[0]["yt.user_name"];
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
        fetch("http://163.172.110.238:5000/med_7/" + result_1)
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
            
            
  /*       fetch("http://163.172.110.238:5000/med_1/" + result_1)
            .then(response => response.json())
    
            .then(function(response) { 
                var news_data_1 = ''
                for (let i = 0; i < response.length; i++) {
                    news_data_1 += response[i]["e2.name"] + ' - ' 
                    }
    
                console.log(response)
                var div_4 = document.getElementById("put_link_4");
                div_4.innerHTML = news_data_1;
            })
                .catch(error => alert("Erreur : " + error)); */
   /*          fetch("http://163.172.110.238:5000/med_2/" + result)
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
                    //div_5.innerHTML = news_data_2;
                    var content=document.getElementById("links_content");

                    div_5.removeChild(content)
                    div_5.innerHTML+="<p id='links_content'>"+"test"+"</p>";

                })
                    .catch(error => alert("Erreur : " + error)); */
        
        // use `url` here inside the callback because it's asynchronous!
    
}
