/*function explain_(){
		var menu = document.getElementById("sample_queries");
		if ($(menu).is(":visible")) {
			$(menu).animate({width: 0}, 1000, function() {$(menu).hide();});
		}
		document.getElementById('tree').src = document.getElementById('tree').src;
		document.getElementById("result2").style.display="block";
		document.getElementById("tree").style.display="none";
		
		if(yasqe.queryStatus == "error"){
			document.getElementById("result2").innerHTML="<pre><font size=\"3\" color=\"red\">ERROR: Your query contains errors. Try again.</font></pre>" ;
		}
		else{
			var ajaxConfig = {
			url : document.getElementById("txt").value+"/explain",
			type : "POST",
			data : [ {
				name : "query",
				value : yasqe.getValue()
			} ],
			 success : function(data) { 
				document.getElementById("result2").innerHTML="<pre>"+data+"</pre>" ;
				document.getElementById("tree").style.display="block";
				setTimeout(function () {
					document.getElementById("tree").contentWindow.init(stringToJSON(data));
				}, 1000);
				},
			error: function(data, textStatus, error){
				document.getElementById("result2").innerHTML="<pre><font size=\"3\" color=\"red\">ERROR:"+data.responseText+"</font></pre>" 
			}
		
			};
			
			$.ajax(ajaxConfig);  
		}  
    }*/

var endpoint;
var yasqeObj;

$("#tab_raw").click(function(){raw_();});
$("#tab_tree").click(function(){tree_();});

function init(endpointString, yasqeObject){
	endpoint = endpointString;
	yasqeObj = yasqeObject;
}

function decompose_(){
		/*var menu = document.getElementById("sample_queries");
		if ($(menu).is(":visible")) {
			$(menu).animate({width: 0}, 1000, function() {$(menu).hide();});
		}*/
		document.getElementById('tree').src = document.getElementById('tree').src;
		document.getElementById("result2").style.display="block";
		document.getElementById("tree").style.display="none";
		if(yasqeObj.queryStatus == "error"){
			document.getElementById("result2").innerHTML="<pre><font size=\"3\" color=\"red\">ERROR: Your query contains errors. Try again.</font></pre>" ;
		}
		else{
		var ajaxConfig = {
		//url : document.getElementById("txt").value+"/decompose",
		url : endpoint+"/decompose",
		type : "POST",
		data : [ {
			name : "query",
			value : yasqeObj.getValue()
		} ],
		 success : function(data) { 
			//document.getElementById("result2").innerHTML="<pre>"+data+"</pre>" ;
			document.getElementById("result2").innerHTML=data ;

			$('.jsoncode').each(function() {
			    
			    var $this = $(this),
			        $code = $this.html(),
			        $unescaped = $('<div/>').html($code).text();
			   
			    $this.empty();

			    CodeMirror(this, {
			        value: $unescaped,
			        mode: 'python',
			        lineNumbers: true,
			        readOnly: true,
                    lineWrapping: true,
					rangeFinder: CodeMirror.fold.indent,
                    foldGutter: true,
                    gutters: ["CodeMirror-linenumbers", "CodeMirror-foldgutter"]
			    });
			    
			});
			document.getElementById("tree").style.display="block";
			setTimeout(function () {
				document.getElementById("tree").contentWindow.init(stringToJSON(data));
			}, 1000);
			},
		error: function(data, textStatus, error){
			document.getElementById("result2").innerHTML="<pre><font size=\"3\" color=\"red\">ERROR:"+data.responseText+"</font></pre>" 
		}
	
		};
		
		$.ajax(ajaxConfig);  
	}  
}

function raw_(){
		document.getElementById('tree').src = document.getElementById('tree').src;
		document.getElementById("result2").style.display="block";
		document.getElementById("tree").style.display="none";
		$("#tab_tree").removeClass("selected");
		$("#tab_raw").addClass("selected");
		if(yasqeObj.queryStatus == "error"){
			document.getElementById("result2").innerHTML="<pre><font size=\"3\" color=\"red\">ERROR: Your query contains errors. Try again.</font></pre>" ;
		}
		else{
		var ajaxConfig = {
		//url : document.getElementById("txt").value+"/decompose",
		url : endpoint+"/decompose",
		type : "POST",
		data : [ {
			name : "query",
			value : yasqeObj.getValue()
		} ],
		 success : function(data) { 
			//document.getElementById("result2").innerHTML="<pre>"+data+"</pre>" ;
			document.getElementById("result2").innerHTML=data ;

			$('.jsoncode').each(function() {
			    
			    var $this = $(this),
			        $code = $this.html(),
			        $unescaped = $('<div/>').html($code).text();
			   
			    $this.empty();

			    CodeMirror(this, {
			        value: $unescaped,
			        mode: 'python',
			        lineNumbers: true,
			        readOnly: true,
                    lineWrapping: true,
					rangeFinder: CodeMirror.fold.indent,
                    foldGutter: true,
                    gutters: ["CodeMirror-linenumbers", "CodeMirror-foldgutter"]
			    });
			    
			});
			/*document.getElementById("tree").style.display="block";
			setTimeout(function () {
				document.getElementById("tree").contentWindow.init(stringToJSON(data));
			}, 1000);*/
			},
		error: function(data, textStatus, error){
			document.getElementById("result2").innerHTML="<pre><font size=\"3\" color=\"red\">ERROR:"+data.responseText+"</font></pre>" 
		}
	
		};
		
		$.ajax(ajaxConfig);  
	} 
}

function tree_(){
		document.getElementById('tree').src = document.getElementById('tree').src;
		document.getElementById("result2").style.display="none";
		document.getElementById("tree").style.display="none";
		$("#tab_tree").toggleClass("selected");
		$("#tab_raw").toggleClass("selected");
		if(yasqeObj.queryStatus == "error"){
			document.getElementById("result2").innerHTML="<pre><font size=\"3\" color=\"red\">ERROR: Your query contains errors. Try again.</font></pre>" ;
		}
		else{
		var ajaxConfig = {
		//url : document.getElementById("txt").value+"/decompose",
		url : endpoint+"/decompose",
		type : "POST",
		data : [ {
			name : "query",
			value : yasqeObj.getValue()
		} ],
		 success : function(data) { 
			//document.getElementById("result2").innerHTML="<pre>"+data+"</pre>" ;
			/*document.getElementById("result2").innerHTML=data ;

			$('.jsoncode').each(function() {
			    
			    var $this = $(this),
			        $code = $this.html(),
			        $unescaped = $('<div/>').html($code).text();
			   
			    $this.empty();

			    CodeMirror(this, {
			        value: $unescaped,
			        mode: 'python',
			        lineNumbers: true,
			        readOnly: true,
                    lineWrapping: true,
					rangeFinder: CodeMirror.fold.indent,
                    foldGutter: true,
                    gutters: ["CodeMirror-linenumbers", "CodeMirror-foldgutter"]
			    });
			    
			});*/
			document.getElementById("tree").style.display="block";
			setTimeout(function () {
				document.getElementById("tree").contentWindow.init(stringToJSON(data));
			}, 1000);
			},
		error: function(data, textStatus, error){
			document.getElementById("result2").style.display="block";
			document.getElementById("result2").innerHTML="<pre><font size=\"3\" color=\"red\">ERROR:"+data.responseText+"</font></pre>" 
		}
	
		};
		
		$.ajax(ajaxConfig);  
	} 
}

function numberOfTabs(text) {
  var count = 0;
  var index = 0;
  while (text.charAt(index++) === " ") {
    count++;
  }
  return count;
}

function stringToJSON(s){
	var rows = s.split("\n");
	var prev = 0;
	var nodes = [];
	for(i = 0;i < rows.length-1;i++){//length-1 because the servers sends an empty line at the end
		var name = rows[i].replace(/^\s+/, function(m){ return m.replace(/\s/g, '');});
		var jsn = {
			id: "node"+i,
			name: name,
			data: {},
			children: []
		};
		nodes.push(jsn);
	}
	
	var available = [];
	available.push(nodes[0]);
	prev = numberOfTabs(rows[0]);
	for(i = 1;i < nodes.length;i++){
		var cur = numberOfTabs(rows[i]);
		if(prev == cur){
			available.pop();
		}
		else if(cur < prev){
			available.pop();
			available.pop();
		}
		available[available.length-1].children.push(nodes[i]);
		available.push(nodes[i]);
		prev = cur;
	}
	/*for(i = 0;i<nodes.length;i++){
		console.log(nodes[i].name+"\n===");
		var l = nodes[i].children.length;
		for(x = 0;x<l;x++){
			console.log(nodes[i].children[x].name+"\n");
		}
		console.log("\n---");
	}*/
	
	//console.log(JSON.stringify(rtn, undefined, 2));
	return available[0];
}

	function samples_(){
			 var menu = document.getElementById("sample_queries");
			 getSamples();
    if ($(menu).is(":visible")) {
        $(menu).animate({width: 0}, 1000, function() {$(menu).hide();});
    } else {
        $(menu).show().animate({width: "100%"}, 1000);           
    }
}

function getSamples(){
	var ajaxConfig = {
		url : document.getElementById("txt").value.substring(0, document.getElementById("txt").value.length - 6)+"samples",
		type : "GET",
		data : [ {
			name : "samples"
		} ],
		 success : function(data) { 
			document.getElementById("table").innerHTML = data ;
			$("#table").find("tr").click( function(){
				if($(this).parent().index() != 0){
					var menu = document.getElementById("sample_queries");
					if ($(menu).is(":visible")) {
						$(menu).animate({width: 0}, 1000, function() {$(menu).hide();});
					}
					var query = $(this).find("td:nth-child(3)").text();
					yasqe.setValue(query+"\n");//Line break in the end, because this method is made for code that consists of more that 1 lines
				}
			});
			$("#table").find("tr").mouseout( function(){
				if($(this).parent().index() != 0){
					restoreBackgroundColor(this);
				}
			});
			$("#table").find("tr").mouseover( function(){
				if($(this).parent().index() != 0){
					changeBackgroundColor(this);
					$(this).css('cursor','pointer');
				}
			});
			},
		error: function(data, textStatus, error){
			document.getElementById("table").innerHTML="<pre><font size=\"3\" color=\"red\">ERROR:"+data.responseText+"</font></pre>" 
		}
		
		};
		
		$.ajax(ajaxConfig); 
}

function changeBackgroundColor(row) { 
	row.style.backgroundColor = "#96ba8a"; 
}

function restoreBackgroundColor(row) { 
	row.style.backgroundColor = ""; 
}
