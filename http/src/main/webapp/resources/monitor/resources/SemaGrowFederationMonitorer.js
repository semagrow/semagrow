SemaGrowFederationMonitorer = function() {
};

SemaGrowFederationMonitorer.prototype = {
	serviceEndpoint:"http://localhost/SemaGrow/monitoring",
	containerElement:"container",
	detailElement:"detail",
	queryElement:"queryContainer",
	queries: new Array(),
	currentQuery:-1,
	parseQueryName: function (queryName) {
		if (this.queries.indexOf(queryName) === -1) {
			this.queries.push(queryName);
		}
		return this.queries.indexOf(queryName);
  },
	parseQueryString: function(queryString){
		return queryString.replace(/\\"/g, '"').replace(/\\'/g, "'").replace(/</g, "&lt;").replace(/>/g, "&gt;");
	},
	start: function(){
		that = this;
		that._visualize();
		this.polling = setInterval(function(){ 
			document.getElementById(that.containerElement).innerHTML = "";
			that._visualize() 
		}, 3000);
	},
	stop: function(){
		clearInterval(this.polling);		
	},
	_visualize: function(){
		that = this;
		d3.csv(this.serviceEndpoint,function(error, data){

		  data.forEach(function(d) {            
					d["QueryID"] = that.parseQueryName(d["Query"]);
					d["Query"] = that.parseQueryString(d["Query"]);
		      d["Time"] = parseFloat(d["Time"]);
		  });

      var dataNest = d3.nest()
              .key(function(d) {
                  return d["Endpoint"];
              })
              .entries(data);

      var margin = {top: 30, right: 20, bottom: 70, left: 70};
      var width = 700 - margin.left - margin.right;
			var height = 300 - margin.top - margin.bottom;
      var color = d3.scale.category20();
      var legendSpace = width / dataNest.length;

      var x = d3.scale.linear().range([0, width]);
      var y = d3.scale.linear().range([height, 0]);

      var xAxis = d3.svg.axis().scale(x).orient("bottom").ticks(that.queries.length*2).tickFormat(
				function(d){ return that.queries[d] }
			);
      var yAxis = d3.svg.axis().scale(y).orient("left").ticks(5);  

      var pointline = d3.svg.line().x(function(d) {
          return x(d["QueryID"]);
      }).y(function(d) {
          return y(d["Time"]);
      });

      var svg = d3.select("#"+that.containerElement)
              .append("svg")
							.on("mousemove", function(){
								var x0 = Math.floor(x.invert(d3.mouse(this)[0]));
								if(x0!=that.currentQuery){
									that.currentQuery = x0;
									var detail = new Object();
									dataNest.forEach(function(endpoint){
										endpoint["values"].forEach(function(query){
											if(query["QueryID"]===x0){
												if(detail["Queries"]===undefined){
													detail["Queries"] = new Object();
												};
												detail["Queries"][endpoint["key"]]=query["Time"];
												detail["Query"]=query["Query"];
												detail["QueryString"]=query["QueryString"];
											}
										});
									});
									var detailElement = document.getElementById(that.detailElement);
											detailElement.innerHTML = "";
									var detailString = "<table>";
											detailString += "<tr>";
											detailString += "<td style=\"color:black\">Query:</td>";
											detailString += "<td style=\"color:black\">"+detail["Query"]+"</td>";
											detailString += "</tr>";
											detailString += "<tr>";
											detailString += "<td style=\"color:black\">QueryString:</td>";
											detailString += "<td style=\"color:black\"><code>"+unescape(detail["QueryString"])+"</code></td>";
											detailString += "</tr>";
											for (var key in detail["Queries"]) {
													if (detail["Queries"].hasOwnProperty(key)) {
														detailString += "<tr style='color:"+color(key)+"'>";
														detailString += "<td>";
														detailString += key;
														detailString += "</td>";
														detailString += "<td align='right'>";
														detailString += detail["Queries"][key];
														detailString += "ms</td>";
														detailString += "</tr>";
													}
											}
											detailString += "</table>";
											detailElement.innerHTML = detailString;
								}
							})
              .attr("width", width + margin.left + margin.right)
              .attr("height", height + margin.top + margin.bottom)
              .append("g")
              .attr("transform","translate(" + margin.left + "," + margin.top + ")");

      x.domain(d3.extent(data, function(d) {
          return d["QueryID"];
      }));

      y.domain([0, d3.max(data, function(d) {
          return d["Time"];
      })]);



      dataNest.forEach(function(d, i) {
          svg.append("path")
                  .attr("class", "line")
                  .attr("id", "path"+i)
                  .style("stroke", function() {
                      return d.color = color(d.key);
                  })
                  .attr("d", pointline(d.values));
/*
          svg.append("text")
                  .attr("x", (legendSpace / 2) + i * legendSpace)
                  .attr("y", height + (margin.bottom / 2) + 5)
                  .attr("class", "legend")
                  .style("fill", function() {
                      return d.color = color(d.key);
                  })
                  .text(d.key);
*/
      });

      svg.append("g")
			  .attr("id", "xaxis")
              .attr("class", "x axis")
              .attr("transform", "translate(0," + height + ")")
              .style("stroke","black")
              .call(xAxis);

      svg.append("text")
              .attr("class", "axisLabel")
              .attr("transform", "translate(" + (width / 2) + " ," + (height + margin.bottom) + ")")
              .style("text-anchor", "middle")
              .text("Queries");

      svg.append("g")
              .attr("class", "y axis")
              .style("stroke","black")
              .call(yAxis);

      svg.append("text")
              .attr("class", "axisLabel")
              .attr("transform", "rotate(-90)")
              .attr("y", 0 - margin.left)
              .attr("x", 0 - (height / 2))
              .attr("dy", "1em")
              .style("text-anchor", "middle")
              .text("Milliseconds");
		});
	}
};

