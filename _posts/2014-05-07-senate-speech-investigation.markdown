---
title: Senatorial Speech Investigation

excerpt: An analysis of which Unix commands appear together more than random chance would suggest.

location: Cleveland, OH
layout: blog-post

---

Introduction
---

Empty Text

Visualization
---

<style>
.tooltip {
  position: absolute;
  width: 300px;
  height: 28px;
  pointer-events: none;
}

.brush {
    fill: teal;
    stroke: teal;
    fill-opacity: 0.2;
    stroke-opacity: 0.8;
}
</style>

<link rel="stylesheet" href="files/css/theme.cstella.css">

<script type="text/javascript" src="files/ref_data/senate_data.js"></script>
<script src="//cdnjs.cloudflare.com/ajax/libs/d3/3.4.6/d3.min.js"></script>
<script src="https://rawgit.com/square/crossfilter/master/crossfilter.min.js"></script>
<script src="//ajax.googleapis.com/ajax/libs/jquery/1.8.3/jquery.min.js"></script>
<script src="http://mottie.github.io/tablesorter/js/jquery.tablesorter.js"></script>
<script src="http://mottie.github.io/tablesorter/js/jquery.tablesorter.widgets.js"></script>
<script src="http://mottie.github.io/tablesorter/js/widgets/widget-scroller.js"></script>



<script>
$(document).ready(function() 
    { 
      $("#resultTable").tablesorter( { 
                            theme: "cstella" 
                          , widthFixed: true
                          , showProcessing: true
                          , widgets: ['zebra','uitheme', 'scroller']
                          , widgetOptions : {
                            scroller_height : 500,
                            scroller_barWidth : 17,
                            scroller_jumpToHeader: true,
                            scroller_idPrefix : 's_'
                            }
                          }
                          );

     } 
); 
</script>
<div id="chart">
</div>
<div id="table">
    <table id="resultTable" class="tablesorter">
        <thead>
            <tr>
                <th>Speaker</th>
                <th>Topic</th>
                <th>Summary</th>
            </tr>
        </thead>
      <tbody>
      </tbody>
    </table>
</div>
<script>

var width = 600,
    height = 600,
    margin = 40;

var xScale = d3.scale.linear()
               .range([0, width])
  , xValue = function(d) { return d["x"];} 
  , xMap = function(d) { return xScale(xValue(d));};

var yScale = d3.scale.linear()
                     .range([height, 0])
  , yValue = function(d) { return d["y"];} 
  , yMap = function(d) { return yScale(yValue(d));};
// setup fill color
var cValue = function(d) { return d["party"];}



function get_topic(d) {
    var maxVal = 0;
    var maxIdx = -1;
    var t = d["topics"];
    for(var i = 0;i < t.length;++i)
    {
        if(t[i] > maxVal)
        {
            maxVal = t[i];
            maxIdx = i;
        }        
    }
    return topics[maxIdx];
};

function update_table(points) {
    var tableDiv = document.getElementById('table');
    while(tableDiv.firstChild) { 
        tableDiv.removeChild(tableDiv.firstChild);
    }
    // create elements <table> and a <tbody>
    var tbl     = document.createElement("table");
    tbl.id = 'resultTable';
    tbl.class = 'tablesorter';
    var thead = document.createElement("thead");
    var tr = document.createElement("tr");
    {
        var cell = document.createElement("th");
        var cellText = document.createTextNode('Speaker');
        cell.appendChild(cellText);
        tr.appendChild(cell);
    }
        {
        var cell = document.createElement("th");
        var cellText = document.createTextNode('Topic');
        cell.appendChild(cellText);
        tr.appendChild(cell); 
    }
    {
        var cell = document.createElement("th");
        var cellText = document.createTextNode('Summary');
        cell.appendChild(cellText);
        tr.appendChild(cell);
    }

    thead.appendChild(tr);
    tbl.appendChild(thead);
    var tblBody = document.createElement("tbody");
    for(var i = 0;i < points.length;++i)
    {
         // table row creation
         var row = document.createElement("tr");
         {
             var cell = document.createElement("td");
             var txt = '<a href="https://raw.githubusercontent.com/cestella/senate_speech_investigation/master/senate_speeches/' + points[i]['speech'] + '">' + points[i]['politician'] + '</a>';
             cell.innerHTML = txt;
             row.appendChild(cell);
         }
         {
             var d = points[i];
             var cell = document.createElement("td");
             var txt = '';
             var topic = get_topic(d);
             for(var j = 0;j < topic.length;++j)
             {
                txt += topic[j] + ' ';
                if(j > 0 && j % 2 == 0)
                {
                  txt += '<br>';
                }
             }    
             cell.innerHTML = txt;
             cell.appendChild(cellText);
             row.appendChild(cell);
         }
         {
             var cell = document.createElement("td");    
             var txt = '' + points[i]['summary'] + '';
             cell.innerHTML = txt; 
             row.appendChild(cell);
         }

        tblBody.appendChild(row);
    }
    
    tbl.appendChild(tblBody);
    tableDiv.appendChild(tbl);
    $("#resultTable").tablesorter( { 
                            theme: "cstella" 
                          , widthFixed: true
                          , showProcessing: true
                          , widgets: ['zebra','uitheme', 'scroller']
                          , widgetOptions : {
                            scroller_height : 500,
                            scroller_barWidth : 17,
                            scroller_jumpToHeader: true,
                            scroller_idPrefix : 's_'
                            }
                          }
                          );
}
// don't want dots overlapping axis, so add in buffer to data domain 
xScale.domain([d3.min(data, xValue)-1, d3.max(data, xValue)+1]);
yScale.domain([d3.min(data, yValue)-1, d3.max(data, yValue)+1]);

var xAxis = d3.svg.axis()
    .scale(xScale)
    .orient('bottom');

var yAxis = d3.svg.axis()
    .scale(yScale)
    .orient('left');

var brush = d3.svg.brush()
    .x(xScale)
    .y(yScale);
// add the tooltip area to the webpage
var tooltip = d3.select("#chart").append("div")
    .attr("class", "tooltip")
    .style("opacity", 0);

var svg = d3.select('#chart')
    .append('svg')
    .attr('width', width+2*margin)
    .attr('height', height+2*margin)
    .append('g')
    .attr('transform', 'translate('+margin+','+margin+')');

svg.append('g')
    .attr('class', 'x axis')
    .attr('transform', 'translate(0,'+height+')')
    .call(xAxis);

svg.append('g')
    .attr('class', 'y axis')
    .call(yAxis);

 svg.append('g')
    .attr('class', 'brush')
    .call(brush);

var xf = crossfilter(data);
var xDim = xf.dimension(xValue);
var yDim = xf.dimension(yValue);

brush.on('brush', function() {
    var extent = brush.extent(),
        xExtent = [extent[0][0], extent[1][0]],
        yExtent = [extent[0][1], extent[1][1]];
    xDim.filterRange(xExtent);
    yDim.filterRange(yExtent);
    update_table(xDim.top(Infinity));
   // console.log(xDim.top(Infinity));
   // updateDots();
});

function updateDots() {
    var dots = svg.selectAll('.dot')
        .data(xDim.top(Infinity));
    
    dots.enter().append('circle')
        .attr('class', 'dot')
        .attr('r', 3)
        .attr('fill', cValue)
        .on("mouseover", function(d) {
          tooltip.transition()
               .duration(200)
               .style("opacity", .9);
          tooltip.html("" + get_topic(d) + "<br>" + d["politician"]) 
               .style("left", (d3.event.pageX + 5) + "px")
               .style("top", (d3.event.pageY - 28) + "px");
      })
    ;
    
    dots
        .attr('cx', xMap)
        .attr('cy', yMap);
    
    dots.exit().remove();
}

updateDots();

</script>

