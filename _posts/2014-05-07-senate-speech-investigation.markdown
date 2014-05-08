---
title: Making Sense of Political Texts with NLP

excerpt: Clustering senatorial speeches from 2008 by topic using t-stochastic neighbor embedding and latent dirichlet allocation.

location: Cleveland, OH
layout: blog-post

---

One of the more interesting that I think can be done with computers is analyze text.  It's one of those
things that, while not true understanding, lets you suspend disbelief and imagine your computer can
understand this non-context-free mishmash of syntax and semantics that we call natural language.  Because
of this, I've always been fascinated by the discipline of natural language processing.

In particular, analyzing political text, some of the most contextual, sentiment-filled text in existence, seems
like a great goal.  This difficulty largely prevents analytical insights from coming easy.  I think, however, that
throwing heavy machinery at such a hard nut will never bear much fruit.  Rather, I think that the heavy machinery
would be better suited to doing what computers are adept at: organizing and visualizing the data in a way which
better allows a human to ask questions of it.

So, to that end, let's take senatorial speeches and press releases, learn natural groupings based on their content and see
if there are topics that are inherrently discussed more by members of one party versus another.

The Data
---
I ran across a great little project by [John Myles White](https://github.com/johnmyleswhite) computing [Ideal Point Analysis](http://jackman.stanford.edu/blog/?p=2084).
During the course of this project, he gathered around 1400 [speeches and press releases](https://github.com/johnmyleswhite/senate_analyses/tree/master/raw_speeches_and_press_releases) from Senators from 2008.

Topic Models
---

I began to think, "What interesting questions can I ask of this data?"  One of my favorite bits of unsupervised learning in natural language 
processing is topic modeling.  This, in short, is generating from the data a set of topics which the data covers.  Further, it gives you a 
distribution of each topic for each document and the ability to generate this topic distribution from arbitrary (unseen) documents.  I like to 
think of this as, given a set of newspapers, determine the sections (e.g. sports, business, comics, etc.).  For our purposes, we will represent 
to us what each of these topics are by a set of keywords which best represent the topic.

From [David Blei](http://www.cs.princeton.edu/~blei/topicmodeling.html):

>Topic models are a suite of algorithms that uncover the hidden thematic structure in document collections. These algorithms help us develop new ways to search, browse and summarize large archives of texts.

There has been much written on topic modeling and, in particular, the favored approach to doing it (latent dirichlet allocation).  Rather than give yet another description, I'll [link](http://www.cs.princeton.edu/~blei/papers/Blei2012.pdf) to my favorite with accompanying [video](http://www.youtube.com/watch?v=7BMsuyBPx90).

So, given this ability to generate topics and get a distribution for each of the speeches or press releases, what can I do with it?  Well, one interesting question is whether there are certain topics that are inherrently partisan in nature.  Which I mean, are there clusters of documents mainly dominated by senators of a certain party.

But, before we can investigate that, we need to visualize these documents by their topics.  One way to do this is to just project down to the dominant topic, which is to say, for each document look at just the topic that is strongest.  This way we can group the documents by topic.  This has the benefit
of being easily represented, but it loses the granularity of a document being a mixture of topics.

Instead, what we'd *like* to do is look at how the documents cluster by their topics.  The issue with this is that we could have 50, 100, or even 300 topics
which means visualizing a 300-dimensional space.  That is clearly out of the question.  What we'd need is some way to transform those high-dimensional points (representing the full distribution of topics) down into a space that we *can* visualize in such a way that the clustering is preserved.  Or, as a mathematician would say, we need to embed a high-dimensional surface into $R^2$ or $R^3$ in a way which (largely) preserves a distance metric.

Embedding
---

There has been quite a lot of discussion, research and high-powered mathematical thinking given to ways to embed high-dimensional space into lower dimensional spaces.  One of the most interesting recent results from this space is [t-Distributed Stochastic Neighbor Embedding](http://homepage.tudelft.nl/19j49/t-SNE.html) by Laurens van der Maaten and the prolific [Geoffrey Hinton](http://www.cs.toronto.edu/~hinton/) of [Deep Learning](http://deeplearning.net/) fame.  In fact, in lieu of a detailed explanation of t-SNE, I'm going to defer to Dr. van der Maaten's fantastic [tech talk at google](https://www.youtube.com/watch?v=RJVL80Gg3lA) about his approach.  

I will, however, give you a taste of the high level attributes of this embedding:

* It preserves "local structure" at the possible expense of "global structure"
* It works particularly well on real-world datasets

When I say "local structure", I mean that points that are nearby in the high dimensional space are likely to continue to be nearby in the low-dimensional space, whereas the individual clusters in the high dimensional space might be farther away, relatively, when projected into the lower dimensional space.  This is ideal for preserving the notion of clusters, which is all we care about.

Visualization
---

As I have set up in earlier sections, I generated a 50-topic model from the 1400 speeches and press releases from senators in 2008.  I then embedded each
document into 2-dimensions using t-distributed stochastic neighbor embedding so that we can better visualize the clusters.  
Some notes about the following visualization:

* The points in the following visualization are colored based on the political party of the author/speaker.  
* Hovering over points will create a tooltip under the point noting the author/speaker and a 5-word description of the topic
* Selecting a set of points (by dragging the mouse and creating a box which can be placed over the points) will fill in the table below with a link to the original text, the author, and a 3-sentence summary of the most likely important sentences from the document.

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
                            scroller_height : 200,
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
        <tr><td colspan="3"><center><b>Highlight some points above for this summary to be filled in.</b></center></td></tr>
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

Analysis
---

I've been playing with this visualization for a bit and here are a few interesting points that I see:

* At cluster near (40, -75), you see an interesting cluster about social justice, women's rights, etc. with the discussion dominated by the democrats
* At cluster near (20, 80), you see a bipartisan senators discussing the Russian START treaty.
* At cluster near (-42, 23), you see a largely republican cluster discussing spending and the national debt.
* At cluster near (79, 35), you see a fairly mixed discussion from gulf-coast senators (largely) about the BP oil spill.  This is interesting because it's an local issue (or at least localized issue) that brought together the two parties.

There are plenty more, but generally, the impressions that I got for this organization/visualization technique were on the whole favorable:

* Clusters were pretty clear and relatively accurate
* Investigating based on looking at clumps of documents in the embedded space helped bring out global structure that would have been hard to evaluate going document by document
* Some topics are partisan and some topics are not

There were some caveats, though, that I'd like to mention:

* Figuring out how many topics to use to model a complex corpus of documents is largely trial and error from what I can tell.  Too few and you end up with topics merging together.
* In general, topic modeling can only take you so far.  There ARE some errors and understanding what the topic *is* by a set of keywords can be challenging.
* Like any analysis of strongly contextual documents, you have to be vigilent to not read too much into the structure.  Sometimes I found myself seeing what I expected to see in those clusters.

On the whole, it was an entertaining and fascinating exercise.  I'm definitely going to be trying this out for other sets of documents.
All of the code and data used to create this is hosted on github and I urge you all to pull it down and try it out!

The Code
---
You can find all of the code and data at the following [github repo](https://github.com/cestella/senate_speech_investigation).

Some notes about the code:

* The t-SNE implementation is forked from the simple Python implementation located [here](http://homepage.tudelft.nl/19j49/t-SNE_files/tsne_python.zip).  The only modification is that I do not proactively PCA the data since it's already pretty low-dimensional (50-dimensions isn't THAT high).
* I use [Drake](https://github.com/Factual/drake) to do the data flow generation of the data.  The output of the flow is a file containing JSON representation of the embedded data and all of the metadata associated (e.g. summarization, author, etc.).
* The topic modeling work was done using the amazing [Mallet](http://mallet.cs.umass.edu/) library, which is great and really easy to use.
* Summarization is done using Latent Semantic Analysis via the [sumy](https://pypi.python.org/pypi/sumy) library.
* Visualization is using the amazing visualization library [d3.js](http://d3js.org/).


Future Work
---

I'd be very interested to see if the t-sne algorithm can be implemented or parallelized via Spark which would open it up to much larger datasets.  As it stands, the algorithm (in naive form) is $O(n^2)$, but there is a variant called [Barnes-Hut SNE](http://arxiv.org/abs/1301.3342) ( L.J.P. van der Maaten. Barnes-Hut-SNE. In Proceedings of the International Conference on Learning Representations, 2013) which is $O(n\log(n))$.

Also, I'm interested in visualizing other texts.  I think this is an interesting and fascinating way to explore a corpus of data.  I'll leave the rest of the ideas for future blog posts.  You can expect one describing the challenges of a spark implementation soon, I expect.
