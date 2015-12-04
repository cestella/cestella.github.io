---
title: Word2Vec with Non-Textual Data
excerpt: I'm going to describe some of the challenges with understanding data and I'll go into some technical detail of how to borrow some scalable unsupervised learning from natural language processing coupled with a very nice data visualization technique to facilitate understanding the natural organization and arrangement of data.

location: Cleveland, OH
layout: blog-post

---

At least half of the battle of data analysis and data science is understanding your data.

That sounds obvious, but I've seen whole data science projects fail
because not nearly enough time was spent on the exercise of understanding
your data. There are only two real ways to go about doing this:

* Ask an expert
* Ask the data

To have a shot at doing this you really have to do both.

In the course of this blog post, I'm going to describe some of the
challenges with understanding data and I'll go into some technical
detail of how to borrow some scalable unsupervised learning from natural language
processing coupled with a very nice data visualization to facilitate
understanding the natural organization and arrangement of data.

Subject Matter Experts
===

I spend a lot of time with healthcare
data and the obvious subject matter experts are nurses and doctors.
These people are very gracious, very knowledgeable and extremely pressed
for time.  The problem with expert knowledge is that it's surprisingly hard to communicate
effectively sufficient nuance to help the working data scientist
accomplish their goals.  Furthermore, it's extremely time consuming.  
This is made doubly hard when the expert is entirely unclear about the goal. 

The second, perhaps less obvious, challenge is that subject matter
experts knowledge is biased toward that which is already known.  Often
data scientists and analysts are trying to understand the data not as an
ends, but rather as a means to gaining insight.  If you only take into
account received knowledge, then making unexpected insights can be
challenging.  That being said, spending time with subject matter experts is 
a necessary yet insufficient part of data analysis.


Unsupervised Data Understanding
===

To complete the task of understanding your data, I have found that it is
necessary to spend time looking at the data.  One can think of the
entire field of statistics as an exercise in building a mechanism to ask
data pointed questions and get answers that we can trust, often with
caveats.  
The goal is generally to get a sense of how the data is organized or arranged.  
With the unbelievable complexity of
most real data, we are forced to simplify our representations.  The
question is just precisely how to simply that representation to find the
proper balance between simplicity and complexity.  More than that, some
representations of the data offer useful views of the data for certain
purposes and not for others.  

Common simplified representations of data are things like distributions, histograms, and plots.  Of course there are other even more complex ways to represent your data.  Whole [companies](http://www.ayasdi.com/) have been formed around providing a way to gain insight through more complex organizations of the data, taking some of the burden of interpretation from our brain and encoding it in an organization scheme.

Today, I'd like to talk about another approach to data simplification
for event data which provides not just an interesting representation, but also a way to
ask the data certain kinds of useful questions of your data.

Word2Vec
---

One common way to impose order on data that is used by engineers and
mathematicians everywhere is to embed your data in a [vector space](https://en.wikipedia.org/wiki/Vector_space) with a
[metric](https://en.wikipedia.org/wiki/Metric_space) on it.  This gives
us a couple of things.  
This gives us a couple things :
 
* Data now has a distance which can be interpreted as the degree of
  "difference" between the data
* Data can be combined via addition and subtraction operations which can
  be interpreted as combination and separation operations

The issue now is how you impose this structure by embedding your data,
which may not even be numeric, into a vector space.  Thankfully, the
nice people at [Google](http://www.google.com) developed a nice way of
doing this in the domain of natural language text called
[Word2Vec](http://arxiv.org/abs/1310.4546).

I won't go into extravagant detail into the implementation as Radim
Řehůřek did a great job [here](http://rare-technologies.com/making-sense-of-word2vec/).  
The major takeaways, however, is that using the inherrent structure of natural
 language, Word2Vec is able to construct a vector space such that a

* Word similarity can be interpreted as a distance calculation
* The notion of analogies can be interpreted using the addition and
  subtraction operators (e.g. the vector representation of king - male + female is near the vector representation of queen).

This is a surprisingly rich organization of data and one that has proven
very effective in enhancing the accuracy of machine learning models that
deal with natural language.  Perhaps the most surprising part of this is
that the vectorization model does not utilize any of the grammatical
structure of the natural language directly.  It simply analyzes the
words within the sentences and through usage it fits the proper
embedding.  This led me to consider whether other, non-textual data
which has some inherrent structure can also be organized this way with
the same algorithm.

Medical Data
---

Whenever we go to the doctor, a set of events happen:

* Measurements are made (e.g. blood pressure, pulse, height, weight)
* Labs are drawn and ordered (e.g. blood tests)
* Procedures are performed (e.g. an x-ray)
* Diagnoses are made
* Drugs are prescribed

These events happen in a certain _overall_ order but the order varies based on the
patient situation and according to the medical staff's best judgement.
We will call this set of events a **medical encounter** and they happen every day all over the world.

This sequence of events has a similar tone to what we're familiar with
in natural language.  The encounter can be thought of as a sort of
medical sentence.  Each medical event within the encounter can be
thought of as a medical word.  The type of event (lab, procedure,
diagnoses, etc.) can be considered as a sort of part-of-speech.

It remains to determine if this structure can be teased out and encoded
into a vector space model like natural language can be. If so, then we
can ask questions like:

* How similar are two diseases based on how they are treated and
  [comorbidities](https://en.wikipedia.org/wiki/Comorbidity) found in
  the same encounter?
* Can we compose diseases and make them similar to other diseases?  For
  instance, is the vector representation of type 2 diabetes - obesity
  close to type 1 diabetes?

When considering trying this technique out the problem, of course, is getting
access to medical data.  This data is extremely sensitive and is covered
by
[HIPAA](https://en.wikipedia.org/wiki/Health_Insurance_Portability_and_Accountability_Act) here in the United States.
What we need is a good, depersonalized set of medical encounter data.

Thankfully, back in 2012 an electronic medical records system, [Practice
Fusion](http://www.practicefusion.com) released a set of 10,000 depersonalized medical records as
part of a kaggle [competition](https://www.kaggle.com/c/pf2012-diabetes).  This opened up the possibility of actually doing this analysis, albeit on a small subset of the population.

Implementation
---

Since I've been doing a lot with Spark lately at work, I wanted to see
if I could use the Word2Vec implementation built into SparkML to
accomplish this.  Also, frankly, having worked with medical data at some
big hospitals and insurance companies, I am aware that there is a real
scale problem when doing something this complex for millions of medical
encounters and I wanted to ensure that anything I did could scale.

The implementation boiled down into a few steps, which are common to
most projects that I've seen run on Hadoop.  I have created a small
github repo to capture the code collateral used to process the data
[here](https://github.com/cestella/presentations/tree/master/NLP_on_non_textual_data).

* Ingest the Practice Fusion database dumps into Hadoop.
  * Shell script [here](https://github.com/cestella/presentations/blob/master/NLP_on_non_textual_data/src/main/bash/ingest_practice_fusion.sh) 
* Pin up Hive tables for each of the tables, roughly corresponding to a table per medical event.
  * The set of DDL's are [here](https://github.com/cestella/presentations/tree/master/NLP_on_non_textual_data/src/main/ddl/practicefusion)
* Transform this tabular data into a corpus of medical event sentences.
  * The ETL pig scripts are [here](https://github.com/cestella/presentations/tree/master/NLP_on_non_textual_data/src/main/pig/practicefusion)
  * The shell script executing the pig scripts are [here](https://github.com/cestella/presentations/blob/master/NLP_on_non_textual_data/src/main/bash/etl.sh)
* Build the word2vec model with Spark.

You can see from the Jupyter notebook detailing the model building
portion and results [here](https://github.com/cestella/presentations/blob/master/NLP_on_non_textual_data/src/main/ipython/clinical2vec.ipynb) that model building is only a scant few lines:

<pre>
<code>
 from pyspark import SparkContext
 from pyspark.mllib.feature import Word2Vec
 sentences = sc.textFile("practice_fusion/sentences_nlp").map(lambda row: row.split(" "))
 word2vec = Word2Vec()
 word2vec.setSeed(0)
 word2vec.setVectorSize(100)
 model = word2vec.fit(sentences)
</code>
</pre>

#Results

One of the problems with unsupervised models is evaluating how well our
model is describing reality.  For the purpose of this entirely
unscientific analysis, we'll restrict ourselves to just diagnoses and
ask a couple of questions of the model:

* Does the model correctly recover what we currently know based on
  medical research?
* Does the model show us anything that is novel and likely, but unknown
  at present? 

One thing to note before we get started.  This model uses [cosine similarity](https://en.wikipedia.org/wiki/Cosine_similarity) as the score.  This measure of similarity ranges from 0 to 1, with 1 being most similar and 0 being least similar.

Atherosclerosis
---

Also known as heart disease or hardening of the arteries. This disease
is the number one killer of Americans. Our model found the following
similar diseases:

<table id="atherosclerosisTable" class="tablesorter">
        <thead>
            <tr>
                <th>ICD9 Code</th><th>Description</th><th>Score</th>
            </tr>
        </thead>
      <tbody>
<tr><td>v12.71</td><td>Personal history of peptic ulcer disease</td><td>0.930</td></tr>
<tr><td>533.40</td><td>Chronic or unspecified peptic ulcer of unspecified site with hemorrhage, without mention of obstruction</td><td>0.926</td></tr>
<tr><td>153.6</td><td>Malignant neoplasm of ascending colon</td><td>0.910</td></tr>
<tr><td>238.75</td><td>Myelodysplastic syndrome, unspecified</td><td>0.910</td></tr>
<tr><td>389.10</td><td>Sensorineural hearing loss, unspecified</td><td>0.907</td></tr>
<tr><td>428.30</td><td>Diastolic heart failure, unspecified</td><td>0.904</td></tr>
<tr><td>v43.65</td><td>Knee joint replacement</td><td>0.902</td></tr>
      </tbody>
</table>
<br/>

**Peptic Ulcers**

There have been long-standing connections noticed between ulcers and
atherosclerosis. Partiaully due to smokers having a higher than average
incidence of peptic ulcers and atherosclerosis. You can see an [editorial](http://www.ncbi.nlm.nih.gov/pmc/articles/PMC1611891/)
in the British Medical Journal all the way back in the 1970's discussing
this.

**Hearing Loss**

From an [article](http://www.ncbi.nlm.nih.gov/pubmed/23102449) from the Journal of Atherosclerosis in 2012:

 > Sensorineural hearing loss seemed to be associated with vascular
 > endothelial dysfunction and an increased cardiovascular risk

**Knee Joint Replacements**

These procedures are common among those with osteoarthritis and there
has been a solid correlation between osteoarthritis and atherosclerosis
in [the](http://www.ncbi.nlm.nih.gov/pubmed/22563029) [literature](http://www.ncbi.nlm.nih.gov/pmc/articles/PMC3196360/).

Crohn's Disease
---
Crohn's disease is a type of inflammatory bowel disease that is caused
by a combination of environmental, immune and bacterial factors. Let's
see if we can recover some of these connections from the data.

<table id="crohnsTable" class="tablesorter">
        <thead>
            <tr>
                <th>ICD9 Code</th><th>Description</th><th>Score</th>
            </tr>
        </thead>
      <tbody>
<tr><td>274.03</td><td>Chronic gouty arthropathy with tophus (tophi)</td><td>0.870</td></tr>
<tr><td>522.5</td><td>Periapical abscess without sinus</td><td>0.869</td></tr>
<tr><td>579.3</td><td>Other and unspecified postsurgical nonabsorption</td><td>0.863</td></tr>
<tr><td>135</td><td>Sarcoidosis</td><td>0.859</td></tr>
<tr><td>112.3</td><td>Candidiasis of skin and nails</td><td>0.855</td></tr>
<tr><td>v16.42</td><td>Family history of malignant neoplasm of prostate</td><td>0.853</td></tr>
      </tbody>
</table>
<br/>

**Arthritis**

From the [Crohn's and Colitis Foundation of
America](http://www.ccfa.org/resources/arthritis.html):

>Arthritis, or inflammation of the joints, is the most common extraintestinal complication of IBD. It may affect as many as 25% of people with Crohn’s disease or ulcerative colitis. Although arthritis is typically associated with advancing age, in IBD it often strikes the youngest patients.

**Dental Abscesses**

While not much medical literature exists with a specific link to dental
abscesses and Crohn's (there are general oral issues noticed
[here](http://www.ncbi.nlm.nih.gov/pmc/articles/PMC1410927/)), you
do see lengthy discussions on the Crohn's [forums](http://www.crohnsforum.com/showthread.php?t=37075) about abscesses being a
common occurance with Crohn's.

**Yeast Infections**

Candidiasis of skin and nails is a form of yeast infection on the skin.  From the journal "Critical Review of Microbiology" [here](http://www.ncbi.nlm.nih.gov/pubmed/23855357).

>It is widely accepted that Candidia could result from an inappropriate
>inflammatory response to intestinal microorganisms in a genetically
>susceptible host. Most studies to date have concerned the involvement of
>bacteria in disease progression. In addition to bacteria, there appears
>to be a possible link between the commensal yeast Candida albicans and
>disease development.

Visualization
---

For further investigation, I have used [t-distributed stochastic neighbor embedding](https://en.wikipedia.org/wiki/T-distributed_stochastic_neighbor_embedding)
to embed the 100-dimensional vector space into 2 dimensions.  This
embedding should retain the general connections within the data, so you
can look at similar diagnoses, drugs and allergies.

* You can choose to look at all types, just diagnoses or just drugs.
* Highlight in the canvas below and drag around.  The points that you've
  selected will show up in the table below along with a description in
plain text.

Please play around with this data and let me know what you find!

<style>
.tooltip {
  position: absolute;
  width: 300px;
  height: auto;
  pointer-events: none;
  border: 1px solid #000;
  background-color: #FFF;
  border-radius: 5px;
  padding:10px;
}

.brush {
    fill: teal;
    stroke: teal;
    fill-opacity: 0.2;
    stroke-opacity: 0.8;
}
</style>

<link rel="stylesheet" href="files/css/theme.cstella.css">

<script type="text/javascript"
src="files/ref_data/word_vectors_rx.js"></script>
<script type="text/javascript"
src="files/ref_data/word_vectors_dx.js"></script>
<script type="text/javascript"
src="files/ref_data/word_vectors_all.js"></script>

<script src="files/js/d3.min.js"></script>
<script src="files/js/crossfilter.min.js"></script>
<script src="files/js/jquery.min.js"></script>
<script src="files/js/jquery.tablesorter.js"></script>
<script src="files/js/jquery.tablesorter.widgets.js"></script>
<script src="files/js/widget-scroller.js"></script>



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
      $("#atherosclerosisTable").tablesorter( { 
                            theme: "cstella" 
                          , widthFixed: true 
                          , showProcessing: true
                          , widgets: ['zebra','uitheme', 'scroller']
                          , widgetOptions : {
                            scroller_height : 300,
                            scroller_barWidth : 17,
                            scroller_jumpToHeader: true,
                            scroller_idPrefix : 's_'
                            }
                          }
                          );
    $("#crohnsTable").tablesorter( { 
                            theme: "cstella" 
                          , widthFixed: true 
                          , showProcessing: true
                          , widgets: ['zebra','uitheme', 'scroller']
                          , widgetOptions : {
                            scroller_height : 300,
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
<table style="border-spacing: 5px">
  <tr>
    <td ><a href="#" onclick="display_plot(data_all);updateDots();return false;">All</a></td>
    <td style="border:solid 2px black" width="50px"></td>
  </tr>  
  <tr>
    <td >Provider Specialty</td> 
    <td style="border:solid 2px black" width="50px" bgcolor="black"></td>
  </tr>  

  <tr>
    <td ><a href="#" onclick="display_plot(data_dx);updateDots();return false;">Diagnoses</a></td> 
    <td style="border:solid 2px black" width="50px" bgcolor="red"></td>
  </tr>  
  <tr>
    <td><a href="#" onclick="display_plot(data_rx);updateDots();return false;">Drugs</a></td>
    <td  style="border:solid 2px black" width="50px" bgcolor="blue"/>
  </tr>  
  <tr>
    <td >Allergies</td> 
    <td  style="border:solid 2px black" width="50px" bgcolor="orange"/>
  </tr>  

</table>

<div id="table">
    <table id="resultTable" class="tablesorter">
        <thead>
            <tr>
                <th>Type</th>
                <th>Name</th>
                <th>Description</th>
            </tr>
        </thead>
      <tbody>
        <tr><td colspan="3"><center><b>Highlight some points above for this summary to be filled in.</b></center></td></tr>
      </tbody>
    </table>
</div>
<script>

var width = 900,
    height = 900,
    margin = 40;



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
        var cellText = document.createTextNode('Type');
        cell.appendChild(cellText);
        tr.appendChild(cell);
    }
        {
        var cell = document.createElement("th");
        var cellText = document.createTextNode('Name');
        cell.appendChild(cellText);
        tr.appendChild(cell); 
    }
    {
        var cell = document.createElement("th");
        var cellText = document.createTextNode('Description');
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
         var d = points[i];
         {
             var cell = document.createElement("td");
             var txt = d['type'];
             if(txt == 'dx') {
                txt = 'Diagnosis';
             }
             else if(txt == 'rx') {
                txt = 'Drugs';
             }
             else if(txt == 'provider specialty') {
                txt = 'Provider Specialty';
             }
             cell.innerHTML = txt;
             row.appendChild(cell);
         }
         {
             var cell = document.createElement("td");
             var txt = '' + d['name'] + '';
             cell.innerHTML = txt;
             row.appendChild(cell);
         }
         {
             var cell = document.createElement("td");    
             var txt = '' + points[i]['description'] + '';
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

var xScale = d3.scale.linear()
               .range([0, width])
  , xValue = function(d) { return d["vec"][0];} 
  , xMap = function(d) { return xScale(xValue(d));};

var yScale = d3.scale.linear()
                     .range([height, 0])
  , yValue = function(d) { return d["vec"][1];} 
  , yMap = function(d) { return yScale(yValue(d));};
// setup fill color
var cValue = function(d) { 
  if(d["type"] == 'dx') {
    return 'red';
  }
  else if(d["type"] == 'rx') {
    return 'blue';
  }
  else if(d["type"] == 'provider specialty') {
    return 'black';
  }
  else
  {
    return 'orange';
  }
};
var xf;
var xDim;
var yDim;
var svg;
var tooltip;
function display_plot(data) {
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
  d3.select('#chart_svg').remove(); 
  d3.select('#chart_tooltip').remove(); 
   tooltip = d3.select("#chart").append("div")
      .attr("class", "tooltip")
      .attr("id", "chart_tooltip")
      .style("opacity", 0);
  svg = d3.select('#chart')
      .append('svg')
      .attr("id", "chart_svg")
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
  
  xf = crossfilter(data);
  xDim = xf.dimension(xValue);
  yDim = xf.dimension(yValue);
  
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
}

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
          tooltip.html("" + d['name']  + "<br>@ (" + d['vec'][0].toFixed(2) + ',' + d['vec'][1].toFixed(2) + ')') 
               .style("left", (d3.event.pageX + 10) + "px")
               .style("top", (d3.event.pageY - 10) + "px");
      })
    ;
    
    dots
        .attr('cx', xMap)
        .attr('cy', yMap);
    
    dots.exit().remove();
}

display_plot(data_all);
updateDots();

</script>


