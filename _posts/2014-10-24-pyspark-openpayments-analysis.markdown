---
title: Data Science and Hadoop&#58; Impressions and Examples

excerpt: A discussion of the challenges of doing Data Science projects with Hadoop.
location: Cleveland, OH
layout: blog-post

---

A somewhat regular part of my job lately is discussing with people how
exactly one might go about doing data science on Hadoop.  It's really a
very interesting subject and one about which almost everyone even cursorily
associated with ``Big Data'' has an opinion. Remarks are
made, emails written, Powerpoint decks created; it's a busy day, for
sure.  


People cannot be blamed for being concerned since
according to [Jeff Kelly](http://www.informationweek.com/big-data/big-data-analytics/3-roadblocks-to-big-data-roi/d/d-id/1111593?)
, a Wikibon analyst, the ROI of these big data projects does not match
expectations:

>In the long term, they expect 3 to 4 dollar return on investment for every
>dollar. But based on our analysis, the average company right now is
>getting a return of about 55 cents on the dollar.  

That's pretty concerning for those of us hoping for Hadoop to [cross the chasm](http://en.wikipedia.org/wiki/Crossing_the_Chasm) soon.  As one might imagine, there's been quite a bit of hand wringing about the problem.  I don't take such a dim view of it, though.  It's a matter of maturity and I'll give some of my impressions shortly on why it may be hard to fulfill the data science portion of the ROI currently.

Data Science Challenges
===

One benefit from my vantage point within the consulting wing of a Hadoop
[distribution](http://www.hortonworks.com) is that I get to see quite a
few Hadoop projects.  Being that I'm part of the Data Science team, I
get to have a decidedly Data Science oriented view of the Hadoop world.
Furthermore, I get to see them in both startups as well as big
enterprises.  Even better, living in and working with
organizations from a [fly-over state](http://en.wikipedia.org/wiki/Flyover_country), I have a decidedly non-Silicon Valley perspective.

From this position, it's not hard to see making the leap from 
owning a cluster to gaining insight from your data can be a daunting task.  I'll
just list a few challenges that I've noticed in my travels:

* Data has inertia
* Hadoop is still maturing as a platform
* Choices can be paralyzing

The first is an organizational challenge, the second a 
technical/product challenges and the final is a challenge of human
nature.

Data has Inertia
---

One of the competitive advantages of Hadoop is that inexpensive, commodity
hardware and a powerful distributed computing environment makes Hadoop a
pretty nice, cozy place for your data.  This all looks great on paper
and in architecture slides.  The challenge, however, is actually getting
the data to the platform.

Turns out moving data can be a tricky prospect.  Much ink and
bits have been spilled discussing the technical approaches and
challenges to collecting
your data into a data lake.  I make you suffer through yet another
discussion of the finer points between [sqoop](http://sqoop.apache.org), [flume](http://flume.apache.org), etc.  The technical challenges are almost never the long poles in the tent.

Rather, what I have witnessed is that getting that data to start moving
can be arduous and require political capital.  I have noticed that there 
is a tendency to treat those who come to you asking for data with a fair
amount of skepticism.

However, once data channels open up, data has a tendency to flow
more and more smoothly.  This is why most of the successful projects
that I've been involved in have the following attributes:

* A sponsor with sufficient political power and the willingness to use it to get the data required to succeed
* An iterative attitude so that the time to value is minimal

These attributes are not specific to data science projects.  Rather, the
same principal applies to all projects that require an abundance of
data.  No data-oriented project can survive if starved of data and
almost all Hadoop projects are data-oriented.

Hadoop is Still Maturing as a Platform
---

When I was young, I liked to climb trees.  Growing up in rural Louisiana, I had plenty of opportunities on this front.  As such, I got fairly good at picking out a good climbing tree.  There is a non-zero intersection of trees which are good for climbing and trees which are pretty to look at or have some satisfying structural characteristics.

Often, however, the properties did not coexist in the same tree.  Climbing trees were best if there were relatively low, thick branches with good spacing.  Trees which were nice to look at were much more manicured with delicate branches and a certain symmetry. 
 
Platforms have the same characteristics, I think.  You have platforms that are very finely manicured with a focus on internal consistency and contained borders.  This yields a good experience for those who use the system as the originators intended.  These systems are pretty to look at, to be sure.

Ironically enough, I've always liked the sprawling systems with an emphasis on many integration points.  They give the feeling that they are reaching out to you.  That act of reaching out is the act of engaging.  Hadoop is transitioning quickly from a finely manicured topiary sculpture to a fantastic climbing tree. 

It started out very self contained and internally consistently.  If you used Java, you were going to have a good time (sometimes ;-).  While you *could* use pipes and streaming to hook up your C++ code or perl scripts, you weren't going to have nearly as good of a time.  Equivalently, on the algorithm front, if you could express what you wanted to do in MapReduce then the world was straightforward.

<img src="files/ref_data/open_payments_files/Topiaryelephant.jpg"
     style="width:650px"
/>
<sub>[Topiary Elephant](http://en.wikipedia.org/wiki/Topiary#mediaviewer/File:Topiaryelephant.jpg) in Bang Pa In Palace, Thailand. CC BY-SA 3.0</sub>

Now, as Hadoop matures, we see branches to other platforms growing
and branches to other distributed computing paradigms growing.  On the
technical side, we can now write pure non-JVM UDF's in Pig, Spark has
proper first-class bindings for Python, you can even write yarn apps in
languages other than those which run on the JVM.  Much of this is thanks
to the new architecture in 2.0, but more than just a technical direction, 
it's the realization by the community that we need more choices.

That being said, it's early days and we're not that far down the path to
the new way of thinking.  This will be solved with time and maturity.

Analysis Paralysis
---

Data science isn't a new thing.  I know, this is a brave statement and a
deep conclusion.  Forgiving its obviousness and pith, I actually mean
that most organizations are already doing and have been doing for years one of the core *things* people talk about as data science: developing insights on their data.

I walk into organizations and I talk with the data analysts and I ask
them about how they do their job on a day-to-day basis.  Most of them
talk to me about things somewhere in the range logistic regression in SAS and doing very complex SQL in a data warehouse.  I ask them what their pains are and almost to a person, they always say something like the following:

* Copies of the data are expensive with my limited quota
* Getting the data I want onto the system I want to analyze it on takes more than 24 hours.

The data scientists aren't clamoring for the things that you see so
often touted as the benefits of ``Big Data'':

* Unstructured data
* Running your models on a petabyte of data
* Running sexy new algorithms at massive scale

Does this mean that those things aren't really needed?  If so, our job
is easy, all we have to do is recreate SQL on Hadoop and convince
organizations to put their data there.  That solves big portions of the
top complaints above.

The answer is obviously that the current gripes do not remove 
the need for more data, differently structure data, other techniques in
the data science toolbag.  So, why aren't the data analysts that I talk
to chomping at the bit for them?

One reason, I think, is that with increasingly complex data comes
increasing complexities in dealing/processing that data.  Furthermore,
in structured data, the act of extraction/transformation/loading of the
data was not a data scientist activity.  It's possible that, given more
complicated data, just extracting features from it might require more
arduous programming than analysts are used to.  A good example of this
is within the realm of natural language processing projects.

Also, ``Big Data'' data science isn't as convenient as small-data data science.  Contrast the ease of using [Mahout](http://mahout.apache.org) or Spark's [MLLib](http://spark.apache.org/docs/1.1.0/mllib-guide.html) with python's [scikit-learn](http://scikit-learn.org/stable/), [R](http://www.r-project.org/) or [SAS](http://www.sas.com/en_us/home.html).  It's not a contest; it's easier and quicker to deal with a few megabytes of data.  Since there is value in dealing with much more data, we have to eat the elephant, but it can be daunting without guidance and examples are few and far between. 

Ultimately I think we focus so heavily on new and novel techniques, the game changing paradigm shifts (with our tongues placed firmly in our cheeks sometimes) without discussing the journey to getting there.  If we constantly look across the chasm without looking at the bridge beneath our feet, we run the risk of falling into the drink.

This brings me to why I wanted to create this post.  I intend to show a
worked example of how you do what I've seen as day-to-day work as data analysts along with  some natural extension points that show how to use the system to do some possibly more interesting analysis.  Namely :

* Understand some fundamental characteristics of the data to be analyzed
* Generate reporting/images to communicate those characteristics to other people
* Mine the data for likely incorrect or interesting data points that break with the characteristics found above.

Example Analysis
===

I think it would be useful to start to fill in the gap of worked examples analyzing data with Hadoop.  I'm targeting some recently opened data from the Center of Medicare and Medicaid detailing the financial relationships between physicians, hospitals, etc and medical manufacturers.

I'm using, primarily, Spark's Python bindings with data on Hadoop to do
the analysis.  The indidual phases have been split into 4 parts:

* [Data Overview and Preprocessing](pyspark-openpayments-analysis-part-2.html)
* [Basic Structural Analysis](pyspark-openpayments-analysis-part-3.html)
* [Outlier Analysis](pyspark-openpayments-analysis-part-4.html)
* [Benford's Law Analysis](pyspark-openpayments-analysis-part-5.html)

Conclusions
===

This is a Dull Blade Exercise
---

I have been very careful to not draw conclusions or explicitly look for
fraud.  This is intended to be a demonstration of technique and I cannot
verify that this dataset isn't rife with bad or misclassified data.  As
such, I intended to demonstrate some of the basic and slightly more
advanced analysis techniques that are open to you using the Hadoop
platform.

PySpark + Hadoop as a Platform
---

If you have the interest/ability to be comfortable in a Python
environment, I think that for data investigation and ad hoc reporting,
interacting with Hadoop via IPython Notebook and the Spark Python
bindings is a fantastic experience.

Interacting between SQL and more complex, fundamental analysis is
seamless.  It all communicates in terms of RDDs for maximum ease of
composition.  I could have used any of the rest of the Spark stack, such
as MLLib or GraphX.

Having all of this running on Hadoop, allowing me to do ETL and work in
the *other* parts of the ecosystem such as Pig, Hive, etc. is an
extremely compelling aspect as well.  Ultimately, we're approaching a
very cost effective and well thought out system for analyzing data.

It's not all roses, however.  When something goes wrong, it can be challenging to trace back the problem from the mix of Java/Scala and Python stack trace that is returned to you.

There can be some IT challenges as well.  If you use a package in python in a RDD operation, you must have the package installed on the cluster.  This may pose some challenges as many different people are going to need differing versions of dependencies.  Traditionally this is handled through things like virtualenv, but executing a function within the context of a virtualenv isn't supported and, even if it were, managing a virtualenv across a set of data nodes can be a challenge in itself.

If you would prefer to see the raw IPython Notebook, you can find it
hosted on [nbviewer.ipython.org](http://nbviewer.ipython.org/url/blog.caseystella.com/files/ref_data/open_payments_files/open_payments.ipynb).
