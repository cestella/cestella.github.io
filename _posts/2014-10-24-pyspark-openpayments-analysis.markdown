---
title: Spark, Python, Data Science and Physician Payment Data

excerpt: Using the Python bindings to Apache Spark and some open data to do some rudimentary analysis of the relationship between physicians and medical manufacturers.

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
 
The Data: Doctors (probably not) Behaving Badly
===

I've been focused lately in the medical industry.  It's a great industry
with fascinating data, but it can be very hard to talk about it as the
data is necessarily very locked down due to patient privacy.  Doing medical data analysis in the open is almost impossible.  However, there are some exceptions to this as of late. 

Open Payments
---
 
As part of the Social Security Act, the Center for Medicare and Medicaid Services has begun to publish [data](http://www.cms.gov/OpenPayments) detailing the relationship between physicians and medical institutions.  This [data](http://www.cms.gov/OpenPayments/Explore-the-Data/Dataset-Downloads.html) is in the open as it is a public good to be fully aware of physician and organizational financial connections.

Ultimately, this dataset isn't very large, it's a single database of
$2,626,674$ rows.  In fact, I could have done all this analysis quicker and on my laptop.  So, why would I do this?  Well, hopefully the techniques here scale to much larger datasets.  Also, some of the analysis can be complex or involved once we get past the basic understanding of the data.  Fundamentally, I'm trying to demonstrate how to get non-obvious analytical work done on the platform.

This is not my attempt to find fraud in this dataset, but rather to show 
how some interesting anomalies of the data show up when we scratch the
surface.  Further, I have redacted the physician names from the output 
and replaced them with physician IDs from the dataset.

The Platform: PySpark + IPython = $\heartsuit$
---

I'm in the mode of tinkering with data, trying to find patterns or
insights, so I generally choose a language/ecosystem with good support
for that kind of thing.  This generally comes down to R or Python. I chose to do this analysis in Python for a few reasons.

* There is quite a bit of data science tooling around Python
* IPython Notebook gives a great visual interface for tinkering with data
* Spark has more robust bindings for Python than R

I have tried to prefer working and commented code here.  I
transitioned the IPython notebook directly into this blog post.

<link rel="stylesheet" href="files/css/theme.cstella.css">
<script src="//ajax.googleapis.com/ajax/libs/jquery/1.8.3/jquery.min.js"></script>
<script src="//cdn.jsdelivr.net/tablesorter/2.15.13/js/jquery.tablesorter.min.js"></script>
<script>
$(document).ready(function() 
    { 
      for (i = 1;i <= 11;++i) {
        $("#resultTable"+i).tablesorter(
                          { theme: "cstella" }
                          ); 
      }
    } 
); 
</script>

Preliminaries and Preprocessing
---

Let's start with some preliminary imports and helper functions.

{% highlight python %}

import random as random
import re
import locale
import itertools

import sympy
from sympy import *
import pandas
from pandas import DataFrame
import math
import scipy.stats as stats
import numpy as np
from sympy import latex

import matplotlib
from matplotlib import pyplot as plt

from pyspark.sql import SQLContext 
from pyspark import Row

import seaborn as sns
from IPython.core.display import HTML

#initialize some things for the IPython session
init_printing()
locale.setlocale(locale.LC_ALL, 'en_US')

#Generate a bar chart with data, title, y-label, x-label and whether
#the chart should be bar scale.
def bar_chart(label_to_count, title, y_label, x_label,log):
    OX = [x[0] for x in label_to_count]
    OY = [y[1] for y in label_to_count]
    fig = plt.figure()
    fig.suptitle(title, size=14)
    ax = plt.subplot(111)
    width = .35
    ind = np.arange(len(OY))
    rects = ax.bar(ind, OY, alpha=0.35, color='b', label=y_label)
    for ii,rect in enumerate(rects):
        height = rect.get_height()
        plt.text(rect.get_x()+rect.get_width()/2., 1.02*height, '%.2fM'% (OY[ii]),
                ha='center', va='bottom')
    ax.legend()
    ax.grid(True)
    ax.set_xticks(np.arange(len(OX)) + width)
    ax.set_xticklabels(OX)
    ax.set_ylabel(y_label)
    
    fig.autofmt_xdate()

#Take a 2D array of data and create a dataframe to display
#the data in tabular form
def print_table(column_labels, row_labels, contents):
    tmp = [[t[0]] + t[1] for t in zip(row_labels, contents)]
    df = DataFrame(tmp, columns=column_labels)
    pandas.set_option('display.max_colwidth', 100)
    display(HTML(df.to_html()))

#Truncate long lines on word boundaries
def truncate(line, length=40):
    if(len(line) > length):
        
        output_line = ""
        for token in line.split(' '):
            next = output_line + " " + token
            if len(next ) >= length:
                return next + "..."
            else:
                if len(output_line) == 0:
                    output_line = token
                else:
                    output_line = next
    else:
        return line
{% endhighlight %}

Before processing this data, I prepped the data from it's original
quoted CSV into a ctrl-A separated file without quotes using Apache Pig
and the magnificent CSVExcelStorage loader to parse quoted CSV files.

A pre-processing step in Pig is a common part of any data
science/analysis pipeline in Hadoop.

    register '/usr/hdp/current/pig/lib/piggybank.jar'
    
    define Loader org.apache.pig.piggybank.storage.CSVExcelStorage();
    
    DATA = load 'open_payments/general/pre' using Loader as (
      General_Transaction_ID:chararray,
      Program_Year:chararray,
      Payment_Publication_Date:chararray,
      Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name:chararray,
      Covered_Recipient_Type:chararray,
      Teaching_Hospital_ID:chararray,
      Teaching_Hospital_Name:chararray,
      Physician_Profile_ID:chararray,
      Physician_First_Name:chararray,
      Physician_Middle_Name:chararray,
      Physician_Last_Name:chararray,
      Physician_Name_Suffix:chararray,
      Recipient_Primary_Business_Street_Address_Line1:chararray,
      Recipient_Primary_Business_Street_Address_Line2:chararray,
      Recipient_City:chararray,
      Recipient_State:chararray,
      Recipient_Zip_Code:chararray,
      Recipient_Country:chararray,
      Recipient_Province:chararray,
      Recipient_Postal_Code:chararray,
      Physician_Primary_Type:chararray,
      Physician_Specialty:chararray,
      Physician_License_State_code1:chararray,
      Physician_License_State_code2:chararray,
      Physician_License_State_code3:chararray,
      Physician_License_State_code4:chararray,
      Physician_License_State_code5:chararray,
      Product_Indicator:chararray,
      Name_of_Associated_Covered_Drug_or_Biological1:chararray,
      Name_of_Associated_Covered_Drug_or_Biological2:chararray,
      Name_of_Associated_Covered_Drug_or_Biological3:chararray,
      Name_of_Associated_Covered_Drug_or_Biological4:chararray,
      Name_of_Associated_Covered_Drug_or_Biological5:chararray,
      NDC_of_Associated_Covered_Drug_or_Biological1:chararray,
      NDC_of_Associated_Covered_Drug_or_Biological2:chararray,
      NDC_of_Associated_Covered_Drug_or_Biological3:chararray,
      NDC_of_Associated_Covered_Drug_or_Biological4:chararray,
      NDC_of_Associated_Covered_Drug_or_Biological5:chararray,
      Name_of_Associated_Covered_Device_or_Medical_Supply1:chararray,
      Name_of_Associated_Covered_Device_or_Medical_Supply2:chararray,
      Name_of_Associated_Covered_Device_or_Medical_Supply3:chararray,
      Name_of_Associated_Covered_Device_or_Medical_Supply4:chararray,
      Name_of_Associated_Covered_Device_or_Medical_Supply5:chararray,
      Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name:chararray,
      Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID:chararray,
      Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State:chararray,
      Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country:chararray,
      Dispute_Status_for_Publication:chararray,
      Total_Amount_of_Payment_USDollars:chararray,
      Date_of_Payment:chararray,
      Number_of_Payments_Included_in_Total_Amount:int,
      Form_of_Payment_or_Transfer_of_Value:chararray,
      Nature_of_Payment_or_Transfer_of_Value:chararray,
      City_of_Travel:chararray,
      State_of_Travel:chararray,
      Country_of_Travel:chararray,
      Physician_Ownership_Indicator:chararray,
      Third_Party_Payment_Recipient_Indicator:chararray,
      Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value:chararray,
      Charity_Indicator:chararray,
      Third_Party_Equals_Covered_Recipient_Indicator:chararray,
      Contextual_Information:chararray,
      Delay_in_Publication_of_General_Payment_Indicator:chararray
    );
    rmf open_payments/general/post
    store DATA into 'open_payments/general/post' using PigStorage('\u0001');



Basic Structural Analysis
===

For the first portion of the analysis, we'll perform some basic queries
to get a sense of the data's shape and size.  This data is
fundamentally relational (it's a single table), so thankfully a query
language was invented a while ago to assist us in this task.  It works
well, so we won't write complex MapReduce or spark to recreate it.


We'll use [SparkSQL](https://spark.apache.org/sql/) to overlay a structure on the data.  I have chosen to use SparkSQL, but I also could have hooked Hive up to do these queries.  There are tradeoffs for both, SparkSQL turns into direct Spark operations, whereas Hive is calling out externally to Hive, so you aren't building up a RDD lazily.  However, Hive is more mature and supports more SQL.  For our purposes we are using very simple queries, so we'll stick to SparkSQL.

Note that the output of the SparkSQL call is
a RDD, a resilient distributed dataset, which is a dataset which can be
operated on by *other* parts of the spark ecosystem.  So, you'll see me
mix fundamental Spark operations (e.g. map, join, groupByKey, filter,
etc.) with SQL calls.

{% highlight python %}
#Let's overlay some structure from our raw data

#after the pig job, we get a ctrl-A separated file
raw_data = sc.textFile("/user/hrt_qa/open_payments/general/post/part-m-*")

#create a SQL Context so that we may use spark-sql
#This allows us to use a very simple subset of SQL, for a more complete
#set of SQL available, you can use Hive as the underlying engine
#by using HiveContext instead of SQLContext
sqlContext = SQLContext(sc)

#split up the line into tokens separated on ctrl-a
parts = raw_data.map(lambda l : l.split('\x01'))

#We're only really concerned about a few fields, so we'll project out only
#the fields we're interested in.
def tokens_to_columns(tokens):
    return Row( physician_id=tokens[7]\
              , physician_name="{} {}".format(tokens[8], tokens[10])\
              , physician_specialty=tokens[21] \
              , payer=tokens[43] \
              , reason=tokens[52] \
              , amount_str=tokens[48] \
              , amount=float(tokens[48]) \
              )

#Consider rows with either empty or null physician ID's to be bad and we want
#to ignore those.
payments = parts.map(tokens_to_columns)\
                .filter(lambda row : len(row.physician_id) > 0)
                       
#Now, we can register this as a table called payments.
#This allows us to refer to the table in our SQL statements
schemaPayments = sqlContext.inferSchema(payments)
schemaPayments.registerAsTable('payments')

{% endhighlight %}

From a much larger schema (defined in [this](http://download.cms.gov/openpayments/09302014_ALLDTL.ZIP) zip bundle ), we are projecting
out a small set of columns:

* Physician ID
* Physician Name (redacted)
* Physician Specialty
* Paying organization (aka payer)
* Reason for payment
* Amount of payment as float

Now, those of you with experience doing analysis are now cringing at my
use of a float for money.  Spark has support for a "decimal" type, which
is backed by Java's BigDecimal.  Unfortunately, I had some trouble
getting the decimal type to work in the python bindings.  It complained:
    
    java.lang.ClassCastException: java.math.BigDecimal cannot be cast to
scala.math.BigDecimal
     scala.math.Numeric$BigDecimalIsFractional$.plus(Numeric.scala:182)
     org.apache.spark.sql.catalyst.expressions.Add$$anonfun$eval$2.apply(arithmetic.scala:58)
     org.apache.spark.sql.catalyst.expressions.Add$$anonfun$eval$2.apply(arithmetic.scala:58)
     org.apache.spark.sql.catalyst.expressions.Expression.n2(Expression.scala:114)
     org.apache.spark.sql.catalyst.expressions.Add.eval(arithmetic.scala:58)
     ...
    (Note: this is with Spark 1.1.0)

Given this bug, I did not see a way to use Spark SQL to
support a data type suitable for money operations, so I treat the amounts as floats.   In production, I'd have probably investigated more what I was doing wrong or moved to the Hive interface for Spark SQL.
 
Top Payment Reasons by Number of Payments
---

First thing we'll look at is a break-down of the payment reasons and how
many payments are in each.  

{% highlight python %}

#Broken down by reasons
count_by_reasons = sqlContext.sql("""select reason, count(*) as num_payments 
                                     from payments 
                                     group by reason 
                                     order by num_payments desc""").collect()

print_table(['Payment Reason', '# of Payments']\
               , [x[0] for x in count_by_reasons]\
               , [ [locale.format("%d", x[1], grouping=True)] \
                   for x in count_by_reasons]\
               )
{% endhighlight %}



<table id="resultTable1" class="tablesorter">
  <thead>
    <tr style="text-align: center;">
      <th>Payment Reason</th>
      <th># of Payments</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>                                                                                   Food and Beverage</td>
      <td> 2,192,057</td>
    </tr>
    <tr>
      <td>                                                                                  Travel and Lodging</td>
      <td>   135,235</td>
    </tr>
    <tr>
      <td>                                                                                           Education</td>
      <td>   122,839</td>
    </tr>
    <tr>
      <td> Compensation for services other than consulting, including serving as faculty or as a speaker at...</td>
      <td>    77,236</td>
    </tr>
    <tr>
      <td>                                                                                      Consulting Fee</td>
      <td>    45,525</td>
    </tr>
    <tr>
      <td>                                                                                                Gift</td>
      <td>    26,422</td>
    </tr>
    <tr>
      <td>                                                                                           Honoraria</td>
      <td>    10,900</td>
    </tr>
    <tr>
      <td> Compensation for serving as faculty or as a speaker for a non-accredited and noncertified contin...</td>
      <td>     4,377</td>
    </tr>
    <tr>
      <td>                                                                                  Royalty or License</td>
      <td>     3,268</td>
    </tr>
    <tr>
      <td>                                                                                               Grant</td>
      <td>     2,881</td>
    </tr>
    <tr>
      <td>                                               Space rental or facility fees(teaching hospital only)</td>
      <td>     2,114</td>
    </tr>
    <tr>
      <td>                                             Current or prospective ownership or investment interest</td>
      <td>     1,559</td>
    </tr>
    <tr>
      <td>                                                                                       Entertainment</td>
      <td>     1,400</td>
    </tr>
    <tr>
      <td>                                                                             Charitable Contribution</td>
      <td>       480</td>
    </tr>
    <tr>
      <td> Compensation for serving as faculty or as a speaker for an accredited or certified continuing ed...</td>
      <td>       381</td>
    </tr>
  </tbody>
</table>

Top 10 Physician Specialties by Number of Payments
---

I'm always intrigued to look at how specialties differ.  Anyone who has
worked with physicians as either managers, colleagues or as subject
matter experts will notice that different specialties have different
characters.  I will leave that observation there and not endeavor to be
more specific, but I wanted to see if anyone was getting paid extremely
well in this dataset.

{% highlight python %}
#Which specialties are getting the most reimbursements?
totals_by_specialty = sqlContext.sql("""select physician_specialty
                                             , count(*) as cnt
                                             , sum(amount) as total
                                        from payments
                                        group by physician_specialty""")\
                                .collect()
total_count_by_specialty = sum([t[1] for t in totals_by_specialty])
top_count_by_specialty = sorted(totals_by_specialty\
                               , key=lambda t : t[1], reverse=True)[0:10]
print_table(['Specialty', '# of Payments', "% of Payments"]\
           , [x[0] for x in top_count_by_specialty]\
           , [ [ locale.format("%d", x[1], grouping=True)\
               , '{0:.2f}%'.format(100*x[1]/total_count_by_specialty)\
               ] \
               for x in top_count_by_specialty]\
           )
{% endhighlight %}


<table id="resultTable2" class="tablesorter">
  <thead>
    <tr style="text-align: center;">
      <th>Specialty</th>
      <th># of Payments</th>
      <th>% of Payments</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>                                         Allopathic &amp; Osteopathic Physicians/ Family Medicine</td>
      <td> 430,771</td>
      <td> 16.00%</td>
    </tr>
    <tr>
      <td>                                       Allopathic &amp; Osteopathic Physicians/ Internal Medicine</td>
      <td> 417,155</td>
      <td> 15.00%</td>
    </tr>
    <tr>
      <td>               Allopathic &amp; Osteopathic Physicians/ Internal Medicine/ Cardiovascular Disease</td>
      <td> 160,342</td>
      <td>  6.00%</td>
    </tr>
    <tr>
      <td>                      Allopathic &amp; Osteopathic Physicians/ Psychiatry &amp; Neurology/ Psychiatry</td>
      <td> 121,067</td>
      <td>  4.00%</td>
    </tr>
    <tr>
      <td>                     Allopathic &amp; Osteopathic Physicians/ Internal Medicine/ Gastroenterology</td>
      <td>  92,531</td>
      <td>  3.00%</td>
    </tr>
    <tr>
      <td>                       Allopathic &amp; Osteopathic Physicians/ Psychiatry &amp; Neurology/ Neurology</td>
      <td>  89,673</td>
      <td>  3.00%</td>
    </tr>
    <tr>
      <td>                                                          Other Service Providers/ Specialist</td>
      <td>  80,576</td>
      <td>  3.00%</td>
    </tr>
    <tr>
      <td>                                 Allopathic &amp; Osteopathic Physicians/ Obstetrics &amp; Gynecology</td>
      <td>  65,434</td>
      <td>  2.00%</td>
    </tr>
    <tr>
      <td> Allopathic &amp; Osteopathic Physicians/ Internal Medicine/ Endocrinology, Diabetes &amp; Metabolism</td>
      <td>  62,862</td>
      <td>  2.00%</td>
    </tr>
    <tr>
      <td>                                                 Allopathic &amp; Osteopathic Physicians/ Urology</td>
      <td>  61,165</td>
      <td>  2.00%</td>
    </tr>
  </tbody>
</table>


Top 10 Specialties by Total Amount Paid
---
Now that we've looked at how many payments were made, let's see who is
getting paid the most.  I'd wager it's not the same list.

{% highlight python %}
#Which specialties are getting the most money?
top_total_by_specialty = sorted(totals_by_specialty\
                               , key=lambda t : t[2], reverse=True)[0:10]
total_amount_by_specialty = sum([t[2] for t in totals_by_specialty])
print_table(['Specialty', 'Amount of Payments', '% of Total Amount']\
           , [x[0] for x in top_total_by_specialty]\
           , [ ['$' + locale.format('%0.2f', x[2], grouping=True)\
               , '{0:.2f}%'.format(100*x[2]/total_amount_by_specialty)\
               ] \
               for x in top_total_by_specialty\
             ]\
           )
{% endhighlight %}


<table id="resultTable3" class="tablesorter">
  <thead>
    <tr style="text-align: center;">
      <th>Specialty</th>
      <th>Amount of Payments</th>
      <th>% of Total Amount</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>                                                                                            "</td>
      <td> $209,192,160.39</td>
      <td> 31.24%</td>
    </tr>
    <tr>
      <td>                                     Allopathic &amp; Osteopathic Physicians/ Orthopaedic Surgery</td>
      <td>  $80,157,503.04</td>
      <td> 11.97%</td>
    </tr>
    <tr>
      <td>                                       Allopathic &amp; Osteopathic Physicians/ Internal Medicine</td>
      <td>  $26,616,544.66</td>
      <td>  3.98%</td>
    </tr>
    <tr>
      <td>               Allopathic &amp; Osteopathic Physicians/ Internal Medicine/ Cardiovascular Disease</td>
      <td>  $24,291,090.52</td>
      <td>  3.63%</td>
    </tr>
    <tr>
      <td>                      Allopathic &amp; Osteopathic Physicians/ Psychiatry &amp; Neurology/ Psychiatry</td>
      <td>  $18,724,216.32</td>
      <td>  2.80%</td>
    </tr>
    <tr>
      <td>                                                          Other Service Providers/ Specialist</td>
      <td>  $17,475,702.37</td>
      <td>  2.61%</td>
    </tr>
    <tr>
      <td>                       Allopathic &amp; Osteopathic Physicians/ Psychiatry &amp; Neurology/ Neurology</td>
      <td>  $16,974,664.19</td>
      <td>  2.54%</td>
    </tr>
    <tr>
      <td>                                    Allopathic &amp; Osteopathic Physicians/ Neurological Surgery</td>
      <td>  $15,848,473.39</td>
      <td>  2.37%</td>
    </tr>
    <tr>
      <td> Allopathic &amp; Osteopathic Physicians/ Internal Medicine/ Endocrinology, Diabetes &amp; Metabolism</td>
      <td>  $15,525,370.10</td>
      <td>  2.32%</td>
    </tr>
    <tr>
      <td>                     Allopathic &amp; Osteopathic Physicians/ Internal Medicine/ Gastroenterology</td>
      <td>  $14,570,253.86</td>
      <td>  2.18%</td>
    </tr>
  </tbody>
</table>

While very many people without specialties listed are getting paid, those Orthopedic Surgeons are getting paid nearly $12\%$ of the
total amount paid to everyone.  General internal medicine is a distant
second.

Top 10 Total Gift Payments by Physician and Payer
---

I'm intrigued by this gift category, so I wanted to figure out who is
getting the most in gifts and by whom.

{% highlight python %}

#who is getting the most gifts?
gift_amount_by_physician = sqlContext.sql("""select physician_id
                                                  , physician_specialty
                                                  , payer
                                                  , count(*) as cnt
                                                  , sum(amount) as total
                                             from payments
                                             where reason = \'Gift\'
                                             group by physician_id
                                                    , physician_specialty
                                                    , payer
                                             order by total desc
                                             """)\
                                     .filter(lambda t:len(t[0]) > 3)\
                                     .take(10)

print_table( ['Physician'\
             ,'Specialty'\
             , 'Payer'\
             , 'Number of Gifts'\
             , 'Total Amount for Gifts'\
             ]\
           , [x[0] for x in gift_amount_by_physician]\
           , [ [ x[1] \
               , x[2] \
               , locale.format('%d', x[3], grouping=True)\
               , '$' + locale.format('%0.2f', x[4], grouping=True)\
               ] \
             for x in gift_amount_by_physician]\
           )
{% endhighlight %}

<table id="resultTable4" class="tablesorter">
  <thead>
    <tr style="text-align: center;">
      <th>Physician</th>
      <th>Specialty</th>
      <th>Payer</th>
      <th>Number of Gifts</th>
      <th>Total Amount for Gifts</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td> 225073</td>
      <td>                            Dental Providers/ Dentist/ General Practice</td>
      <td>    Dentalez Alabama, Inc.</td>
      <td>  1</td>
      <td> $56,422.00</td>
    </tr>
    <tr>
      <td> 364744</td>
      <td>                                    Other Service Providers/ Specialist</td>
      <td>      Ellman International</td>
      <td>  1</td>
      <td> $37,699.00</td>
    </tr>
    <tr>
      <td> 446958</td>
      <td>                                 Dental Providers/ Dentist/ Endodontics</td>
      <td> Tulsa Dental Products LLC</td>
      <td>  6</td>
      <td> $37,216.28</td>
    </tr>
    <tr>
      <td> 523360</td>
      <td>                              Dental Providers/ Dentist/ Prosthodontics</td>
      <td>     GAC International LLC</td>
      <td> 14</td>
      <td> $23,562.07</td>
    </tr>
    <tr>
      <td> 244739</td>
      <td>                   Allopathic &amp; Osteopathic Physicians/ Family Medicine</td>
      <td>          Mallinckrodt LLC</td>
      <td>  1</td>
      <td> $19,488.75</td>
    </tr>
    <tr>
      <td>  92931</td>
      <td> Allopathic &amp; Osteopathic Physicians/ Psychiatry &amp; Neurology/ Neurology</td>
      <td>          Mallinckrodt LLC</td>
      <td>  1</td>
      <td> $19,370.35</td>
    </tr>
    <tr>
      <td> 481461</td>
      <td>    Dental Providers/ Dentist/ Orthodontics and Dentofacial Orthopedics</td>
      <td>     GAC International LLC</td>
      <td>  1</td>
      <td> $18,573.00</td>
    </tr>
    <tr>
      <td>   9126</td>
      <td>    Dental Providers/ Dentist/ Orthodontics and Dentofacial Orthopedics</td>
      <td>     GAC International LLC</td>
      <td>  7</td>
      <td> $15,750.58</td>
    </tr>
    <tr>
      <td> 523314</td>
      <td>    Dental Providers/ Dentist/ Orthodontics and Dentofacial Orthopedics</td>
      <td>     GAC International LLC</td>
      <td>  1</td>
      <td> $15,000.00</td>
    </tr>
    <tr>
      <td> 224960</td>
      <td>                              Laboratories/ Clinical Medical Laboratory</td>
      <td>    Tosoh Bioscience, Inc.</td>
      <td>  5</td>
      <td> $14,001.00</td>
    </tr>
  </tbody>
</table>

Dentists have obviously been good this year, because they feature prominently in the big gift receiver list.  $56,422 in a single gift to a dentist from Dentalez, a dental chair manufacturer.  That's very generous.  This may also be bad data.  The [Wallstreet journal](http://online.wsj.com/articles/u-s-agency-reveals-drug-makers-payments-to-doctors-1412100323) found that at least some of these categories are misreported after someone racked up a $70k+ in food and beverage costs that should've been in consulting.

Top 10 Food and Beverage Payments to Physicians by Payers and Total Amount Paid
---

{% highlight python %}
#who is getting the most Food?
food_amount_by_physician = sqlContext.sql("""select physician_id
                                                  , physician_specialty
                                                  , payer, count(*) as cnt
                                                  , sum(amount) as total
                                             from payments
                                             where reason = \'Food and Beverage\'
                                             group by physician_id
                                                    , physician_specialty
                                                    , payer
                                             order by total desc
                                             """)\
                                     .filter(lambda t:len(t[0]) > 3)\
                                     .take(10)

print_table( [ 'Physician','Specialty'\
             , 'Payer', 'Number of Payments'\
             , 'Total Amount for Payments'\
             ]\
           , [x[0] for x in food_amount_by_physician]\
           , [ [ x[1] \
               , x[2] \
               , locale.format('%d', x[3], grouping=True)\
               , '$' + locale.format('%0.2f', x[4], grouping=True)\
               ] \
             for x in food_amount_by_physician]\
           )
{% endhighlight %}

<table id="resultTable5" class="tablesorter">
  <thead>
    <tr style="text-align: center;">
      <th>Physician</th>
      <th>Specialty</th>
      <th>Payer</th>
      <th>Number of Payments</th>
      <th>Total Amount for Payments</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td> 200720</td>
      <td>                    Allopathic &amp; Osteopathic Physicians/ Surgery</td>
      <td>  Teleflex Medical Incorporated</td>
      <td>  4</td>
      <td> $78,183.81</td>
    </tr>
    <tr>
      <td>  28946</td>
      <td>                     Dental Providers/ Dentist/ General Practice</td>
      <td>                  BIOLASE, INC.</td>
      <td>  6</td>
      <td> $29,608.22</td>
    </tr>
    <tr>
      <td> 405597</td>
      <td>              Allopathic &amp; Osteopathic Physicians/ Ophthalmology</td>
      <td>                   Lundbeck LLC</td>
      <td> 67</td>
      <td> $14,955.02</td>
    </tr>
    <tr>
      <td>  29943</td>
      <td>                    Allopathic &amp; Osteopathic Physicians/ Urology</td>
      <td> Auxilium Pharmaceuticals, Inc.</td>
      <td> 62</td>
      <td> $13,138.35</td>
    </tr>
    <tr>
      <td> 245373</td>
      <td>             Allopathic &amp; Osteopathic Physicians/ Anesthesiology</td>
      <td>                  Depomed, Inc.</td>
      <td> 36</td>
      <td> $12,647.92</td>
    </tr>
    <tr>
      <td> 232708</td>
      <td>       Allopathic &amp; Osteopathic Physicians/ Neurological Surgery</td>
      <td>          Baxano Surgical, Inc.</td>
      <td> 28</td>
      <td> $10,641.94</td>
    </tr>
    <tr>
      <td> 328465</td>
      <td>                                                                </td>
      <td>          SonaCare Medical, LLC</td>
      <td>  4</td>
      <td>  $9,997.92</td>
    </tr>
    <tr>
      <td> 440053</td>
      <td>          Allopathic &amp; Osteopathic Physicians/ Internal Medicine</td>
      <td>                    Pfizer Inc.</td>
      <td> 36</td>
      <td>  $9,690.36</td>
    </tr>
    <tr>
      <td> 201967</td>
      <td> Allopathic &amp; Osteopathic Physicians/ Surgery/ Surgical Oncology</td>
      <td>       Intuitive Surgical, Inc.</td>
      <td> 18</td>
      <td>  $8,601.26</td>
    </tr>
    <tr>
      <td> 154591</td>
      <td>                             Other Service Providers/ Specialist</td>
      <td>                   Ranbaxy Inc.</td>
      <td> 10</td>
      <td>  $8,347.49</td>
    </tr>
  </tbody>
</table>

As I was saying earlier, that first entry is a miscategoried set of consulting
fees as found by the WSJ.  However, looking past that, nearly $30k to a
dentist over 6 food and beverage reimbursements.  I would definitely be
looking closer if this was data that came to me as part of my day-job.

Top 10 Payers and Reasons by Total Amount Paid
---

{% highlight python %}
#Who is paying the most and for what?
amount_by_payer = sqlContext.sql("""select payer
                                         , reason
                                         , count(*) as cnt
                                         , sum(amount) as total
                                    from payments
                                    group by payer, reason
                                    order by total desc
                                 """)\
                            .filter(lambda t:len(t[0]) > 3)\
                            .take(10)
print_table( ['Payer','Reason'\
             , 'Number of Payments'\
             , 'Total Amount for Payments'\
             ]\
           , [x[0] for x in amount_by_payer]\
           , [ [ x[1] \
               , locale.format('%d', x[2], grouping=True)\
               , '$' + locale.format('%0.2f', x[3], grouping=True)\
               ] \
             for x in amount_by_payer]\
           )
{% endhighlight %}


<table id="resultTable6" class="tablesorter">
  <thead>
    <tr style="text-align: center;">
      <th>Payer</th>
      <th>Reason</th>
      <th>Number of Payments</th>
      <th>Total Amount for Payments</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>                     Genentech, Inc.</td>
      <td>                                                                                  Royalty or License</td>
      <td>    65</td>
      <td> $122,548,134.00</td>
    </tr>
    <tr>
      <td>            DePuy Synthes Sales Inc.</td>
      <td>                                                                                  Royalty or License</td>
      <td>   247</td>
      <td>  $27,730,373.58</td>
    </tr>
    <tr>
      <td>                       Arthrex, Inc.</td>
      <td>                                                                                  Royalty or License</td>
      <td>   259</td>
      <td>  $11,524,088.26</td>
    </tr>
    <tr>
      <td>                        Biomet, Inc.</td>
      <td>                                                                                  Royalty or License</td>
      <td>   301</td>
      <td>   $9,966,304.43</td>
    </tr>
    <tr>
      <td>      AstraZeneca Pharmaceuticals LP</td>
      <td> Compensation for services other than consulting, including serving as faculty or as a speaker at...</td>
      <td> 5,237</td>
      <td>   $9,529,667.44</td>
    </tr>
    <tr>
      <td>                  Zimmer Holding Inc</td>
      <td>                                                                                  Royalty or License</td>
      <td>   115</td>
      <td>   $9,132,692.18</td>
    </tr>
    <tr>
      <td>                         Pfizer Inc.</td>
      <td>                                                                                               Grant</td>
      <td>   107</td>
      <td>   $7,989,769.90</td>
    </tr>
    <tr>
      <td>           Forest Laboratories, Inc.</td>
      <td> Compensation for services other than consulting, including serving as faculty or as a speaker at...</td>
      <td> 5,165</td>
      <td>   $7,633,516.85</td>
    </tr>
    <tr>
      <td>        Janssen Pharmaceuticals, Inc</td>
      <td> Compensation for services other than consulting, including serving as faculty or as a speaker at...</td>
      <td> 3,742</td>
      <td>   $7,423,565.00</td>
    </tr>
    <tr>
      <td> Otsuka America Pharmaceutical, Inc.</td>
      <td>                                                                                      Consulting Fee</td>
      <td> 4,098</td>
      <td>   $6,972,416.19</td>
    </tr>
  </tbody>
</table>

Turns out royalties and licensing fees are pretty expensive.  [Genentech](https://www.google.com/finance?cid=144958217628598) spent more on 65 royalties/licensing payments in 2013 than the [gross domestic product](http://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)) of [Montserrat](http://en.wikipedia.org/wiki/Montserrat) and [Tuvalu](http://en.wikipedia.org/wiki/Tuvalu).  Considering, however, their revenue is in the orders of $10 Billion, that's still only a tiny fraction.  Even so, as you can see from the bar chart below, they dominate the rest of the payments across all reasons for any company by a fair margin.

{% highlight python %}
#Take the data above and generate a bar chart with it
bar_chart([ [x[0] + ' - ' + truncate(x[1], length=20) \
            , x[3] /1000000.0 \
            ] for x in amount_by_payer ]\
         , 'Most Paid'\
         , 'Total Paid in $1M'\
         , 'Payer/Reason'\
         , False\
         )
{% endhighlight %}

![png](files/ref_data/open_payments_files/open_payments_9_0.png)

Outlier Analysis
===

Generating summary statistics is very helpful for 

* Understanding the overall shape of the data 
* Looking at trends at the extreme (answering most, least, etc style questions)

One thing we're doing when we're looking for fishy things in data is
looking to quickly zoom in on things which are outside of the norm.
Furthermore, the aim is to automatically tag these as data is ingested
so they can be acted on.  This action can be raising an alert, logging a
warning or generating a report, but ultimately we want a technique to
find these outliers quickly and without human intervention.

Median Absolute Divergence
---

We're looking to create a mechanism to rank data points by their likelihood of being an outlier along with a threshold to differentiate them from inliers.

The area of [outlier analysis](http://en.wikipedia.org/wiki/Anomaly_detection) is a vibrant one and there are
quite a few techniques to choose from.  Ranging from the exotic, like
[fitting an elliptic
envelope](http://scikit-learn.org/stable/modules/outlier_detection.html#id1)
to the straightforward, like setting a threshold based on standard
deviations away from the mean.  For our purposes, we'll choose a middle
path, but be aware that there are
[book-length](http://www.itl.nist.gov/div898/handbook/eda/section4/eda43.htm#Barnett) treatments of the subject of outlier analysis.
of 

[Median Absolute
Divergence](http://en.wikipedia.org/wiki/Median_absolute_deviation) is a
robust statistic used, as standard deviations, as a measure of
variability in a univariate dataset.  It's definition is
straightforward:

>Given univariate data $X$ with $\tilde{x}=$median($X$), 
>MAD($X$)=median($\forall x_i \in X, |x_i - \tilde{x}|$).

As compared to standard deviation, it's a bit more resilient to outliers because it doesn't have a square weighing large values very heavily.  Quoting from the [Engineering Statistics
Handbook](http://www.itl.nist.gov/div898/handbook/eda/section3/eda35h.htm):

>The standard deviation is an example of an estimator that is the best
>we can do if the underlying distribution is normal. However, it lacks
>robustness of validity. That is, confidence intervals based on the
>standard deviation tend to lack precision if the underlying
>distribution is in fact not normal.
>
>The median absolute deviation and the interquartile range are estimates
>of scale that have robustness of validity. However, they are not
>particularly strong for robustness of efficiency.
>
>If histograms and probability plots indicate that your data are in fact
>reasonably approximated by a normal distribution, then it makes sense to
>use the standard deviation as the estimate of scale. However, if your
>data are not normal, and in particular if there are long tails, then
>using an alternative measure such as the median absolute deviation,
>average absolute deviation, or interquartile range makes sense. 

In the implementation we'll be taking guidance the [Engineering Statistics
Handbook](http://www.itl.nist.gov/div898/handbook/eda/section3/eda35h.htm)
 and [Iglewicz and Hoaglin](http://www.itl.nist.gov/div898/handbook/eda/section4/eda43.htm#Iglewicz).  As such, define an outlier like so:

>For a set of univariate data $X$ with $\tilde{x} =$ median($X$), an outlier is an element $x_i \in X$
>such that $M_i = \frac{0.6745(x_i − \tilde{x} )}{MAD(X)} > 3.5$, where
>$M_i$ is denoted the *modified Z-score*. 

Before we jump into the actual algorithm, we create some helper
functions to make the code a bit more readable and allow us to display
the outliers.

{% highlight python %}
#Some useful functions for more advanced analytics

#Joins in spark take RDD[K,V] x RDD[K,U] => RDD[K, [U,V] ]
#This function returns U
def join_lhs(t):
    return t[1][0]

#Joins in spark take RDD[K,V] x RDD[K,U] => RDD[K, [U,V] ]
#This function returns V
def join_rhs(t):
    return t[1][1]

#Add a key/value to a dictionary and return the dictionary
def annotate_dict(d, k, v):
    d[k] = v
    return d

#Plots a density plot of a set of points representing inliers and outliers
#A rugplot is used to indicate the points and the outliers are marked in red.
def plot_outliers(inliers, outliers, reason):
    fig, ax = plt.subplots(nrows=1)
    sns.distplot(inliers + outliers, ax=ax, rug=True, hist=False)
    ax.plot(outliers, np.zeros_like(outliers), 'ro', clip_on=False)
    fig.suptitle('Distribution for {} Values'.format(reason), size=14)

{% endhighlight %}

Now, onto the implementation of the outlier analysis.  But, before we
start, I'd like to make a couple notes about the implementation and
possible scalability challenges going forward.

We are partitioning the data by (payment reason, physician specialty).  I do not want to analyze outliers based on a cohort of data across a whole reason, but rather I want to know if a point is an outlier for a given specialty *and* reason. 

If a coarser partitioning strategy is taken or the amount of data per
  partition becomes very large, the median implementation may become a
limiting factor scalability wise.  There are a few things to do,
including using a tighter implementation (numpy's implementation could
be tighter as of this writing) or a streaming estimate.  Needless to say, this is something that bears some thought going forward.

 
{% highlight python %}
#Outlier analysis using Median Absolute Divergence

#Using reservoir sampling, uniformly sample N points
#requires O(N) memory
def sample_points(points, N):
    sample = [];
    for i,point in enumerate(points):
        if i < N:
            sample.append(point)
        elif i >= N and random.random() < N/float(i+1):
            replace = random.randint(0,len(sample)-1)
            sample[replace] = point
    return sample

#Returns a function which will extract the median at location 'key'
#a list of dictionaries.
def median_func(key):
    #Right now it uses numpy's median, but probably a quickselect
    #implementation is called for as I expect this doesn't scale
    return lambda partition_value : ( partition_value[0]\
                                    , np.median( 
                                        [ d[key] \
                                         for d in partition_value[1]\
                                        ]\
                                               )\
                                    )

#Compute the modified z-score for use by  as per Iglewicz and Hoaglin:
#Boris Iglewicz and David Hoaglin (1993),
#"Volume 16: How to Detect and Handle Outliers",
#The ASQC Basic References in Quality Control: Statistical Techniques
#, Edward F. Mykytka, Ph.D., Editor.
def get_z_score(reason_to_diff):
    med = join_rhs(reason_to_diff)
    if med > 0:
        return 0.6745 * join_lhs(reason_to_diff)['diff'] / med
    else:
        return 0

def is_outlier(thresh):
    return lambda reason_to_diff : get_mad(reason_to_diff) > thresh

#Return a RDD of a uniform random sample of a specified size per key
def get_inliers(reason_amount_pairs, size=2000):
    group_by_reason = reason_amount_pairs.groupByKey()
    return group_by_reason.map(lambda t : (t[0], sample_points(t[1], size)))


#Return the outliers based on Median Absolute Divergence
#See http://www.itl.nist.gov/div898/handbook/eda/section3/eda35h.htm 
#for more info.
#The input key structure is reason_specialty => dict(amount
#                                                   , physician
#                                                   , payer
#                                                   , specialty
#                                                   )
def get_outliers(reason_amount_pairs, thresh=3.5):
    """
        This uses the median absolute divergence (MAD) statistic to find
        outliers for each reason x specialty partitions.
        
        Outliers are computed as follows: 
        * Let X be all the payments for a given specialty, reason pair
        * Let x_i be a payment in X
        * Let MAD be the median absolute divergence, defined as
          MAD = median( for all x in X, | x - median(X)| )
        * Let M_i be the modified z-score for payment x_i, defined as
          0.6745*(x_i − median(X) )/MAD
        
        As per the recommendations by Iglewicz and Hoaglin, a payment is
        considered an outlier if the modified z-score, M_i > thresh, which
        is 3.5 by default.
        
        
        REFERENCE:
        Boris Iglewicz and David Hoaglin (1993),
        "Volume 16: How to Detect and Handle Outliers",
        The ASQC Basic References in Quality Control: Statistical Techniques,
        Edward F. Mykytka, Ph.D., Editor.
    """
    
    group_by_reason = reason_amount_pairs.groupByKey()
   
    #Filter by only reason/specialty's with more than 1k entries
    #and compute the median of the amounts across the partition.
    
    #NOTE: There may be some scalability challenges around median,
    #so some care should be taken to reimplement this if partitioning
    #by (reason, specialty) does not yield small enough numbers to 
    #handle in an individual map function.
    reason_to_median = group_by_reason.filter(lambda t: len(t[1]) > 1000) \
                                      .map(median_func('amount'))
   
    #Join the base, non-grouped data, with the median per key,
    #consider just the payments more than the median
    #since we're looking for large money outliers and annotate 
    #the dictionary for each entry x_i with the following:
    # * diff = |x_i - median(X)| in the parlance of the comment above.
    #   NOTE: Strictly speaking I can drop the absolute value since 
    #         x_i > median(X), but I choose not to.
    # * median = median(X)
    # 
    reason_abs_dist_from_median = \
        reason_amount_pairs.join(reason_to_median) \
                           .filter(lambda t : \
                              join_lhs(t)['amount'] > join_rhs(t)\
                                  ) \
                           .map(lambda t :\
                                 ( t[0]\
                                 , dict( diff=abs(\
                                      join_lhs(t)['amount'] - join_rhs(t)\
                                                 )\
                                       , row=annotate_dict(join_lhs(t) \
                                                          , 'median' \
                                                          , join_rhs(t) \
                                                          )\
                                       )\
                                 )\
                               )
                
    # Given diff cached per element, we need only compute the median 
    # of the diffs to compute the MAD.  
    #Remember, MAD = median( for all x in X, | x - median(X)| )
    reason_to_MAD = reason_abs_dist_from_median.groupByKey() \
                                               .map(median_func('diff'))
    reason_to_MAD.take(1)
    
    # Joining the grouped data to get both | x_i - median(X) | 
    # and MAD in the same place, we can compute the modified z-score
    # , 0.6475*| x_i - median(X)| / MAD, and filter by the ones which 
    # are more than threshold we can then do some pivoting of keys and 
    #sort by that threshold to give us the ranked list of outliers.
    return reason_abs_dist_from_median\
              .join(reason_to_MAD) \
              .filter(is_outlier(thresh))\
              .map(lambda t: (get_z_score(t)\
                             , annotate_dict(join_lhs(t)['row']\
                                            , 'key'\
                                            , t[0]\
                                            )\
                             )\
                  ) \
              .sortByKey(False) \
              .map(lambda t: ( t[1]['key']\
                             , annotate_dict( t[1]\
                                            , 'mad'\
                                            , t[0]\
                                            )\
                             )\
                  )

#Filter the outliers by reason and return a RDD with just the outliers 
#of a specified reason.
def get_by_reason(outliers, reason):
    return outliers.filter(lambda t: str.startswith(t[0].encode('ascii'\
                                                               , 'ignore'\
                                                               )\
                                                   ,reason\
                                                   )\
                          )

#Grab data using Spark-SQL and filter with spark core RDD operations 
#to only yield the data we want, ones with physicians, payers and reasons
reason_amount_pairs = \
    sqlContext.sql("""select reason
                           , physician_specialty
                           , amount
                           , physician_id
                           , payer 
                      from payments"""\
                  )\
              .filter(lambda row:len(row.reason) > 3 \
                              and len(row.physician_id) > 3\
                              and len(row.payer) > 3\
                     ) \
              .map(lambda row: ( "{}_{}".format( row.reason\
                                               , row.physician_specialty\
                                               )\
                             , dict(amount=row.amount\
                                   ,physician_id=row.physician_id\
                                   ,payer=row.payer\
                                   ,specialty=row.physician_specialty\
                                   )\
                             )\
                   )

#Get the outliers based on a modified z-score threshold of 3.5
outliers = get_outliers(reason_amount_pairs, 3.5)
#Get a sample per specialty/reason partition
inliers = get_inliers(reason_amount_pairs)
{% endhighlight %}

Now that we have found the outliers per specialty/reason partition, and
a sample of inliers, let's display them so that we can get a sense of
how sensitive the outlier detection is.

{% highlight python %}
#display the top k outliers in a table and a distribution plot
#of an inlier sample along with the outliers rug-plotted in red
def display_info(inliers_raw, outliers_raw_tmp, reason, k=None):
    outliers_raw = []
    if k is None:
        outliers_raw = sorted(outliers_raw_tmp\
                             , key=lambda d:d[1]['amount']\
                             , reverse=True\
                             )
    else:
        outliers_raw = sorted(outliers_raw_tmp\
                             , key=lambda d:d[1]['amount']\
                             , reverse=True\
                             )[0:k]
    inlier_pts = []
    for i in [d[1] for d in inliers_raw]:
        for j in i:
            inlier_pts.append(j['amount'])
    outlier_pts= [d[1]['amount'] for d in outliers_raw]
    plot_outliers(inlier_pts[0:1500], outlier_pts, reason)

    print_table(['Physician','Specialty', 'Payer', 'Amount']\
               , [d[1]['physician_id'] for d in outliers_raw]\
               , [ [ d[1]['specialty'] \
                   , d[1]['payer'].encode('ascii', 'ignore') \
                   , '$' + locale.format('%0.2f', d[1]['amount'], grouping=True)\
                   ] \
                 for d in outliers_raw]\
               )
{% endhighlight %}

Food and Beverage Purchase Outliers
---

Let's look at the top 4 outliers for Food and Beverage payments.  I
could have shown all of the outliers, but I found the first few to be
the biggest bang for our buck in terms of interesting findings.

{% highlight python %}
#outliers for food and beverage purchases
food_outliers = get_by_reason(outliers, 'Food and Beverage').collect()
food_inliers = get_by_reason(inliers, 'Food and Beverage').collect()
display_info(food_inliers, food_outliers, 'Food and Beverage', 4)
{% endhighlight %}

As we can see, the misclassified data from Teleflex is rearing its head
again with a huge single payment for food.  However, looking down the
list, Biolase is paying quite a bit to some dentist for food.


<table id="resultTable7" class="tablesorter">
  <thead>
    <tr style="text-align: center;">
      <th>Physician</th>
      <th>Specialty</th>
      <th>Payer</th>
      <th>Amount</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td> 200720</td>
      <td> Allopathic &amp; Osteopathic Physicians/ Surgery</td>
      <td> Teleflex Medical Incorporated</td>
      <td> $68,750.00</td>
    </tr>
    <tr>
      <td>  28946</td>
      <td>  Dental Providers/ Dentist/ General Practice</td>
      <td>                 BIOLASE, INC.</td>
      <td> $13,297.15</td>
    </tr>
    <tr>
      <td>  28946</td>
      <td>  Dental Providers/ Dentist/ General Practice</td>
      <td>                 BIOLASE, INC.</td>
      <td>  $8,111.82</td>
    </tr>
    <tr>
      <td>  28946</td>
      <td>  Dental Providers/ Dentist/ General Practice</td>
      <td>                 BIOLASE, INC.</td>
      <td>  $8,111.82</td>
    </tr>
  </tbody>
</table>

Below is a density plot of outliers and a sample of inliers with a rug
plot and the outliers marked in red.  You can see how far along the tail
of the densitiy plot the outliers here are.  Most food and payment data
hovers much closer to $0$.

![png](files/ref_data/open_payments_files/open_payments_13_1.png)

Travel and Lodging Outliers
---

{% highlight python %}
travel_outliers = get_by_reason(outliers, 'Travel and Lodging').collect()
travel_inliers = get_by_reason(inliers, 'Travel and Lodging').collect()
display_info(travel_inliers, travel_outliers, 'Travel and Lodging', 10)
{% endhighlight %}

All I can say is that Physician 106320 travels far more than I do to get
paid $155k in 2013.  Hope that triple platinum on Delta is worth it. :)

<table id="resultTable8" class="tablesorter">
  <thead>
    <tr style="text-align: left;">
      <th>Physician</th>
      <th>Specialty</th>
      <th>Payer</th>
      <th>Amount</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td> 106320</td>
      <td>                  Allopathic &amp; Osteopathic Physicians/ Psychiatry &amp; Neurology/ Neurology</td>
      <td> Boehringer Ingelheim Pharma GmbH &amp; Co.KG</td>
      <td> $155,772.00</td>
    </tr>
    <tr>
      <td> 472722</td>
      <td>                      Allopathic &amp; Osteopathic Physicians/ Internal Medicine/ Nephrology</td>
      <td>          Merck Sharp &amp; Dohme Corporation</td>
      <td>  $75,000.00</td>
    </tr>
    <tr>
      <td> 371379</td>
      <td>                                Allopathic &amp; Osteopathic Physicians/ Orthopaedic Surgery</td>
      <td>                           Exactech, Inc.</td>
      <td>  $65,798.00</td>
    </tr>
    <tr>
      <td> 198801</td>
      <td>          Allopathic &amp; Osteopathic Physicians/ Internal Medicine/ Cardiovascular Disease</td>
      <td>                 Medtronic Vascular, Inc.</td>
      <td>  $41,232.80</td>
    </tr>
    <tr>
      <td> 382697</td>
      <td>                      Allopathic &amp; Osteopathic Physicians/ Internal Medicine/ Nephrology</td>
      <td>                          Genentech, Inc.</td>
      <td>  $39,978.80</td>
    </tr>
    <tr>
      <td> 169095</td>
      <td>                                            Allopathic &amp; Osteopathic Physicians/ Surgery</td>
      <td>                 Medtronic Vascular, Inc.</td>
      <td>  $37,683.00</td>
    </tr>
    <tr>
      <td>  80052</td>
      <td>                                    Allopathic &amp; Osteopathic Physicians/ Family Medicine</td>
      <td> Boehringer Ingelheim Pharma GmbH &amp; Co.KG</td>
      <td>  $24,911.25</td>
    </tr>
    <tr>
      <td> 202461</td>
      <td> Allopathic &amp; Osteopathic Physicians/ Thoracic Surgery (Cardiothoracic Vascular Surgery)</td>
      <td>                              Covidien LP</td>
      <td>  $21,594.51</td>
    </tr>
    <tr>
      <td> 378722</td>
      <td>                                  Allopathic &amp; Osteopathic Physicians/ Internal Medicine</td>
      <td>                    GlaxoSmithKline, LLC.</td>
      <td>  $20,112.40</td>
    </tr>
    <tr>
      <td> 243205</td>
      <td>       Allopathic &amp; Osteopathic Physicians/ Internal Medicine/ Interventional Cardiology</td>
      <td>                 Medtronic Vascular, Inc.</td>
      <td>  $19,273.90</td>
    </tr>
  </tbody>
</table>

You can see on the density plot that the next nearest outlier is pretty far away and we have clumps of outliers around 20k and 40k. Interesting things to look into. 


![png](files/ref_data/open_payments_files/open_payments_14_1.png)

Consulting Fee Outliers
---

{% highlight python %}
consulting_outliers = get_by_reason(outliers, 'Consulting Fee').collect()
consulting_inliers = get_by_reason(inliers, 'Consulting Fee').collect()
display_info(consulting_inliers, consulting_outliers, 'Consulting Fee', 10)
{% endhighlight %}

Looking at consulting fee outliers, you can see some clumping, but most
of the data is lower than 50k.  That makes the 200k outlier from Teva
all that much more interesting.  Of course, none of this is any
indication of wrong-doing, just interesting spikes in the data.


<table id="resultTable9" class="tablesorter">
  <thead>
    <tr style="text-align: center;">
      <th>Physician</th>
      <th>Specialty</th>
      <th>Payer</th>
      <th>Amount</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td> 104930</td>
      <td>                       Allopathic &amp; Osteopathic Physicians/ Psychiatry &amp; Neurology/ Neurology</td>
      <td>             Teva Pharmaceuticals USA, Inc.</td>
      <td> $207,500.00</td>
    </tr>
    <tr>
      <td> 151515</td>
      <td>                                                          Other Service Providers/ Specialist</td>
      <td>                         Alcon Research Ltd</td>
      <td> $150,000.00</td>
    </tr>
    <tr>
      <td> 309376</td>
      <td>                                       Allopathic &amp; Osteopathic Physicians/ Internal Medicine</td>
      <td>             Teva Pharmaceuticals USA, Inc.</td>
      <td> $137,559.67</td>
    </tr>
    <tr>
      <td> 231913</td>
      <td>                                     Allopathic &amp; Osteopathic Physicians/ Orthopaedic Surgery</td>
      <td>                             Exactech, Inc.</td>
      <td> $108,125.00</td>
    </tr>
    <tr>
      <td> 465481</td>
      <td>                         Allopathic &amp; Osteopathic Physicians/ Internal Medicine/ Rheumatology</td>
      <td>               Vision Quest Industries Inc.</td>
      <td> $102,196.09</td>
    </tr>
    <tr>
      <td> 409799</td>
      <td> Allopathic &amp; Osteopathic Physicians/ Internal Medicine/ Endocrinology, Diabetes &amp; Metabolism</td>
      <td>                                Pfizer Inc.</td>
      <td> $100,000.00</td>
    </tr>
    <tr>
      <td> 206227</td>
      <td>                                     Allopathic &amp; Osteopathic Physicians/ Orthopaedic Surgery</td>
      <td>                   DePuy Synthes Sales Inc.</td>
      <td>  $93,750.00</td>
    </tr>
    <tr>
      <td> 436192</td>
      <td>                                       Allopathic &amp; Osteopathic Physicians/ Internal Medicine</td>
      <td>                                Pfizer Inc.</td>
      <td>  $90,000.00</td>
    </tr>
    <tr>
      <td> 306965</td>
      <td>                       Allopathic &amp; Osteopathic Physicians/ Psychiatry &amp; Neurology/ Neurology</td>
      <td>             Teva Pharmaceuticals USA, Inc.</td>
      <td>  $64,125.00</td>
    </tr>
    <tr>
      <td> 163888</td>
      <td>               Allopathic &amp; Osteopathic Physicians/ Internal Medicine/ Cardiovascular Disease</td>
      <td> Boehringer Ingelheim Pharmaceuticals, Inc.</td>
      <td>  $61,025.00</td>
    </tr>
  </tbody>
</table>

You can see most of the density is less than 20k, which makes that 200k
outlier so interesting.

![png](files/ref_data/open_payments_files/open_payments_15_1.png)

Gift Outliers
---

{% highlight python %}
gift_outliers = get_by_reason(outliers, 'Gift').collect()
gift_inliers = get_by_reason(inliers, 'Gift').collect()
display_info(gift_inliers, gift_outliers, 'Gift', 10)
{% endhighlight %}

Gifts, I think, are the most interesting payment reasons in this whole
dataset.  I am intrigued when a physician might get a gift versus
an outright fee.  I imagined, going in, that gifts would be low-value
items, but the table clearly shows that dentists are getting substantial gifts.  What is most interesting to me is that all of the top 10 outliers are
dentists.

<table id="resultTable10" class="tablesorter">
  <thead>
    <tr style="text-align: center;">
      <th>Physician</th>
      <th>Specialty</th>
      <th>Payer</th>
      <th>Amount</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td> 225073</td>
      <td> Dental Providers/ Dentist/ General Practice</td>
      <td>  Dentalez Alabama, Inc.</td>
      <td> $56,422.00</td>
    </tr>
    <tr>
      <td> 167931</td>
      <td>                   Dental Providers/ Dentist</td>
      <td>        DENTSPLY IH Inc.</td>
      <td>  $8,672.50</td>
    </tr>
    <tr>
      <td> 380517</td>
      <td>                   Dental Providers/ Dentist</td>
      <td>        DENTSPLY IH Inc.</td>
      <td>  $8,672.50</td>
    </tr>
    <tr>
      <td> 380073</td>
      <td> Dental Providers/ Dentist/ General Practice</td>
      <td> Benco Dental Supply Co.</td>
      <td>  $7,570.00</td>
    </tr>
    <tr>
      <td> 403926</td>
      <td>                   Dental Providers/ Dentist</td>
      <td>             A-dec, Inc.</td>
      <td>  $5,430.00</td>
    </tr>
    <tr>
      <td> 429612</td>
      <td>                   Dental Providers/ Dentist</td>
      <td>           PureLife, LLC</td>
      <td>  $5,058.72</td>
    </tr>
    <tr>
      <td> 404935</td>
      <td>                   Dental Providers/ Dentist</td>
      <td>             A-dec, Inc.</td>
      <td>  $5,040.00</td>
    </tr>
    <tr>
      <td>   8601</td>
      <td> Dental Providers/ Dentist/ General Practice</td>
      <td>          DentalEZ, Inc.</td>
      <td>  $3,876.35</td>
    </tr>
    <tr>
      <td> 385314</td>
      <td> Dental Providers/ Dentist/ General Practice</td>
      <td>      Henry Schein, Inc.</td>
      <td>  $3,789.99</td>
    </tr>
    <tr>
      <td> 389592</td>
      <td> Dental Providers/ Dentist/ General Practice</td>
      <td>      Henry Schein, Inc.</td>
      <td>  $3,789.99</td>
    </tr>
  </tbody>
</table>



![png](files/ref_data/open_payments_files/open_payments_16_1.png)

Benford's Law Analysis
===

[Benford's Law](http://en.wikipedia.org/wiki/Benford's_law) is an
interesting observation made by physicist Frank Benford in the 30's
about the distribution of the first digits of many naturally occurring
datasets.  In particular, the distribution of digits in each position
follows an expected distribution.  

I will leave the explanation of why Benford's law exists to better
[sources](http://www.dspguide.com/ch34.htm), but this observation has
become a staple of forensic accounting analysis.  The reason for this
interest is that financial data often fits the law and humans, when
making up numbers, have a terrible time picking numbers that fit the
law.  In particular, we'll often intuit that digits should be more
uniformly distributed than they naturally occur.

Even so, violation of Benford's law alone is insufficient to cry foul.
It's only an indicator to be used with other evidence to point toward
fraud.  It can be a misleading indicator because not all datasets abide
by the law and it can be very sensitive to number of observations.

Ranking by Goodness of Fit
---

It's of interest to us to figure out if this payment data fits
the law and, if so, are there any payers who do not fit the law in a
strange way.  That leaves us with the technical challenge of determining how close two distributions are so that we can rank goodness of fit.  There are a few approaches to this problem:

* [$\chi^2$ Statistical Test](http://en.wikipedia.org/wiki/Chi-squared_test)
* [Kullback-Leibler Divergence](http://en.wikipedia.org/wiki/Kullback%E2%80%93Leibler_divergence)

They are two approaches, one statistical and one information-theoretic,
that will accomplish similar goals: tell how close two distributions are
together. 

I chose to rank based on KL divergence, but I compute the $\chi^2$ test as well.
I'll [quote](http://en.wikipedia.org/wiki/Kullback%E2%80%93Leibler_divergence) briefly about Kullback-Leibler divergence to give a sense of what the ranking means:

> The Kullback–Leibler divergence of Q from P, denoted $D_{KL}(P || Q)$
> , is a measure of the information lost 
> when Q is used to
> approximate P. The KL divergence measures the expected number of
> extra bits required to code samples from P when using a code based on
> Q, rather than using a code based on P. Typically P represents the
> "true" distribution of data, observations, or a precisely calculated
> theoretical distribution. The measure Q typically represents a theory,
> model, description, or approximation of P.

In our case, P is the expected distribution based on Benford's law and Q
is the observed distribution.  We'd like to see just how much more
"complex" in some sense Q is versus P.

The Benford Digit Distribution
---

For the first digit, Benford's law states that the probability of digit
$d$ occurring is $$P(d) = \log_{10}(1 + \frac{1}{d})$$.

There is a generalization beyond the first digit which can be used as
well.  The probability of digit $d$ occurring in the second digit is $$ P(d) = \sum_{k=1}^{9} \log_{10}(1 + \frac{1}{10k + d})$$.

{% highlight python %}
#Compute benford's distribution for first and second digit respectively
benford_1=np.array([0] + [math.log10(1+1.0/i) for i in xrange(1,10)])
benford_2=np.array([ sum( [ math.log10(1 + 1.0/(j*10 + i)) \
                            for j in xrange(1, 10) \
                          ]\
                        )\
                     for i in xrange(0,10)\
                   ]\
                  )
{% endhighlight %}

Implementation of Benford's Law Ranking
---

The way we'll approach this problem is to determine the first and second
digit distributions of payments by payer/reason and rank by goodness of fit for the first digit.  Then we'll look at the top fitting and bottom fitting payers for a few different reasons and see if we can see any patterns.  One caveat is that we'll be throwing out data under the following scenarios:

* The payer/specialty does not have at least 400 payments
* The amount is less than $10 (therefore not having a second digit)

Obviously the second one may skew things as it throws out some data
within a partition but not the whole partition.  In real analysis, I'd
find a way to include the data points.


{% highlight python %}

#Return a numpy array of zeros of specified $length except at
#position $index, which has value $value
def array_with_value(length, index, value):
    arr = np.zeros(length)
    arr[index] = value
    return arr

#Perform chi-square test between an expected probability
#distribution and a list of empirical frequencies.
#Returns the chi-square statistic and the p-value for the test.
def goodness_of_fit(emp_counts, expected_probabilities):
    #convert from probabilities to counts
    exp_distr = expected_probabilities*np.sum(emp_counts)
    return stats.chisquare(emp_counts, f_exp=exp_distr)
    
        
#For each (reason, payer) pair compute the first and second digit distribution
#for all payments.  Return a RDD with a ranked list based on likely goodness 
#of fit to the distribution of first digits predicted by Benford's "Law".
def benfords_law(min_payments=350):
    """
    Benford's "law" is a rough observation that the distribution of numbers 
    for each digit position of certain data fits a specific distribution.  
    It holds for quite a bit real-world data and, thus, has become of 
    interest to forensic accountants.
    
    This function computes the distribution of first and second digits for 
    each (reason, payer) pair and ranks them by goodness of fit to 
    Benford's Law based on the first digit distribution.
    In particular, the goodness of fit metric that it is ranked by is 
    kullback-liebler divergence, but chi-squared goodness of fit test 
    is also computed and the results are cached.
    """
    
    #We use this one quite a bit in reducers, so it's nice to have it handy here
    sum_values = lambda x,y:x+y

    #Project out the reason, payer, amount, and amount_str, throwing 
    #away amounts < 10 since they don't have 2nd digits.  This probably 
    #skews the results, so in real-life, I'd not throw out entries so
    #cavalierly, but for the purpose of simplicity, I've done it here.
    
    #Also, we're pulling out the first and second digits here
    reason_payer_amount_info = sqlContext.sql("""select reason
                                                      , payer
                                                      , amount
                                                      , amount_str 
                                                 from payments
                                              """)\
                                    .filter(lambda t:len(t[0]) > 3 
                                                 and t[2] > 9.99\
                                           ) \
                                    .map(lambda t: ( (t[1], t[0]) \
                                                   ,dict( payer=t[1]\
                                                        , reason=t[0] \
                                                        , first_digit=t[3][0] \
                                                        , second_digit=t[3][1] \
                                                        )\
                                                   )\
                                        )
    reason_payer_amount_info.take(1)
    
    #filter out the reason / payer combos that have fewer payments than 
    #the minimum number of payments                       
    reason_payer_count = reason_payer_amount_info.map(lambda t: (t[0],1)) \
                                                 .reduceByKey(sum_values) \
                                                 .filter(lambda t: \
                                                    t[1] > min_payments
                                                        )
                                              
    #inner join with the reason/payer's that fit the count requirement 
    #and annotate value with the num payments
    reason_payer_digits = \
      reason_payer_amount_info.join(reason_payer_count) \
                              .map(lambda t: (t[0] \
                                             , annotate_dict(join_lhs(t)\
                                                            , 'num_payments'\
                                                            , join_rhs(t)\
                                                            )\
                                              )\
                                   )
    #compute the first digit distribution.
    #First we count each of the 9 possible first digits, then we translate 
    #that count into a vector of dimension 10 with count for digit i 
    #in position i.  We then sum those vectors, thereby getting the 
    #full frequency per digit.
    first_digit_distribution = \
      reason_payer_digits.map(lambda t: ( (t[0], t[1]['first_digit'] ) , 1) ) \
                         .reduceByKey(sum_values) \
                         .map(lambda t: (t[0][0]\
                                        , array_with_value(10\
                                                          , int(t[0][1])\
                                                          , t[1]\
                                                          )\
                                        )\
                              ) \
                         .reduceByKey(sum_values)
    #same thing with the 2nd digit
    second_digit_distribution = \
      reason_payer_digits.map(lambda t: ( (t[0], t[1]['second_digit']) , 1) ) \
                         .reduceByKey(sum_values) \
                         .map(lambda t: (t[0][0]\
                                        , array_with_value(10\
                                                          , int(t[0][1])\
                                                          , t[1]\
                                                          )\
                                        )\
                              ) \
                         .reduceByKey(lambda x,y:np.array(x) + np.array(y))

    #We join the two, compute the goodness of fit based on chi-square test
    #and the distance from benford's distribution based on kl divergence.
    #Finally we sort by kl-divergence ascending (good fits come first).
    return first_digit_distribution\
            .join(second_digit_distribution) \
            .map(lambda t : ( t[0]\
                            , dict( payer=t[0][0] \
                                  , reason=t[0][1] \
                                  , first_digit_distr=join_lhs(t) \
                                  , second_digit_distr=join_rhs(t) \
                                  , first_digit_fit = \
                                      goodness_of_fit(join_lhs(t)[1:10] \
                                                     , benford_1[1:10] \
                                                     ) \
                                  , second_digit_fit = \
                                      goodness_of_fit(join_rhs(t), benford_2) \
                                  , kl_divergence = \
                                      stats.entropy( benford_1[1:10]\
                                                   , join_lhs(t)[1:10]\
                                                   )\
                                  ) \
                             ) \
                 ) \
            .map(lambda t : (t[1]['kl_divergence'], t[1]) )\
            .sortByKey(True) \
            .map(lambda t : ( (t[1]['payer'], t[1]['reason']), t[1]) )

benford_data = benfords_law(400)                   
{% endhighlight %}

{% highlight python %}
#Plot the distribution of first and second digit side-by-side for a set of payers.
def plot_figure(title,entries):
    num_rows = len(entries)
    fig, axes = plt.subplots(nrows=len(entries), ncols=2, figsize=(12,12))
    fig.tight_layout()
    plt.subplots_adjust(top=0.91, hspace=0.55, wspace=0.3)
    fig.suptitle(title, size=14)
    
    bar_width = .4
    
    for i,entry in enumerate(entries):
        first_ax = axes[i][0]
        first_digit_distr = entry[1]['first_digit_distr'][1:10]
        sample_label_1 = \
          """$\chi$={}, $p$={}, kl={}, n={}"""\
            .format( locale.format('%0.2f'\
                                 , float(entry[1]['first_digit_fit'][0])\
                                 ) \
                   , locale.format('%0.2f'\
                                  , float(entry[1]['first_digit_fit'][1])\
                                  )\
                   , locale.format('%0.2f'\
                                  , float(entry[1]['kl_divergence'])\
                                  )\
                   , int(np.sum(first_digit_distr))\
                   )

        first_digit_distr = first_digit_distr/np.sum(first_digit_distr)
        
        first_ax.bar(np.arange(1,10) ,first_digit_distr, alpha=0.35\
                    , color='blue', width=bar_width, label="Sample"\
                    )
        first_ax.bar(np.arange(1,10)+bar_width ,benford_1[1:10]\
                    , alpha=0.35, color='red', width=bar_width\
                    , label='Benford')
        first_ax.set_xticks(np.arange(1,10))
        first_ax.legend()
        first_ax.grid()
        
        first_ax.set_ylabel('Probability')
        first_ax.set_title("{} First Digit\n{}"\
                            .format(entry[0][0].encode('ascii', 'ignore')\
                                   , sample_label_1\
                                   )\
                          )
        
        second_ax = axes[i][1]
        second_digit_distr = entry[1]['second_digit_distr']
        sample_label_2 = \
          '$\chi$={}, $p$={}, n={}'\
            .format(locale.format('%0.2f'\
                   , float(entry[1]['second_digit_fit'][0])\
                                 ) \
                   , locale.format('%0.2f'\
                                  , float(entry[1]['second_digit_fit'][1])\
                                  )\
                   , int(np.sum(second_digit_distr))\
                   )
        second_digit_distr = second_digit_distr/np.sum(second_digit_distr)
        
        second_ax.bar(np.arange(0,10) ,second_digit_distr, alpha=0.35\
                     , color='blue', width=bar_width, label="Sample")
        second_ax.bar(np.arange(0,10) + bar_width,benford_2, alpha=0.35\
                     , color='red', width=bar_width, label='Benford')
        second_ax.set_xticks(np.arange(0,10))
        second_ax.legend()
        second_ax.grid()
        second_ax.set_ylabel('Probability')
        second_ax.set_title("{} Second Digit\n{}"\
                             .format(entry[0][0].encode('ascii', 'ignore')\
                                    , sample_label_2\
                                    )\
                           )
        
#Take n-worst or best (depending on t) entries for reason based on goodness
# of fit for benford's law and plot the first/second digit distributions 
#versus benford's distribution side-by-side as well as the distribution 
#of kl-divergences.
def benford_summary(reason, data = benford_data, n=5, t='best'):
    raw_data = data.filter(lambda t:t[0][1] == reason).collect()
    s = []
    if t == 'best':
        s=raw_data[0:n]
        plot_figure("Top 5 Best Fitting Benford Analysis for {}"\
                   .format(reason)\
                   , s\
                   )
    else:
        s=raw_data[-n:][::-1]
        plot_figure("Top 5 Worst Fitting Benford Analysis for {}"\
                   .format(reason)\
                   , s\
                   )
    plot_outliers( [d[1]['kl_divergence'] for d in raw_data]\
                 , [d[1]['kl_divergence'] for d in s]\
                 , reason + " KL Divergence"\
                 )
{% endhighlight %}

Best and Worst Fitting Payers for Gifts
---

Pretty much across the board the p-values for the $\chi^2$ test were
super weak, but the first 4 are a pretty good fit.  The last one is
interesting, that spike at the 6 digit is the kind of thing that are of interest to forensic accountants.  I repeat, however, that this is **not** an indicator which can be used safely alone to level a charge of fraud.

{% highlight python %}
# Gift Benford Analysis (Best)
benford_summary('Gift', t='best')
{% endhighlight %}    


<img src="files/ref_data/open_payments_files/open_payments_21_0.png"
     style="width:650px"
/>

Below is the density plot for the Kullback-Leibler divergences for the top best fits.  You can see there's a clump at 0 and a clump a bit farther out, but no real outliers.

![png](files/ref_data/open_payments_files/open_payments_21_1.png)

Now we look at the worst fitting gift payers.  The lists overlap, as you
can see, because there just aren't that many organizations that pay more
than 350 gifts out over the course of the year.

Two things that are interesting:

* Mentor Worldwide does not fitting the decreasing probability distribution that we would expect given the Benford distribution.  Many payers diverge from Benford's law, but it's interesting when they break the basic form of decreasing probabilities as digits progress from 1 through 9.
* Benco Dental Supply has a huge amount of payments starting with 1.  This is likely an indication that they have a standard gift that they give out.

{% highlight python %}
benford_summary('Gift', t='worst')
{% endhighlight %}


<img src="files/ref_data/open_payments_files/open_payments_22_0.png"
     style="width:650px"
/>


![png](files/ref_data/open_payments_files/open_payments_22_1.png)

Best and Worst Fitting Payers for Travel and Lodging 
---

{% highlight python %}
# Travel and Lodging Benford Analysis
benford_summary("Travel and Lodging")
{% endhighlight %}

The top fitting payers for travel and lodging fit fairly well, so it's
certainly possible to fit the distribution well.

<img src="files/ref_data/open_payments_files/open_payments_23_0.png"
     style="width:650px"
/>



![png](files/ref_data/open_payments_files/open_payments_23_1.png)



{% highlight python %}
benford_summary("Travel and Lodging", t='worst')
{% endhighlight %}

You can see a few payers diverging from the general form of Benford's
distribution here.  LDR Holding is the outlier in terms of goodness of
fit as you can see from the density plot below as well.

<img src="files/ref_data/open_payments_files/open_payments_24_0.png"
     style="width:650px"
/>



![png](files/ref_data/open_payments_files/open_payments_24_1.png)

Best and Worst Fitting Payers for Consulting Fees
---

{% highlight python %}
benford_summary("Consulting Fee")
{% endhighlight %}

The good fits are pretty good.
<img src="files/ref_data/open_payments_files/open_payments_25_0.png"
     style="width:650px"
/>

![png](files/ref_data/open_payments_files/open_payments_25_1.png)



{% highlight python %}
benford_summary("Consulting Fee", t='worst')
{% endhighlight %}

For both UCB and Merck, we see a payer with a huge distribution of payments starting with 7.  This indicates a standardized payment of some sort, I'd wager.  The interesting thing about Merck is that the 1's distribution is pretty spot on, the rest of the density gets pushed into 7.

<img src="files/ref_data/open_payments_files/open_payments_26_0.png"
     style="width:650px"
/>



![png](files/ref_data/open_payments_files/open_payments_26_1.png)

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
