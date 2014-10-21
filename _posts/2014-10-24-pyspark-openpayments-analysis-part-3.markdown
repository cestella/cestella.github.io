---
title: Data Science and Hadoop&#58; Basic Structural Analysis
excerpt: Basic structural analysis of healthcare payment data using Spark SQL and Python.
location: Cleveland, OH
layout: blog-post

---

For the first portion of the analysis, we'll perform some basic queries
to get a sense of the data's shape and size.  This data is
fundamentally relational (it's a single table), so thankfully a query
language was invented a while ago to assist us in this task.  It works
well, so we won't write complex MapReduce or spark to recreate it.

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

Technical Details
===
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


Spark SQL
---
We'll use [Spark SQL](https://spark.apache.org/sql/) to overlay a structure on the data.  I have chosen to use Spark SQL, but I also could have hooked Hive up to do these queries.  There are tradeoffs for both, Spark SQL turns into direct Spark operations, whereas Hive is calling out externally to Hive, so you aren't building up a RDD lazily.  However, Hive is more mature and supports more SQL.  For our purposes we are using very simple queries, so we'll stick to Spark SQL.

Note that the output of the Spark SQL call is
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

Types and Money
---

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
===

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
===

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
===
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
===

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
===

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
===

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

Up Next
---

[Next](pyspark-openpayments-analysis-part-4.html), we do some outlier
analysis on the payment data. This is part of a
broader [series](pyspark-openpayments-analysis.html) of posts about Data
Science and Hadoop.
