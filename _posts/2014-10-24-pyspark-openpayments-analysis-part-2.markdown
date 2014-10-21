---
title: Data Science and Hadoop&#58; Data Overview and Preprocessing

excerpt: Data Overview and preprocessing for Center for Medicare and Medicaid open payments data.

location: Cleveland, OH
layout: blog-post

---

As part of the Social Security Act, the Center for Medicare and Medicaid Services has begun to publish [data](http://www.cms.gov/OpenPayments/Explore-the-Data/Dataset-Downloads.html) detailing the relationship between physicians and medical institutions. 

From the Open Payments [website](http://www.cms.gov/OpenPayments):

>Sometimes, doctors and hospitals have financial relationships with
>health care manufacturing companies. These relationships can include
>money for research activities, gifts, speaking fees, meals, or travel.
>The Social Security Act requires CMS to collect information from
>applicable manufacturers and group purchasing organizations (GPOs) in
>order to report information about their financial relationships with
>physicians and hospitals.

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

Data Size
---

Ultimately, this dataset isn't very large, it's a single database of
63 columns across 2,626,674 rows.  In fact, I could have done all this analysis on my laptop.  So, why would I do this with Hadoop?  Well, hopefully the techniques here scale to much larger datasets.  Also, some of the analysis can be complex or involved once we get past the basic understanding of the data.  Fundamentally, I'm trying to demonstrate how to get non-obvious analytical work done on the platform.

Overall Purpose
---

This is not my attempt to find fraud in this dataset, but rather to show 
how some interesting anomalies of the data show up when we scratch the
surface.  For the purpose of this analysis, I refer to physicians by
ID's rather than names.  This is merely a demonstration of techniques on
the Hadoop platform for doing this sort of analysis.

The Platform: PySpark + IPython = $\heartsuit$
===

When I'm in the mode of tinkering with data, trying to find patterns or
insights, so I generally choose a language/ecosystem with good support
for that kind of thing.  This generally comes down to R or Python. I chose to do this analysis in Python for a few reasons.


* There is quite a bit of data science tooling around Python
* IPython Notebook gives a great visual interface for tinkering with data
* Spark has more robust bindings for Python than R

Preliminaries and Preprocessing
===

Before processing this data, I prepped the data from it's original
quoted CSV into a ctrl-A separated file without quotes using Apache Pig
and the magnificent CSVExcelStorage loader to parse quoted CSV files.

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

Up Next
---

[Next](pyspark-openpayments-analysis-part-3.html), we do some basic
structural analysis of the data with Spark SQL.
