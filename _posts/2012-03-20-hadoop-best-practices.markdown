---
title: Hadoop Best Practices

excerpt: I recently gave a talk at the Cleveland Hadoop User Group on Hadoop Best Practices

location: Cleveland, OH
layout: blog-post

---

In the course of my day-to-day work, I've had the opportunity to work with home-grown Map-Reduce
frameworks as well as Hadoop.  On the whole, I can say that I'm really pleased with the stability
that Hadoop provides.  As such, I've had the opportunity to talk at a few venues about Hadoop
and map reduce related things.

We at Explorys are trying very hard to build up a vibrant big-data community in Cleveland.  Part of
that effort surrounds the care and feeding of a [user group](http://www.meetup.com/Cleveland-Hadoop/).
Doing my part, I spoke recently about the set of best practices that I recommend based on my experiences
with Hadoop.

In the course of preparing the slide deck, I wanted to demonstrate a fully working, self-contained example
demonstrating my preferred methods to testing and designing a map reduce job.  Also, I wanted the job to be
slightly more complex than the canonical word-count example, yet not so complex that the average developer
would not be able to wrap their head around it in the course of an hour.

I ended up going with doing an analysis of a smallish dataset that I keep of senatorial speeches from the 111th congress.
I wanted to, particularly, investigate the words that were most "important" for each political grouping.  In order
to do that, I computed the [IDF](http://en.wikipedia.org/wiki/Okapi_BM25) ranking function for each word and trimmed the top-k
words according to that ranking function.

* A more detailed summary of the project and the code for the project is [here](https://github.com/cestella/SenatorialSpeechInvestigation).
* The slide deck is [here](https://github.com/cestella/SenatorialSpeechInvestigation/raw/master/political-nlp-analysis/src/main/latex/CHUG_presentation.pdf).
