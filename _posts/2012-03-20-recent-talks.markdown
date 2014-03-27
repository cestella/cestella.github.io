---
title: Better News through Computational Political Science

excerpt: I recently gave a talk on a NLP project that I worked on for Kent's ACM

location: Cleveland, OH
layout: blog-post

---

In the last year this blog has sort of languished.  
I have been busy giving talks and doing personal projects.

One of the most interesting things that I've had the pleasure of doing is
giving a talk on some NLP work that I did in my free time.  In particular, I
talked about using computational political science to try to classify
arbitrary news articles as politically biased using a variety of techniques.

In particular, I found this great dataset of political speeches from the members
of the 111th congress.  Using an [ideal-point mapping](http://jackman.stanford.edu/blog/?p=2084) onto the real spectrum from [voteview.com](http://www.voteview.com),
I had an ample set of training data and a model with some statistical soundness to
use to classify those documents into political buckets.

From this it's an easy to use a variety of supervised learning techniques to map arbitrary text
onto a political spectrum.  The wonderful people at Kent video taped the whole talk so everyone can
see just how poor of a public speaker I am.

* The video is [here](http://www.youtube.com/watch?v=pvFxcxEHLas)
* The slide deck (in PDF form) is [here](https://github.com/cestella/better_news/blob/master/presentation/Better_News.pdf?raw=true).
* The source code used on my github account [here](https://github.com/cestella/better_news).
