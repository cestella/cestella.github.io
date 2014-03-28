---
title: First Post

excerpt: This is the first post for my technical blog.

location: Cleveland, OH
layout: blog-post

---


First off, thank you very much for visiting my technical blog.
I want to take a brief moment to express what is on my pipeline
to cover over the next few months in this space.

In general, you'll find my personal projects and, subsequently,
the blog entries surrounding them to be quasi-mathematical or
statistical and generally in the domain of machine learning
or data structures.

* I'd like to write a series of entries around some novel
applications of some standard unsupervised learning techniques
to do some textual analysis of Wikileaks data.
* I'd like to port and talk about the counting/resizeable bloom
  filter in Hadoop to C++ and discuss that process.
* I'd like to port and talk about the lock free concurrent skip
  list in the Java standard library to C++.

If I can do half of that in a year, I'll be overjoyed.  Anyway,
I hope you enjoy the posts.

$$
\begin{align*}
  & \phi(x,y) = \phi \left(\sum_{i=1}^n x_ie_i, \sum_{j=1}^n y_je_j
\right)
  = \sum_{i=1}^n \sum_{j=1}^n x_i y_j \phi(e_i, e_j) = \\
  & (x_1, \ldots, x_n) \left( \begin{array}{ccc}
      \phi(e_1, e_1) & \cdots & \phi(e_1, e_n) \\
      \vdots & \ddots & \vdots \\
      \phi(e_n, e_1) & \cdots & \phi(e_n, e_n)
    \end{array} \right)
  \left( \begin{array}{c}
      y_1 \\
      \vdots \\
      y_n
    \end{array} \right)
\end{align*}
$$
