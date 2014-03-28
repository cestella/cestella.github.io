---
layout: blog
title: Home
section: Home

keywords: Blog, Programming, Machine Learning
---

Casey's Technical Ramblings
=====================

This is a place where I attempt to form coherent thoughts about current
technology, computer science, math and the general things happening
on the Internet.

[![Feed icon](/files/css/feed-icon-14x14.png){:title="Atom feed of recent posts" .right}][feed]
A [feed][] of the most recent posts is available.

[feed]: atom.xml

Recent Posts
------------

{% for cat in site.categories limit:5 %}
<li>{{ cat[0] }}</li>
{% endfor %}

{% for post in site.categories.blog limit:5 %}
<div class="section list">
  <h1>{{ post.date | date_to_string }}</h1>
  <p class="line">
  <a class="title" href="{{ post.url }}">{{ post.title }}</a>
  <a class="comments" href="{{ post.url }}#disqus_thread">View Comments</a>
  </p>
  <p class="excerpt">{{ post.excerpt }}</p>
</div>
{% endfor %}

<script type="text/javascript">
//<![CDATA[
(function() {
		var links = document.getElementsByTagName('a');
		var query = '?';
		for(var i = 0; i < links.length; i++) {
			if(links[i].href.indexOf('#disqus_thread') >= 0) {
				query += 'url' + i + '=' + encodeURIComponent(links[i].href) + '&';
			}
		}
		document.write('<script charset="utf-8" type="text/javascript" src="http://disqus.com/forums/caseystechnicalblog/get_num_replies.js' + query + '"></' + 'script>');
	})();
//]]>
</script>
