---
title: A Blockchain Story Told Through The Eyes of Two Users
excerpt: Cryptocurrencies are generating a lot of hype right now.  Blockchain-based systems provide a huge amount of transparent detail about how a currency is actually used.  Despite this fact, analysis has been largely based around traditional security analysis which ignores the full amount of data available in favor of simpler metrics which treat the system as a black box.  Here, we look at two deeper analytics that tell a story of how a cryptocurrency is actually used, which may be of interest to blockchain developers and investors alike.

location: Cleveland, OH
layout: blog-post

---

Blockchains are Big Data
---

I saw a commercial for Enterprise blockchains by Oracle during a football game this weekend.
I'll just pause to let that sink in.  It is undeniable that this 
little (slightly) esoteric corner distributed computing is fully riding
the hype train right now.  It's no doubt that the runup in price by the
core cryptocurrencies combined with pointed skepticism from mainline
economists and financial analysts are driving interest in the technology.  
It's the perfect mixture of nerdiness, drama and money to pique the
interests of even the most bloodless in the tech industry.

I'm a [data scientist](https://www.linkedin.com/in/casey-stella-84b9a11) working in a very specific niche, a "Big Data" data scientist.  When blockchains came to my notice the sheer transparency of them was damned near inescapably exciting.  Traditionally things like currencies operate like a black box, where we look at the inputs, the outputs and try our best to develop our best guesses of what's going on inside the black box.  With blockchains, due to the fact that they are essentially immutable ledgers of transactions, we can crack open the nut and get at the juicy transaction details kept inside.

Blockchains as they stand right now operate at relatively anemic transaction rates as compared to other financial transaction systems that we use in our daily lives (e.g. Visa).  Also, they've been around for a somewhat limited amount of time.  These things together put into question whether this truly is a "big data" problem or just a regular data problem.  I contend, and hopefully will show in a bit here, that nontrivial analysis of blockchains puts us in a "medium data, big compute" territory.  As such, this fits well within my preferred toolchain of [Apache Spark](http://spark.apache.org), Python and Jupyter.

Ethereum: Putting the Virtual Machine on the Chain
---

The most attractive blockchain to analyze, for me, is Ethereum.  From [Wikipedia](https://en.wikipedia.org/wiki/Ethereum):

> Ethereum is an open-source, public, blockchain-based distributed computing platform featuring smart contract (scripting) 
> functionality. Ether is a cryptocurrency whose blockchain is maintained by the Etherium platform, which provides a 
> distributed ledger for transactions. Ether can be transferred between accounts and used to compensate participant 
> nodes for computations performed. Ethereum provides a decentralized Turing-complete virtual machine, the Ethereum 
> Virtual Machine (EVM), which can execute scripts using an international network of public nodes. "Gas", an internal
> transaction pricing mechanism, is used to mitigate spam and allocate resources on the network.

I like a couple aspects of this project:

* It seems to have a broad vision; the blockchain as a platform for smart contracts is enticing
* It's moving away from a proof of work model, which results in huge energy consumption
* Gathering transaction data from [geth](https://github.com/ethereum/go-ethereum/wiki/geth), the ethereum node, is do-able via a JSON-RPC interface they provide.

The thing that I like the most, however, is that it seems to be a
multi-use chain.  You see a lot happening on this blockchain:

* [Cat breeding games](https://www.prnewswire.com/news-releases/cryptokitties-the-worlds-first-ethereum-game-launches-today-660494083.html)
* A proper cryptocurrency (named Ether)
* Other cryptocurrencies (e.g.  [ERC-20](https://en.wikipedia.org/wiki/ERC20)) and [initial coin offerings](https://en.wikipedia.org/wiki/Initial_coin_offering)

This seems like an ideal place to look for some analytics that might yield insights on how this chain works on a day-to-day basis.  And, most interestingly to investors, how changes in usage might impact price.

The Tale of Two Users
---

It's easy to say we should be looking at advanced analytics using the full data from the blockchain.  It's quite a different story to actually suggest *what* to look at here.  I will proceed from a couple of observations:

* Transaction data forms a graph, so we can borrow machinery from Graph Theory if necessary
* There are at least two interesting actors in this scenario: the new user and the established player

I maintain that these are two interesting actors insomuch as observing the blockchain transactions through the lens of these users will yield insights as to the general health, well-being and state of the blockchain.  If either of these actors change their behavior appreciably, it's worth knowing and will probably have some impact on the fundamental usage patterns of the blockchain in question.  Maybe even, if we're very lucky, give us a hint on how the price may change.

So, now we're left with a couple of challenges:

* Formally defining these two actors in such a way that we can distinguish between them could be computationally daunting
* What should we measure through the lens of these actors?

Starting from the bottom, I think a sensible starting point here is to measure the daily percentage of transactions being done by each of these actors.  Plotting this opposite the price, we may see the effect that each of these actors may have on the price.

The New User Impact
---

We'll call the daily percentage of transactions involving a hash never before seen to be the "new user impact." Just the act of picking out hashes that have never been seen before can be rather daunting given that there have been $20,321,934$ distinct hashes between ethereum's inception and January 18, 2018.  Doing this sort of analysis belies a simple SQL query but is well within Spark's sweet spot of enabling more low level operations and distributed computing primitives.  Judicious usage of bloom filters in Spark opens us up to performing these kinds of computations in a scalable way.

<img src="files/ref_data/ethereum_analysis/new_hashes.png"
     style="width:1024px"
/>

Here we have the timerange between January 1st, 2017 until January 18, 2018 with the closing price per day plotted opposite the percentage of the daily transactions involving a hash never before seen (the daily new user impact).

Note the discordant nature of the new user impact and how little correlation to price is happening prior to mid-November.  This is in stark contrast to the run-up in price and strong connection to the new user impact that happens from mid-November until early January.  The fascinating thing here is that the new user impact seems to dip prior to the price dip in early January.  It's unclear whether this is a reliable early indicator (especially given its chaos earlier in the year), but it's certainly worth investigating.  It is somewhat unfortunate how volatile the new user impact becomes from mid-December onward.


The Established Player Impact
---

In contrast to the "new user" as an actor, whose definition is easy to pin down in a technical way, the established player is tougher to specify in a rigorous way.  Given the fact that the transactions on a blockchain form a graph, we can borrow from graph analytics some tooling to help us out.  Specifically, we will define an "established player" for a specific day to be a hash such that the [undirected pagerank](https://en.wikipedia.org/wiki/PageRank#PageRank_of_an_undirected_graph) of the hash is in the top 10% of pageranks given the transaction graph of the previous 14 days.  The intuition here is that this will define a set of "important" hashes in the network.  Tracking how much of the network operates from these important hashes daily will give us some idea of the impact of the big players, such as exchanges and market makers, in the network. 

<img src="files/ref_data/ethereum_analysis/pagerank_plot.png"
     style="width:1024px"
/>

Here we have an abbreviated range of July 2017 until January 18th, 2018 mostly because it's fairly costly to compute the pagerank of even 2 weeks worth of transaction data (note that a more serious analysis would imply more serious compute and thus might adjust these parameters).

The thing I immediately notice here is that like the new user impact, the established player impact seems to couple with the price starting in mid-November.  Also, similar to the new user impact, it deviates prior to the actual cost drop, but is decidedly less chaotic immediately prior to the mid-January dip and thus possibly more reliable.

In Conclusion
---

I am reminded of one of my favorite lessons as a consulting data
scientist: "Garbage In $\implies$ Garbage Out".  To put a finer point on
it, it is imperative to monitor the makeup of your data as it changes 
over time, because if you cannot trust your data, then you cannot 
trust your conclusions. That is the core impulse behind this exercise, find some
core analytic to summarize behavior of the network from a particular
vantage point (or set of vantage points).

Responsibility demands that we be silent on drawing solid conclusions of 
predictive leading indicators of price per se.  Rather, stepping 
back, we now have the beginnings of a set of analytics which we can
monitor over time to better understand how ethereum, as a blockchain, moves, lives and 
breathes on a day-to-day basis.  Inflection points in these analytics
tie to usage shifts and assumptions in the technical analysis of this
blockchain should be reevaluated or else risk becoming stale or
less-effective.  For instance, if we see a precipitous drop in the new
user impact over a week, then either users are not using the chain
(which you can see in early 2017 in the "New User Impact" plot) or
ethereum has reached saturation (i.e. no new users, but still much usage).  
For a young blockchain, new user usage is imperative for robust growth 
and thus it'll be a turning point when the chain is saturated.

Thinking beyond this analysis, I plan to go on and look at some of the other graph theoretic analytics that we can track over time in both Ethereum as well as other established blockchains, most obviously Bitcoin:

* The number of [transaction triangles](https://www.geeksforgeeks.org/number-of-triangles-in-a-undirected-graph/) per day to get an indication of the transaction movement in the chain
* The number of "communities" in the transaction graph by applying a [label propagation algorithm](https://en.wikipedia.org/wiki/Label_Propagation_Algorithm) to the transaction graph daily.
