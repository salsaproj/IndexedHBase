IndexedHBase
============

IndexedHBase is a storage system that extends HBase with a customizable indexing
framework to support fast queries and analysis of interesting data subsets.
Leveraging an architecture based on YARN, IndexedHBase can be integrated with
various parallel computing platforms, such as Hadoop MapReduce and Twister, to
complete efficient analysis of the query results.

IndexedHBase has been successfully used to support Truthy, a public social media
observatory that allows analysis of large scale (Hundred TBs) of social
networking data collected through Twitter’s streaming application program
interface (API). We have demonstrated the reproducibility of previous social
media data analysis experiments enabled by IndexedHBase in a recent paper.
Meanwhile, IndexedHBase is currently employed to support multiple Truthy
research projects related to topics such as meme lifetime distribution analysis
and academic information diffusion on Twitter. We automatically get data from
public Twitter streams, split them into partitions, then parse and index such
data daily into IndexedHBase. With multiple parallel partition loaders, loading
of one day's data can finish within a few hours. With extensions based on the
Storm stream processing engine, we can support non-trivial parallel analysis
applications such as clustering over streaming data, and process the 10% Twitter
stream (gardenhose) in real time.

Furthermore, based on the experience in IndexedHBase, we propose a general
customizable and scalable indexing framework that can be implemented on top of
many NoSQL databases, such as Cassandra, MongoDB, etc. 

The official website of IndexedHBase is
http://salsaproj.indiana.edu/IndexedHBase/, and the API page is
http://salsaproj.indiana.edu/IndexedHBase/coreHBase94JavaAPI/index.html.

Instructions about running the Truthy application commands on the Moe cluster
are available at http://salsaproj.indiana.edu/IndexedHBase/truthyCmdMoe.html.
(TODO some API of this html page need to be updated)

A detailed document about configuration of the Moe cluster is available at
conf/moeConfiguration.md.
