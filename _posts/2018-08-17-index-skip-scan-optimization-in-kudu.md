---
layout: post
title: "Index Skip Scan Optimization in Kudu"
author: Anupama Gupta
---

This summer I got the opportunity to intern with the Apache Kudu Team at Cloudera.
My project was to optimize the Kudu scan path by implementing a technique called
[index skip scan][1].

<!--more-->

Let's begin with discussing the current query flow in Kudu.
Consider the following table:

![png]({{ site.github.url }}/img/index-skip-scan/example-table.png){: .img-responsive}

*Sample rows of Table `metrics` (sorted by key columns).*

In this case, by default, Kudu internally builds a primary key index (implemented as a
[B-tree](https://en.wikipedia.org/wiki/B-tree)) for the table `metrics`.
As shown in the table above, the index data is sorted by the composite of all key columns.
When the user query contains the first key column (`host`), Kudu uses the index (as the index data is
primarily sorted on the first key column).

Now, what if the user query does not contain the first key column and instead contains any of the non-first key column(s)
(`tstamp` and/or `clusterid`)? In this case, since the column value might be present anywhere in the index structure,
the current query execution plan does not use the index. Instead, a full table scan is done by default.
To optimize this scan time, a possible solution is to build secondary index on the required key column (although, it might be
redundant to build secondary index on composite key column).
However, we do not consider this solution as Kudu does not support secondary indexes yet.

The question is, can we still do better than full table scan here?

The answer is yes! Let's observe the column(s) preceding the `tstamp` column (we will refer them as
`prefix columns` and their specific value as `prefix key`). Note that the prefix keys are sorted in the index and,
all rows of a given prefix key are also sorted by the remaining key columns.
Therefore, we can use the index to seek to the rows containing distinct prefix keys
and satisfying the query predicate on the `tstamp` column.

For example, consider the query:
SELECT clusterid FROM metrics WHERE tstamp = 100;

![png]({{ site.github.url }}/img/index-skip-scan/skip-scan-example-table.png){: .img-responsive}
*Skip scan flow illustration. The rows in green are scanned and the rest are skipped.*

The query server can use the index to **scan** all rows for which `host` = `helium` and `tstamp` = 100 and consequently,
**skip** all the rows for which host = `helium` and `tstamp` != 100
(holds true for all distinct keys of `host` such as `ubuntu`, `westeros`).
Hence, this method is popularly known as skip scan optimization.

## Performance

This optimization can speed up queries significantly, depending on the cardinality (number of distinct values) of the
prefix column(s). Lower the prefix column cardinality, better the skip scan performance. In fact, when the prefix column
cardinality is high, skip scan is not a viable approach. The performance graph (obtained using the example schema
and query pattern mentioned earlier) is shown below.

Based on our experiments, on up to 10 million rows per tablet (as shown below), we found that the skip scan performance begins to get worse
with respect to the full table scan performance when the prefix column(s) cardinality exceeds ![](https://latex.codecogs.com/gif.latex?%5Csqrt%7B%5C%23total%20rows%7D).
Therefore, in order to use skip scan performance benefits when possible and maintain a consistent performance with respect to the
prefix column(s) cardinality, we decide to dynamically disable skip scan when the number of seeks for distinct prefix
keys exceeds ![](https://latex.codecogs.com/gif.latex?%5Csqrt%7B%5C%23total%20rows%7D).
It will be an interesting take to further explore sophisticated heuristics to decide when
to dynamically disable skip scan.

![png]({{ site.github.url }}/img/index-skip-scan/skip-scan-performance-graph.png){: .img-responsive}

## Conclusion

Skip scan optimization in Kudu can lead to huge performance benefits that scale with the size of
data in Kudu tables. An important point to note is that although, in the above specific example, the number of prefix
columns is 1(`host`), this approach is generalized to work with any number of prefix columns.
Currently, this is a work-in-progress [patch](https://gerrit.cloudera.org/#/c/10983/).

The current implementation also lays the groundwork to leverage the skip scan approach and
optimize query processing time in the following use cases:

- Range predicates on the non-first key columns(s)
- IN list predicates on the key column(s)

This was my first time working on an open source project. I thoroughly enjoyed working on this challenging problem,
right from understanding the scan path in Kudu to working on a full fledged implementation of
skip scan approach. I am very grateful to the Kudu Team for guiding and supporting me throughout the
internship period.


[1]: https://oracle-base.com/articles/9i/index-skip-scanning/
[2]: https://storage.googleapis.com/pub-tools-public-publication-data/pdf/42851.pdf

