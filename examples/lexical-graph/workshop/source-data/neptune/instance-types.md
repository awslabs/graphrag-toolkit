

# **Choosing instance types for Amazon Neptune**

Amazon Neptune offers a number of different instance sizes and families. that offer different capabilities suited to different graph workloads. This section is meant to help you choose the best instance type for your needs.
For the pricing of each instance-type in these families, please see the [Neptune pricing page](https://aws.amazon.com/neptune/pricing/).

## Overview of instance resource allocation

Each Amazon EC2 instance type and size used in Neptune offers a defined amount of compute (vCPUs) and system memory. The primary storage for Neptune is external to the DB instances in a cluster, which lets compute and storage capacity scale independently of each other.

This section focuses on how the compute resources can be scaled, and on the differences between each of the various instance families.

In all instance families, vCPU resources are allocated to support two (2) query execution threads per vCPU. This support is dictated by the instance size. When determining the proper size of a given Neptune DB instance, you need to consider the possible concurrency of your application and the average latency of your queries. You can estimate the number of vCPUs needed as follows, where latency is measured as the average query latency in seconds and concurrency is measured as the target number of queries per second:

```
vCPUs=(latencyxconcurrency)/2
```

>Note
SPARQL queries, openCypher queries, and Gremlin read queries that use the DFE query engine can, under certain circumstances, use more than one execution thread per query. When initially sizing your DB cluster, start with the assumption that each query will consume a single execution thread per execution and scale up if you observe back pressure into query queue. This can be observed by using the `/gremlin/status`, `/oc/status`, or `/sparql/status` APIs, or it can also be observed using the `MainRequestsPendingRequestsQueue` CloudWatch metric.


System memory on each instance is divided into two primary allocations: buffer pool cache and query execution thread memory.

Approximately two thirds of the available memory in an instance is allocated for buffer-pool cache. Buffer-pool cache is used to cache the most recently used components of the graph for faster access on queries that repeatedly access those components. Instances with a larger amount of system memory have larger buffer pool caches that can store more of the graph locally. A user can tune for the appropriate amount of buffer-pool cache by monitoring the buffer cache hit and miss metrics available in CloudWatch.

You may want to increase the size of your instance if the cache hit rate drops below 99.9% for a consistent period of time. This suggests that the buffer pool is not big enough, and the engine is having to fetch data from the underlying storage volume more often than is efficient.

The remaining third of system memory is distributed evenly across query execution threads, with some memory remaining for the operating system and a small dynamic pool for threads to use as needed. The memory available for each thread increases slightly from one instance size to the next up to an `8xl` instance type, at which size the memory allocated per thread reaches a maximum.

The time to add more thread memory is when you encounter an `OutOfMemoryException` (OOM). OOM exceptions occur when one thread needs more than the maximum memory allocated to it (this is not the same as the entire instance running out of memory).

## `t3` and `t4g` instance types

The `t3` and `t4g` family of instances offers a low-cost option for getting started using a graph database and also for initial development and testing. These instances are eligible for the Neptune [free-tier offer](https://aws.amazon.com/neptune/free-trial/), which lets new customers use Neptune at no cost for the first 750 instance hours used within a standalone AWS account or rolled up underneath an AWS Organization with Consolidated Billing (Payer Account).

The `t3` and `t4g` instances are only offered in the medium size configuration (`t3.medium` and `t4g.medium`).

They are not intended for use in a production environment.

Because these instances have very constrained resources, they are not recommended for testing query execution time or overall database performance. To assess query performance, upgrade to one of the other instance families.

## `r4` family of instance types

*DEPRECATED*  – The `r4` family was offered when Neptune was launched in 2018, but now newer instance types offer much better price/performance. As of engine version [1.1.0.0](https://docs.aws.amazon.com/neptune/latest/userguide/engine-releases-1.1.0.0.html), Neptune no longer supports `r4` instance types.

## `r5` family of instance types

The `r5` family contains memory-optimized instance types that work well for most graph use cases. The `r5` family contains instance types from `r5.large` up to `r5.24xlarge`. They scale linearly in compute performance as you increase in size. For example, an `r5.xlarge` (4 vCPUs and 32GiB of memory) has twice the vCPUs and memory of an `r5.large` (2 vCPUs and 16GiB of memory), and an `r5.2xlarge` (8 vCPUs and 64GiB of memory) has twice the vCPUs and memory of an `r5.xlarge`. You can expect query performance to scale directly with compute capacity up to the `r5.12xlarge` instance type.

The `r5` instance family has a 2-socket Intel CPU architecture. The `r5.12xlarge` and smaller types use a single socket and the system memory owned by that single-socket processor. The `r5.16xlarge` and `r5.24xlarge` types use both sockets and available memory. Because there's some memory-management overhead required between two physical processors in a 2-socket architecture, the performance gains scaling up from a `r5.12xlarge` to a `r5.16xlarge` or `r5.24xlarge` instance type are not as linear as you get scaling up at the smaller sizes.

## `r5d` family of instance types

Neptune has a [lookup-cache feature](https://docs.aws.amazon.com/neptune/latest/userguide/feature-overview-lookup-cache.html) that can be used to improve the performance of queries which need to fetch and return large numbers of property values and literals. This feature is used primarily by customers with queries that need to return many attributes. The lookup cache boosts performance of these queries by fetching these attribute values locally rather than looking up each one over and over in Neptune indexed storage.

The lookup cache is implemented using a NVMe-attached EBS volume on an `r5d` instance type. It is enabled using a cluster's parameter group. As data is fetched from Neptune indexed storage, property values and RDF literals are cached within this NVMe volume.

If you don't need the lookup cache feature, use a standard `r5` instance type rather than an `r5d`, to avoid the higher cost of the `r5d`.

The `r5d` family has instance types in the same sizes as the `r5` family, from `r5d.large` to `r5d.24xlarge`.

## `r6g` family of instance types

AWS has developed its own ARM-based processor called [Graviton](https://aws.amazon.com/ec2/graviton/), that delivers better price/performance than the Intel and AMD equivalents. The `r6g` family uses the Graviton2 processor. In our testing, the Graviton2 processor offers 10-20% better performance for OLTP-style (constrained) graph queries. Larger, OLAP-ish queries, however, may be slightly less performant with the Graviton2 processors than with Intel ones owing to slightly less performant memory-paging performance.

It's also important to note that the `r6g` family has a single-socket architecture, which means that performance scales linearly with compute capacity from an `r6g.large` to an `r6g.16xlarge` (the largest type in the family).

## `r6i` family of instance types

[Amazon R6i instances](https://aws.amazon.com/ec2/instance-types/r6i/) are powered by 3rd-generation Intel Xeon Scalable processors (code named Ice Lake) and are an ideal fit for memory-intensive workloads. As a general rule they offer up to 15% better compute price performance and up to 20% higher memory bandwidth per vCPU than comparable R5 instance types.

## `x2g` family of instance types

Some graph use cases see better performance when instances have larger buffer-pool caches. The `x2g` family was launched to better support those use cases. The `x2g` family has a larger memory-to-vCPU ratio than the `r5` or `r6g` family. The `x2g` instances also use the Graviton2 processor, and have many of the same performance characteristics as `r6g` instance types, as well as a larger buffer-pool cache.

If you're using `r5` or `r6g` instance types with low CPU utilization and a high buffer-pool cache miss rate, try using the `x2g` family instead. That way, you'll be getting the additional memory you need without paying for more CPU capacity.

## `r8g` family of instance types

The `r8g` family contains memory-optimized instance types powered by AWS Graviton4 processors. These instances offer significant performance improvements over previous generations, making them well-suited for memory-intensive graph workloads. The r8g instances provide approximately 15-20% better performance for graph queries compared to r7g instances.

The `r8g` family has a single-socket architecture, which means that performance scales linearly with compute capacity from an `r8g.large` to an `r8g.16xlarge` (the largest type in the family).

Key features of the `r8g` family include:

* Powered by AWS Graviton4 ARM-based processors
* Higher memory bandwidth per vCPU compared to previous generations
* Excellent price/performance ratio for both OLTP-style (constrained) graph queries and OLAP-style analytical workloads
* Improved memory management capabilities that benefit complex graph traversals

The `r8g` family is ideal for production workloads that require high memory capacity and consistent performance. They're particularly effective for applications with high query concurrency requirements.

## `r7g` family of instance types

The `r7g` family uses the AWS Graviton3 processor, which delivers better price/performance than previous Graviton2-based instances. In testing, the Graviton3 processor offers 25-30% better performance for OLTP-style graph queries compared to r6g instances.

Like the `r6g` family, the `r7g` family has a single-socket architecture, which means that performance scales linearly with compute capacity from an `r7g.large` to an `r7g.16xlarge` (the largest type in the family).

Key features of the `r7g` family include:

* Powered by AWS Graviton3 ARM-based processors
* Improved memory-paging performance compared to r6g, benefiting both OLTP and OLAP workloads
* Enhanced buffer-pool cache efficiency
* Lower latency for memory-intensive operations

The `r7g` family is well-suited for production environments with varied query patterns and is particularly effective for workloads that benefit from improved memory bandwidth.

## `r7i` family of instance types

The `r7i` family is powered by 4th-generation Intel Xeon Scalable processors (code named Sapphire Rapids) and offers significant improvements over r6i instances. These instances provide approximately 15% better compute price/performance and up to 20% higher memory bandwidth per vCPU than comparable r6i instance types.

The `r7i` instance family has a 2-socket Intel CPU architecture, similar to the `r5` family. The `r7i.12xlarge` and smaller types use a single socket and the system memory owned by that single-socket processor. The `r7i.16xlarge` and `r7i.24xlarge` types use both sockets and available memory. Because there's some memory-management overhead required between two physical processors in a 2-socket architecture, the performance gains scaling up from a `r7i.12xlarge` to a `r7i.16xlarge` or `r7i.24xlarge` instance type are not as linear as you get scaling up at the smaller sizes.

Key features of the `r7i` family include:

* Powered by 4th-generation Intel Xeon Scalable processors
* Performance scales linearly with compute capacity up to r7i.12xlarge
* Enhanced memory management between physical processors in the 2-socket architecture
* Improved performance for memory-intensive graph operations

For all of these instance families, you can estimate the number of vCPUs needed using the same formula mentioned previously:

```
vCPUs=(latencyxconcurrency)/2
```

Where latency is measured as the average query latency in seconds and concurrency is measured as the target number of queries per second.

## `serverless` instance type

The [Neptune Serverless](https://docs.aws.amazon.com/neptune/latest/userguide/neptune-serverless.html) feature can scale instance size dynamically based on a workload's resource needs. Instead of calculating how many vCPUs are needed for your application, Neptune Serverless lets you [set lower and upper limits on compute capacity](https://docs.aws.amazon.com/neptune/latest/userguide/neptune-serverless-capacity-scaling.html) (measured in Neptune Capacity Units) for the instances in your DB cluster. Workloads with varying utilization can be cost-optimized by using serverless rather than provsioned instances.

You can set up both provisioned and serverless instances in the same DB cluster to achieve an optimal cost-performance configuration.
