# Hbase 2.1.0 with rdma

This is a part of RDMA for HBase, The Sixth Student RDMA Programming Competition, 2018. [RDMA library for hbase](https://github.com/recolic/infinity),
which is also part of the project.

# License
As for my changes, they are under Apache License, the same as Hbase. If you
wish to redistributed it in any other license,
please send me a email and we can discuss it(zouyoo@outlook.com)


# Documentations
I will upload it [here](https://z-y00.github.io/tech/rdma_2018) later. Please
feel free to ask me any question about the code.

# Current status
On master branch, we are using kind of sequencial connection, which is very slow but mostly debugged and tested.

On parallel_conn branch, one thread may be reading while the other is writing to the connection.
Commits after tag 0.1.4 are trying to optimize it, you can just drop those
commits.

