# Redirection and Identity
This repo is for a project that studies the implicit semantics of redirection and how properties of the redirection graphs.

The repo consists of the code, the results, and the redirect graphs.
The identity graphs are online at https://sameas.cc.

We perform 4 samplings. The sampled entities are then examined under the HTTP GET request.
The chains of redirection are stored as pairs of URIs. These pairs are the edges of the redirection graphs.

These sampled files are:
- ite_uniform... the edges of redirection graph corresponding to uniform samplings
- cc_sample_2... the sampling regarding connected components of size 2, 3-10, 10+, respectively.

The Python scripts are:
- chains_check.py for the sampling of 100 chains for manual check.
- pairs_check.py for the sampling of 4,000 pairs (edges) for manual check.

The sampled data files are:
- sample4000.tsv for the 4,000 sampled edges.
- sampled_chains_100.tsv 100 sampled chains with greater than 2 hops of redirect.

##### Shuai Wang @ VU Amsterdam
##### shuai.wang@vu.nl
#### All rights reserved.
