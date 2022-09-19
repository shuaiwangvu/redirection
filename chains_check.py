# This script samples 100 chains and perform some analysis.
# 1) it performs sampling of chains with greater than 2 hops of redirection.
# 2) it then studies what these chains of redirection about.
#  - For those that are redirection within the same domain, it outputs the domain and the corresponding percentage
#  - For those that are not in the same domain (redirection cross domains), we label them as `multiple'
# The results are in the column `Comment'
# The analytical results are in file checking_result_pairs.txt
# the sampled chains are in file sampled_chains_100.tsv

# All rights reserved.
# Shuai Wang
# @ VU Amsterdam 

from random import sample
from rfc3987 import  parse
import urllib.parse
import networkx as nx
import pandas as pd
from collections import Counter
from random import sample


redirect_graph = nx.DiGraph()

f = open("ite_uniform_sampling_redirect_edges.nt", "r")
# f = open("cc_sample_2_redirect_edges.nt", "r")
# f = open("cc_samples_3_9_redirect_edges.nt", "r")
# f = open("cc_samples_10_redirect_edges.nt", "r")

pairs = set()


for x in f:
	sts = x.split(" ")
	s = sts[0]
	p = sts[1]
	o = sts[2]
	subj = s[1:-1]
	obj = o[1:-2]
	pairs.add((subj, obj))
	redirect_graph.add_edge(subj, obj)


print ('There are in total: ',len (pairs), ' redirections')
print ('There are in total: ', redirect_graph.number_of_edges(), ' edges')
print ('There are in total: ', redirect_graph.number_of_nodes(), ' nodes')



# Load data from a CSV file into a Pandas DataFrame
df = pd.read_csv("ite_uniform_sampling_redirect_nodes.tsv", sep='\t')
# df = pd.read_csv("cc_sample_2_redirect_nodes.tsv", sep='\t')
# df = pd.read_csv("cc_samples_3_9_redirect_nodes.tsv", sep='\t')
# df = pd.read_csv("cc_samples_10_redirect_nodes.tsv", sep='\t')




print("\nReading the CSV file...\n", df)

count_not_in_graph = 0
count_in_graph = 0
count_redirected_entities = 0

for index, row in df.iterrows():
	e = row['Entity']
	r = row['Remark']
	# print(e, r)
	if e in redirect_graph.nodes():
		redirect_graph.nodes[e]['label'] = r
		if 'Redirected' in r:
			count_redirected_entities += 1
		count_in_graph += 1
	else:
		count_not_in_graph += 1

# print ('number of redirected entities (in the redirection graph, not only about the original graph): ', count_redirected_entities)
# print ('count_not_in_graph = ', count_not_in_graph)
# print ('count_in_graph = ', count_in_graph)



# sorted_nodes = list(nx.topological_sort(redirect_graph))
# # print (sorted_nodes)
# s1 = sorted_nodes[-1]
# print ('first element: ',s1)

# 'http://lod.b3kat.de/isbn/9282604500'

#
# while len(list(redirect_graph.successors(s1))) != 0:
# 	scc_s1 = list(redirect_graph.successors(s1))[0]
# 	print ('successor = ', scc_s1)
# 	s1 = scc_s1


def find_path(nd):
	path = [nd]
	while len(list(redirect_graph.successors(nd))) != 0:
		nd_ss = list(redirect_graph.successors(nd))[0]
		path.append(nd_ss)
		nd =  nd_ss

	return path



if not nx.is_directed_acyclic_graph(redirect_graph): # it is DAG:
	print('Not a DAG')
	print (sorted(nx.strongly_connected_components(redirect_graph), key=len, reverse=True)[0])
	# print([len(c) for c in ])
else:
	print ('it is a DAG')

	l_path = nx.dag_longest_path(redirect_graph)
	print('the longest path = ',l_path)
	print ('it has ', len(l_path) - 1, ' hops')

	ct = Counter ()
	ct_path_length = Counter ()

	# RedirectedUntilTimeout
	# RedirectedUntilNotFound
	# RedirectedUntilError
	# RedirectedUntilLanded

	redi_paths = []

	for n in redirect_graph.nodes():
		if len(list(redirect_graph.predecessors(n))) != 0:
			p = find_path(n)
			path_length = len(p)
			lb = None
			try:
				lb = redirect_graph.nodes[n]['label']
			except Exception as e:
				print (n)
				pass

			# print(n, path_length)
			if (lb == 'RedirectedUntilTimeout'):
				ct_path_length ['RedirectedUntilTimeout'] += path_length
				ct ['RedirectedUntilTimeout'] += 1
				if len(p)>2:
					redi_paths.append((lb, p))
			elif (lb == 'RedirectedUntilNotFound'):
				ct_path_length ['RedirectedUntilNotFound'] += path_length
				ct ['RedirectedUntilNotFound'] += 1
				if len(p)>2:
					redi_paths.append((lb, p))
			elif (lb == 'RedirectedUntilError'):
				ct_path_length ['RedirectedUntilError'] += path_length
				ct ['RedirectedUntilError'] += 1
				if len(p)>2:
					redi_paths.append((lb, p))
			elif (lb == 'RedirectedUntilLanded'):
				ct_path_length ['RedirectedUntilLanded'] += path_length
				ct ['RedirectedUntilLanded'] += 1
				if len(p)>2:
					redi_paths.append((lb, p))
			# elif path_length != 1:
			# 	print (n, lb, path_length)

	print (ct)
	print ('total number of entities redirected = ',sum(ct.values()))
	print ('average number of hops = ', sum(ct_path_length.values())/sum(ct.values()) -1)
	print ('----------------')
	print (ct_path_length)
	for k in ['RedirectedUntilTimeout', 'RedirectedUntilNotFound','RedirectedUntilError','RedirectedUntilLanded']:
		print (k)
		if ct[k] != 0:
			print ('has average num of hop = ', ct_path_length[k]/ct[k] - 1)
		else:
			print('zero for ct[k]')

	num_sample = 100
	samples = sample(redi_paths, num_sample)

	output_file_name = 'sampled_chains_' + str(num_sample)+'.tsv'
	f = open(output_file_name, "w")
	f.write('Label' + '\t'+ 'Comment' '\t'+ 'Num_Hops' + '\t'+ 'PATH' +'\n')

	count_multiple_domain = 0
	count_same_domain = 0

	count_chains_dbpedia = 0 # dbpedia.org
	count_chains_wikidata = 0 # wikidata.org
	count_chains_d_nb = 0 #d-nb.info
	count_chains_viaf = 0 #viaf.org
	count_chains_zdb = 0 #zdb-services.de
	count_chains_bibsonomy = 0 #bibsonomy.org

	acc_path_dbpedia = 0

	for (label, path) in samples:
		# print ('label = ', label)
		# print ('path = ', path)
		annotation = None
		for p in path:
			p_domain = urllib.parse.urlparse(p).netloc
			# print (p)
			# print ('has domain ', p_domain)
			if 'www' == p_domain[:3]:
				p_domain = p_domain[4:]

			if annotation == None:
				annotation = p_domain
			else:
				if p_domain != annotation:
					annotation = 'multiple'
					break

		if annotation == 'dbpedia.org':
			count_chains_dbpedia +=1 # dbpedia.org
			acc_path_dbpedia += len(path)
		elif annotation == 'wikidata.org':
			count_chains_wikidata +=1 # wikidata.org
		elif annotation == 'd-nb.info':
			count_chains_d_nb +=1  #d-nb.info
		elif annotation == 'viaf.org':
			count_chains_viaf +=1 #viaf.org
		elif annotation == 'zdb-services.de':
			count_chains_zdb +=1 #zdb-services.de
		elif annotation == 'bibsonomy.org':
			count_chains_bibsonomy += 1 #bibsonomy.org
		elif annotation == 'multiple':
			count_multiple_domain += 1
		else:
			count_same_domain += 1
		f.write(label + '\t' + annotation + '\t' + str(len(path)) + '\t' + str(path) +'\n')

count_same_domain += count_chains_dbpedia + count_chains_wikidata + count_chains_d_nb + count_chains_viaf + count_chains_zdb + count_chains_bibsonomy

print ('#dbpedia chains = ', count_chains_dbpedia)
print ('\tavg length chains DBpedia = ', acc_path_dbpedia/count_chains_dbpedia)
print ('#wikidata chains = ', count_chains_wikidata)
print ('#d-nb chains = ', count_chains_d_nb)
print ('#viaf chains = ', count_chains_viaf)
print ('#zdb chains = ', count_chains_zdb)
print ('#bibsonomy chains = ', count_chains_bibsonomy)
print ('\n#multiple domain = ', count_multiple_domain)
print ('#same domain = ', count_same_domain)
