# sample 100 redirect links from ite_uniform_sampling_redirect_edges.nt

from random import sample
from rfc3987 import  parse
import urllib.parse


def get_name (e):
	name = ''
	prefix = ''
	sign = ''
	if e.rfind('/') == -1 : # the char '/' is not in the iri
		if e.split('#') != [e]: # but the char '#' is in the iri
			name = e.split('#')[-1]
			prefix = '#'.join(e.split('#')[:-1]) + '#'
			sign = '#'
		else:
			name = None
			sign = None
			prefix =  None
	else:
		name = e.split('/')[-1]
		prefix = '/'.join(e.split('/')[:-1]) + '/'
		sign = '/'

	return prefix, sign, name



f = open("ite_uniform_sampling_redirect_edges.nt", "r")

pairs = set()

count = 0
for x in f:
	sts = x.split(" ")
	s = sts[0]
	p = sts[1]
	o = sts[2]
	subj = s[1:-1]
	obj = o[1:-2]
	pairs.add((subj, obj))

print ('There are in total: ',len (pairs), ' entries')






# Next we sample 100 pairs from 116031
num_sample = 4000
sample100 = sample(list(pairs), num_sample)
count_http_https = 0
count_https_http = 0
count_encoding = 0
count_resource_page = 0
count_page_resource = 0
count_miss_hash = 0
count_namespace = 0
count_capital = 0
count_html = 0
count_json = 0
count_rdf = 0
count_owl = 0
count_other = 0

output_file_name = 'sample' + str(num_sample)+'.tsv'
f = open(output_file_name, "w")
f.write('Subject' + '\t'+ 'Object' + '\t'+ 'Annotation' +'\n')

print ('sampled ', len (sample100), ' entries')
for (subj, obj) in sample100:
	annotation = 'other'
	# print ('\nsubj = ', subj)
	# print ('obj = ', obj)
	if ('http' in subj and 'https' in obj and subj[4:] == obj[5:]): # if they only differ in http https:
		annotation = 'http->https'
		count_http_https += 1
	elif ('https' in subj and 'http' in obj and subj[5:] == obj[4:]): # if they only differ in http https:
		annotation = 'https->http'
		count_https_http += 1
	elif obj == subj+'.json': # if they only differ in http https:
		annotation = '+json'
		count_json += 1
	elif obj == subj+'.html': # if they only differ in http https:
		annotation = '+html'
		count_html += 1
	elif obj == subj+'.rdf': # if they only differ in http https:
		annotation = '+rdf'
		count_rdf += 1
	elif obj == subj+'.owl': # if they only differ in http https:
		annotation = '+owl'
		count_owl += 1
	else: # if they differ only in encoding
		variance = set()
		uq = urllib.parse.unquote(subj)
		variance.add(uq)
		prefix, sign, name  = get_name(subj)
		quote_name = urllib.parse.quote(name)
		new_iri = prefix + quote_name
		variance.add(new_iri)
		if obj in variance:
			annotation = 'encoding'
			count_encoding += 1
		else:
			s_prefix, s_sign, s_name  = get_name(subj)
			o_prefix, o_sign, o_name  = get_name(obj)
			if s_name == o_name and s_prefix != o_prefix and urllib.parse.urlparse(subj).netloc == urllib.parse.urlparse(obj).netloc: # if only different in namespace: updated_namespace
				annotation = 'updated_namespace'
				count_namespace += 1
				if ('dbpedia.org/resource' in subj and 'dbpedia.org/page' in obj):
					count_resource_page +=1
					annotation = 'dbpedia_resource_to_page'
				elif ('dbpedia.org/page' in subj and 'dbpedia.org/resource' in obj):
					count_page_resource +=1
					annotation = 'dbpedia_page_to_resource'
			elif s_prefix == o_prefix and s_name!= o_name and s_name.lower() == o_name.lower():
				annotation == 'capital'
				count_capital += 1
			else:
				variance = set()
				uq = urllib.parse.unquote(obj)
				variance.add(uq)

				quote_name = urllib.parse.quote(o_name)
				new_iri = o_prefix + o_sign+ quote_name
				variance.add(new_iri)

				if subj in variance and s_name != o_name:
					annotation = 'encoding'
					count_encoding +=1

				elif (subj.split('#')[0] == obj):
					count_miss_hash += 1
					annotation = 'miss_hash'
				else:
					print ('\nTBD: subj = ', subj)
					print ('TBD: obj = ', obj)

	if annotation == 'other':
		count_other += 1

	f.write(subj + '\t'+ obj + '\t'+ annotation +'\n')

f.close()

print ('#num_sample', num_sample)

print ('#http -> https: ', count_http_https)
print(" -> %0.1f"% (count_http_https/num_sample*100), '%')

print ('#https -> http: ', count_https_http)
print(" -> %0.1f"% (count_https_http/num_sample*100), '%')
# count_html = 0
# count_json = 0
# count_rdf = 0
# count_owl = 0
print ('\n# +html: ', count_html)
print(" -> %0.1f"% (count_html/num_sample*100), '%')

print ('# +json: ', count_json)
print(" -> %0.1f"% (count_json/num_sample*100), '%')

print ('# +rdf: ', count_rdf)
print(" -> %0.1f"% (count_rdf/num_sample*100), '%')

print ('# +owl: ', count_owl)
print(" -> %0.1f"% (count_owl/num_sample*100), '%')


print ('\n#encoding : ', count_encoding)
print(" -> %0.1f"% (count_encoding/num_sample*100), '%')

print ('#hash convention: ', count_miss_hash)
print(" -> %0.1f"% (count_miss_hash/num_sample*100), '%')

print ('#namespace (within the same domain): ', count_namespace)
print(" -> %0.1f"% (count_namespace/num_sample*100), '%')

print ('\t#DBpedia resource -> page: ', count_resource_page)
print("\t - %0.1f"% (count_resource_page/num_sample*100), '%')

print ('\t#DBpedia page -> resource: ', count_page_resource)
print("\t - %0.1f"% (count_page_resource/num_sample*100), '%')

print ('#upper or lower case: ', count_capital)
print(" -> %0.1f"% (count_capital/num_sample*100), '%')

print ('#remaining (TBD) = ', count_other)
print(" -> %0.1f"% (count_other/num_sample*100), '%')

#
# file =  open(file_name, 'w', newline='')
# 	writer = csv.writer(file,  delimiter='\t')
# 	writer.writerow([ "Entity", "Remark"])
#

# print ('')
# updated_namespace
# missing_hash
# other
# capital
