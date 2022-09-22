# this script is for testing only and is not being maintained anymore
import time
import networkx as nx
import sys
import csv
import requests
from requests.exceptions import Timeout
import pickle
# import re
# from urllib.parse import urlparse

# function that accepts a string
# extracts the url from that string
# return the extracted url
# def extract_url(string):
#     # regex to extract url from string
#     url_regex = 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
#     url = re.findall(url_regex, string)
#     print(url)
#     return url[0]


sameas = 'http://www.w3.org/2002/07/owl#sameAs'
my_redirect = "https://krr.triply.cc/krr/metalink/def/redirectedTo"  # a relation

NOTFOUND = 1
FOUNDWITHOUTREDIRECT = 2
ERROR = 3
TIMEOUT = 4
REDIRECT = 5

standard_timeout = (0.01, 0.05)
retry_timeout = (0.5, 2.5)
final_try_timeout = (1, 5)


def find_redirects(iri, timeout=standard_timeout):
    try:
        collect_urls = []
        response = requests.get(iri, timeout=timeout, allow_redirects=True) # HEAD requests instead of GET for performance reasons?

        if response.status_code == 404:
            return NOTFOUND, None

        if response.history:
            if response.url == iri:  # instead of this we do: encodingequivalent
                return FOUNDWITHOUTREDIRECT, None
            else:  # return graph that distinguishing encoding equivalent and redirect
                # print("Request was redirected")
                for resp in response.history:
                    # print(resp.status_code, resp.url)
                    # TODO save in sep file(kvstore?)
                    collect_urls.append(resp.url)
                # print("Final destination:")
                # print(response.status_code, response.url)

                collect_urls.append(response.url)
                return REDIRECT, collect_urls  # also return the ee_url
        else:
            # print("Request was not redirected")
            #print(response.status_code)
            return FOUNDWITHOUTREDIRECT, None
    except Timeout:
        # print('The request timed out', iri)
        return TIMEOUT, None
    except Exception as e:
        #print (f'error: ', iri, e)
        return ERROR, None


def load_entities(graph_id):
    entities = set()
    with open(graph_id, encoding='utf-8') as in_file:
        csvfile = csv.reader(in_file)
        for line in csvfile:
            # print(line)
            entities.add(line[0])
            #entities.add(extract_url(line))
    return entities


def export_redirect_graph_edges(file_name, graph):
    # count_line = 0
    with open(file_name, 'w') as output:
        for (l, r) in graph.edges():
            line = '<' + l + '> '
            line += '<' + my_redirect + '> '
            if r[0] == '"':
                if "^^" in r:
                    line += '' + r + ' .\n'
                    # edited = splited[-2][1:-1] # keep that original one
                    # example "http://xmlns.com/foaf/0.1/"^^<http://www.w3.org/2001/XMLSchema#anyURI>
                else:  # else, add the following
                    line += '' + r + "^^<http://www.w3.org/2001/XMLSchema#string>" + ' .\n'
                    # edited = splited[-2][1:-1] + "^^<http://www.w3.org/2001/XMLSchema#string>"
            else:
                line += '<' + r + '>. \n'
            output.write(str(line))
            # count_line += 1
    # print ('count line = ', count_line)


def export_redirect_graph_nodes(file_name, graph):
    file = open(file_name, 'w', newline='')
    writer = csv.writer(file,  delimiter='\t')
    writer.writerow(["Entity", "Remark"])
    for n in graph.nodes:
        if graph.nodes[n]['remark'] == 'not_found':
            writer.writerow([n, 'NotFound'])
        elif graph.nodes[n]['remark'] == 'found_without_redirect':
            writer.writerow([n, 'NotRedirect'])
        elif graph.nodes[n]['remark'] == 'error':
            writer.writerow([n, 'Error'])
        elif graph.nodes[n]['remark'] == 'timeout':
            writer.writerow([n, 'Timeout'])
        elif graph.nodes[n]['remark'] == 'redirected':
            writer.writerow([n, 'Redirected'])
        elif graph.nodes[n]['remark'] == 'redirect_until_timeout':
            writer.writerow([n, 'RedirectedUntilTimeout'])
        elif graph.nodes[n]['remark'] == 'redirect_until_error':
            writer.writerow([n, 'RedirectedUntilError'])
        elif graph.nodes[n]['remark'] == 'redirect_until_found':
            writer.writerow([n, 'RedirectedUntilLanded'])
        elif graph.nodes[n]['remark'] == 'redirect_until_not_found':
            writer.writerow([n, 'RedirectedUntilNotFound'])
        else:
            print('Error: ', graph.nodes[n]['remark'])


def create_redi_graph(graph_id):
    start = time.time()
    redi_graph = nx.DiGraph()

    entities_to_test = load_entities(graph_id)
    print('there are ', len(entities_to_test), ' entities in the graph ')

    for entity in entities_to_test:
        redi_graph.add_node(entity, remark='unknown')

    count_not_found = 0
    count_found_without_redirect = 0
    count_error = 0
    count_timeout = 0
    count_redirect_until_timeout = 0
    count_redirect_until_not_found = 0
    count_redirect_until_error = 0
    count_redirect_until_found = 0
    count_other = 0

    for timeout_parameters in [standard_timeout, retry_timeout, final_try_timeout]:
        timeout_entities = set()
        for e in redi_graph.nodes():
            if redi_graph.nodes[e]['remark'] == 'unknown':
                entities_to_test.add(e)
        for e in entities_to_test:
            result, via_entities = find_redirects(
                e, timeout=timeout_parameters)
            if result == NOTFOUND:
                redi_graph.nodes[e]['remark'] = 'not_found'
                count_not_found += 1
            elif result == FOUNDWITHOUTREDIRECT:
                redi_graph.nodes[e]['remark'] = 'found_without_redirect'
                count_found_without_redirect += 1
            elif result == ERROR:
                redi_graph.nodes[e]['remark'] = 'error'
                count_error += 1
            elif result == TIMEOUT:
                timeout_entities.add(e)
                redi_graph.nodes[e]['remark'] = 'timeout'
                if final_try_timeout == timeout_parameters:
                    count_timeout += 1
            elif result == REDIRECT:
                redi_graph.nodes[e]['remark'] = 'redirected'
                if via_entities[0] != e:
                    # the resolved IRI is in a different encoding
                    # print ('working on ', e)
                    # print ('error at the first! ')
                    # print ('via_entities', via_entities)
                    # redi_graph.add_node(t, remark = 'unknown')
                    # redi_graph.add_edge(e, via_entities[0])
                    via_entities = [e] + via_entities
                if len(via_entities) > 1:
                    for i, s in enumerate(via_entities[:-1]):
                        t = via_entities[i+1]
                        if s not in redi_graph.nodes():
                            redi_graph.add_node(s, remark='redirected')
                        else:
                            redi_graph.nodes[s]['remark'] = 'redirected'

                        # if t not in redi_graph.nodes():
                        redi_graph.add_node(t, remark='unknown')

                        redi_graph.add_edge(s, t)

                    valid_remarks = [
                        'error', 'found_without_redirect', 'not_found']
                    last_redirect = via_entities[-1]
                    if not redi_graph.nodes[last_redirect]['remark'] in valid_remarks:
                        result, _ = find_redirects(
                            last_redirect, timeout=standard_timeout)
                        #print(last_redirect, result)
                        if result == NOTFOUND:
                            redi_graph.nodes[last_redirect]['remark'] = 'not_found'
                            count_redirect_until_not_found += 1
                           # print(last_redirect)
                        elif result == FOUNDWITHOUTREDIRECT:
                            redi_graph.nodes[last_redirect]['remark'] = 'found_without_redirect'
                            count_redirect_until_found += 1
                        elif result == ERROR:
                            redi_graph.nodes[last_redirect]['remark'] = 'error'
                            count_redirect_until_error += 1
                    if not redi_graph.nodes[last_redirect]['remark'] in valid_remarks:
                        result, _ = find_redirects(
                            last_redirect, timeout=retry_timeout)
                        #print(last_redirect, result)
                        if result == NOTFOUND:
                            redi_graph.nodes[last_redirect]['remark'] = 'not_found'
                            count_redirect_until_not_found += 1
                        elif result == FOUNDWITHOUTREDIRECT:
                            redi_graph.nodes[last_redirect]['remark'] = 'found_without_redirect'
                            count_redirect_until_found += 1
                           # print(last_redirect)
                        elif result == ERROR:
                            redi_graph.nodes[last_redirect]['remark'] = 'error'
                            count_redirect_until_error += 1
                    if not redi_graph.nodes[last_redirect]['remark'] in valid_remarks:
                        result, _ = find_redirects(
                            last_redirect, timeout=final_try_timeout)
                        #print(last_redirect, result)
                        if result == NOTFOUND:
                            redi_graph.nodes[last_redirect]['remark'] = 'not_found'
                            count_redirect_until_not_found += 1
                        elif result == FOUNDWITHOUTREDIRECT:
                            redi_graph.nodes[last_redirect]['remark'] = 'found_without_redirect'
                            count_redirect_until_found += 1
                            # print(last_redirect)
                        elif result == ERROR:
                            redi_graph.nodes[last_redirect]['remark'] = 'error'
                            count_redirect_until_error += 1
                    if redi_graph.nodes[last_redirect]['remark'] not in valid_remarks:
                        redi_graph.nodes[last_redirect]['remark'] = 'timeout'
                        count_redirect_until_timeout += 1
                else:
                    print('error: ', via_entities)

                # print ('\n')
                # for v in via_entities:
                # 	print ('after update ',v,' with mark ', redi_graph.nodes[v]['remark'], ' with outdegree', redi_graph.out_degree(v))
            else:
                print('error')

        print('TIMEOUT: there are ', len(timeout_entities), ' timeout entities')
        entities_to_test = timeout_entities

        for e in redi_graph.nodes():

            if redi_graph.nodes[e]['remark'] == 'redirected' and redi_graph.out_degree(e) == 0:
                print('ERROR:')
                print(e, ' was redirected but has out-degree 0')
                print(e, ' was redirected is with in-degree ',
                      redi_graph.in_degree(e))
                # if e in graph.nodes():
                # 	print ('it is in the original graph!')

    update_against = set()
    for n in redi_graph.nodes():
        if redi_graph.nodes[n]['remark'] != 'redirected':
            update_against.add(n)

    # count_redirect_until_timeout = 0

    for n in redi_graph.nodes():

        if redi_graph.nodes[n]['remark'] == 'redirected':
            # print ('updating node : ', n)
            for m in update_against:
                if nx.has_path(redi_graph, n, m):
                    if redi_graph.nodes[m]['remark'] == 'timeout' or redi_graph.nodes[m]['remark'] == 'redirect_until_timeout':
                        redi_graph.nodes[n]['remark'] = 'redirect_until_timeout'
                        # print ('\tupdating against m = ', redi_graph.nodes[m]['remark'])
                    elif redi_graph.nodes[m]['remark'] == 'not_found' or redi_graph.nodes[m]['remark'] == 'redirect_until_not_found':
                        redi_graph.nodes[n]['remark'] = 'redirect_until_not_found'
                        # print ('\tupdating against m = ', redi_graph.nodes[m]['remark'])
                    elif redi_graph.nodes[m]['remark'] == 'error' or redi_graph.nodes[m]['remark'] == 'redirect_until_error':
                        redi_graph.nodes[n]['remark'] = 'redirect_until_error'
                        # print ('\tupdating against m = ', redi_graph.nodes[m]['remark'])
                    elif redi_graph.nodes[m]['remark'] == 'found_without_redirect' or redi_graph.nodes[m]['remark'] == 'redirect_until_found':
                        redi_graph.nodes[n]['remark'] = 'redirect_until_found'
                        # print ('\tupdating against m = ', redi_graph.nodes[m]['remark'])
                    else:
                        # pass
                        print('Error? reaching m = ',
                              redi_graph.nodes[m]['remark'])

            # redirected until redirected
            if redi_graph.nodes[n]['remark'] == 'redirected':

                print('\n\nredirected but not to anywhere?')
                print('entitiy: ', n)
                print('outdegree: ', redi_graph.out_degree(n))
                print('indegree: ', redi_graph.in_degree(n))
                result, via_entities = find_redirects(
                    n, timeout=timeout_parameters)
                print('result ', result)
                print('via_entities = ', via_entities)
                redi_graph.nodes[n]['remark'] = 'redirect_until_timeout'

    # for n in redi_graph.nodes():
    #     if redi_graph.nodes[n]['remark'] == 'not_found':
    #         count_not_found += 1
    #     elif redi_graph.nodes[n]['remark'] == 'found_without_redirect':
    #         count_found_without_redirect += 1
    #     elif redi_graph.nodes[n]['remark'] == 'error':
    #         count_error += 1
    #     elif redi_graph.nodes[n]['remark'] == 'timeout':
    #         count_timeout += 1
    #     elif redi_graph.nodes[n]['remark'] == 'redirect_until_timeout':
    #         count_redirect_until_timeout += 1
    #     elif redi_graph.nodes[n]['remark'] == 'redirect_until_not_found':
    #         count_redirect_until_not_found += 1
    #     elif redi_graph.nodes[n]['remark'] == 'redirect_until_error':
    #         count_redirect_until_error += 1
    #     elif redi_graph.nodes[n]['remark'] == 'redirect_until_found':
    #         count_redirect_until_found += 1
    #     else:
    #         print('strange : ', redi_graph.nodes[n]['remark'])
    #         count_other += 1

    count_redirected = count_redirect_until_timeout + count_redirect_until_not_found + \
        count_redirect_until_error + count_redirect_until_found

    print('Regarding the original graph:')
    print('\tcount not found: ', count_not_found)
    print('\tcount found (not redirected): ', count_found_without_redirect)
    print('\tcount error: ', count_error)
    print('\tcount timeout: ', count_timeout)
    print('*****')
    print('\tcount redirect until timeout: ', count_redirect_until_timeout)
    print('\tcount redirect until not found: ', count_redirect_until_not_found)
    print('\tcount redirect until error: ', count_redirect_until_error)
    print('\tcount redirect until found: ', count_redirect_until_found)
    print('\tTOTAL REDIRECTED: ', count_redirected)
    print('\tcount other (mistake): ', count_other)

    # Validate
    count = 0
    for n in redi_graph.nodes():
        if redi_graph.nodes[n]['remark'] == 'unknown':
            print('unknown exists (but should not)!')
            print(n)
            count += 1

        if redi_graph.nodes[n]['remark'] == 'end_of_redirect':
            print('end of redirect exists (but should not)!')
            print(n)

    # print('count unknown = ', count)
    print('total num edges in the new redirect graph = ', len(redi_graph.edges()))

    end = time.time()
    hours, rem = divmod(end-start, 3600)
    minutes, seconds = divmod(rem, 60)

    time_formated = "{:0>2}:{:0>2}:{:05.2f}".format(
        int(hours), int(minutes), seconds)
    print("Time taken = ", time_formated)

    return redi_graph


graph_ids = sys.argv[1:]
for graph_id in graph_ids:
    print('\n\n\nprocessing file = ', graph_id)
    redi_graph = create_redi_graph(graph_id)
    # export_redirect_graph_edges(
    #     f"{graph_id.split('.')[0]}_redirect_edges.nt", redi_graph)
    # export_redirect_graph_nodes(
    #     f"{graph_id.split('.')[0]}_redirect_nodes.tsv", redi_graph)
    # pickle.dump(redi_graph, open(
    #     f"{graph_id.split('.')[0]}_redirect_graph.p", "wb"))
    # save a directed graph
    nx.write_graphml(
        redi_graph, f"{graph_id.split('.')[0]}_redirect_graph.graphml")