import time
from tkinter import E
import networkx as nx
import csv
import asyncio
from aiohttp import ClientSession, ClientTimeout, ServerTimeoutError
import argparse

# TODO add regex for load_entities

# argparse for csv file input and graphml output
parser = argparse.ArgumentParser(description='Create redirect graph from csv file')
parser.add_argument('-i', '--input', help='input csv file', required=True)
parser.add_argument('-o', '--output', help='output graphml file', required=True)
args = parser.parse_args()
input_file = args.input
output_file = args.output

LIMIT = 1000
NOTFOUND = 1
FOUNDWITHOUTREDIRECT = 2
ERROR = 3
TIMEOUT = 4
REDIRECT = 5
TIMEOUT_PARAMETERS = [ClientTimeout(sock_connect=0.01, sock_read=0.05),ClientTimeout(sock_connect=0.5, sock_read=2.5),ClientTimeout(sock_connect=1, sock_read=5)]

async def find_redirects(timeout, urls):
    sem = asyncio.Semaphore(LIMIT)
    tasks = []
    async with ClientSession(timeout=timeout) as session:
        async def fetch(e):
            try:
                async with session.get(e) as response:
                    if response.history:
                        if e == str(response.url) and response.ok:
                            return e, FOUNDWITHOUTREDIRECT, None
                        elif e == str(response.url) and not response.ok:
                            return e, NOTFOUND, None
                        else: # TODO send back REDIRECTUNTILFOUND or REDIRECTUNTILNOTFOUND instead of REDIRECT such that redirects dont need another get requests
                            via_entities = [str(resp.url) for resp in response.history]
                            via_entities.append(str(response.url))
                            return e, REDIRECT, via_entities
                    elif response.ok:
                        return e, FOUNDWITHOUTREDIRECT, None
                    else:
                        return e, NOTFOUND, None
            except ServerTimeoutError:
                return e, TIMEOUT, None
            except Exception as error:
                #print(error)
                return e, ERROR, None

        async def bound_fetch(url):
            async with sem:
                return await fetch(url)
        for i in urls:
            task = asyncio.ensure_future(bound_fetch(i))
            tasks.append(task)
        responses = await asyncio.gather(*tasks)
        return responses

def load_entities(filename):
    with open(filename, encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        entities = {row[0] for row in reader}
        return entities

def main():
    print(f'\n\n\nprocessing file = {input_file}')
    start = time.time()
    redi_graph = nx.DiGraph()
    sample_entities = load_entities(input_file)
    entities_to_test = sample_entities
    print('there are ', len(entities_to_test), ' entities in the graph ')
    for entity in entities_to_test:
        redi_graph.add_node(entity, remark='unknown')

    count_not_found = 0
    count_found_without_redirect = 0
    count_error = 0
    count_timeout = 0
    count_redirect_until_not_found = 0
    count_redirect_until_timeout = 0
    count_redirect_until_error = 0
    count_redirect_until_found = 0
    count_merged_redirect = 0

    for timeout_parameter in TIMEOUT_PARAMETERS:
        timeout_entities = set()
        entities_to_test.union({e for e in redi_graph.nodes if redi_graph.nodes[e]['remark'] == 'unknown'})
        
        result_redirect = asyncio.run(find_redirects(timeout_parameter, entities_to_test))
        redirect_entities = []
        for e, result, via_entities in result_redirect:
            if result == NOTFOUND:
                count_not_found += 1
                redi_graph.nodes[e]['remark'] = 'not_found'
            elif result == FOUNDWITHOUTREDIRECT:
                count_found_without_redirect += 1
                redi_graph.nodes[e]['remark'] = 'found_without_redirect'
            elif result == ERROR:
                count_error += 1
                redi_graph.nodes[e]['remark'] = 'error'
            elif result == TIMEOUT:
                timeout_entities.add(e)
                redi_graph.nodes[e]['remark'] = 'timeout'
                if timeout_parameter == TIMEOUT_PARAMETERS[2]:
                    count_timeout += 1
            elif result == REDIRECT:
                if via_entities[0] != e:
                    via_entities = [e] + via_entities
                # checks to see if it redirects to a node that is already in the redirect graph but not part of the original sample node
                if any(i for i in via_entities[1:] if i in redi_graph and not i in sample_entities):
                    count_merged_redirect += 1
                for i, s in enumerate(via_entities[:-1]):
                    t = via_entities[i+1]
                    if s not in redi_graph.nodes():
                        redi_graph.add_node(s, remark='redirected')
                    else:
                        redi_graph.nodes[s]['remark'] = 'redirected'
                    redi_graph.add_node(t, remark='unknown')
                    redi_graph.add_edge(s, t)
                redirect_entities.append(via_entities[-1])
        
        for timeout_parameter in TIMEOUT_PARAMETERS:
            redirect_timeout_entities = set()
            result_redirect_entities = asyncio.run(find_redirects(timeout_parameter, redirect_entities))
            for e, result, _ in result_redirect_entities:
                if result == NOTFOUND:
                    redi_graph.nodes[e]['remark'] = 'not_found'
                    count_redirect_until_not_found += 1
                elif result == FOUNDWITHOUTREDIRECT:
                    redi_graph.nodes[e]['remark'] = 'found_without_redirect'
                    count_redirect_until_found += 1
                elif result == ERROR:
                    redi_graph.nodes[e]['remark'] = 'error'
                    count_redirect_until_error += 1
                elif result == TIMEOUT:
                    redi_graph.nodes[e]['remark'] = 'timeout'
                    redirect_timeout_entities.add(e)
                    if timeout_parameter == TIMEOUT_PARAMETERS[2]:
                        count_redirect_until_timeout += 1
            redirect_entities = redirect_timeout_entities

        print('TIMEOUT: there are ', len(timeout_entities), ' timeout entities')
        entities_to_test = timeout_entities

        err = [e for e in redi_graph.nodes if redi_graph.nodes[e]['remark'] == 'redirected' and redi_graph.out_degree(e) == 0]
        if err:
            for e in err:
                print('ERROR:')
                print(e, ' was redirected but has out-degree 0')
                print(e, ' was redirected is with in-degree ',
                    redi_graph.in_degree(e))

    update_against = {n for n in redi_graph.nodes() if redi_graph.nodes[n]['remark'] != 'redirected'}
    redirected_nodes = {n for n in redi_graph.nodes() if redi_graph.nodes[n]['remark'] == 'redirected'}
    for n in redirected_nodes:
        for m in update_against:
            if nx.has_path(redi_graph, n, m):
                if redi_graph.nodes[m]['remark'] == 'timeout' or redi_graph.nodes[m]['remark'] == 'redirect_until_timeout':
                    redi_graph.nodes[n]['remark'] = 'redirect_until_timeout'
                elif redi_graph.nodes[m]['remark'] == 'not_found' or redi_graph.nodes[m]['remark'] == 'redirect_until_not_found':
                    redi_graph.nodes[n]['remark'] = 'redirect_until_not_found'
                elif redi_graph.nodes[m]['remark'] == 'error' or redi_graph.nodes[m]['remark'] == 'redirect_until_error':
                    redi_graph.nodes[n]['remark'] = 'redirect_until_error'
                elif redi_graph.nodes[m]['remark'] == 'found_without_redirect' or redi_graph.nodes[m]['remark'] == 'redirect_until_found':
                    redi_graph.nodes[n]['remark'] = 'redirect_until_found'
                else:
                    print('Error? reaching m = ',
                            redi_graph.nodes[m]['remark'])

    # Validation
    other = [e for e in redi_graph.nodes if redi_graph.nodes[e]['remark'] not in ['not_found','found_without_redirect','error','timeout','redirect_until_not_found','redirect_until_found','redirect_until_error','redirect_until_timeout']]
    count_other = len(other)
    if other:
        print(f'unknown exists (but should not)! {other}')

    # Results
    # TODO change results by using sample_entities and then Counter instead of counters
    count_redirected = count_redirect_until_timeout + count_redirect_until_not_found + \
            count_redirect_until_error + count_redirect_until_found
    count_valid = count_found_without_redirect + count_redirect_until_found
    count_invalid = count_not_found + count_error + count_timeout + count_redirect_until_timeout + count_redirect_until_not_found + count_redirect_until_error

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
    print('\tcount redirect that are merged ', count_merged_redirect)
    print('\tTOTAL REDIRECTED: ', count_redirected)
    print('\tcount other (mistake): ', count_other)

    print('TOTAL working IRIs: ', count_valid)
    print('TOTAL invalid IRIs: ', count_invalid)
    print('total num edges in the new redirect graph = ', len(redi_graph.edges()))

    end = time.time()
    hours, rem = divmod(end-start, 3600)
    minutes, seconds = divmod(rem, 60)

    time_formated = "{:0>2}:{:0>2}:{:05.2f}".format(
        int(hours), int(minutes), seconds)
    print("Time taken = ", time_formated)

    # save graph
    nx.write_graphml(redi_graph, output_file)

main()
