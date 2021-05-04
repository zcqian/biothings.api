import argparse
import hashlib
import json
import logging
import os
import pickle
import sys

from datetime import datetime
from typing import Any, Dict, List, Optional, Set, FrozenSet
from functools import reduce

import requests
from urllib.parse import urljoin

import tomlkit


class MiniClient:
    """
    Miniature ES client

    Due to handling and testing multiple versions of ES Python client and
    ES server combination is harder, and only a small subset of things are used
    in this script, hence this client
    """
    def __init__(self, host: str, port: int, timeout: float = 30.0):
        self._logger = logging.getLogger('mini_client')
        self._host = f'http://{host}:{port}'
        self._session = requests.Session()  # so the conn. gets reused
        self._timeout = timeout
        ver_nums = self.api_req('get', '')['version']['number'].split('.')
        self.major_version = int(ver_nums[0])
        self.minor_version = int(ver_nums[1])
        if self.major_version < 6 or \
                self.major_version == 6 and self.minor_version < 8:
            raise RuntimeError("Requires ES6.8+")
        elif self.major_version > 7:
            self._logger.warning("Only tested with ES6.8-ES7, running: %s",
                                 ver_nums.join('.'))

        # inlined version fn, only used once

    def api_req(self, method: str, endpoint: str = '', data=None, params=None):
        url = urljoin(self._host, endpoint)
        r = self._session.request(method=method, url=url,
                                  json=data, params=params,
                                  timeout=self._timeout)
        if not 200 <= r.status_code < 300:
            raise RuntimeError
        return r.json()

    def search(self, body, index: str, scroll: Optional[str] = None,
               size: int = 1000):
        params = {"size": size}
        if scroll:
            params.update(scroll=scroll)
        ret = self.api_req('get', f'/{index}/_search',
                           data=body, params=params)
        return ret

    def scroll(self, scroll_id: str, scroll: str):
        # ES requires the scroll parameter every time or weird things happen
        return self.api_req('get', '/_search/scroll',
                            data={'scroll_id': scroll_id,
                                  'scroll': scroll}
                            )

    def get_mapping(self, index: str):
        if self.major_version == 6:
            resp = self.api_req('get', f'/{index}',
                               params={'include_type_name': 'false'})
        else:
            resp = self.api_req('get', f'/{index}')
        if len(resp) != 1:
            logging.warning("%s is alias with more than one index?", index)
        resp = next(iter(resp.values()))  # pull the value
        if 'analysis' in resp["settings"]["index"]:
            resp["settings"]["index"] = {
                "analysis": resp["settings"]["index"]["analysis"]
            }
        else:
            resp["settings"]["index"] = {}
        return resp

    def clear_scroll(self, scroll_id: str) -> None:
        self.api_req('delete', '/_search/scroll',
                     data={'scroll_id': scroll_id})

    def get_doc_type(self, index: str) -> str:
        if self.major_version >= 7:
            raise RuntimeError("Document type removed in ES7+")
        mapping = self.api_req('get', f'/{index}')
        mapping = next(iter(mapping.values()))['mappings']
        return next(iter(mapping.keys()))

    def get_source(self, index: str, doc_id):
        if self.major_version > 6:
            src = self.api_req('get', f'/{index}/_source/{doc_id}')
        else:
            doc_type = self.get_doc_type(index)
            src = self.api_req('get', f'/{index}/{doc_type}/{doc_id}/_source')
        return src


def write_ckpt(o, path: str):
    with open(path, 'wb') as f:
        pickle.dump(o, f)


def load_ckpt(path: str):
    with open(path, 'rb') as f:
        return pickle.load(f)


def dict_hash(d: dict) -> bytes:
    js = json.dumps(d, sort_keys=True)
    return hashlib.sha256(js.encode('ascii')).digest()


def get_fields(d: Dict[str, Any], parents: Optional[list] = None):
    if not parents:
        parents = []
    for k, v in d.items():
        field_name = '.'.join(parents + [k])
        if k.startswith('_') or k in ['all']:
            continue
        if isinstance(v, dict):
            if 'properties' in v:
                yield from get_fields(v['properties'], parents + [k])
            elif 'type' in v:  # okay when the field name is 'type'
                yield field_name
        else:
            logging.warning("unable to handle %s: %s", field_name, v)


def generate_query_all_fields(fields: List[str], page_size: int) -> List[dict]:
    queries = []
    for field_name in fields:
        queries.append({
            'query': {'exists': {'field': field_name}},
            'size': page_size,
            '_source': False
        })
    return queries


def dump_ids_from_queries(client: MiniClient,
                          index: str, queries: List[dict], target_size: int,
                          checkpoint_path: Optional[str] = None,
                          size: Optional[int] = 1000) \
        -> Dict[str, Set[int]]:
    id_to_query = {}
    done = set()
    if checkpoint_path:
        try:
            id_to_query, done = load_ckpt(checkpoint_path)
        except FileNotFoundError:
            pass
    num_total = len(queries)
    try:
        for query_idx, query in enumerate(queries):
            query_hash = dict_hash(query)
            if query_hash in done:
                logging.info("Skipping query %d because it was done", query_idx)
                continue
            query.update(_source=False)
            logging.info("Processing query %d of %d", query_idx, num_total)
            query_hit_running_total = 0
            resp = client.search(query, index=index, scroll='5m', size=size)
            sc_id = resp['_scroll_id']
            while len(resp['hits']['hits']) > 0:
                for hit in resp['hits']['hits']:
                    id_to_query.setdefault(hit['_id'], set()).add(query_idx)
                    query_hit_running_total += 1
                logging.info("Obtained %d queries", query_hit_running_total)
                if query_hit_running_total >= target_size:
                    # stop early if we have enough, or scroll through
                    logging.info(
                        "Stop scrolling early because got %d results for query %d",
                        query_hit_running_total, query_idx
                    )
                    break
                resp = client.scroll(scroll_id=sc_id, scroll='1m')
                sc_id = resp['_scroll_id']
            else:
                logging.info("End of results, got %d", query_hit_running_total)
            client.clear_scroll(scroll_id=sc_id)
            done.add(query_hash)
    except KeyboardInterrupt:
        logging.warning("Got KeyboardInterrupt")
        if checkpoint_path:
            write_ckpt((id_to_query, done), checkpoint_path)
        sys.exit(0)
    logging.info("done with all queries")
    if checkpoint_path:
        write_ckpt((id_to_query, done), checkpoint_path)
    return id_to_query


def minimal_cover_set(subsets: Dict[FrozenSet[int], str])\
        -> Dict[str, FrozenSet[int]]:
    universe: FrozenSet[int] = reduce(lambda a, b: a | b, subsets.keys())
    covered = set()
    picked = {}
    while covered != universe:
        uncovered = universe - covered
        remove_list = []
        coverage = {}
        for subset, doc_id in subsets.items():
            can_cover = len(uncovered & subset)
            if can_cover == 0:
                remove_list.append(subset)
                continue
            coverage[subset] = can_cover
        pick = sorted(coverage, key=lambda k: coverage[k])[-1]
        covered |= pick
        picked[subsets[pick]] = pick
        for rm_subset in remove_list:
            subsets.pop(rm_subset)

    return picked


def dump_mapping_and_docs(client: MiniClient, index: str, ids: List[str],
                          output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)
    setting = client.get_mapping(index)
    with open(os.path.join(output_dir, 'data.json'), 'w') as file:
        json.dump(setting, file, indent=2)
    with open(os.path.join(output_dir, 'data.ndjson'), 'w') as file:
        for doc_id in ids:
            doc_src = client.get_source(index=index, doc_id=doc_id)
            json.dump({"index": {"_id": doc_id}}, file)
            file.write('\n')
            json.dump(doc_src, file)
            file.write('\n')


def dump_documents(args: argparse.Namespace):
    fn_mapping = f'{args.output_prefix}.json'
    fn_docs = f'{args.output_prefix}.ndjson'
    if os.path.exists(fn_mapping) or os.path.exists(fn_docs):
        # obviously we can overwrite the files that pops into existence
        # after the check, but we don't care
        logging.error("File already exists!")
        sys.exit(-1)
    with open(args.input) as f:
        t_doc = tomlkit.loads(f.read())
    doc_ids = set()
    for q_body in t_doc['queries'].values():
        ids = q_body.get('doc_id', [])
        doc_ids |= set(ids)
    client = MiniClient(args.host, args.port)
    mapping = client.get_mapping(args.index)
    with open(fn_mapping, 'x') as f:
        json.dump(mapping, f)
    with open(fn_docs, 'x') as f:
        for doc_id in doc_ids:
            src = client.get_source(args.index, doc_id)
            json.dump({"index": {"_id": doc_id}}, f)
            f.write('\n')
            json.dump(src, f)
            f.write('\n')


def perform_query(args: argparse.Namespace):
    client = MiniClient(args.host, args.port)
    dt_str = get_iso8601_dt_str()
    # dump mapping
    with open(args.queries) as f:
        doc = tomlkit.loads(f.read())

    all_queries = []
    query_names = []
    # TODO: pre-populate with existing results -- if something we have
    #  already satisfies the new queries, use that instead
    for query_name, q_body in doc['queries'].items():
        if 'doc_id' in q_body and not q_body.get('re-query', False):
            continue
        all_queries.append(json.loads(q_body['query']))
        query_names.append(query_name)

    # FIXME: checkpoint file name
    id_to_query = dump_ids_from_queries(client, args.index,
                                        all_queries, args.num,
                                        checkpoint_path='ckpt.pkl',
                                        size=args.size)

    # do a deduplication before we do the set cover problem
    # better (?) if we consider superset/subset before doing the
    # minimal set cover
    subsets = {}
    universe = set()
    for doc_id, subset in id_to_query.items():
        universe |= subset
        subsets[frozenset(subset)] = doc_id

    missing = set(range(len(all_queries))) - universe
    missing_cmt = tomlkit.comment(f"No match was found on {dt_str}")
    for missing_id in missing:
        query_name = query_names[missing_id]
        doc['queries'][query_name].add('doc_id', [])
        doc['queries'][query_name].add(missing_cmt)
    picked_ids = minimal_cover_set(subsets)
    found_cmt = tomlkit.comment(f"Updated on {dt_str}")
    for doc_id, subsets in picked_ids.items():
        for query_idx in subsets:
            query_name = query_names[query_idx]
            q_body = doc['queries'][query_name]
            docs = q_body.get('doc_id', [])
            docs.append(doc_id)
            found_cmt = tomlkit.comment(f"{doc_id} added on {dt_str}")
            # if 'doc_id' in q_body:
            #   q_body.remove('doc_id')  # here is the thing, pop is broken
            q_body['doc_id'] = docs
            q_body.add(found_cmt)
            # FIXME: can't precisely control the presentation,
            #  tomlkit is really lacking in terms of documentation
            #  but on the other hand it is kind of insane to use it this way
    with open(args.queries, 'w') as f:
        f.write(tomlkit.dumps(doc))


def generate_queries(args: argparse.Namespace):
    client = MiniClient(args.host, args.port)
    # dump mapping
    mappings = client.get_mapping(index=args.index)['mappings']

    all_fields = list(get_fields(mappings['properties']))

    doc = tomlkit.document()
    dt_str = get_iso8601_dt_str()
    comment = tomlkit.comment(f"Automatically generated on {dt_str}")

    # build all queries
    queries = tomlkit.table()
    for field_name in all_fields:
        t = tomlkit.table()
        t.add(comment)
        replace_name = field_name.replace('.', '_')
        query_name = f'field_{replace_name}_exists'
        query = json.dumps({
            'query': {'exists': {'field': field_name}}
        })
        t.add('query', query)
        t.add('auto_gen', True)
        queries.add(query_name, t)

    doc["queries"] = queries
    with open(args.output, 'w') as f:
        f.write(tomlkit.dumps(doc))


def get_iso8601_dt_str():
    return datetime.now().replace(microsecond=0).astimezone().isoformat()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--host', '-s', metavar='ES_HOST', type=str, default='127.0.0.1',
        help="Elasticsearch host to pull data from"
    )
    parser.add_argument(
        '--port', '-p', metavar='ES_PORT', type=int, default=9200,
    )
    parser.add_argument(
        '--index', '-i', metavar='INDEX', type=str, required=True
    )
    parser.add_argument('--verbose', '-v', action='count', default=0)

    subparsers = parser.add_subparsers(dest="cmd")
    generate_parser = subparsers.add_parser('generate')
    generate_parser.add_argument(
        '--output', '-o', metavar='OUTPUT', type=str, required=True
    )
    query_parser = subparsers.add_parser('query')
    query_parser.add_argument(
        '--queries', '-q', metavar='Q'
    )
    query_parser.add_argument(
        '--num', '-n', metavar='N', type=int, default=10000,
        help="target number of _id to obtain, may get more or less than N"
    )
    query_parser.add_argument(
        '--size', '-s', metavar='SIZE', type=int, default=1000,
        help="number of results from ES per request"
    )
    query_parser.add_argument(
        '--checkpoint', '-c', metavar='CHECKPOINT_FILE', type=str,
        required=False
    )
    dump_parser = subparsers.add_parser('dump')
    dump_parser.add_argument(
        '--input', '-i', metavar='INPUT', type=str, required=True,
    )
    dump_parser.add_argument(
        '--output-prefix', '-o', metavar='OUTPUT_PREFIX', type=str,
        required=True,
    )

    args = parser.parse_args()
    if args.verbose == 1:
        logging.getLogger().setLevel(logging.INFO)
    elif args.verbose >= 2:
        logging.getLogger().setLevel(logging.DEBUG)

    cmd_map = {
        'generate': generate_queries,
        'query': perform_query,
        'dump': dump_documents
    }

    cmd_map[args.cmd](args)
