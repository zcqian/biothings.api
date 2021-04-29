import argparse
import hashlib
import json
import logging
import os
import pickle
import sys

from datetime import datetime
from typing import Any, Dict, List, Optional, Set, FrozenSet, Tuple
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

    def scroll(self, scroll_id: str):
        return self.api_req('get', '/_search/scroll',
                            data={'scroll_id': scroll_id})

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
        mapping = next(iter(mapping.values()))['mapping']
        return next(iter(mapping.keys()))

    def get_source(self, index: str, doc_id):
        if self.major_version > 6:
            src = self.api_req('get' f'/{index}/_source/{doc_id}')
        else:
            pass
        ret = self.api_req(f'')


def checkpoint(d: dict, f: str):
    """
    Checkpoint format

    Essentially maintain state of everything inside a TOML document
    TOML should be more readable than YAML?

    [queries]

    [queries.unique_name]
    query = '{"json": "in a string"}'
    doc_id = 'to_be_populated, will be preserved on future runs, empty for nil'
    updated_date = 2021-04-27T14:00:00-07:00  # auto populated, informational
    force_update = false  # change to true to re-evaluate

    """
    toml_doc = tomlkit.document()


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
            with open(checkpoint_path, 'rb') as f:
                id_to_query, done = pickle.load(f)
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
                resp = client.scroll(scroll_id=sc_id)
            else:
                logging.info("End of results, got %d", query_hit_running_total)
            client.clear_scroll(scroll_id=sc_id)
            done.add(query_hash)
    except KeyboardInterrupt:
        logging.warning("Got KeyboardInterrupt")
        if checkpoint_path:
            with open(checkpoint_path, 'wb') as f:
                pickle.dump((id_to_query, done), f)
            logging.info("Written checkpoint.")
        sys.exit(0)
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
            doc_src = client.get_source(index=index, id=doc_id)
            json.dump({"index": {"_id": doc_id}}, file)
            file.write('\n')
            json.dump(doc_src, file)
            file.write('\n')


def perform_query(args: argparse.Namespace):
    client = MiniClient(args.host, args.port)
    dt_str = get_iso8601_dt_str()
    # dump mapping
    with open(args.queries) as f:
        doc = tomlkit.loads(f.read())

    all_queries = []
    query_names = []
    for query_name, q_body in doc['queries'].items():
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
        doc['queries'][query_name].add('doc_id', tomlkit.string(''))
        doc['queries'][query_name].add(missing_cmt)
    picked_ids = minimal_cover_set(subsets)
    found_cmt = tomlkit.comment(f"No match was found on {dt_str}")
    for doc_id, subsets in picked_ids.items():
        for query_idx in subsets:
            query_name = query_names[query_idx]
            doc['queries'][query_name].add('doc_id', tomlkit.string(doc_id))
            doc['queries'][query_name].add(found_cmt)
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


def dump_mapping(client: MiniClient, index: str, filename: str):
    mapping = client.get_mapping(index)
    with open(filename, 'w') as f:
        json.dump(mapping, f)


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

    args = parser.parse_args()
    if args.verbose == 1:
        logging.getLogger().setLevel(logging.INFO)
    elif args.verbose >= 2:
        logging.getLogger().setLevel(logging.DEBUG)

    cmd_map = {
        'generate': generate_queries,
        'query': perform_query,
    }

    cmd_map[args.cmd](args)
