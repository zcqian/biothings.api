import argparse
import hashlib
import json
import logging
import os
import pickle
from pprint import pprint
from typing import Any, Dict, List, Optional, Set, FrozenSet
from functools import reduce

from elasticsearch import Elasticsearch


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


def get_es_index_mappings(client: Elasticsearch, index: str) -> dict:
    setting: dict = client.indices.get(index)
    if len(setting) != 1:
        logging.warning("%s is alias with more than one index?", index)
    setting = next(iter(setting.values()))  # pull the value
    setting["settings"]["index"] = {
        "analysis": setting["settings"]["index"]["analysis"]
    }
    return setting


def dump_ids_from_queries(client: Elasticsearch,
                          index: str, queries: List[dict], target_size: int,
                          checkpoint_path: Optional[str] = None) \
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
    for query_idx, query in enumerate(queries):
        query_hash = dict_hash(query)
        if query_hash in done:
            logging.info("Skipping query %d because it was done", query_idx)
            continue
        logging.info("Processing query %d of %d", query_idx, num_total)
        query_hit_running_total = 0
        resp = client.search(index=index, body=query, scroll='30s')
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
            resp = client.scroll(scroll_id=sc_id, scroll='30s')
        else:
            logging.info("End of results, got %d", query_hit_running_total)
        client.clear_scroll(scroll_id=sc_id)
        done.add(query_hash)
        if checkpoint_path:
            with open(checkpoint_path, 'wb') as f:
                pickle.dump((id_to_query, done), f)
            logging.info("Written checkpoint.")
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


def dump_mapping_and_docs(client: Elasticsearch, index: str, ids: List[str],
                          output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)
    setting = get_es_index_mappings(client, index=index)
    with open(os.path.join(output_dir, 'data.json'), 'w') as file:
        json.dump(setting, file, indent=2)
    dt = next(iter(setting['mappings'].keys()))
    with open(os.path.join(output_dir, 'data.ndjson'), 'w') as file:
        for doc_id in ids:
            doc_src = client.get_source(index=index, id=doc_id, doc_type=dt)
            json.dump({"index": {"_id": doc_id}}, file)
            file.write('\n')
            json.dump(doc_src, file)
            file.write('\n')


def main(args: argparse.Namespace):
    client = Elasticsearch(args.host)
    # dump mapping
    setting = get_es_index_mappings(client, index=args.index)

    # assume only one doc type
    mappings = setting['mappings']
    mappings = mappings[next(iter(mappings.keys()))]

    all_fields = list(get_fields(mappings['properties']))

    # build all queries
    all_queries = generate_query_all_fields(all_fields, args.page_size)
    id_to_query = dump_ids_from_queries(client, args.index,
                                        all_queries, args.num_ids_per_query,
                                        checkpoint_path=os.path.join(
                                            args.output, 'ckpt.pkl'
                                        ))

    # do a deduplication before we do the set cover problem
    # better (?) if we consider superset/subset before doing the
    # minimal set cover
    subsets = {}
    universe = set()
    for doc_id, subset in id_to_query.items():
        universe |= subset
        subsets[frozenset(subset)] = doc_id

    missing = sorted(set(range(len(all_queries))) - universe)
    field_to_id = {}
    for missing_id in missing:
        field_to_id[all_fields[missing_id]] = None
    logging.warning("Follwing elements are missing:\n%s", missing)
    picked_ids = minimal_cover_set(subsets)
    for doc_id, subsets in picked_ids.items():
        for field_idx in subsets:
            field_to_id[all_fields[field_idx]] = doc_id

    with open(os.path.join(args.output, 'picked.json',), 'w') as f:
        json.dump(field_to_id, f, indent=2, sort_keys=True)
    doc_ids = list(picked_ids.keys())
    dump_mapping_and_docs(client, args.index, doc_ids, args.output)


    # find minimal set of _id's
    # minimal hitting set is NP-complete so this is just an approximation

    # write output: ndjson, mapping, metadata


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--host', '-s', metavar='ES_HOST', type=str, default='127.0.0.1',
        help="Elasticsearch host to pull data from"
    )
    parser.add_argument('--verbose', '-v', action='count', default=0)
    parser.add_argument(
        '--index', metavar='INDEX', type=str, required=True
    )
    parser.add_argument(
        '--input', metavar='INPUT', required=True
    )
    parser.add_argument(
        '--output', '-o', metavar='OUTPUT_DIR', required=True
    )
    # subparsers = parser.add_subparsers(help='sub-command help')
    # parser_dump_id = subparsers.add_parser('dump_id')
    parser.add_argument(
        '--num_ids_per_query', metavar='NR', type=int, default=10000
    )

    parser.add_argument(
        '--page-size', metavar='SIZE', type=int, default=1000
    )
    args = parser.parse_args()
    if args.verbose == 1:
        logging.getLogger().setLevel(logging.INFO)
    elif args.verbose >= 2:
        logging.getLogger().setLevel(logging.DEBUG)
    main(args)
