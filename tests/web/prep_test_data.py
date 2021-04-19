import logging
import argparse
import os
import json

from pprint import pprint
from typing import Any, Dict, Optional

from elasticsearch import Elasticsearch


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


def main(args: argparse.Namespace):
    client = Elasticsearch(args.host)
    os.makedirs(args.output, exist_ok=True)
    # dump mapping
    setting = client.indices.get(args.index)
    if len(setting) != 1:
        logging.warning("%s is alias with more than one index?", args.index)
    setting = setting[next(iter(setting.keys()))]
    setting["settings"]["index"] = {
        "analysis": setting["settings"]["index"]["analysis"]
    }
    with open(os.path.join(args.output, 'data.json'), 'w') as file:
        json.dump(setting, file, indent=2)
    # assume only one doc type
    mappings = setting['mappings']
    mappings = mappings[next(iter(mappings.keys()))]
    query = {
        'query': {
            'bool': {
                'must': []
            }
        },
        'stored_fields': []
    }
    all_fields = list(get_fields(mappings['properties']))
    # two things:
    #  1. this gets nowhere,
    #     throw in a couple of fields and search gets no hits
    #  2. too_many_clauses: maxClauseCount is set to 1024
    for field_name in all_fields:
        query['query']['bool']['must'].append(
            {
                'exists': {'field': field_name}
            }
        )
    result = client.search(index=args.index, body=query)
    pprint(query)
    pprint(result)


    # find minimal set of _id's
    # minimal hitting set is NP-complete so this is just an approximation

    # write output: ndjson, mapping, metadata


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--host', '-s', metavar='ES_HOST', type=str, default='127.0.0.1',
        help="Elasticsearch host to pull data from"
    )
    parser.add_argument(
        '--index', metavar='INDEX', type=str, required=True
    )
    parser.add_argument(
        '--input', metavar='INPUT_FILE', required=True
    )
    parser.add_argument(
        '--output', '-o', metavar='OUTPUT_DIR', required=True
    )
    args = parser.parse_args()
    main(args)
