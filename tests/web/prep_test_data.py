import argparse
import os
import json

from elasticsearch import Elasticsearch


def main(args: argparse.Namespace):
    client = Elasticsearch(args.host)
    os.makedirs(args.output, exist_ok=True)
    # dump mapping
    with open(os.path.join(args.output, 'data.json'), 'w') as file:
        setting = client.indices.get(args.index)[args.index]
        setting["settings"]["index"] = {
            "analysis": setting["settings"]["index"]["analysis"]}
        json.dump(setting, file, indent=2)

    #
    query_ids = []
    with open(args.input, 'r') as input_file:
        for line in input_file:
            try:
                q = json.loads(line)
            except json.JSONDecodeError:
                # I was thinking maybe we can have something like
                #  field:value, regex:field:.*, !field, !field:value, etc.
                #  but it turns out that it would be too complex right now
                query_ids.append(set())  # empty
                continue

    # pull _id's from ES

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
