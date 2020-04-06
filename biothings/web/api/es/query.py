"""
    Elasticsearch Query Execution
"""
import asyncio

import elasticsearch
from elasticsearch import NotFoundError, RequestError, TransportError

from biothings.web.api.handler import BadRequest, EndRequest


class ESQuery(object):
    '''
    Execute an Elasticsearch query
    '''
    ES_VERSION = elasticsearch.__version__[0]

    def __init__(self, web_settings):

        self.client = web_settings.get_async_es_client()

        # es indices
        self.indices = web_settings.ES_INDICES
        self.default_index = web_settings.ES_INDEX
        self.default_type = web_settings.ES_DOC_TYPE

        # for scroll queries
        self.scroll_time = web_settings.ES_SCROLL_TIME
        self.scroll_size = web_settings.ES_SCROLL_SIZE

    async def execute(self, query, options):
        '''
        Execute the corresponding query.
        May override to add more. Handle uncaught exceptions.
        Must return an awaitable.
        '''
        if options.scroll_id:
            try:
                res = await self.client.scroll(
                    scroll_id=options.scroll_id,
                    scroll=self.scroll_time)
            except (NotFoundError, RequestError, TransportError):
                raise BadRequest(reason="Invalid or stale scroll_id.")
            else:
                if not res['hits']['hits']:
                    raise EndRequest(reason="No more results to return.")
                return res

        if query:
            biothing_type = options.pop('biothing_type', None) or self.default_type
            query = query.index(self.indices.get(biothing_type, self.default_index))

            if options.pop('fetch_all', False):
                query = query.params(scroll=self.scroll_time)
                query = query.extra(size=self.scroll_size)

            if options.sort:  # accept '-' prefixed field names
                query = query.sort(*options.pop('sort'))

            try:
                return await self.dsl_query(query, **options)
            except elasticsearch.RequestError as err:
                raise BadRequest(root_cause=err.info['error']['root_cause'][0]['reason'])

        return asyncio.sleep(0, {})

    async def dsl_query(self, query, **options):

        query = query.extra(**options)
        res = await query.using(self.client).execute()
        if isinstance(res, list):
            return [res_.to_dict() for res_ in res]
        return res.to_dict()
