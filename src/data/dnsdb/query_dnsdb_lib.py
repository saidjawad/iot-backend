"""
Module with classes and functions to query different DNSDB APIs
"""
import gzip
import json
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import List, Any, Union, Dict

import dnsdb2
import pandas as pd

log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.DEBUG, format=log_fmt)
logger = logging.getLogger(__name__)


class DNSDBQueryExecutorBasic:
    """
    Class encapsulating the necessary functions to query domains using DNSDB Basic Search API.
    """

    def __init__(self, input_file_path: Path, output_file_path: Path, api_key, time_fence: int = 1609459200,
                 query_fields=None):
        """

        @param input_file_path: path to query file
        @param output_file_path: output file path
        @param api_key: dnsdb api key
        @param time_fence: timefence value for dnsdb (lasttime that a mapping is seen.)
        @param query_fields: name of columns that contain dnsdb queries
        """
        if query_fields is None:
            query_fields = ['domain_A_queries', 'domain_AAAA_queries', 'domain_CNAME_queries']
        self.query_fields = query_fields
        self.input_file_path = input_file_path
        self.output_file_path = output_file_path
        self.api_key = api_key
        self.time_fence = time_fence
        self.query_list = self.prepare_query_list()
        self.client = dnsdb2.Client(self.api_key)

    def prepare_query_list(self) -> pd.DataFrame:
        """
        cleans up the list of dnsdb queries to be consumed by the dnsdb2 python client,
        removes 'rrset/name' strings and drops the duplicate queries, to reduce number of queries.
        """
        query_list = pd.read_csv(self.input_file_path).drop_duplicates()
        for field in self.query_fields:
            if field in query_list.columns:
                query_list[field] = query_list[field].apply(lambda x: x.replace('rrset/name/', ''))
        return query_list

    def send_dnsdb_query_helper(self, query, *args, **kwargs):
        """
        This is a wrapper function to run different dnsdb queries. This allows the subclasses to override and run
        different dnsdb queries.
        @param query:
        @param args:
        @param kwargs:
        @return:
        """
        query, rrtype = query.split('/')
        kwargs['rrtype'] = rrtype
        return self.client.lookup_rrset(query, *args, **kwargs)

    def dnsdb_send_query(self, query: str) -> List[Dict]:
        """
        performs a lookup in dnsdb for rrset records that contain the query string.
        If the maximum number of results is reached, it will try to walk the dnsdb starting from the offset value.
        This method is not recommended by Farsight, for a complete download of the dataset contact them.
        """
        logger.debug('sending dnsdb query')
        offset = 0
        num_retries = 0
        total_retries = 0
        max_retries = 1
        results = list()
        logger.debug(f'querying {query}')
        logger.debug(f'queries: {query=},')
        while True and num_retries <= max_retries:
            try:
                for res in self.send_dnsdb_query_helper(query, limit=0, offset=offset,
                                                        time_last_after=self.time_fence):
                    results.append(res)

                else:
                    # if the query execution was successful, reset the retry counter
                    num_retries = 0
            except dnsdb2.QueryLimited:
                offset = len(results)
            except dnsdb2.QueryTruncated:
                logger.exception(f'query truncated, {query=}. retrying once more')
                num_retries += 1
                total_retries += 1
            except Exception as e:
                logger.exception(e)
                # Some unknown exception happened, we should not retry
                num_retries += 3
            else:
                break
        logger.info(f'finished executing the {query=}, {num_retries=}, {total_retries=},'
                    f' len_results: {len(results)}')
        return results

    def process_dnsdb_query(self, query_str: str, fout, query_metadata: Union[Mapping[Any, Any], None] = None):
        """
        For each query string, perform the lookup,
        convert the result to json and write it to the output stream.
        """
        for result in self.dnsdb_send_query(query_str):
            try:
                js: Dict = result
                if query_metadata is not None:
                    js.update(query_metadata)
                for_out = json.dumps(result)
                fout.write(f'{for_out}\n'.encode())
            except Exception as e:
                logger.exception(f'An error happened {query_str=} {result=} {e=}')

    def run_dnsdb_queries(self) -> None:
        """
        Main function to execute the DNSDB queries and save the results.
        """
        with gzip.open(self.output_file_path, 'wb') as fout:
            # counter = 0
            for idx, row in self.query_list.iterrows():
                logger.info(row)
                # if counter > 1:
                #     break
                # counter += 1
                for query_field in self.query_fields:
                    if query_field in row:
                        query = f'{row[query_field]}'
                        company = row['company']
                        query_meta_data = {'company': company}
                        logger.info(f'{query=}, {query_meta_data=}')
                        self.process_dnsdb_query(query, fout, query_meta_data)
                    else:
                        logger.warning(f'{query_field=} was not in the list of queries')


class DNSDBQueryExecutorFlexibleRegexSearch(DNSDBQueryExecutorBasic):
    """
    Class encapsulating the functions and state required to run regex queries using DNSDB Flexible Search API.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def send_dnsdb_query_helper(self, query, *args, **kwargs):
        """
        helper function calling the DNSDB flexible search api (Regex)
        @param args:
        @param kwargs:
        @return:
        """
        query, rrtype = query.split('/')
        kwargs['rrtype'] = rrtype
        return self.client.flex_rrnames_regex(query, *args, **kwargs)
