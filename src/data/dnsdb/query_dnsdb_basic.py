"""
Script to run queries using DNSDB basic lookup API
"""
import logging
from datetime import datetime as dt
from os import getenv
from pathlib import Path

import click
from dotenv import load_dotenv

from src.data.dnsdb.query_dnsdb_lib import DNSDBQueryExecutorBasic

log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)


@click.command()
@click.argument('env_file', type=click.Path(exists=True))
def execute_dnsdb_query_script(env_file):
    """
    Main function of the script.
    @param env_file:
    @return:
    """
    logger.info('Reading input files')
    load_dotenv(env_file)
    project_dir = Path(__file__).resolve().parents[3]
    logger.info('Reading config file and generating necessary files.')
    input_file_path = project_dir.joinpath(getenv('INPUT_FILE_PATH'))
    output_file_dir = project_dir.joinpath(getenv('OUTPUT_FILE_DIR'))

    api_key = getenv('DNSDB_API_KEY')
    if not output_file_dir.exists():
        output_file_dir.mkdir(parents=True)
    output_file_name = input_file_path.stem
    query_round = getenv('QUERY_ROUND', None)
    output_file_name = f'{output_file_name}_query_round_{query_round}' if query_round is not None else output_file_name
    output_file_path = output_file_dir.joinpath(f'{output_file_name}_dnsdb_results.json.gz')
    execution_time_iso8601 = dt.utcnow().strftime("%Y-%m-%d_%H:%M:%SZ")
    new_output_file_name = f"{execution_time_iso8601}_{output_file_path.name}"
    output_file_path = output_file_path.with_name(new_output_file_name)

    query_fields = getenv('QUERY_FIELDS', None)
    if query_fields is not None:
        query_fields = query_fields.split(',')
    logger.info(f'Reading input and running dnsdb queries with config:\n {input_file_path=}'
                f'\n{output_file_path=}'
                f'\n{query_fields=}')

    query_executor = DNSDBQueryExecutorBasic(input_file_path, output_file_path, api_key, query_fields=query_fields)
    query_executor.run_dnsdb_queries()


if __name__ == "__main__":
    execute_dnsdb_query_script()
