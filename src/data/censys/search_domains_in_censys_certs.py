from datetime import datetime as dt
from os import getenv

import click
from dotenv import load_dotenv

from src.data.censys.censys_search_lib import *
from src.experiment_lib.experiment_base import ExperimentProcWithWriter

log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(lineno)d-  %(message)s'
logging.basicConfig(level=logging.DEBUG, format=log_fmt)
logger = logging.getLogger(__name__)


@click.command()
@click.argument('env_file', type=click.Path(exists=True))
def execute_domain_search_in_censys_certs(env_file):
    logger.info(env_file)
    logger.info('Reading input files')
    load_dotenv(env_file)
    project_dir = Path(__file__).resolve().parents[3]
    logger.info(f'{project_dir=}')
    logger.info('Reading config file and generating necessary files.')
    input_files = generate_censys_input_files(project_dir)
    logger.debug(f'{input_files=}')
    domain_search_path = project_dir.joinpath(Path(getenv('CENSYS_DOMAIN_LIST_PATH')))
    output_file_path = project_dir.joinpath(Path(getenv('OUTPUT_FILE_PATH')))
    execution_time_iso8601 = dt.utcnow().strftime("%Y-%m-%d_%H:%M:%SZ")
    new_output_file_name = f"{execution_time_iso8601}_{output_file_path.name}"
    output_file_path = output_file_path.with_name(new_output_file_name)
    if not output_file_path.parent.exists():
        output_file_path.parent.mkdir(parents=True)

    num_processes = int(getenv('NUM_PROCESSES', 1))

    process_list = []
    job_queue = Queue()
    output_queue = Queue()

    logger.info('Creating jobs')
    for input_file_path in input_files:
        job = CensysCertificateDomainSearchExperimentJob(input_file_path, domain_search_path)
        logger.info(f'putting the job : {job}')
        job_queue.put(job)

    logger.info(f'num jobs: {job_queue.qsize()}')

    logger.info('Initializing and starting the processes')

    for proc in range(num_processes):
        exp = ExperimentProcWithWriter(output_queue, job_queue, logger)
        process_list.append(exp)
        job_queue.put(None)
        exp.start()

    writer_process = CensysResultWriter(output_queue, num_processes, output_file_path, logger)
    writer_process.start()

    logger.info('Waiting for processes to finish the jobs')
    for proc in process_list:
        proc.join()
    writer_process.join()
    logger.info('All tasks are finished. ')


if __name__ == "__main__":
    execute_domain_search_in_censys_certs()
