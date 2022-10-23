"""
library of classes and functions necessary to search in json files from censys.  using censys json files.
"""
import gzip
import logging
import os
import sys
from multiprocessing import Queue
from pathlib import Path
from typing import Union, Mapping, Tuple, Dict

import pandas as pd
import rapidjson

from src.experiment_lib import experiment_base

log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)


class CensysResultWriter(experiment_base.WriterProcess):
    """
    A Python process writing the result of experiments to a compressed file.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.result_df = None
        self.results_uptil_now = 0

    def open_output_file(self):
        """
        If the directory tree leading to the output file does not exists, it will create it.
        """
        if not self.output_file_path.parent.exists():
            self.output_file_path.parent.mkdir(parents=True)
        super().open_output_file()

    def write_output_file(self, line: pd.DataFrame):
        """
        writes the data frame into the output buffer/ or file.
        Flushes the buffer after every 100 file.

        """
        if line is not None:
            line.to_json(self.output_file, orient='records', lines=True)
            self.results_uptil_now += 1
            if (self.results_uptil_now % 100) == 0:
                self.output_file.flush()
            logger.info(f'{self.results_uptil_now=}')


class CensysSearchExperimentJob(experiment_base.ExperimentJob):
    """

    """

    def __init__(self, input_file_path: Path, target_search_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.input_file_path: Path = input_file_path
        self.target_search_path: Path = target_search_path
        self._input_file = None
        self.result = None
        self.output_queue: Union[Queue, None] = None
        self.logger = None

    def deferred_init(self, logger: logging.Logger, output_queue: Queue, **kwargs):
        """
        Called inside the process. Performs some remaining initialization steps of an experiment jobs.
        Experiment jobs are passed through queues to processes, Python has issues pickling complex objects.
        Thus, we defer some initialization steps to be performed by the process executing the job.
        """
        self.logger = logger
        self.output_queue = output_queue

    @property
    def input_file(self):
        """
        Opens the input file.
        """
        if self._input_file is None:
            if self.input_file_path.exists():
                self._input_file = gzip.open(self.input_file_path, 'rb')
            else:
                logger.warning(f"The input file {self.input_file_path} does not exist")
        return self._input_file

    def extract_host_info(self, data: Dict) -> Tuple[str, Union[str, None]]:
        """
        Extracts the host information part from a censys record.
        @param data: a censys record dictionary
        @return: IPv4 address and the ASN info
        """
        ip = data['host_identifier']['ipv4']
        as_info = data.get('autonomous_system', None)
        asn = as_info.get('asn', None) if as_info else None
        return ip, asn

    def __repr__(self):
        return f"Job: input_file: {self.input_file_path}"


class CensysCertificateSearchExperimentJobMixin(CensysSearchExperimentJob):
    """
    Experiment Job encapsulating the functions and necessary data to extract information from censys certificates.
    """

    def extract_certificates_from_censys_file(self, data: Dict,
                                              expected_fields: Union[Mapping, None] = None) -> \
            Union[Mapping, None]:
        """
        A generator function extracting certificate info from censys records.
        @param data: A Censys record
        @param expected_fields: A dictionary of additional fields to be extracted from a censys records.
        """
        ip, asn = self.extract_host_info(data)
        for service in data['services']:
            try:
                port = service.get('port', None)
                service_name = service.get('service_name', None)
                snapshot_date = service.get('snapshot_date', None)
                if 'tls' in service:
                    service = service['tls']
                    certificate = service['certificates']
                    certificate = certificate.get('leaf_data', None)
                    issuer_dn = certificate.get('issuer_dn', None)
                    name_field = certificate.get('names', '')
                    names = ";".join(name_field)
                    splitted_name_field = name_field
                    subject_dn = certificate.get('subject_dn', None)
                    res = {"ip": ip, "asn": asn, "issuer_dn": issuer_dn,
                           "names": names, "subject_dn": subject_dn, "port": port,
                           "service_name": service_name, "splitted_names": splitted_name_field,
                           "snapshot_date": snapshot_date}
                    if expected_fields is not None:
                        for field_name, field_address in expected_fields.items():
                            res.setdefault(field_name, data[field_address])
                    yield res
            except Exception as e:
                logger.debug(f'{service=}')
                logger.debug(f'{e}')
                continue

    def flatten_censys_file_to_dict(self) -> Mapping:
        """
        Calls the extractor function and converts the records to a dictionary where each key is a field name and
        its value is a list. Something like {'ip':['1.1.1.1','2.2.2.2'], 'asn':['1234','2345']...} sbw6557
        @return: A dictionary of requested fields and their list of values.
        """
        result_dict = dict()
        for line in self.input_file:
            data = rapidjson.loads(line)
            for cert_info in self.extract_certificates_from_censys_file(data, None):
                if cert_info:
                    for key, value in cert_info.items():
                        result_dict.setdefault(key, list()).append(value)
        self.input_file.close()
        return result_dict

    def prepare_input_and_target_files(self):
        """

        @return:
        """
        certificates_dict = self.flatten_censys_file_to_dict()
        if not self.target_search_path.exists():
            raise FileNotFoundError(f'{self.target_search_path=} does not exists')
        target_list = pd.read_csv(self.target_search_path)
        return certificates_dict, target_list

    def search_for_targets_in_censys_certificates(self):
        """

        @return:
        """
        certificates_dict, target_list = self.prepare_input_and_target_files()
        return self.search_for_target_func(certificates_dict, target_list)

        # match_list = []
        # for name, pattern in zip(final_result.name, final_result.search_pattern):
        #     match_list.append(re.match(pattern, name) is not None)
        # match_list = pd.Series(match_list)
        # final_result = (final_result.reset_index(drop=True)[match_list]).drop('timestamp', axis=1).drop_duplicates()

    def run_job(self):
        """

        @return:
        """
        try:
            result = self.search_for_targets_in_censys_certificates()
            if result is not None:
                self.output_queue.put(result)
        except Exception as e:
            self.logger.error(f'Exception happened while processing the file: {e}, {self}')
        print(f'finished a file {self}, {self.input_file_path}')


class CensysCertificateDomainSearchExperimentJob(CensysCertificateSearchExperimentJobMixin):
    """
    Given a list of domain names, searches for the domain names in the certificates
    """

    @staticmethod
    def search_for_target_func(certificates_dict, target_list):
        """

        @param certificates_dict:
        @param target_list:
        @return:
        """
        final_result = None
        try:
            result = pd.DataFrame.from_dict(certificates_dict)
            result = result.loc[result.names.notna(),]
            for row in target_list.to_dict('records'):
                new_res = None
                new_res = result[result.names.str.contains(row['generalized_domain'])].copy()
                if new_res.empty:
                    continue
                #
                new_res['company'] = None
                new_res['search_method'] = None
                new_res['generalized_domain'] = None
                new_res[['company', 'search_method',
                         'generalized_domain']] = [row['Company_name'],
                                                   'domain_in_cn_san', row['generalized_domain']]

                final_result = new_res if final_result is None else pd.concat([new_res, final_result],
                                                                              ignore_index=True)
            if final_result is None:
                return None
            final_result = final_result.loc[~final_result.company.isna(),]
            final_result['splitted_names'] = final_result.names.str.split(";")
            final_result = final_result.assign(name=final_result['splitted_names']).explode('names').drop(
                ['splitted_names'], axis=1)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.exception(f'{exc_type=}, {fname=}, {exc_tb.tb_lineno=}, Exception: {e=}')
        finally:
            return final_result


def generate_censys_input_files(project_dir: Path):
    """

    @param project_dir:
    @return:
    """
    input_file_path = project_dir.joinpath(Path(os.getenv("INPUT_FILE_PATH")))
    if input_file_path.exists():
        input_file_list = list(sorted(input_file_path.glob("*.json.gz")))
        return input_file_list
    else:
        logger.fatal(f'{input_file_path} does not exist, terminating..')
        exit(-1)
