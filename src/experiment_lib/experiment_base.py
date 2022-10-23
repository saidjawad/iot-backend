import gzip
import queue
from multiprocessing import Process
from multiprocessing import Queue
from pathlib import Path


class ExperimentJob:
    """
    A base class encapsulating the functions and state required for running an specific experiment.
    """

    def run_job(self):
        """
        The main function of an experiment that will be called by a python process.
        @return:
        """
        pass


class ExperimentProc(Process):
    """
    A python process subclass that executes ExperimentJob from an input queue.
    """

    def __init__(self, input_queue: Queue, logger, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.input_queue = input_queue
        self.job_list = []
        self.logger = logger

    def run(self):
        """
        reads ExperimentJobs from queue and executes them.
        """
        terminate = False
        while not terminate:
            try:
                next_job = self.input_queue.get(timeout=30)
            except queue.Empty:
                self.logger.info(f"Input queue is empty: {self}")
                break
            if next_job is None:
                break
            self.job_list.append(next_job)
            self.logger.info(f"{self.name} starting the job {next_job}")
            next_job.run_job()
            self.logger.info(f"{self.name} finished the job {next_job}")
        self.logger.info(f"{self.name} : terminating, processed {len(self.job_list)} jobs")


class ExperimentProcWithWriter(ExperimentProc):
    """
    A python process that executes the Experiment jobs but also supports an output queue for writing the results.
    """

    def __init__(self, output_queue: Queue, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_queue = output_queue
        self.hitlist: set = kwargs['hitlist'] if 'hitlist' in kwargs else None
        self.num_jobs = 0

    def run(self):
        """
        Overrides the python process run method. Obtains the jobs from the job queue.
        if there is no job in the queue, after 30 seconds, it sends a None(sentinel) value to the writer process
        """
        terminate = False
        while not terminate:
            try:
                next_job = self.input_queue.get(timeout=30)
            except queue.Empty:
                self.logger.info(f"Input queue is empty: {self}")
                break
            if next_job is None:
                break
            next_job.deferred_init(self.logger, self.output_queue, hitlist=self.hitlist)
            self.num_jobs += 1
            self.logger.info(f"{self.name} starting the job {next_job}")
            next_job.run_job()
            self.logger.info(f"{self.name} finished the job {next_job}")
        self.logger.info(f"{self.name} : terminating, processed {self.num_jobs} jobs")
        self.output_queue.put(None)


class WriterProcess(Process):
    """
    A python process suitable for Multiproducer, single consumer scenarios.
    It consumes input from a queue shared by multiple processes. It writes the input into an output file.
    It takes a None value as a sentinel signal. If it receives max_signals number of None values from its input_queue,
    it will die.
    """

    def __init__(self, input_queue: Queue, max_signals: int, output_file_path: Path, logger, *args, **kwargs):
        """
        @param input_queue:
        @param max_signals:
        @param output_file_path:
        @param logger:
        @param args:
        @param kwargs:
        """
        super().__init__(*args, **kwargs)
        self.input_queue: Queue = input_queue
        self.max_signals: int = max_signals
        self.output_file = None
        self.output_file_path: Path = output_file_path
        self.logger = logger

    def run(self):
        """
        Read input from input_queue and write them into a file.
        close the queue if max_signal number of None values are received.
        """
        num_signals = 0
        self.open_output_file()
        while True:
            line = self.input_queue.get()
            try:
                if line is not None:
                    self.write_output_file(line)
                else:
                    num_signals += 1
            except Exception as e:
                self.logger.warn(f'Exception happened {e}')
            if num_signals >= self.max_signals:
                break
        self.close_output_file()
        self.logger.info('Writer Process is finished. Terminating...')

    def close_output_file(self):
        """
        closes the output file and flush the buffer
        """
        if self.output_file:
            self.output_file.flush()
            self.output_file.close()

    def write_output_file(self, line):
        """
        Write the lines into the output file. Override this function for more elaborate writing.
        @param line:
        """
        self.output_file.write(line)

    def open_output_file(self):
        """
        Opens a compressed gzip file by default.
        """
        self.output_file = gzip.open(self.output_file_path, 'wb')
