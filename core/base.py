import logging
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass


@dataclass
class JobResult:
    """Result container for job execution."""
    success: bool
    data: Any
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class BaseJob(ABC):
    """Abstract base class for all jobs."""

    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"{self.__class__.__name__}")

    @abstractmethod
    def process_chunk(self, chunk: Any) -> JobResult:
        """Process a single chunk of data."""
        pass

    @abstractmethod
    def split_workload(self, path: str, num_workers: int) -> List[Any]:
        """Split the workload into chunks for parallel processing."""
        pass

    @abstractmethod
    def merge_results(self, results: List[JobResult]) -> str:
        """Merge results from all workers into final output."""
        pass

    def execute(self, path: str, num_workers: int, mode: str) -> str:
        """Execute the job with the specified number of workers and mode."""
        self.logger.info(f"Starting {self.name} with {num_workers} workers in {mode} mode")

        # Split workload
        chunks = self.split_workload(path, num_workers)
        self.logger.info(f"Split workload into {len(chunks)} chunks")

        # Process chunks in parallel
        results = self._process_parallel(chunks, num_workers, mode)

        # Merge and return results
        return self.merge_results(results)

    def _process_parallel(self, chunks: List[Any], num_workers: int, mode: str) -> List[JobResult]:
        """Process chunks in parallel using the specified mode."""
        results = []

        if mode == 'threading':
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                future_to_chunk = {
                    executor.submit(self.process_chunk, chunk): chunk
                    for chunk in chunks
                }

                for future in as_completed(future_to_chunk):
                    chunk = future_to_chunk[future]
                    try:
                        result = future.result()
                        results.append(result)
                        self.logger.debug(f"Processed chunk: {chunk}")
                    except Exception as e:
                        self.logger.error(f"Error processing chunk {chunk}: {e}")
                        results.append(JobResult(success=False, data=None, error=str(e)))

        elif mode == 'process':
            with ProcessPoolExecutor(max_workers=num_workers) as executor:
                future_to_chunk = {
                    executor.submit(self.process_chunk, chunk): chunk
                    for chunk in chunks
                }

                for future in as_completed(future_to_chunk):
                    chunk = future_to_chunk[future]
                    try:
                        result = future.result()
                        results.append(result)
                        self.logger.debug(f"Processed chunk: {chunk}")
                    except Exception as e:
                        self.logger.error(f"Error processing chunk {chunk}: {e}")
                        results.append(JobResult(success=False, data=None, error=str(e)))

        return results


class BaseWorker(ABC):
    """Abstract base class for workers."""

    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"{self.__class__.__name__}")

    @abstractmethod
    def work(self, task: Any) -> Any:
        """Perform the actual work on a task."""
        pass

    def __call__(self, task: Any) -> Any:
        """Make the worker callable."""
        return self.work(task)
