import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, List, Optional

from core.base import BaseWorker


class ThreadWorker(BaseWorker):
    """Thread-based worker for I/O-bound tasks."""

    def __init__(self, name: str = "ThreadWorker"):
        super().__init__(name)
        self._executor: Optional[ThreadPoolExecutor] = None

    def work(self, task: Any) -> Any:
        """Execute work in a thread."""
        try:
            # For thread workers, we typically handle I/O operations
            if hasattr(task, '__call__'):
                return task()
            else:
                return task
        except Exception as e:
            self.logger.error(f"Error in thread worker: {e}")
            raise

    def process_batch(self, tasks: List[Any], max_workers: int = None) -> List[Any]:
        """Process a batch of tasks using thread pool."""
        results = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {
                executor.submit(self.work, task): task
                for task in tasks
            }

            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result = future.result()
                    results.append(result)
                    self.logger.debug(f"Completed task: {task}")
                except Exception as e:
                    self.logger.error(f"Task failed: {task}, Error: {e}")
                    results.append(None)

        return results

    def __enter__(self):
        """Context manager entry."""
        self._executor = ThreadPoolExecutor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self._executor:
            self._executor.shutdown(wait=True)


class FileIOWorker(ThreadWorker):
    """Specialized worker for file I/O operations."""

    def __init__(self):
        super().__init__("FileIOWorker")

    def read_file_chunk(self, file_path: str, start_line: int, end_line: int) -> List[str]:
        """Read a specific chunk of lines from a file."""
        try:
            lines = []
            with open(file_path, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f, 1):
                    if start_line <= i <= end_line:
                        lines.append(line.strip())
                    elif i > end_line:
                        break
            return lines
        except Exception as e:
            self.logger.error(f"Error reading file chunk {file_path}: {e}")
            return []

    def write_file_chunk(self, file_path: str, data: List[str]) -> bool:
        """Write data to a file chunk."""
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                for line in data:
                    f.write(line + '\n')
            return True
        except Exception as e:
            self.logger.error(f"Error writing file chunk {file_path}: {e}")
            return False