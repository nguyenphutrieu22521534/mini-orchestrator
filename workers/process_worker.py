import logging
import math
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any, Callable, List, Optional

from core.base import BaseWorker


class ProcessWorker(BaseWorker):
    """Process-based worker for CPU-bound tasks."""

    def __init__(self, name: str = "ProcessWorker"):
        super().__init__(name)
        self._executor: Optional[ProcessPoolExecutor] = None

    def work(self, task: Any) -> Any:
        """Execute work in a separate process."""
        try:
            # For process workers, we typically handle CPU-intensive operations
            if hasattr(task, '__call__'):
                return task()
            else:
                return task
        except Exception as e:
            self.logger.error(f"Error in process worker: {e}")
            raise

    def process_batch(self, tasks: List[Any], max_workers: int = None) -> List[Any]:
        """Process a batch of tasks using process pool."""
        results = []

        # Use CPU count if max_workers not specified
        if max_workers is None:
            max_workers = multiprocessing.cpu_count()

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
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
        self._executor = ProcessPoolExecutor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self._executor:
            self._executor.shutdown(wait=True)


class CPUIntensiveWorker(ProcessWorker):
    """Specialized worker for CPU-intensive mathematical operations."""

    def __init__(self):
        super().__init__("CPUIntensiveWorker")

    def calculate_primes_range(self, start: int, end: int) -> List[int]:
        """Calculate prime numbers in a given range."""
        import math

        primes = []
        for num in range(max(2, start), end + 1):
            if self._is_prime(num):
                primes.append(num)
        return primes

    def _is_prime(self, n: int) -> bool:
        """Check if a number is prime using optimized trial division."""
        if n < 2:
            return False
        if n == 2:
            return True
        if n % 2 == 0:
            return False

        # Check odd divisors up to square root
        sqrt_n = int(math.sqrt(n)) + 1
        for i in range(3, sqrt_n, 2):
            if n % i == 0:
                return False

        return True

    def fibonacci_sequence(self, n: int) -> List[int]:
        """Generate Fibonacci sequence up to n terms."""
        if n <= 0:
            return []
        elif n == 1:
            return [0]
        elif n == 2:
            return [0, 1]

        sequence = [0, 1]
        for i in range(2, n):
            sequence.append(sequence[i - 1] + sequence[i - 2])

        return sequence

    def factorial_calculation(self, n: int) -> int:
        """Calculate factorial of n."""
        if n < 0:
            raise ValueError("Factorial is not defined for negative numbers")
        if n == 0 or n == 1:
            return 1

        result = 1
        for i in range(2, n + 1):
            result *= i

        return result
