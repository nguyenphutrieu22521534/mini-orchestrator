import logging
import math
from typing import List, Tuple, Dict, Any

from core.base import BaseJob, JobResult
from core.decorators import register_feature, timing_decorator
from core.utils import load_json_file


@register_feature('prime')
class PrimeCalculationJob(BaseJob):
    """Job for CPU-intensive prime number calculations."""

    def __init__(self):
        super().__init__("Prime Number Calculator")

    def split_workload(self, path: str, num_workers: int) -> List[Tuple[int, int]]:
        """Split number ranges among workers."""
        try:
            # Load configuration from JSON file
            config = load_json_file(path)
            ranges = config.get('ranges', [])

            if not ranges:
                self.logger.warning("No ranges found in configuration file")
                return []

            # Distribute ranges among workers
            chunks = [[] for _ in range(num_workers)]
            for i, range_data in enumerate(ranges):
                start = range_data.get('start', 1)
                end = range_data.get('end', 1000)
                chunks[i % num_workers].append((start, end))

            # Filter out empty chunks
            return [chunk for chunk in chunks if chunk]

        except Exception as e:
            self.logger.error(f"Error loading configuration: {e}")
            return []

    def process_chunk(self, chunk: List[Tuple[int, int]]) -> JobResult:
        """Process a chunk of number ranges to find primes."""
        try:
            all_primes = []
            total_numbers_checked = 0

            for start, end in chunk:
                self.logger.debug(f"Processing range {start}-{end}")

                # Find primes in this range
                primes_in_range = self._find_primes_in_range(start, end)
                all_primes.extend(primes_in_range)
                total_numbers_checked += (end - start + 1)

            # Calculate statistics
            stats = {
                'primes_found': len(all_primes),
                'total_numbers_checked': total_numbers_checked,
                'largest_prime': max(all_primes) if all_primes else 0,
                'smallest_prime': min(all_primes) if all_primes else 0,
                'prime_density': len(all_primes) / total_numbers_checked if total_numbers_checked > 0 else 0
            }

            return JobResult(
                success=True,
                data={
                    'primes': all_primes,
                    'statistics': stats,
                    'ranges_processed': len(chunk)
                },
                metadata={'ranges_processed': len(chunk)}
            )

        except Exception as e:
            self.logger.error(f"Error processing chunk: {e}")
            return JobResult(success=False, data=None, error=str(e))

    def _find_primes_in_range(self, start: int, end: int) -> List[int]:
        """Find all prime numbers in the given range."""
        primes = []

        # Ensure start is at least 2
        if start < 2:
            start = 2

        for num in range(start, end + 1):
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

    def merge_results(self, results: List[JobResult]) -> str:
        """Merge results from all workers."""
        all_primes = []
        total_numbers_checked = 0
        total_ranges_processed = 0

        # Aggregate results
        for result in results:
            if result.success and result.data:
                all_primes.extend(result.data['primes'])
                total_numbers_checked += result.data['statistics']['total_numbers_checked']
                total_ranges_processed += result.data['statistics'].get('ranges_processed', 0)
            else:
                self.logger.warning(f"Worker result failed: {result.error}")

        # Calculate overall statistics
        if all_primes:
            overall_stats = {
                'total_primes_found': len(all_primes),
                'total_numbers_checked': total_numbers_checked,
                'largest_prime': max(all_primes),
                'smallest_prime': min(all_primes),
                'prime_density': len(all_primes) / total_numbers_checked if total_numbers_checked > 0 else 0,
                'ranges_processed': total_ranges_processed
            }
        else:
            overall_stats = {
                'total_primes_found': 0,
                'total_numbers_checked': total_numbers_checked,
                'largest_prime': 0,
                'smallest_prime': 0,
                'prime_density': 0,
                'ranges_processed': total_ranges_processed
            }

        # Format output
        output_lines = []
        output_lines.append("Prime Number Calculation Results")
        output_lines.append("=" * 40)
        output_lines.append(f"Total Primes Found: {overall_stats['total_primes_found']:,}")
        output_lines.append(f"Total Numbers Checked: {overall_stats['total_numbers_checked']:,}")
        output_lines.append(f"Prime Density: {overall_stats['prime_density']:.6f}")
        output_lines.append(f"Largest Prime: {overall_stats['largest_prime']:,}")
        output_lines.append(f"Smallest Prime: {overall_stats['smallest_prime']:,}")
        output_lines.append(f"Ranges Processed: {overall_stats['ranges_processed']}")

        # Show first 20 primes if any found
        if all_primes:
            output_lines.append("")
            output_lines.append("First 20 Primes Found:")
            first_20 = sorted(all_primes)[:20]
            output_lines.append(", ".join(map(str, first_20)))

        self.logger.info(
            f"Found {overall_stats['total_primes_found']} primes across {overall_stats['ranges_processed']} ranges")

        return "\n".join(output_lines)
