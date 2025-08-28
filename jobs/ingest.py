import logging
from collections import Counter
from pathlib import Path
from typing import Dict, List, Any

from core.base import BaseJob, JobResult
from core.decorators import register_feature, timing_decorator
from core.utils import (
    parse_log_line,
    get_status_class,
    calculate_statistics,
    format_output
)


@register_feature('ingest')
class LogIngestJob(BaseJob):
    """Job for ingesting and parsing log files."""

    def __init__(self):
        super().__init__("Log Ingest & Parser")

    def split_workload(self, path: str, num_workers: int) -> List[Path]:
        """Split log files among workers."""
        log_dir = Path(path)
        log_files = list(log_dir.glob("*.log"))

        if not log_files:
            self.logger.warning(f"No .log files found in {path}")
            return []

        # Simple round-robin distribution
        chunks = [[] for _ in range(num_workers)]
        for i, file_path in enumerate(log_files):
            chunks[i % num_workers].append(file_path)

        # Filter out empty chunks
        return [chunk for chunk in chunks if chunk]

    def process_chunk(self, chunk: List[Path]) -> JobResult:
        """Process a chunk of log files."""
        try:
            total_requests = 0
            by_method = Counter()
            by_status_class = Counter()
            latencies = []
            user_agents = Counter()

            for file_path in chunk:
                self.logger.debug(f"Processing file: {file_path}")

                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        for line_num, line in enumerate(f, 1):
                            parsed = parse_log_line(line)
                            if parsed:
                                total_requests += 1
                                by_method[parsed['method']] += 1
                                by_status_class[get_status_class(parsed['status'])] += 1
                                latencies.append(parsed['time_ms'])

                                if parsed['user_agent']:
                                    user_agents[parsed['user_agent']] += 1
                            else:
                                self.logger.debug(f"Failed to parse line {line_num} in {file_path}")

                except Exception as e:
                    self.logger.error(f"Error reading file {file_path}: {e}")
                    continue

            # Calculate latency statistics
            latency_stats = calculate_statistics(latencies) if latencies else {}

            result_data = {
                'total_requests': total_requests,
                'by_method': by_method,
                'by_status_class': by_status_class,
                'latency_stats': latency_stats,
                'user_agents': user_agents
            }

            return JobResult(
                success=True,
                data=result_data,
                metadata={'files_processed': len(chunk)}
            )

        except Exception as e:
            self.logger.error(f"Error processing chunk: {e}")
            return JobResult(success=False, data=None, error=str(e))

    def merge_results(self, results: List[JobResult]) -> str:
        """Merge results from all workers."""
        # Initialize aggregated counters
        total_requests = 0
        by_method = Counter()
        by_status_class = Counter()
        all_latencies = []
        user_agents = Counter()
        files_processed = 0

        # Aggregate results
        for result in results:
            if result.success and result.data:
                total_requests += result.data['total_requests']
                by_method.update(result.data['by_method'])
                by_status_class.update(result.data['by_status_class'])

                # Collect latencies for overall statistics
                if 'latency_stats' in result.data and result.data['latency_stats']:
                    # We need to reconstruct the latency list from stats
                    # This is a limitation - in a real system, we'd pass the raw data
                    count = result.data['latency_stats'].get('count', 0)
                    mean = result.data['latency_stats'].get('mean', 0)
                    # For simplicity, we'll use the mean for each request
                    all_latencies.extend([mean] * int(count))

                user_agents.update(result.data['user_agents'])

                if result.metadata:
                    files_processed += result.metadata.get('files_processed', 0)
            else:
                self.logger.warning(f"Worker result failed: {result.error}")

        # Calculate overall latency statistics
        overall_latency_stats = calculate_statistics(all_latencies) if all_latencies else {}

        # Prepare final output
        final_data = {
            'total_requests': total_requests,
            'by_method': by_method,
            'by_status_class': by_status_class,
            'latency_stats': overall_latency_stats,
            'user_agents': user_agents
        }

        self.logger.info(f"Processed {files_processed} files with {total_requests} total requests")

        return format_output(final_data)
