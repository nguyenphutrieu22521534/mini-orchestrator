import json
import logging
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional


def setup_logging(level: int = logging.INFO) -> None:
    """Setup logging configuration."""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def calculate_statistics(values: List[float]) -> Dict[str, float]:
    """Calculate statistical measures for a list of values."""
    if not values:
        return {}

    return {
        'count': len(values),
        'mean': statistics.mean(values),
        'median': statistics.median(values),
        'min': min(values),
        'max': max(values),
        'std_dev': statistics.stdev(values) if len(values) > 1 else 0.0
    }


def parse_log_line(line: str) -> Optional[Dict[str, Any]]:
    """Parse a single log line and extract relevant information."""
    try:
        # Remove leading/trailing whitespace
        line = line.strip()
        if not line:
            return None

        # Parse the log format: [METHOD] /path, status=XXX, time=XXXms user-agent=XXX
        # First split by comma to get the main parts
        main_parts = line.split(', ')
        if len(main_parts) < 2:
            return None

        # Extract method and path
        method_path = main_parts[0]
        if not method_path.startswith('[') or ']' not in method_path:
            return None

        method_end = method_path.find(']')
        method = method_path[1:method_end]
        path = method_path[method_end + 2:]  # Skip '] '

        # Extract status
        status_part = main_parts[1]
        if not status_part.startswith('status='):
            return None
        status = int(status_part.split('=')[1])

        # Extract time and user agent from the third part
        if len(main_parts) >= 3:
            time_ua_part = main_parts[2]

            # Split by space to separate time and user-agent
            time_ua_parts = time_ua_part.split(' user-agent=')
            if len(time_ua_parts) >= 2:
                time_part = time_ua_parts[0]
                user_agent = time_ua_parts[1]
            else:
                time_part = time_ua_part
                user_agent = None

            # Extract time
            if time_part.startswith('time='):
                time_str = time_part.split('=')[1]
                if time_str.endswith('ms'):
                    time_ms = float(time_str[:-2])
                else:
                    time_ms = float(time_str)
            else:
                time_ms = 0.0
        else:
            time_ms = 0.0
            user_agent = None

        return {
            'method': method,
            'path': path,
            'status': status,
            'time_ms': time_ms,
            'user_agent': user_agent
        }

    except (ValueError, IndexError) as e:
        return None


def get_status_class(status: int) -> str:
    """Get the status class (2xx, 3xx, 4xx, 5xx) from HTTP status code."""
    return f"{status // 100}xx"


def format_output(data: Dict[str, Any]) -> str:
    """Format output data as a readable string."""
    output_lines = []

    # Total requests
    output_lines.append(f"Total Requests: {data.get('total_requests', 0):,}")
    output_lines.append("")

    # By method
    if 'by_method' in data:
        output_lines.append("Requests by Method:")
        for method, count in data['by_method'].most_common():
            output_lines.append(f"  {method}: {count:,}")
        output_lines.append("")

    # By status class
    if 'by_status_class' in data:
        output_lines.append("Requests by Status Class:")
        for status_class, count in data['by_status_class'].most_common():
            output_lines.append(f"  {status_class}: {count:,}")
        output_lines.append("")

    # Latency statistics
    if 'latency_stats' in data:
        output_lines.append("Latency Statistics (ms):")
        stats = data['latency_stats']
        output_lines.append(f"  Count: {stats.get('count', 0):,}")
        output_lines.append(f"  Mean: {stats.get('mean', 0):.2f}")
        output_lines.append(f"  Median: {stats.get('median', 0):.2f}")
        output_lines.append(f"  Min: {stats.get('min', 0):.2f}")
        output_lines.append(f"  Max: {stats.get('max', 0):.2f}")
        output_lines.append(f"  Std Dev: {stats.get('std_dev', 0):.2f}")
        output_lines.append("")

    # User agents
    if 'user_agents' in data:
        output_lines.append("Top User Agents:")
        for ua, count in data['user_agents'].most_common(10):
            output_lines.append(f"  {ua}: {count:,}")

    return "\n".join(output_lines)


def load_json_file(file_path: str) -> Dict[str, Any]:
    """Load and parse a JSON file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        raise ValueError(f"Error loading JSON file {file_path}: {e}")


def ensure_directory(path: str) -> None:
    """Ensure a directory exists, create if it doesn't."""
    Path(path).mkdir(parents=True, exist_ok=True)
