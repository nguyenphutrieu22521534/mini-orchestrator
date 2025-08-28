# Python Mini Orchestrator

A powerful Python-based orchestration tool that supports both threading and process-based parallel execution for different types of workloads.

## Features

- **Dual Execution Modes**: Threading (I/O-bound) and Process (CPU-bound)
- **Configurable Workers**: Specify the number of workers for optimal performance
- **Extensible Architecture**: Easy to add new features and job types
- **Clean CLI Interface**: Simple command-line interface with comprehensive help
- **Error Handling**: Robust error handling and logging
- **Type Hints**: Full type annotation support

## Project Structure

```
mini-orchestrator/
├── main.py                 # CLI entry point
├── core/                   # Core framework
│   ├── __init__.py
│   ├── base.py            # Base classes for jobs and workers
│   ├── registry.py        # Feature registration system
│   ├── decorators.py      # Utility decorators
│   └── utils.py           # Utility functions
├── jobs/                   # Job implementations
│   ├── __init__.py
│   ├── ingest.py          # Log ingestion job (I/O-bound)
│   └── prime.py           # Prime calculation job (CPU-bound)
├── workers/                # Worker implementations
│   ├── __init__.py
│   ├── thread_worker.py   # Threading workers
│   └── process_worker.py  # Process workers
├── serializers/            # Data serialization
│   ├── __init__.py
│   └── json_ser.py        # JSON serialization utilities
├── data/                   # Sample data
│   ├── logs/              # Sample log files
│   │   └── sample.log
│   └── ranges.json        # Prime calculation ranges
├── requirements.txt        # Dependencies
└── README.md              # This file
```

## Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd mini-orchestrator
```

2. Ensure Python 3.7+ is installed:

```bash
python --version
```

3. No external dependencies required - uses only Python standard library!

## Usage

### Basic Syntax

```bash
python main.py <feature> --path <path> --worker <num> --mode <threading|process>
```

### Available Features

#### 1. Log Ingest & Parser (I/O-bound)

Processes log files and extracts metrics:

```bash
python main.py ingest --path ./data/logs --worker 4 --mode threading
```

**Output Metrics:**

- Total requests count
- Requests by HTTP method (GET, POST, etc.)
- Requests by status class (2xx, 3xx, 4xx, 5xx)
- Latency statistics (mean, median, min, max, std dev)
- User agent analysis

#### 2. Prime Number Calculator (CPU-bound)

Calculates prime numbers in specified ranges:

```bash
python main.py prime --path ./data/ranges.json --worker 8 --mode process
```

**Output:**

- Total primes found
- Prime density statistics
- Largest and smallest primes
- First 20 primes found

### Command Line Options

- `feature`: Choose between `ingest` or `prime`
- `--path`: Path to input data (directory for logs, file for ranges)
- `--worker`: Number of workers (default: 4)
- `--mode`: Execution mode - `threading` or `process` (default: threading)
- `--verbose` or `-v`: Enable verbose logging

### Examples

```bash
# Process logs with 6 workers using threading
python main.py ingest --path ./data/logs --worker 6 --mode threading

# Calculate primes with 8 workers using process mode
python main.py prime --path ./data/ranges.json --worker 8 --mode process

# Enable verbose logging
python main.py ingest --path ./data/logs --worker 4 --mode threading --verbose
```

## Architecture

### Core Components

1. **BaseJob**: Abstract base class for all jobs
2. **FeatureRegistry**: Factory pattern for feature management
3. **ThreadWorker/ProcessWorker**: Worker implementations
4. **JSONSerializer**: Data serialization utilities

### Execution Modes

#### Threading Mode

- **Best for**: I/O-bound operations (file reading, network requests)
- **Use case**: Log parsing, data ingestion
- **Benefits**: Efficient for concurrent I/O operations

#### Process Mode

- **Best for**: CPU-bound operations (mathematical calculations)
- **Use case**: Prime number calculation, data processing
- **Benefits**: True parallel execution, bypasses GIL


## Performance Considerations

### Threading vs Process Selection

- **Use Threading for**:

  - File I/O operations
  - Network requests
  - Database operations
  - Any I/O-bound tasks

- **Use Process for**:
  - Mathematical calculations
  - Data processing
  - CPU-intensive algorithms
  - Any CPU-bound tasks

### Worker Count Optimization

- **Threading**: Can use more workers than CPU cores (I/O bound)
- **Process**: Optimal at CPU core count or slightly higher
- **Testing**: Start with 4 workers and adjust based on performance

## Error Handling

The orchestrator includes comprehensive error handling:

- **File not found**: Graceful handling with informative messages
- **Invalid input**: Validation with helpful error messages
- **Worker failures**: Individual worker failures don't stop the entire job
- **Logging**: Detailed logging for debugging and monitoring

## Logging

The system provides multiple logging levels:

- **INFO**: General execution information
- **DEBUG**: Detailed debugging information (use `--verbose`)
- **WARNING**: Non-critical issues
- **ERROR**: Critical errors that affect execution

## Sample Data

### Log Files

Sample log files are provided in `data/logs/` with the format:

```
[METHOD] /path, status=XXX, time=XXXms user-agent=XXX
```

### Prime Calculation Ranges

Configuration file `data/ranges.json` contains number ranges for prime calculation testing.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Implement your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is open source and available under the MIT License.

## Support

For issues and questions, please open an issue on the repository.
