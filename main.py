import argparse
import sys
import logging
from pathlib import Path
from typing import Optional

from core.registry import FeatureRegistry, get_registry
from core.utils import setup_logging

# Import job modules to register features
import jobs.ingest
import jobs.prime

def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Python Mini Orchestrator - CLI Management Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py ingest --path ./data/logs --worker 4 --mode threading
  python main.py prime --path ./data/ranges.json --worker 8 --mode process
        """
    )

    parser.add_argument(
        'feature',
        choices=['ingest', 'prime'],
        help='Feature to execute (ingest: log parsing, prime: CPU-bound calculation)'
    )

    parser.add_argument(
        '--path',
        required=True,
        help='Path to input data (directory for logs, file for ranges)'
    )

    parser.add_argument(
        '--worker',
        type=int,
        default=4,
        help='Number of workers (default: 4)'
    )

    parser.add_argument(
        '--mode',
        choices=['threading', 'process'],
        default='threading',
        help='Execution mode (default: threading)'
    )

    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Enable verbose logging'
    )

    return parser.parse_args()


def validate_arguments(args: argparse.Namespace) -> None:
    """Validate command line arguments."""
    path = Path(args.path)

    if not path.exists():
        raise ValueError(f"Path does not exist: {args.path}")

    if args.feature == 'ingest':
        if not path.is_dir():
            raise ValueError(f"Log ingest requires a directory path: {args.path}")
    elif args.feature == 'prime':
        if not path.is_file():
            raise ValueError(f"Prime calculation requires a file path: {args.path}")

    if args.worker < 1:
        raise ValueError("Number of workers must be at least 1")

    # Validate mode selection based on feature
    if args.feature == 'ingest' and args.mode == 'process':
        print("Warning: Log ingest is I/O-bound and works better with threading mode")
    elif args.feature == 'prime' and args.mode == 'threading':
        print("Warning: Prime calculation is CPU-bound and works better with process mode")


def main() -> None:
    """Main entry point."""
    try:
        args = parse_arguments()
        validate_arguments(args)

        # Setup logging
        log_level = logging.DEBUG if args.verbose else logging.INFO
        setup_logging(log_level)

        logger = logging.getLogger(__name__)
        logger.info(f"Starting {args.feature} feature with {args.worker} workers in {args.mode} mode")

        # Get and execute the feature
        registry = get_registry()
        feature = registry.get_feature(args.feature)

        if not feature:
            logger.error(f"Feature '{args.feature}' not found")
            sys.exit(1)

        # Execute the feature
        result = feature.execute(
            path=args.path,
            num_workers=args.worker,
            mode=args.mode
        )

        # Display results
        print("\n" + "=" * 50)
        print(f"FEATURE: {args.feature.upper()}")
        print(f"MODE: {args.mode.upper()}")
        print(f"WORKERS: {args.worker}")
        print("=" * 50)
        print(result)

    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
