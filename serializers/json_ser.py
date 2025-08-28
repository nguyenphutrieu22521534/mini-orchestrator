import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional


class JSONSerializer:
    """JSON serialization utility class."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def serialize(self, data: Any, file_path: str, indent: int = 2) -> bool:
        """Serialize data to JSON file."""
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=indent, ensure_ascii=False)
            self.logger.debug(f"Data serialized to {file_path}")
            return True
        except Exception as e:
            self.logger.error(f"Error serializing data to {file_path}: {e}")
            return False

    def deserialize(self, file_path: str) -> Optional[Any]:
        """Deserialize data from JSON file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            self.logger.debug(f"Data deserialized from {file_path}")
            return data
        except Exception as e:
            self.logger.error(f"Error deserializing data from {file_path}: {e}")
            return None

    def serialize_results(self, results: Dict[str, Any], output_dir: str, filename: str) -> str:
        """Serialize job results to JSON file."""
        output_path = Path(output_dir) / f"{filename}.json"

        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if self.serialize(results, str(output_path)):
            return str(output_path)
        else:
            raise RuntimeError(f"Failed to serialize results to {output_path}")

    def load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from JSON file."""
        config = self.deserialize(config_path)
        if config is None:
            raise ValueError(f"Failed to load configuration from {config_path}")
        return config

    def save_config(self, config: Dict[str, Any], config_path: str) -> bool:
        """Save configuration to JSON file."""
        return self.serialize(config, config_path)


class ResultSerializer(JSONSerializer):
    """Specialized serializer for job results."""

    def serialize_job_result(self, result_data: Dict[str, Any], job_name: str, output_dir: str) -> str:
        """Serialize job result with metadata."""
        result_with_metadata = {
            'job_name': job_name,
            'timestamp': self._get_timestamp(),
            'data': result_data
        }

        filename = f"{job_name}_result_{self._get_timestamp()}"
        return self.serialize_results(result_with_metadata, output_dir, filename)

    def _get_timestamp(self) -> str:
        """Get current timestamp as string."""
        from datetime import datetime
        return datetime.now().strftime("%Y%m%d_%H%M%S")
