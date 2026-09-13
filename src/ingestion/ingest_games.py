import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.ingestion.config import IngestionConfig
from src.ingestion.runner import IngestionRunner
from src.ingestion.writers import NoopWriter, SnowflakeWriter


def main():
	parser = argparse.ArgumentParser(description="Run modular MaxPreps ingestion units")
	parser.add_argument("--config", required=True, help="Path to a JSON ingestion config")
	parser.add_argument("--dry-run", action="store_true", help="Scrape/cache without Snowflake writes")
	args = parser.parse_args()

	config = IngestionConfig.from_json(args.config)
	if args.dry_run:
		writer = NoopWriter()
	else:
		from src.utils.snowflake_utils import get_snowflake_connection

		writer = SnowflakeWriter(
			get_snowflake_connection,
			chunk_size=config.snowflake_chunk_size,
		)
	results = IngestionRunner(config, writer).run()
	for unit_id, status in results:
		print(f"{status}: {unit_id}")


if __name__ == "__main__":
	main()
