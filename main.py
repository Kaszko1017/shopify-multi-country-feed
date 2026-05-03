"""Command-line entry for Shopify multi-country feed generation."""
import argparse
import logging
import sys
from pathlib import Path
from typing import Optional, Sequence

try:
    from dotenv import load_dotenv
    env_path = Path(__file__).resolve().parent / "project.env"
    load_dotenv(env_path)
except Exception:
    pass

from core.config import load_config
from core.orchestrator import ConfigValidator, SyncOrchestrator
from targets.google import TSVExporter
from targets.meta import CSVExporter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

COMMANDS = ("smart", "full", "incremental", "refresh-mapping", "debug")
TARGETS = ("google", "meta")


def _project_root() -> Path:
    """Repository root (directory containing main.py)."""
    return Path(__file__).resolve().parent


def usage(parser: argparse.ArgumentParser):
    parser.print_help()
    logger.info("Commands: smart, full, incremental, refresh-mapping, debug")
    logger.info("Targets: google (TSV), meta (CSV)")


def main(argv: Optional[Sequence[str]] = None):
    parser = argparse.ArgumentParser(
        description="Shopify multi-country feed sync (Google Merchant Center TSV / Meta catalog CSV)"
    )
    parser.add_argument(
        "command",
        nargs="?",
        choices=COMMANDS,
        help="Sync command to run",
    )
    parser.add_argument(
        "--target",
        choices=TARGETS,
        default="google",
        help="Output target: google (TSV) or meta (CSV). Default: google",
    )
    args = parser.parse_args(argv)

    if not args.command:
        usage(parser)
        sys.exit(1)

    base_dir = _project_root()
    load_config(base_dir=base_dir, target=args.target)

    if args.target == "meta":
        exporter = CSVExporter()
    else:
        exporter = TSVExporter()

    orchestrator = SyncOrchestrator(exporter=exporter)

    MODES = {
        "smart": orchestrator.run_smart,
        "full": orchestrator.run_full,
        "incremental": orchestrator.run_incremental,
        "refresh-mapping": orchestrator.refresh_mapping_cache,
        "debug": orchestrator.debug_state,
    }

    handler = MODES[args.command]

    if args.command != "debug":
        config_errors = ConfigValidator.validate_all()
        if config_errors:
            logger.error("Configuration validation failed:")
            for e in config_errors:
                logger.error(f"  - {e}")
            sys.exit(1)

    try:
        handler()
    except KeyboardInterrupt:
        logger.info("Sync interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Command '{args.command}' failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
