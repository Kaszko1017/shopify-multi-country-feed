import sys
import logging
from orchestrator.sync_orchestrator import SyncOrchestrator
from orchestrator.config_validator import ConfigValidator
from orchestrator.usage import usage

# Configure logging once
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

logger = logging.getLogger(__name__)

def main():
    orchestrator = SyncOrchestrator()
    
    # CLI dispatch table
    MODES = {
        "smart": orchestrator.run_smart,
        "full": orchestrator.run_full,
        "incremental": orchestrator.run_incremental,
        "cleanup": orchestrator.cleanup_orphaned_files,
        "refresh-mapping": orchestrator.refresh_mapping_cache,
        "clear-cache": orchestrator.clear_mapping_cache,
        "debug": orchestrator.debug_state,
    }

    if len(sys.argv) < 2:
        usage()
        sys.exit(1)

    mode = sys.argv[1].lower()
    handler = MODES.get(mode)
    
    if not handler:
        logger.error(f"Unknown command: '{mode}'")
        usage()
        sys.exit(1)

    try:
        # Quick validation for non-debug commands
        if mode not in ['debug', 'clear-cache']:
            ConfigValidator.validate_all()  # Will raise if critical errors
        
        handler()
        
    except KeyboardInterrupt:
        logger.info("Sync interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Command '{mode}' failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
