import logging

logger = logging.getLogger(__name__)

def usage():
    """Print usage information."""
    logger.error("Usage: python main.py [command]")
    logger.info("Commands:")
    logger.info(" smart - Sync with change detection")
    logger.info(" full - Force full sync")
    logger.info(" incremental - Incremental sync")
    logger.info(" refresh-mapping - Refresh mapping cache")
    logger.info(" debug - Show debug information")
