import logging

logger = logging.getLogger(__name__)

def usage():
    logger.error("Usage: python main.py [command]")
    logger.info("Commands:")
    logger.info(" smart - Sync with mapping change detection")
    logger.info(" full - Force full sync")
    logger.info(" incremental - Incremental sync")
    logger.info(" cleanup - Cleanup orphaned files")
    logger.info(" refresh-mapping - Refresh mapping cache")
    logger.info(" clear-cache - Clear mapping hash")
    logger.info(" debug - Show system state")
    logger.info("")
    logger.info("Mapping Detection:")
    logger.info(" Detects mapping changes and adjusts sync strategy:")
    logger.info(" - Mapping changed → Full sync")
    logger.info(" - Mapping unchanged → Incremental sync (if previous state exists)")
    logger.info(" - No previous state → Full sync")
