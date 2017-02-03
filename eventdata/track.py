from parameter_sources.elasticlogs_bulk_source import ElasticlogsBulkSource

import runners.rollover_runner
import runners.scanscroll_runner
import runners.createindex_runner

def register(registry):
    registry.register_param_source("elasticlogs", ElasticlogsBulkSource)
    registry.register_runner("rollover", rollover_runner.rollover)
    registry.register_runner("scanscroll", scanscroll_runner.scanscroll)
    registry.register_runner("createindex", createindex_runner.createindex)
