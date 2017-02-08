from eventdata.parameter_sources.elasticlogs_bulk_source import ElasticlogsBulkSource
from eventdata.runners import rollover_runner, scanquery_runner, createindex_runner

def register(registry):
    registry.register_param_source("elasticlogs", ElasticlogsBulkSource)
    registry.register_runner("rollover", rollover_runner.rollover)
    registry.register_runner("scanquery", scanquery_runner.scanquery)
    registry.register_runner("createindex", createindex_runner.createindex)
