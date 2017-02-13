from eventdata.parameter_sources.elasticlogs_bulk_source import ElasticlogsBulkSource
from eventdata.parameter_sources.elasticlogs_kibana_source import ElasticlogsKibanaSource
from eventdata.runners import rollover_runner, createindex_runner
from eventdata.runners import kibana_runner, indexstats_runner

def register(registry):
    registry.register_param_source("elasticlogs_bulk", ElasticlogsBulkSource)
    registry.register_param_source("elasticlogs_kibana", ElasticlogsKibanaSource)
    registry.register_runner("rollover", rollover_runner.rollover)
    registry.register_runner("createindex", createindex_runner.createindex)
    registry.register_runner("kibana", kibana_runner.kibana)
    registry.register_runner("indexstats", indexstats_runner.indexstats)
