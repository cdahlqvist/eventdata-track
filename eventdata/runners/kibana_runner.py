import elasticsearch
import json
import time

import logging

logger = logging.getLogger("track.elasticlogs")

def __find_time_interval(query):
    interval_found = False
    ts_min = 0
    ts_max = 0
    ts_format = ""
    field = ""

    if 'query' in query and 'bool' in query['query'] and 'must' in query['query']['bool']:
        query_clauses = query['query']['bool']['must']

        for clause in query_clauses:
            if 'range' in clause:
                for key in clause.keys():
                    if 'lte' in clause[key] and 'gte' in clause[key] and 'format' in clause[key]:
                        field = key
                        ts_min = clause[key]['gte']
                        ts_max = clause[key]['lte']
                        format = clause[key]['format']

    return interval_found, field, ts_min, ts_max, ts_format

def __index_wildcard(index_spec):
    if isinstance(index_spec['index'], str):
        if '*' in index_spec['index']:
            return True, index_spec['index']
        else:
            return False, ""
    elif isinstance(index_spec['index'], list) and len(index_spec['index']) == 1:
        if '*' in index_spec['index'][0]:
            return True, index_spec['index'][0]
        else:
            return False, ""

def __perform_field_stats_lookup(es, index_pattern, field, min_val, max_val, fmt):
    req_body = { 'fields': field, 'index_constraints': {}}
    req_body['index_constraints'][field] = {'max_value': {'gte': min_val, 'format': fmt}, 'min_value': {'lte': max_val, 'format': fmt}}
    result = es.field_stats(index=index_pattern, level='indices')

    indices_list = result['indices'].keys()

    return indices_list

def __get_ms_timestamp():
    return int(round(time.time() * 1000))

def kibana(es, params):
    """
    Simulates Kibana msearch dashboard queries.

    It expects the parameter hash to contain the following keys:
        "body"    - msearch request body representing the Kibana dashboard in the  form of an array of dicts.
        "timeout" - Timeout for dashboard request in seconds. This excludes calls to the field stats API. Defaults to 30s.
    """
    request = params['body']

    logger.debug("[kibana_runner] Received request: {}".format(json.dumps(request)))

    field_stat_start = __get_ms_timestamp()

    # Loops through visualisations and calls field stats API for each one without caching, which is what 
    # Kibana currently does
    visualisations = len(request) / 2

    for i in range(0,len(request),2):
        pattern_found, pattern = __index_wildcard(request[i])

        cache = {}

        if pattern_found:
            interval_found, field, ts_min, ts_max, ts_fmt = __find_time_interval(request[i + 1])
            key = "{}-{}-{}".format(pattern, ts_min, ts_max)

            if key in cache.keys():
                request[i]['index'] = cache[key]
            else:
                request[i]['index'] = __perform_field_stats_lookup(es, pattern, field, ts_min, ts_max, ts_fmt)
                cache[key] = request[i]['index']

    field_stat_duration = __get_ms_timestamp() - field_stat_start

    logger.debug("[kibana_runner] Updated request: {}".format(json.dumps(request)))

    if 'timeout' in params.keys():
        tout = params['timeout']
    else:
        tout = 30000

    response = es.msearch(body = request, timeout = tout)

    logger.debug("[kibana_runner] response: {}".format(json.dumps(response)))

    return {
        "weight": 1,
        "unit": "ops",
        "timeout_ms": tout,
        "field_stats_duration_ms": field_stat_duration,
        "visualisation_count": visualisations
    }
