import elasticsearch

def scanscroll(es, params):
    """
    Runs a scan and scroll search against Elasticsearch retrieving either all (default) or a specific number of results.

    It expects the parameter hash to contain the following keys:
        "index"           - Specifies the index pattern to search
        "doc_type"        - List of document types to search. Leave out or blank to search all types.
        "query"           - Specified the query to base the scan and scroll on
        "size"            - Specifies the size (per shard) of the batch sent at each iteration (optional)
        "scroll"          - Specifies how long a consistent view of the index should be maintained for scrolled search (optional)
        "result_size"     - Specifies the maximum number of documents to be fetched. Defalts to all matching documents if not specified
    """
    if 'doc_type' in params:
      dt = params['doc_type']
    else:
      dt = ""

    if 'size' in params:
      sz = params['size']
    else:
      sz = 1000

    if 'scroll' in params:
      sc = params['scroll']
    else:
      sc = "5m"

    if 'result_size' in params:
      result_size = params['result_size']
    else:
      result_size = 0

    result_count = 0
    scroll = es.helpers.scan(index=params["index"], doc_type=dt, query=params['query'], size=sz, scroll=sc)
    
    for res in scroll:
        result_count += 1
        if result_size > 0 and result_count == result_size:
            break

    return {
        "weight": 1,
        "unit": "ops",
        "result_count": result_count
    }