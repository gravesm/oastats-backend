def add_summary_data(req, sums):
    aggregate_query = [
        { '$group': {
            '_id': '$handle',
            'downloads': { '$sum': 1 }
        } },
        { '$group': {
            '_id': 'null',
            'size': { '$sum': 1 },
            'downloads': { '$sum': '$downloads' }
        } }
    ]

    results = req.aggregate(aggregate_query)
    for item in results['result']:
        sums.update({'_id': 'Overall'},
                    { '$set': {
                        'type': 'overall',
                        'size': item['size'],
                        'downloads': item['downloads'] } },
                    True)

def add_summary_map(req, sums):
    aggregate_query = [
        { '$group': {
            '_id': '$country',
            'downloads': { '$sum': 1 }
        } },
        { '$sort': { '_id': 1 } }
    ]
    countries = []
    res = req.aggregate(aggregate_query)

    for result in res['result']:
        countries.append({
            'country': result['_id'],
            'downloads': result['downloads']
        })

    sums.update({'_id': 'Overall'}, {'$set': {'countries': countries}}, True)

def add_summary_date(req, sums):
    aggregate_query = [
        { '$group': {
            '_id': { '$substr': ['$time', 0, 10] },
            'downloads': { '$sum': 1 }
        } },
        { '$sort': { '_id': 1 } }
    ]
    dates = []
    res = req.aggregate(aggregate_query)

    for result in res['result']:
        dates.append({
            'date': result['_id'],
            'downloads': result['downloads']
        })

    sums.update({'_id': 'Overall'}, {'$set': {'dates': dates}}, True)

def add_field_summary_date(req, sums, field):
    for item in req.distinct(field):
        results = req.aggregate(_generate_date_query(field, item))
        dates = []
        for result in results['result']:
            dates.append({
                'date': result['_id'],
                'downloads': result['downloads']
            })
        sums.update({'_id': item}, {'$set': {'dates': dates}}, True)

def add_field_summary_map(req, sums, field):
    results = req.aggregate(_generate_map_query(field))
    for res in results['result']:
        sums.update({'_id': res['_id']},
                    {'$set': {'countries': res['countries']}},
                    True)

def add_field_summary_overall(req, sums, field):
    results = req.aggregate(_generate_overall_query(field))
    for res in results['result']:
        sums.update({'_id': res['_id']},
                    {'$set': {
                        'type': field,
                        'size': res['size'],
                        'downloads': res['downloads']}},
                    True)

def add_author_dlcs(req, sums):
    results = req.aggregate([
        { '$group': { '_id': { 'author': '$author', 'dlc': '$dlc' } } },
        { '$group': { '_id': '$_id.author', 'parents': { '$push': '$_id.dlc' } } }])
    for res in results['result']:
        sums.update({'_id': res['_id']},
                    {'$set': { 'parents': res['parents']}},
                    True)

def add_handle_author(req, sums):
    results = req.aggregate([
        { '$group': { '_id': { 'handle': '$handle', 'author': '$author' } } },
        { '$group': { '_id': '$_id.handle', 'parents': { '$push': '$_id.author' } } }])
    for res in results['result']:
        sums.update({'_id': res['_id']},
                    {'$set': { 'parents': res['parents'] }},
                    True)

def _generate_overall_query(field):
    group_one = {
        '$group': {
            '_id': { 'handle': '$handle' },
            'downloads': { '$sum': 1 }
        }
    }
    group_one['$group']['_id'][field] = '$' + field
    group_two = {
        '$group': {
            '_id': '$_id.' + field,
            'size': { '$sum': 1 },
            'downloads': { '$sum': '$downloads' }
        }
    }
    return [ group_one, group_two, { '$sort': { '_id': 1 } } ]

def _generate_date_query(field, value):
    match = {}
    match[field] = value
    query = [
        { '$match': match },
        { '$group': {
            '_id': { '$substr': ['$time', 0, 10] },
            'downloads': { '$sum': 1 }
        } },
        { '$sort': { '_id': 1 } }
    ]
    return query

def _generate_map_query(field):
    group_one = {
        '$group': {
            '_id': { 'country': '$country' },
            'downloads': { '$sum': 1 }
        }
    }
    group_one['$group']['_id'][field] = '$' + field
    group_two = {
        '$group': {
            '_id': '$_id.' + field,
            'countries': {
                '$push': {
                    'country': '$_id.country',
                    'downloads': '$downloads'
                }
            }
        }
    }
    query = [ group_one, group_two, { '$sort': { '_id': 1 } } ]
    return query
