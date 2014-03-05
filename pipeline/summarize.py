def create_summary_collection(requests, summary):

    set_overall_summary(requests, summary)
    set_overall_countries(requests, summary)
    set_overall_date(requests, summary)

    set_handle_countries(requests, summary)
    set_handle_dates(requests, summary)
    set_handle_authors(requests, summary)

    set_author_dates(requests, summary)
    set_author_countries(requests, summary)
    set_author_summary(requests, summary)
    set_author_dlcs(requests, summary)

    set_dlc_summary(requests, summary)
    set_dlc_dates(requests, summary)
    set_dlc_countries(requests, summary)

def aggregate(coll, query):
    results = coll.aggregate(query)
    for result in results['result']:
        yield result

def update(coll, id, data):
    coll.update({'_id': id}, data, True)

def set_overall_summary(requests, summary):
    query = [
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
    for result in aggregate(requests, query):
        update(summary, 'Overall',
               {'$set': {'type': 'overall', 'size': result['size'], 'downloads': result['downloads']}})

def set_overall_countries(requests, summary):
    query = [
        { '$group': {
            '_id': '$country',
            'downloads': { '$sum': 1 }
        } },
        { '$group': {
            '_id': 'null',
            'countries': {
                '$push': {
                    'country': '$_id',
                    'downloads': '$downloads'
                }
            }
        } }
    ]
    for result in aggregate(requests, query):
        update(summary, 'Overall', {'$set': {'countries': result['countries']}})

def set_overall_date(requests, summary):
    query = [
        { '$group': {
            '_id': { '$substr': ['$time',0,10] },
            'downloads': { '$sum': 1 }
        } },
        { '$group': {
            '_id': 'null',
            'dates': {
                '$push': {
                    'date': '$_id',
                    'downloads': '$downloads'
                }
            }
        } }
    ]
    for result in aggregate(requests, query):
        update(summary, 'Overall', {'$set': {'dates': result['dates']}})

def set_handle_countries(requests, summary):
    query = [
        { '$group': {
            '_id': { 'country': '$country', 'handle': '$handle' },
            'downloads': { '$sum': 1 }
        } },
        { '$group': {
            '_id': '$_id.handle',
            'countries': {
                '$push': {
                    'country': '$_id.country',
                    'downloads': '$downloads'
                }
            }
        } }
    ]
    for result in aggregate(requests, query):
        update(summary, result['_id'], { '$set': { 'countries': result['countries'] } })

def set_handle_dates(requests, summary):
    for handle in requests.distinct('handle'):
        dates = []
        query = [
            { '$match': { 'handle': handle } },
            { '$group': {
                '_id': { '$substr': ['$time',0,10] },
                'downloads': { '$sum': 1 }
            } }
        ]
        for result in aggregate(requests, query):
            dates.append({'date': result['_id'], 'downloads': result['downloads']})
        update(summary, handle, { '$set': { 'dates': dates } })

def set_handle_authors(requests, summary):
    query = [
        { '$match': { 'authors': { '$exists': True } } },
        { '$unwind': '$authors' },
        { '$group': {
            '_id': '$handle',
            'parents': {
                '$addToSet': '$authors'
            }
        } }
    ]
    for result in aggregate(requests, query):
        update(summary, result['_id'], { '$set': { 'parents': result['parents'] } })

def set_author_dates(requests, summary):
    for author in requests.distinct('authors'):
        dates = []
        query = [
            { '$match': { 'authors': { '$in': [ author ] } } },
            { '$group': {
                '_id': { '$substr': ['$time', 0, 10] },
                'downloads': { '$sum': 1 }
            } }
        ]
        for result in aggregate(requests, query):
            dates.append({'date': result['_id'], 'downloads': result['downloads']})
        update(summary, author, { '$set': { 'dates': dates} })

def set_author_countries(requests, summary):
    query = [
        { '$match': { 'authors': { '$exists': True } } },
        { '$unwind': '$authors' },
        { '$group': {
            '_id': { 'country': '$country', 'author': '$authors' },
            'downloads': { '$sum': 1 }
        } },
        { '$group': {
            '_id': '$_id.author',
            'countries': {
                '$push': {
                    'country': '$_id.country',
                    'downloads': '$downloads'
                }
            }
        } }
    ]
    for result in aggregate(requests, query):
        update(summary, result['_id'], {'$set': {'countries': result['countries']}})

def set_author_summary(requests, summary):
    query = [
        { '$match': { 'authors': { '$exists': True } } },
        { '$unwind': '$authors' },
        { '$group': {
            '_id': { 'handle': '$handle', 'author': '$authors' },
            'downloads': { '$sum': 1 }
        } },
        { '$group': {
            '_id': '$_id.author',
            'downloads': { '$sum': '$downloads' },
            'size': { '$sum': 1 }
        } }
    ]
    for result in aggregate(requests, query):
        update(summary, result['_id'],
               {'$set': {'type': 'author', 'size': result['size'], 'downloads': result['downloads']}})

def set_author_dlcs(requests, summary):
    query = [
        { '$match': { 'authors': { '$exists': True } } },
        { '$unwind': '$authors' },
        { '$unwind': '$dlcs' },
        { '$group': {
            '_id': '$authors',
            'parents': { '$addToSet': '$dlcs' }
        } }
    ]
    for result in aggregate(requests, query):
        update(summary, result['_id'], {'$set': {'parents': result['parents']}})

def set_dlc_summary(requests, summary):
    query = [
        { '$unwind': '$dlcs' },
        { '$group': {
            '_id': { 'handle': '$handle', 'dlc': '$dlcs' },
            'downloads': { '$sum': 1 }
        } },
        { '$group': {
            '_id': '$_id.dlc',
            'downloads': { '$sum': '$downloads' },
            'size': { '$sum': 1 }
        } }
    ]
    for result in aggregate(requests, query):
        update(summary, result['_id'],
               {'$set': {'type': 'dlc', 'size': result['size'], 'downloads': result['downloads']}})

def set_dlc_dates(requests, summary):
    for dlc in requests.distinct('dlcs'):
        dates = []
        query = [
            { '$match': { 'dlcs': { '$in': [ dlc ] } } },
            { '$group': {
                '_id': { '$substr': ['$time', 0, 10] },
                'downloads': { '$sum': 1 }
            } }
        ]
        for result in aggregate(requests, query):
            dates.append({'date': result['_id'], 'downloads': result['downloads']})
        update(summary, dlc, { '$set': { 'dates': dates } })

def set_dlc_countries(requests, summary):
    query = [
        { '$unwind': '$dlcs' },
        { '$group': {
            '_id': { 'country': '$country', 'dlc': '$dlcs' },
            'downloads': { '$sum': 1 }
        } },
        { '$group': {
            '_id': '$_id.dlc',
            'countries': {
                '$push': {
                    'country': '$_id.country',
                    'downloads': '$downloads'
                }
            }
        } }
    ]
    for result in aggregate(requests, query):
        update(summary, result['_id'], {'$set': {'countries': result['countries']}})
