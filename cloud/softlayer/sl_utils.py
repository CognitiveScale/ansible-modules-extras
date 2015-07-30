__author__ = 'kestenbroughton'


def get_instance_name_id_pairs(sl, regex=None):
    results = sl.list_instances(hostname=regex)
    pairs = []
    for r in results:
        pairs.append((r['hostname'], r['id']))
    return pairs

def get_tags(sl, instance_id):
    tag_refs = sl.get_instance(instance_id)['tagReferences']
    tags = []
    for ref in tag_refs:
        tags.append(ref['tag']['name'])
    return tags