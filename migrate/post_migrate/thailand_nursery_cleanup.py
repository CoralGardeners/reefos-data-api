# clean up fragments placed in REMOVED nursery

import reefos_data_api.query_firestore as qq

# %%
project_id = "restoration-ios"
creds = "restoration-ios-firebase-adminsdk-wg0a4-18ff398018.json"

qf = qq.QueryFirestore(project_id=project_id, creds=creds)

org_id = qf.get_org_by_name("Coral Gardeners")[0]

branch_name = 'Thailand'
branch_id = qf.get_branch_by_name(org_id, branch_name)[0]


def get_nursery_data(nursery_name, branch_id):
    nursery_query = qf.query_nurseries({'branchID': branch_id})
    nursery_query = qf.add_filter(nursery_query, {'name': nursery_name})
    docs = qf.get_docs(nursery_query)
    if len(docs) == 0:
        return None, [], []
    nursery_id, nursery = docs[0]
    
    loc = {'nurseryID': nursery_id}
    # get structures in the nursery
    structures = qf.get_docs(qf.query_structures(loc))
    # get fragments in tyhe nursery
    fragments = qf.get_docs(qf.query_fragments(loc))
    return nursery_id, structures, fragments


def delete_nursery(qf, nursery_id, structures, fragments, debug):
    if nursery_id is None:
        return
    updater = qq.UpdateFirestore(qf)
    
    # delete fragments
    for idx, frag in enumerate(fragments):
        if idx % 100 == 0:
            print(f"Deleting {idx} of {len(fragments)}")
        updater.delete_document('_fragments', frag[0], debug=debug)
        if debug and idx > 10:
            break
    
    # delete nursery structures
    for idx, struc in enumerate(structures):
        updater.delete_document('_sites', struc[0], debug=debug)
        if debug and idx > 10:
            break
    
    # delete the nursery
    updater.delete_document('_sites', nursery_id, debug=debug)

# %%
# remove the 'trashcan' nursery
# nursery_id, structures, fragments = get_nursery_data("REMOVE", branch_id)


# %%
# do the cleanup
# delete_nursery(qf, nursery_id, structures, fragments, False)

# %%
# remove sandbox nursery
nursery_id, structures, fragments = get_nursery_data("Sandbox", branch_id)
delete_nursery(qf, nursery_id, structures, fragments, False)


