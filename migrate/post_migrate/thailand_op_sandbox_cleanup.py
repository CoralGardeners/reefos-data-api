# remove sandbox outplant and fragments 

import reefos_data_api.query_firestore as qq

# %%
project_id = "restoration-ios"
creds = "restoration-ios-firebase-adminsdk-wg0a4-18ff398018.json"

qf = qq.QueryFirestore(project_id=project_id, creds=creds)

org_id = qf.get_org_by_name("Coral Gardeners")[0]

branch_name = 'Thailand'
branch_id = qf.get_branch_by_name(org_id, branch_name)[0]


# %%
def get_outplant_data(op_name, branch_id):
    op_query = qf.query_outplantsites({'branchID': branch_id})
    op_query = qf.add_filter(op_query, {'name': op_name})
    docs = qf.get_docs(op_query)
    if len(docs) == 0:
        return None, [], []
    op_id, outplant = docs[0]
    
    loc = {'outplantID': op_id}
    # get structures in the nursery
    cells = qf.get_docs(qf.query_outplantcells(loc))
    # get fragments in the outplant
    fragments = qf.get_docs(qf.query_fragments(loc))
    return op_id, cells, fragments


def delete_outplant(qf, op_id, cells, fragments, debug):
    if op_id is None:
        return
    updater = qq.UpdateFirestore(qf)
    
    # delete fragments
    print(f"Deleting {len(fragments)} fragments")
    for idx, frag in enumerate(fragments):
        updater.delete_document('_fragments', frag[0], debug=debug)
        if debug and idx > 10:
            break
    
    print("Deleting cells")
    # delete outplant cells
    for idx, cell in enumerate(cells):
        updater.delete_document('_sites', cell[0], debug=debug)
        if debug and idx > 10:
            break
    
    # delete the outplant
    print("Deleting outplant")
    updater.delete_document('_sites', op_id, debug=debug)

# %%
op_id, cells, fragments = get_outplant_data("sand box to test", branch_id)

# %%
delete_outplant(qf, op_id, cells, fragments, False)
