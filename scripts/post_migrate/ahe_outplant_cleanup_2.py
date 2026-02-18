# run in reefos-data-api folder
#
# initial ahe fix:
# ahe fragments were outplanted without proper data recoreding
# Salome's records show that 3924 coeals were outplanted from
# structures L1 and L2 in Ahe nursery
# and that the nursery is empty
#
# was incorrect.  The 'removed frags were still in place and outplanted week of Feb 9 2026
#
# this script
# 1. gets the Ahe outplant site and cell
# 2. finds the completed Ahe fragments
# 3. 'outplants them on Feb 12
#
# the outplanting occured during Aug and Sep 2025 so all events will be given 
#  date of Sep 1 2025

import datetime as dt
import reefos_data_api.query_firestore as qq
from reefos_data_api.firestore_constants import FragmentState as fs

debug = False

nursery_name = 'Ahe'
outplant_name = "Pana Pana"
outplant_date = dt.datetime(2026, 2, 12)

project_id = "restoration-ios"
creds = "restoration-ios-firebase-adminsdk-wg0a4-18ff398018.json"

qf = qq.QueryFirestore(project_id=project_id, creds=creds)
uf = qq.UpdateFirestore(qf)

org_id = qf.get_org_by_name("Coral Gardeners")[0]

branch_name = 'French Polynesia'
branch_id = qf.get_branch_by_name(org_id, branch_name)[0]
op = qf.get_docs(qf.add_filter(
        qf.query_outplantsites({'branchID': branch_id}),
        {'name': outplant_name}))

op_id = op[0][0]
outplant = op[0][1]

# %%

def get_nursery_data(nursery_name, branch_id):
    nursery_query = qf.query_nurseries({'branchID': branch_id})
    nursery_query = qf.add_filter(nursery_query, {'name': nursery_name})
    docs = qf.get_docs(nursery_query)
    if len(docs) == 0:
        return None, [], []
    nursery_id, nursery = docs[0]
    
    loc = {'nurseryID': nursery_id}
    # get fragments in the nursery
    fragments = qf.get_docs(qf.query_fragments(loc))
    return nursery_id, fragments

nursery_id, fragments = get_nursery_data(nursery_name, branch_id)

cells = qf.get_docs(qf.query_outplantcells({'outplantID': op_id}))

cell_id = cells[0][0]
# %%

# make frags into dataframe
fdf = qf.documents_to_dataframe(fragments, ['location'])

cmplt_fdf = fdf[fdf.state == fs.completed.value]

# %%
frag_collection = '_fragments'

# update completed fragments to outplanted
print("Updating removed")
n = 0
for idx in cmplt_fdf.index:
    if n % 10 == 0:
        print(f"Processing {n} of {len(cmplt_fdf)}")
    n += 1
    frag = fragments[idx]
    frag_id = frag[0]
    frag_data = frag[1]
    frag_data['state'] = fs.outplanted.value
    frag_data['completedAt'] = outplant_date
    frag_data['location']['outplantID'] = op_id
    frag_data['location']['outplantCellID'] = cell_id
    uf.update_document(frag_collection, frag_id, frag_data, debug=debug)    

