# clean up fragments outplanted from the incorrect nursery

import reefos_data_api.query_firestore as qq
from reefos_data_api.firestore_constants import FragmentState as fs

project_id = "restoration-ios"
creds = "restoration-ios-firebase-adminsdk-wg0a4-18ff398018.json"

qf = qq.QueryFirestore(project_id=project_id, creds=creds)
uf = qq.UpdateFirestore(qf)

org_id = qf.get_org_by_name("Coral Gardeners")[0]
branch_name = 'Thailand'
branch_id = qf.get_branch_by_name(org_id, branch_name)[0]

nursery_name = "KM AO LOM RT-A"
outplant_name = "KM AO LOM EAST SAND"

nursery_query = qf.query_nurseries({'branchID': branch_id})
nursery_query = qf.add_filter(nursery_query, {'name': nursery_name})
nursery_id, nursery = qf.get_docs(nursery_query)[0]

outplant_name = "KM AO LOM EAST SAND"
op_query = qf.query_outplantsites({'branchID': branch_id})
op_query = qf.add_filter(op_query, {'name': outplant_name})
outplant_id, outplant = qf.get_docs(op_query)[0]

coral_tree = "Coral Tree"
structures = qf.get_docs(qf.query_structures({'nurseryID': nursery_id}))

structure_id = None
for sid, struc in structures:
    if (struc['siteData']['structuretype'] == coral_tree and
        struc['siteData']['structure'] == 1):
        structure_id = sid

fragments = qf.get_docs(qf.query_fragments({'structureID': structure_id, 'outplantID': outplant_id}))

# %%
# get fragments on branch 1, 3, 4
branches = [1, 3, 4]
for fid, frag in fragments:
    if frag['location']['_1'] in branches:
        frag['location']['outplantID'] = None
        frag['location']['outplantCellID'] = None
        frag['state'] = fs.in_nursery.value
        uf.update_document('_fragments', fid, frag)
