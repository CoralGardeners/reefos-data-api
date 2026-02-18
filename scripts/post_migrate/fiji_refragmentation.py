# some mature fragments were refragmented and used to reseed
# they were marked as outplanted

# look for fragments seeded in 2025 that have the same mother colony as 
# fragment outplanted in 2025, then the outplanted fragment was the source
# of the new fragments

# reassign the correct bnumber of fragments based on outplanted number as
# reported by Fiji resto team

import datetime as dt
import reefos_data_api.query_firestore as qq
from reefos_data_api.firestore_constants import FragmentState as fs

# %%
debug = False

n_outplanted = 4312  # actual outplanted according to resto team records

start_date = dt.datetime(year=2025, month=1, day=1)

project_id = "restoration-ios"
creds = "restoration-ios-firebase-adminsdk-wg0a4-18ff398018.json"

qf = qq.QueryFirestore(project_id=project_id, creds=creds)
uf = qq.UpdateFirestore(qf)

org_id = qf.get_org_by_name("Coral Gardeners")[0]

branch_name = 'Fiji'
branch_id = qf.get_branch_by_name(org_id, branch_name)[0]

# %%
# get all fragments
loc = {'branchID': branch_id}
q = qf.query_fragments(loc)
opq = qf.add_filter(q, {"state": fs.outplanted.value})
opq = qf.add_filter(opq, {"completedAt": start_date}, '>=')
outplanted = qf.get_docs(opq)
op_df = qf.documents_to_dataframe(outplanted, ['location'])

# seeded after outplants started
nq = qf.add_date_filter(q, start_date=op_df.completedAt.min())
nq = qf.add_filter(nq, {"state": fs.in_nursery.value})
in_nursery = qf.get_docs(nq)
n_df = qf.documents_to_dataframe(in_nursery, ['location'])

actual_n_outplanted = len(op_df)

n_to_remove = actual_n_outplanted - n_outplanted

# %%
#in_nursery_mcs = [{'fragID': val[0], 'donorID': val[1]['donorID']} for val in in_nursery]
#outplanted_mcs = [{'fragID': val[0], 'donorID': val[1]['donorID']} for val in outplanted]

#ndf = pd.DataFrame(in_nursery_mcs)
#odf = pd.DataFrame(outplanted_mcs)

# %%
n_df['from_op'] = n_df.donorID.isin(op_df.donorID)
op_df['from_nursery'] = op_df.donorID.isin(n_df.donorID)

_ndf = n_df[n_df['from_op']]
_odf = op_df[op_df['from_nursery']]

# donor colonies that were refreagmented
donor_fragments = _odf.donorID.unique()
n_donors = len(donor_fragments)

# %%
# get outplanted fragments which are rom a mother colony that is also in the nursery
# these might have been reseeded
# of these, 

# get fragments tht were outplanted that will be re-labeled as refragmented
grps = _odf.groupby('donorID')
donors = grps['from_nursery'].count().sort_values(ascending=False)

# re-assign one or two outplanted fragments from each mother colony
frag_collection = '_fragments'

reassign = []
for idx, donor_id in enumerate(donors.index):
    cnt = 2 if idx < (n_to_remove - n_donors) else 1
    _df = grps.get_group(donor_id)
    reassign.extend(_df.index[0:cnt])


print("Updating outplanted")
for n, idx in enumerate(reassign):
    if n % 100 == 0:
        print(f"Processing {n} of {len(reassign)}")
    frag = outplanted[idx]
    frag_id = frag[0]
    frag_data = frag[1]
    frag_data['state'] = fs.refragmented.value
    frag_data['location']['outplantID'] = None
    frag_data['location']['outplantCellID'] = None
    uf.update_document(frag_collection, frag_id, frag_data, debug=debug)    
    