# run in firestore-cleanup folder
#
# ahe fragments were outplanted without proper data recoreding
# Salome's records show that 3924 coeals were outplanted from
# structures L1 and L2 in Ahe nursery
# and that the nursery is not empty
#
# this script
# 1. creates an outplant site
# 2. create an outplant cell
# 3. move 3924 fragments from the nursery to the outplant cell
# 4. marks remaining fragments as 'removed'
#
# the outplanting occured during Aug and Sep 2025 so all events will be given 
#  date of Sep 1 2025

import datetime as dt
import numpy as np
import pandas as pd
import reefos_data_api.query_firestore as qq
from reefos_data_api.firestore_constants import FragmentState as fs
from reefos_data_api.firestore_constants import SiteType as st
from reefos_data_api.firestore_constants import CellType as ct
from reefos_data_api.firestore_constants import EventType as et

debug = False

n_outplanted = 3924
nursery_name = 'Ahe'
outplant_name = "Pana Pana"
outplant_date = dt.datetime(2025, 9, 1)

project_id = "restoration-ios"
creds = "restoration-ios-firebase-adminsdk-wg0a4-18ff398018.json"

qf = qq.QueryFirestore(project_id=project_id, creds=creds)
uf = qq.UpdateFirestore(qf)

org_id = qf.get_org_by_name("Coral Gardeners")[0]

branch_name = 'French Polynesia'
branch_id = qf.get_branch_by_name(org_id, branch_name)[0]

def set_metadata(doc, createdAt=None, createdBy=None):
    doc['metadata'] = {'createdAt': createdAt,
                       'createdBy': createdBy,
                       'deleted': False}

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

nursery_id, structures, fragments = get_nursery_data(nursery_name, branch_id)

# %%
# 1. create outplant
outplant_perimeter = np.array([
    [-146.3598683408703,-14.46735125686665],
    [-146.3598685294884,-14.46744105122881],
    [-146.3595860673707,-14.46749816861273],
    [-146.3592142940266,-14.46750231631898],
    [-146.3586302163384,-14.46775320068572],
    [-146.3586331377749,-14.46735402080431],
    [-146.3591279097387,-14.467163930135],
    [-146.3592280048475,-14.46736709561321],
    [-146.3598683408703,-14.46735125686665],
])

perimeter = [{'latitude': val[1], 'longitude': val[0]} for val in outplant_perimeter]

geolocation = {'latitude': outplant_perimeter[:,1].mean(),
               'longitude': outplant_perimeter[:,0].mean()}

loc = {'orgID': org_id, 'branchID': branch_id}

siteData = {
    'controlSiteID': None,
    'monitoringSettings': [],
    'reefos_id': None,
    'restorationGoals': None,
    'schedules': None
}

outplant_data = {
    'name': outplant_name,
    'siteType': st.outplant.value,
    'location': loc,
    'geolocation': geolocation,
    'perimeter': perimeter,
    'siteData': siteData
    }
set_metadata(outplant_data, createdAt=outplant_date)

op_id = uf.add_document('_sites', outplant_data, debug=debug)

# %%
# 2. create outplant cell
cell_name = 'Kaua1'
cell_loc = {'orgID': org_id, 'branchID': branch_id, 'outplantID': op_id}

cell_data = {
    'name': cell_name,
    'location': cell_loc,
    'geolocation': geolocation,
    'siteType': st.outplantcell.value,
    'cellType': ct.region.value
    }
set_metadata(cell_data, createdAt=outplant_date)

cell_id = uf.add_document('_sites', cell_data, debug=debug)

# %%

# make frags into dataframe
fdf = qf.documents_to_dataframe(fragments, ['location'])

# get health of frags
healths = qf.get_docs(qf.query_events({'nurseryID': nursery_id},
                                      event_type=et.frag_monitor.value))
hdf = qf.documents_to_dataframe(healths, ['location', 'eventData'])
frag_healths = hdf.groupby('fragmentID')['health'].max()

# add max health score, if any, to each frag
fdf['health'] = fdf.doc_id.map(frag_healths)
# use health and state to separate dead and alive
alive_fdf = fdf[(fdf.health != 6) & (fdf.state != fs.removed.value)]
dead_fdf = fdf[(fdf.health == 6) | (fdf.state == fs.removed.value)]

# random select alive fragments for 'outplanting'
op_fdf = alive_fdf.sample(n=n_outplanted)
remove_fdf = alive_fdf[~alive_fdf.index.isin(op_fdf.index)]

# %%
frag_collection = '_fragments'

# 3. set fragments to outplanted
print("Updating outplanted")
n = 0
for idx in op_fdf.index:
    if n % 10 == 0:
        print(f"Processing {n} of {len(op_fdf)}")
    n += 1
    frag = fragments[idx]
    frag_id = frag[0]
    frag_data = frag[1]
    frag_data['state'] = fs.outplanted.value
    frag_data['completedAt'] = outplant_date
    frag_data['location']['outplantID'] = op_id
    frag_data['location']['outplantCellID'] = cell_id
    uf.update_document(frag_collection, frag_id, frag_data, debug=debug)    

# %%
# 4. set fragments not outplanted to removed
print("Updating removed")
n = 0
for idx in remove_fdf.index:
    if n % 10 == 0:
        print(f"Processing {n} of {len(remove_fdf)}")
    n += 1
    frag = fragments[idx]
    frag_id = frag[0]
    frag_data = frag[1]
    frag_data['state'] = fs.completed.value
    frag_data['completedAt'] = outplant_date
    uf.update_document(frag_collection, frag_id, frag_data, debug=debug)    

# set dead fragments to removed, set completed time in removed frags
print("Updating dead")
n = 0
for idx in dead_fdf.index:
    if n % 10 == 0:
        print(f"Processing {n} of {len(dead_fdf)}")
    n += 1
    frag = fragments[idx]
    frag_id = frag[0]
    frag_data = frag[1]
    if frag_data['state'] != fs.removed.value:
        frag_data['state'] = fs.completed.value
    if pd.isnull(frag_data['completedAt']):
        frag_data['completedAt'] = outplant_date
    uf.update_document(frag_collection, frag_id, frag_data, debug=debug)    

