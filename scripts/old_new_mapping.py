# get id of names for each document type (org, branch etc.
# move images from fragments to new fragment monitoring events

import migrate.current_firestore as cf
import pandas as pd

import reefos_data_api.query_firestore as qq
from reefos_data_api.firestore_constants import EventType as et

creds = 'restoration-ios-firebase-adminsdk-wg0a4-18ff398018.json'
project_id="restoration-ios"

old_firestore = 'restoration-ios-firebase-adminsdk-wg0a4-18ff398018.json'

# %%
def not_in_sandbox(doc):
    path = doc.reference.path
    parts = path.split('/')
    if len(parts) >= 4 and parts[3] == 'sandbox':
        return False
    return True


# the current firestore documents
all_docs_df = cf.get_current_firestore(creds=old_firestore)
# remove sandbox
docs_df = all_docs_df[all_docs_df.doc.apply(not_in_sandbox)]
# group by document type
doc_grps = {dt: df for dt, df in docs_df.groupby('doc_type')}

qf = qq.QueryFirestore(project_id=project_id, creds=creds)
uf = qq.UpdateFirestore(qf)


# %%
def get_orgs(doc_grps):
    orgs = []
    for doc in doc_grps['Orgs']['doc']:
        org = {
            'name': doc.get('name'),
            'orgID': doc.id,
            'type': 'org',
            'path': doc.reference.path
            }
        org['newOrgID'] = qf.get_org_by_name(org['name'])[0]
        orgs.append(org)
    return orgs


def get_branches(doc_grps, org_map):
    sites = []
    for doc in doc_grps['Branches']['doc']:
        path = doc.reference.path.split('/')
        orgID = path[1]
        branch = {
            'orgID': orgID,
            'branchID': doc.id,
            'name': doc.get('name'),
            'type': 'branch',
            'path': doc.reference.path
            }
        branch['newBranchID'] = qf.get_branch_by_name(org_map[orgID], branch['name'])[0]
        sites.append(branch)
    return sites


def get_nurseries(doc_grps, org_map, branch_map):
    sites = []
    for doc in doc_grps['Nurseries']['doc']:
        path = doc.reference.path.split('/')
        orgID = path[1]
        branchID = path[3]
        nursery = {
            'orgID': orgID,
            'branchID': branchID,
            'nurseryID': doc.id,
            'name': doc.get('name'),
            'type': 'nursery',
            'path': doc.reference.path
            }
        new_nursery = qf.get_nursery_by_name(org_map[orgID],
                                             branch_map[branchID],
                                             nursery['name'])
        if new_nursery:
            nursery['newNurseryID'] = new_nursery[0]
        else:
            continue
        sites.append(nursery)
    return sites


def get_structures(doc_grps, nursery_map):
    def get_new_struct_id(struc, new_df):
        _df = new_df[(new_df.structuretype == struc['structuretype']) &
                     (new_df.structure == struc['structure'])]
        if len(_df) > 0:
            return _df.doc_id.iloc[0]
        return None
    
    sites = []
    new_structures = qf.get_docs(qf.query_structures({}))
    new_struc_df = qf.documents_to_dataframe(new_structures, ['location', 'siteData'])
    for doc in doc_grps['Structures']['doc']:
        path = doc.reference.path.split('/')
        nurseryID = path[5]
        new_nid = nursery_map.get(nurseryID)
        if new_nid is None:
            print(f"Missing nursery: {path}")
            continue
        new_df = new_struc_df[new_struc_df.nurseryID == new_nid]
        struc = {'orgID': path[1],
               'branchID': path[3],
               'nurseryID': nurseryID,
               'structureID': doc.id,
               'type': 'structure',
               'path': doc.reference.path,
               'structuretype': doc.get('type'),
               'structure': doc.get('position')
            }
        new_struct_id = get_new_struct_id(struc, new_df)
        if new_struct_id is not None:
            struc['newStructureID'] = new_struct_id
            sites.append(struc)
    return sites

# %%
org_info = get_orgs(doc_grps)
org_map = {val['orgID']: val['newOrgID'] for val in org_info}

branch_info = get_branches(doc_grps, org_map)
branch_map = {val['branchID']: val['newBranchID'] for val in branch_info}

nursery_info = get_nurseries(doc_grps, org_map, branch_map)
nursery_map = {val['nurseryID']: val['newNurseryID'] for val in nursery_info}
full_nursery_map = {val['nurseryID']: val for val in nursery_info}

struc_info = get_structures(doc_grps, nursery_map)
struc_map = {val['structureID']: val['newStructureID'] for val in struc_info}

# %%

# get new db events, which should contain image paths
events = qf.get_docs(qf.query_events({}, et.frag_monitor.value))
events_df = qf.documents_to_dataframe(events, ['location', 'eventData', 'metadata'])
events_df.set_index('structureID', inplace=True)

# %%
# process fragments and get images

fragment_images = []
fragments = doc_grps['Fragments']['doc']
for idx, doc in enumerate(fragments):
    if idx % 1000 == 0:
        print(f"Processing fragment {idx} of {len(fragments)}")
    frag_id = doc.id
    doc = doc.to_dict()
    if len(doc['healths']) > 0:
        if 'location' in doc:
            loc = doc['location']
        elif 'previousLocations' in doc:
            loc = doc['previousLocations'][0]
        else:
            loc = None
        if loc is not None and loc['structureID'] != 'unknown':
            # get monitoring events for this structure
            new_struc_id = struc_map.get(loc['structureID'])
            if new_struc_id is None:
                continue
            edf = events_df.loc[new_struc_id]
            # isolate events for this fragment
            edf = edf[(edf._1 == loc['substructure']) & (edf._2 == loc['position'])]
            for health in doc['healths']:
                if 'images' in health and len(health['images']) > 0:
                    # get new db monitoring of this healths record
                    event = edf[edf.createdAt == health['created']]
                    if len(event) == 0:
                        continue
                    event = event.iloc[0]
                    eid = event.doc_id
                    if len(event['images']) == len(health['images']):
                        continue
                    for img in health['images']:
                        fragment_images.append({
                            'image': img,
                            'event_id': eid,
                            'orgID': full_nursery_map[loc['nurseryID']]['orgID'],
                            'branchID': full_nursery_map[loc['nurseryID']]['branchID'],
                            'loc': loc
                            })

# %%

# add image names to events and move the files if needed

debug = False
bucket = qf.get_bucket("restoration-ios.appspot.com")

events_dict = dict(events)
updated_events = []
missing_files = []

for idx, fragment_image in enumerate(fragment_images):
    if idx % 1000 == 0:
        print(f"Processing image {idx} of {len(fragment_images)}")
    loc = fragment_image['loc']
    org_id = fragment_image['orgID']
    branch_id = fragment_image['branchID']
    event_id = fragment_image['event_id']
    event = events_dict[event_id]
    image = fragment_image['image']
    new_image = {}
    for img_type, img_name in image.items():
        if img_type == 'side':
            new_image[img_type] = img_name
            continue
        name_parts = img_name.split('/')
        if len(name_parts) > 2 and name_parts[1] == 'images':
            full_name = (org_id + '/' +  branch_id + img_name)
            new_name = name_parts[-1]
            new_path = 'images/' + event_id + '/' + new_name
            # copy
            blob = bucket.blob(full_name)
            if not blob.exists():
                if len(missing_files) % 100 == 0:
                    print(f'Missing {len(missing_files)} {full_name}')
                info = loc.copy()
                info['file'] = full_name
                missing_files.append(info)
                continue
            new_blob = bucket.blob(new_path)
            if new_blob.exists():
                print(f'New file already exists {new_path}')
            elif not debug:
                print(f'Copying to {new_path}')
                blob_copy = bucket.copy_blob(blob,
                                             bucket,
                                             new_path)
            else:
                print(f"Would copy {full_name}")
            new_image[img_type] = new_path
    if len(new_image) > 1:
        images = event['eventData'].get('images')
        if images is None or len(images) > 4:
            event['eventData']['images'] = []
        event['eventData']['images'].append(new_image)
        print(f"Updating event {event_id}")
        if not debug:
            uf.update_document('_events', event_id, event)
        else:
            updated_events.append((event_id, event))

# %%
# tiaia_nid = 'C75MK720PAXGsMr4W4HK'

# mdf = pd.DataFrame(missing_files)
# mdf.to_csv('missing_files.csv', index=None)

# tdf = mdf[mdf.nurseryID == tiaia_nid]
# %%
# tdf2 = tdf[tdf.structure == 2]

# tdf_2_1 = tdf2[tdf2.substructure == 1]

# %%
# mdf['year'] = mdf.date.dt.year