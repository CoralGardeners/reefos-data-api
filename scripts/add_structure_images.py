# get id of names for each document type (org, branch etc.

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
        doc_dict = doc.to_dict()
        struc = {'orgID': path[1],
               'branchID': path[3],
               'nurseryID': nurseryID,
               'structureID': doc.id,
               'type': 'structure',
               'path': doc.reference.path,
               'structuretype': doc_dict.get('type'),
               'structure': doc_dict.get('position'),
               'image_urls': doc_dict.get('image_urls')
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
# %%
struc_info = get_structures(doc_grps, nursery_map)
struc_map = {val['structureID']: val['newStructureID'] for val in struc_info}
full_struc_map = {val['structureID']: val for val in struc_info}

# %%

debug = False

for structureID, struc_info in full_struc_map.items():
    new_struc = qf.get_document("_sites", struc_info["newStructureID"])
    if struc_info['image_urls'] is not None:
        new_struc['siteData']['image_urls'] = struc_info['image_urls']
        if not debug:
            print("Updating structure")
            uf.update_document('_sites', struc_info["newStructureID"], new_struc)
        else:
            print(f"Would update structure {new_struc}")
    