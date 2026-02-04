# run in reefos-data-api folder

from datetime import datetime, date
from reefos_data_api import endpoints as ep
from reefos_data_api import query_firestore as qq
import json
import numpy as np

# %%
creds = 'restoration-ios-firebase-adminsdk-wg0a4-18ff398018.json'
proj = "restoration-ios"
devcreds = "restoration-app---dev-6df41-firebase-adminsdk-fbsvc-fd29c504a1.json"
devproj = "restoration-app---dev-6df41"
qf = qq.QueryFirestore(project_id=proj, creds=creds)

orgs = qf.get_orgs()

cg_org_id = ep.get_orgbyname(qf, 'Coral Gardeners')
#cg_org_id = cg[0][0]

branches = qf.get_branches(cg_org_id)
branches.sort(key=lambda x: x[1]['name'])

# %%
branch_id = branches[1][0]
nurseries = qf.get_docs(qf.query_nurseries({"branchID": branch_id}))
nurseries.sort(key=lambda x: x[1]['name'])
outplants = qf.get_docs(qf.query_outplantsites({"branchID": branch_id}))
outplants.sort(key=lambda x: x[1]['name'])
controls = qf.get_docs(qf.query_controlsites({"branchID": branch_id}))
controls.sort(key=lambda x: x[1]['name'])


def nan_to_none(obj):
    if isinstance(obj, dict):
        return {k: nan_to_none(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [nan_to_none(v) for v in obj]
    elif isinstance(obj, float) and np.isnan(obj):
        return None
    return obj


class _custom_json_encoder(json.JSONEncoder):
    def encode(self, obj, *args, **kwargs):
        return super().encode(nan_to_none(obj), *args, **kwargs)

    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, np.datetime64):
            return str(obj)
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(_custom_json_encoder, self).default(obj)


def _make_return_data(data):
    ret = {
        'data': data,
        'status': 200,
        'error': None
        }
    return json.dumps(ret, cls=_custom_json_encoder)


# %%
# endpoint: global
# data.coralgardeners.org/data/global?org=coral-gardeners&pwd=showmethedata
print("Global")
global_data = _make_return_data(ep.global_stats(qf, cg_org_id))

# %%
# endpoint: by_year
# data.coralgardeners.org/data/byyear?org=coral-gardeners&pwd=showmethedata
print("By-year")
by_year = _make_return_data(ep.by_year_stats(qf, cg_org_id))

# %%
# endpoint: branches
# data.coralgardeners.org/data/branches?org=coral-gardeners&pwd=showmethedata
print("Branches")
branches_data = _make_return_data(ep.branch_stats(qf, {'orgID': cg_org_id}))

# %%
# endpoint: branch
# data.coralgardeners.org/data/branch?org=coral-gardeners&branch=moorea&pwd=showmethedata
print("Branch")
bd = ep.branch_stats(qf, branches[1][0])
branch_data = _make_return_data(bd)

#%%
# endpoint nursery
# data.coralgardeners.org/data/nursery?org=coral-gardeners&nursery=xxx&pwd=showmethedata
print("Nursery")
nid = nurseries[3][0]
nd = ep.full_nursery_stats(qf, nid)
nursery_data = _make_return_data(nd)
# %%
# endpoint outplant
# data.coralgardeners.org/data/outplant?org=coral-gardeners&outplant=aceCTY014DM4DBejGaWX&pwd=showmethedata
print("Outplant")
op_id = 'yFGyv9RB6YmdNWktBGpw'  # outplants[-1][0]
outplant_data = ep.outplant_stats(qf, op_id, details=False)

# endpoint mothercolonies
# data.coralgardeners.org/data/mothercolonies?org=coral-gardeners&branch=moorea&pwd=showmethedata
# %%
# endpoint fragments
# data.coralgardeners.org/data/fragments?nursery=aceCTY014DM4DBejGaWX&pwd=showmethedata
print("Fragments")
fragment_data = ep.nursery_fragment_stats(qf, nurseries[2][0])
# %%
print("Donor colonies")
mc_data = ep.get_donor_stats(qf, branch_id)
