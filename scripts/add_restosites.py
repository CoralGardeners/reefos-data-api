# add restoration sites as a new site type between branch and
# (donor/nursery/outplant/contol) in current data

import numpy as np
import reefos_data_api.query_firestore as qq
from reefos_data_api.firestore_constants import SiteType as st

creds = 'restoration-ios-firebase-adminsdk-wg0a4-18ff398018.json'
project_id="restoration-ios"

qf = qq.QueryFirestore(project_id=project_id, creds=creds)
uf = qq.UpdateFirestore(qf)


cc_restosite_info = [
    {'branch': 'Gili Air', 'type': 'nursery', 'name': 'Trees Coral mouth', 'restosite': 'Gili Air'},
    {'branch': 'Gili Air', 'type': 'nursery', 'name': 'CC Underwater Nursery Lab', 'restosite': 'Gili Air'},
    {'branch': 'Gili Air', 'type': 'nursery', 'name': 'Pilot Project', 'restosite': 'Gili Air'},
    {'branch': 'Gili Air', 'type': 'nursery', 'name': 'Cordap project', 'restosite': 'Gili Air'}
]

cg_restosite_info = [
    {'branch': 'Fiji', 'type': 'nursery', 'name': 'RORO', 'restosite': 'Roro'},
    {'branch': 'Fiji', 'type': 'nursery', 'name': 'RORO 2', 'restosite': 'Roro'},
    {'branch': 'Fiji', 'type': 'nursery', 'name': 'RORO 3', 'restosite': 'Roro'},
    {'branch': 'Fiji', 'type': 'nursery', 'name': 'SIX SENSES FIJI', 'restosite': 'Six Senses Fiji'},
    {'branch': 'Fiji', 'type': 'nursery', 'name': 'SIX SENSES FIJI 2', 'restosite': 'Six Senses Fiji'},
    {'branch': 'Fiji', 'type': 'nursery', 'name': 'VAKELECIRI', 'restosite': 'Vakeleciri'},
    {'branch': 'Fiji', 'type': 'nursery', 'name': 'VAKELECIRI 2', 'restosite': 'Vakeleciri'},
    {'branch': 'Fiji', 'type': 'nursery', 'name': 'VAKELECIRI 3', 'restosite': 'Vakeleciri'},
    {'branch': 'Fiji', 'type': 'nursery', 'name': 'SAVUTI', 'restosite': 'Six Senses Fiji'},
    {'branch': 'Fiji', 'type': 'nursery', 'name': 'WADIGI', 'restosite': 'Six Senses Fiji'},

    {'branch': 'Fiji', 'type': 'outplant', 'name': 'RORO', 'restosite': 'Roro'},
    {'branch': 'Fiji', 'type': 'outplant', 'name': 'SSF REEF SITE 1', 'restosite': 'Six Senses Fiji'},
    {'branch': 'Fiji', 'type': 'outplant', 'name': 'SSF REEF SITE 2', 'restosite': 'Six Senses Fiji'},
    {'branch': 'Fiji', 'type': 'outplant', 'name': 'VAKELECIRI SITE 1', 'restosite': 'Vakeleciri'},

    {'branch': 'French Polynesia', 'type': 'nursery', 'name': 'momoa nursery', 'restosite': 'Bora Bora'},
    {'branch': 'French Polynesia', 'type': 'nursery', 'name': 'Toopua', 'restosite': 'Bora Bora'},
    {'branch': 'French Polynesia', 'type': 'nursery', 'name': 'Toopua 2', 'restosite': 'Bora Bora'},
    {'branch': 'French Polynesia', 'type': 'nursery', 'name': 'Ahe', 'restosite': 'Ahe'},
    {'branch': 'French Polynesia', 'type': 'nursery', 'name': 'TENUKUPARA', 'restosite': 'Ahe'},
    {'branch': 'French Polynesia', 'type': 'nursery', 'name': 'HQ', 'restosite': 'HQ'},
    {'branch': 'French Polynesia', 'type': 'nursery', 'name': 'Tiaia', 'restosite': 'Tiaia'},
    {'branch': 'French Polynesia', 'type': 'nursery', 'name': 'Tiaia 2', 'restosite': 'Tiaia'},
    {'branch': 'French Polynesia', 'type': 'nursery', 'name': 'Tiaia 3', 'restosite': 'Tiaia'},
    {'branch': 'French Polynesia', 'type': 'nursery', 'name': 'Tiaia 4', 'restosite': 'Tiaia'},
    {'branch': 'French Polynesia', 'type': 'nursery', 'name': 'HAURU 1', 'restosite': 'Hauru'},
    {'branch': 'French Polynesia', 'type': 'nursery', 'name': 'Hauru 2', 'restosite': 'Hauru'},
    {'branch': 'French Polynesia', 'type': 'nursery', 'name': 'AHOTOTEINA TEAHUPOO', 'restosite': 'Teahupoo'},
    {'branch': 'French Polynesia', 'type': 'nursery', 'name': 'Pihaena', 'restosite': 'Pihaena'},
    {'branch': 'French Polynesia', 'type': 'nursery', 'name': 'TIKEHAU CORAL GARDEN', 'restosite': 'Tikehau'},
    {'branch': 'French Polynesia', 'type': 'nursery', 'name': 'TOA ORA', 'restosite': 'Toa Ora'},
    {'branch': 'French Polynesia', 'type': 'nursery', 'name': 'Teavaro 1', 'restosite': 'Teavaro'},
    {'branch': 'French Polynesia', 'type': 'nursery', 'name': 'Titouan Nursery', 'restosite': 'Titouan House'},

    {'branch': 'French Polynesia', 'type': 'outplant', 'name': 'TOA ORA', 'restosite': 'Toa Ora'},
    {'branch': 'French Polynesia', 'type': 'outplant', 'name': 'HQ', 'restosite': 'HQ'},
    {'branch': 'French Polynesia', 'type': 'outplant', 'name': 'AHOTOTEINA', 'restosite': 'Teahupoo'},
    {'branch': 'French Polynesia', 'type': 'outplant', 'name': 'MHPN Bommies', 'restosite': 'Pihaena'},
    {'branch': 'French Polynesia', 'type': 'outplant', 'name': 'PHN Pilot', 'restosite': 'Pihaena'},
    {'branch': 'French Polynesia', 'type': 'outplant', 'name': 'Tiaia Experiment', 'restosite': 'Tiaia'},
    {'branch': 'French Polynesia', 'type': 'outplant', 'name': 'Titouan Outplant Site', 'restosite': 'Titouan House'},
    {'branch': 'French Polynesia', 'type': 'outplant', 'name': 'Pana Pana', 'restosite': 'Ahe'},

    {'branch': 'French Polynesia', 'type': 'control', 'name': 'Tiaia Experiment Control', 'restosite': 'Tiaia'},

    {'branch': 'Thailand', 'type': 'nursery', 'name': 'KK Koh Mai Si CT-STING R&D', 'restosite': 'KK Koh Mai Si'},
    {'branch': 'Thailand', 'type': 'nursery', 'name': 'KK Koh Mai Si CTREE-R&D', 'restosite': 'KK Koh Mai Si'},
    {'branch': 'Thailand', 'type': 'nursery', 'name': 'KK Koh Mai Si Rope-R&D', 'restosite': 'KK Koh Mai Si'},
    {'branch': 'Thailand', 'type': 'nursery', 'name': 'KK Koh Mai Si CT-A', 'restosite': 'KK Koh Mai Si'},
    {'branch': 'Thailand', 'type': 'nursery', 'name': 'KK Koh Mai Si CT-B', 'restosite': 'KK Koh Mai Si'},
    {'branch': 'Thailand', 'type': 'nursery', 'name': 'KK Koh Mai Si CT-C', 'restosite': 'KK Koh Mai Si'},
    {'branch': 'Thailand', 'type': 'nursery', 'name': 'KK Koh Mai Si RT-A', 'restosite': 'KK Koh Mai Si'},
    {'branch': 'Thailand', 'type': 'nursery', 'name': 'KK Koh Mai Si RT-B', 'restosite': 'KK Koh Mai Si'},
    {'branch': 'Thailand', 'type': 'nursery', 'name': 'KK Koh Mai Si RT-C', 'restosite': 'KK Koh Mai Si'},
    {'branch': 'Thailand', 'type': 'nursery', 'name': 'KM AO LOM Rescue R&D', 'restosite': 'KM Ao Lom'},
    {'branch': 'Thailand', 'type': 'nursery', 'name': 'KM AO LOM Rope - R&D (pre bleaching 24’)', 'restosite': 'KM Ao Lom'},
    {'branch': 'Thailand', 'type': 'nursery', 'name': 'KM Ao Lom CT-R&D (pre Bleaching ‘24)', 'restosite': 'KM Ao Lom'},
    {'branch': 'Thailand', 'type': 'nursery', 'name': 'KM AO LOM CT-A', 'restosite': 'KM Ao Lom'},
    {'branch': 'Thailand', 'type': 'nursery', 'name': 'KM AO LOM RT-A', 'restosite': 'KM Ao Lom'},
    {'branch': 'Thailand', 'type': 'nursery', 'name': 'KM AO LOM RT-B', 'restosite': 'KM Ao Lom'},

    {'branch': 'Thailand', 'type': 'outplant', 'name': 'KM AO LOM EAST SAND', 'restosite': 'KM Ao Lom'},
]

# %%
cc = False

if cc:
    org_name = 'Coral Catch'
    restosite_info = cc_restosite_info
else:
    org_name = 'Coral Gardeners'
    restosite_info = cg_restosite_info

resto_id = 'restositeID'

org_info = qf.get_org_by_name(org_name)
org_id = org_info[0]
loc = {'orgID': org_id}
branches = qf.get_branches(org_id)

nurseries = qf.get_docs(qf.query_nurseries(loc))
nursery_map = {data['name']: (_id, data) for _id, data in nurseries}

restosites = qf.get_docs_all(qf.query_restosites(loc))
restosites_map = {data['name']: (_id, data) for _id, data in restosites}

outplants = qf.get_docs(qf.query_outplantsites(loc))
outplant_map = {data['name']: (_id, data) for _id, data in outplants}

controls = qf.get_docs(qf.query_controlsites(loc))
control_map = {data['name']: (_id, data) for _id, data in controls}

donorcolonies = qf.get_docs(qf.query_donorcolonies(loc))
dc_dict = dict(donorcolonies)
nu_dict = dict(nurseries)

fragments = qf.get_docs(qf.query_fragments(loc))
# %%
nursery_siteinfo = {}
outplant_siteinfo = {}
control_siteinfo = {}
restosites = {}
for rs_info in restosite_info:
    rs_name = rs_info['restosite']
    if rs_name not in restosites:
        restosites[rs_name] = []
    branch_name = rs_info['branch']
    site_type = rs_info['type']
    site_name = rs_info['name']
    if site_type == 'nursery':
        nursery_siteinfo[site_name] = rs_info
        n_info = nursery_map.get(site_name, None)
        if n_info is None:
            print(f"Missing nursery {site_name}")
        else:
            site_info = {
                'loc': n_info[1]['location'],
                'geoloc': n_info[1]['geolocation'],
                'createdAt': n_info[1]['metadata']['createdAt']
                }
    elif site_type == 'outplant':
        outplant_siteinfo[site_name] = rs_info
        o_info = outplant_map.get(site_name, None)
        if o_info is None:
            print(f"Missing outplant {site_name}")
        else:
            site_info = {
                'loc': o_info[1]['location'],
                'geoloc': o_info[1]['geolocation'],
                'createdAt': n_info[1]['metadata']['createdAt']
                }
    elif site_type == 'control':
        control_siteinfo[site_name] = rs_info
        c_info = control_map.get(site_name, None)
        if c_info is None:
            print(f"Missing control {site_name}")
        else:
            site_info = {
                'loc': c_info[1]['location'],
                'geoloc': c_info[1]['geolocation'],
                'createdAt': n_info[1]['metadata']['createdAt']
                }
    else:
        site_info = None
    if site_info is not None:
        restosites[rs_name].append(site_info)

# compute mean location of each site
resto_site_data = {}
for rs_name, site_info in restosites.items():
    lats = []
    lons = []
    times = []
    for info in site_info:
        lats.append(info['geoloc']['latitude'])
        lons.append(info['geoloc']['longitude'])
        times.append(info['createdAt'])
    data = {
        'name': rs_name,
        'siteType': st.restosite.value,
        'location': site_info[0]['loc'],
        'geolocation': {
            'latitude': np.array(lats).mean(),
            'longitude': np.array(lons).mean(),
            },
        'metadata': {'createdAt': min(times),
                     'deleted': False}
        }
    resto_site_data[rs_name] = data

# %%
# make restosites in _site collection

if False: # len(restosites_map) == 0:
    debug = False
    restosite_map = {}
    
    for rs_name, data in resto_site_data.items():
        new_id = uf.add_document("_sites", data, debug=debug)
        restosite_map[rs_name] = (new_id, data)
        
    # update sites with restosite
    nursery_id_map = {} 
    outplant_id_map = {} 
    control_id_map = {} 
    
    for _id, data in nurseries:
        rs_name = nursery_siteinfo[data['name']]['restosite']
        site_id = restosite_map[rs_name][0]
        data['location'][resto_id] = site_id
        uf.update_document("_sites", _id, data, debug=debug)
        nursery_id_map[_id] = site_id
    
    for _id, data in outplants:
        rs_name = outplant_siteinfo[data['name']]['restosite']
        site_id = restosite_map[rs_name][0]
        data['location'][resto_id] = site_id
        uf.update_document("_sites", _id, data, debug=debug)
        outplant_id_map[_id] = site_id
    
    for _id, data in controls:
        rs_name = control_siteinfo[data['name']]['restosite']
        site_id = restosite_map[rs_name][0]
        data['location'][resto_id] = site_id
        uf.update_document("_sites", _id, data, debug=debug)
        control_id_map[_id] = site_id
    
    
    idx = 0
    for _id, frag in fragments:
        if idx % 100 == 0:
            print(f"Updating fragment {idx}")
        idx += 1
        loc = frag['location']
        donor_id = frag['donorID']
        if resto_id in loc:
            pass
        if 'nurseryID' in loc:
            site_id = nursery_id_map.get(loc['nurseryID'])
        elif 'outplantID' in loc:
            site_id = outplant_id_map.get(loc['outplantID'])
        else:
            site_id = None
        if site_id is not None:
            frag['location'][resto_id] = site_id
            uf.update_document("_fragments", _id, frag, debug=debug)
            if donor_id in dc_dict:
                donor = dc_dict[donor_id]
                if resto_id not in donor['location']:
                    donor['location'][resto_id] = site_id
                    uf.update_document("_sites", donor_id, donor, debug=debug)
            
    
    # get and update all events
    
    loc = {'orgID': org_id}
    events = qf.get_docs(qf.query_events(loc))
    
    idx = 0
    for _id, event in events:
        if idx % 100 == 0:
            print(f"Updating event {idx}")
        idx += 1
        loc = event['location']
        if resto_id in loc:
            pass
        if 'nurseryID' in loc:
            site_id = nursery_id_map.get(loc['nurseryID'])
        elif 'outplantID' in loc:
            site_id = outplant_id_map.get(loc['outplantID'])
        elif 'controlID' in loc:
            site_id = control_id_map.get(loc['controlID'])
        else:
            site_id = None
        if site_id is not None:
            event['location'][resto_id] = site_id
            uf.update_document("_events", _id, event, debug=debug)
# %%
# patch up restosites - add metadata

for name, data in resto_site_data.items():
    sid, _data = restosites_map[name]
    uf.update_document("_sites", sid, data, debug=False)
