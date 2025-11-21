# Run in reefod-data-api/functions
from collections import defaultdict
import pandas as pd

from firestore_constants import EventType as et
from firestore_constants import SiteType as st
from firestore_constants import FragmentState as fs
import migrate.current_firestore as cf
import reefos_analysis.dbutils.firestore_util as fsu
import query_firestore as qq
import compute_statistics as cs

to_production = False
limit = None  # set to None to write all, 0 to write none, or a value > 0 to write some

old_firestore = '../restoration-ios-firebase-adminsdk-wg0a4-a59664d92f.json'
if to_production:
    new_firestore = '../restoration-ios-firebase-adminsdk-wg0a4-a59664d92f.json'
    new_project_id="restoration-ios"
else:
    new_firestore = "../restoration-app---dev-6df41-firebase-adminsdk-fbsvc-37ee88f0d4.json"
    new_project_id="restoration-app---dev-6df41"

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

# %%
# grouping in current data

restosites = [
#    {'branch': 'Fiji', 'nursery': 'RORO', 'group': 'Roro'},
#    {'branch': 'Fiji', 'nursery': 'RORO 2', 'group': 'Roro'},
#    {'branch': 'Fiji', 'nursery': 'RORO 3', 'group': 'Roro'},
#    {'branch': 'Fiji', 'nursery': 'SIX SENSES FIJI', 'group': 'Six Senses Fiji'},
#    {'branch': 'Fiji', 'nursery': 'SIX SENSES FIJI 2', 'group': 'Six Senses Fiji'},
#    {'branch': 'Fiji', 'nursery': 'VAKELECIRI', 'group': 'Vakeleciri'},
#    {'branch': 'Fiji', 'nursery': 'VAKELECIRI 2', 'group': 'Vakeleciri'},
#    {'branch': 'Fiji', 'nursery': 'VAKELECIRI 3', 'group': 'Vakeleciri'},
#    {'branch': 'Fiji', 'nursery': 'SAVUTI', 'group': 'Savuti'},
#    {'branch': 'Fiji', 'nursery': 'WADIGI', 'group': 'Wadigi'},

#    {'branch': 'Fiji', 'outplant': 'RORO', 'group': 'Roro'},
#    {'branch': 'Fiji', 'outplant': 'SSF REEF SITE 1', 'group': 'Six Senses Fiji'},
#    {'branch': 'Fiji', 'outplant': 'SSF REEF SITE 2', 'group': 'Six Senses Fiji'},
#    {'branch': 'Fiji', 'outplant': 'VAKELECIRI SITE 1', 'group': 'Vakeleciri'},

    {'branch': 'French Polynesia', 'nursery': 'momoa nursery', 'group': 'Bora Bora'},
    {'branch': 'French Polynesia', 'nursery': 'Toopua', 'group': 'Bora Bora'},
    {'branch': 'French Polynesia', 'nursery': 'Toopua 2', 'group': 'Bora Bora'},
    {'branch': 'French Polynesia', 'nursery': 'Ahe', 'group': 'Ahe'},
    {'branch': 'French Polynesia', 'nursery': 'HQ', 'group': 'HQ'},
    {'branch': 'French Polynesia', 'nursery': 'Tiaia', 'group': 'Tiaia'},
    {'branch': 'French Polynesia', 'nursery': 'Tiaia 2', 'group': 'Tiaia'},
    {'branch': 'French Polynesia', 'nursery': 'Tiaia 3', 'group': 'Tiaia'},
    {'branch': 'French Polynesia', 'nursery': 'Tiaia 4', 'group': 'Tiaia'},
    {'branch': 'French Polynesia', 'nursery': 'HAURU 1', 'group': 'Hauru'},
    {'branch': 'French Polynesia', 'nursery': 'Hauru 2', 'group': 'Hauru'},
    {'branch': 'French Polynesia', 'nursery': 'AHOTOTEINA TEAHUPOO', 'group': 'Teahupoo'},
    {'branch': 'French Polynesia', 'nursery': 'Pihaena', 'group': 'Pihaena'},
    {'branch': 'French Polynesia', 'nursery': 'TIKEHAU CORAL GARDEN', 'group': 'Tikehau'},
    {'branch': 'French Polynesia', 'nursery': 'TOA ORA', 'group': 'Toa Ora'},
    {'branch': 'French Polynesia', 'nursery': 'Teavaro 1', 'group': 'Teavaro'},

    {'branch': 'French Polynesia', 'outplant': 'TOA ORA', 'group': 'Toa Ora'},
    {'branch': 'French Polynesia', 'outplant': 'HQ', 'group': 'HQ'},
    {'branch': 'French Polynesia', 'outplant': 'AHOTOTEINA', 'group': 'Teahupoo'},
    {'branch': 'French Polynesia', 'outplant': 'MHPN Bommies', 'group': 'Pihaena'},
    {'branch': 'French Polynesia', 'outplant': 'PHN Pilot', 'group': 'Pihaena'},
    {'branch': 'French Polynesia', 'outplant': 'Tiaia Experiment', 'group': 'Tiaia'},

    {'branch': 'French Polynesia', 'control': 'Tiaia Experiment Control', 'group': 'Tiaia'},

#    {'branch': 'Thailand', 'nursery': 'KK Koh Mai Si R & D', 'group': 'KK Koh Mai Si'},
#    {'branch': 'Thailand', 'nursery': 'KK Koh Mai Si CT-A', 'group': 'KK Koh Mai Si'},
#    {'branch': 'Thailand', 'nursery': 'KK Koh Mai Si CT-B', 'group': 'KK Koh Mai Si'},
#    {'branch': 'Thailand', 'nursery': 'KK Koh Mai Si CT-C', 'group': 'KK Koh Mai Si'},
#    {'branch': 'Thailand', 'nursery': 'KK Koh Mai Si CT-D', 'group': 'KK Koh Mai Si'},
#    {'branch': 'Thailand', 'nursery': 'KK Koh Mai Si RT-A', 'group': 'KK Koh Mai Si'},
#    {'branch': 'Thailand', 'nursery': 'KK Koh Mai Si RT-B', 'group': 'KK Koh Mai Si'},
#    {'branch': 'Thailand', 'nursery': 'KK Koh Mai Si RT-C', 'group': 'KK Koh Mai Si'},
#    {'branch': 'Thailand', 'nursery': 'KK BANG BAO R&D (pre bleaching ‘24)', 'group': 'KK Bang Bao'},
#    {'branch': 'Thailand', 'nursery': 'KK BANG BAO R&D (post bleaching ‘24)', 'group': 'KK Bang Bao'},
#    {'branch': 'Thailand', 'nursery': 'KM Ao Lom  R&D 1 (pre Bleaching ‘24)', 'group': 'KM Ao Lom'},
#    {'branch': 'Thailand', 'nursery': 'KM AO LOM RT-B', 'group': 'KM Ao Lom'},
#    {'branch': 'Thailand', 'nursery': 'KM AO LOM R&D (pre bleaching 24’)', 'group': 'KM Ao Lom'},
#    {'branch': 'Thailand', 'nursery': 'KM AO LOM CT-A', 'group': 'KM Ao Lom'},
#    {'branch': 'Thailand', 'nursery': 'KM AO LOM RT-A', 'group': 'KM Ao Lom'},
#    {'branch': 'Thailand', 'nursery': 'KM Koh Rayang Nok RT-A', 'group': 'KM Koh Rayang Nok'},
#    {'branch': 'Thailand', 'nursery': 'KM Koh Rayang Nok CT-A', 'group': 'KM Koh Rayang Nok'},
#    {'branch': 'Thailand', 'nursery': 'KM Rescue R&D', 'group': 'Koh Mak'},
#    {'branch': 'Thailand', 'nursery': 'Sandbox', 'group': 'Koh Mak'},

#    {'branch': 'Thailand', 'outplant': 'KM Ao Lom', 'group': 'KM Ao Lom'},
]

fields = ['nursery', 'outplant', 'control']


def set_restosite_branch_id(name, org_id, branch_id):
    for grp in restosites:
        if grp['branch'] == name:
            grp['orgID'] = org_id
            grp['branchID'] = branch_id


def get_restosite(branch_id, field, name, siteID, geoLoc):
    if field in fields:
        for grp in restosites:
            if grp['branchID'] == branch_id and field in grp and grp[field] == name:
                grp['restoSiteID'] = siteID
                grp['geolocation'] = geoLoc
                return grp['group']
    print(f"RestoSite: {field} {name} not found")
    return None


def get_restosites_of_sites():
    site_restosite_map = {}
    for grp in restosites:
        for field in fields:
            if field in grp:
                sid = grp.get(field)
                if sid is not None:
                    site_restosite_map[sid] = grp['group']
    return site_restosite_map


def get_restosites():
    sites = {}
    for grp in restosites:
        if grp['group'] not in sites:
            restosite = {'name': grp['group'],
                         'siteType': st.restosite.value,
                         'location': {'orgID': grp['orgID'],
                                      'branchID': grp['branchID']},
                         'geolocations': [],
                         }
            set_metadata(restosite)
            sites[grp['group']] = restosite
        site = sites[grp['group']]
        if 'geolocation' in grp:
            site['geolocations'].append(grp['geolocation'])
        else:
            print(f'Restoration site {grp["group"]} missing geolocation')
    return sites

# %%
# build new database documents from the current database documents


taxon_errors = {"Acropora unknown": "Acropora sp.",
                "Pocilopora verucosa": "Pocillopora verrucosa"}

missing_sp = [('Psammocora', 'sp.'), ('Porites', 'sp.'), ('Symphyllia', 'sp.'), ('Montipora', 'sp.')]


def set_metadata(doc, createdAt=None, createdBy=None):
    doc['metadata'] = {'createdAt': createdAt,
                       'createdBy': createdBy,
                       'deleted': False}


def get_organisms(doc_grps):
    organisms = {}
    # get corals specified for an organization
    for doc in doc_grps['Corals']['doc']:
        genus = doc.get('name').capitalize()
        spp = doc.get('species')
        for species, info in spp.items():
            taxon = genus + ' ' + species
            org = doc.reference.path.split('/')[1]
            if taxon not in organisms:
                organism_info = {'organism_type': 'coral',
                                 'description': "",
                                 'ecological_role': {'morphology': info['morphology']},
                                 'name': "",
                                 'genus': genus,
                                 'species': species,
                                 'locations': [{'orgID': org}]
                                 }
                set_metadata(organism_info)
                organisms[taxon] = organism_info
            else:
                organisms[taxon]['locations'].append({'orgID': org})
    # get global-level corals
    for doc in doc_grps['CoralGenera']['doc']:
        genus = doc.get('name').capitalize()
        spp = doc.get('species')
        for species in spp:
            taxon = genus + ' ' + species
            organism_info = {'organism_type': 'coral',
                             'description': "",
                             'ecological_role': {},
                             'name': "",
                             'genus': genus,
                             'species': species,
                             'locations': []
                             }
            set_metadata(organism_info)
            organisms[taxon] = organism_info

    for sp in missing_sp:
        genus = sp[0]
        spp = sp[1]
        taxon = genus + ' ' + species
        if taxon not in organisms and taxon != 'Genus':
            organism_info = {'organism_type': 'coral',
                             'description': "",
                             'ecological_role': {'morphology': ''},
                             'name': "",
                             'genus': genus,
                             'species': species,
                             'locations': []
                             }
            set_metadata(organism_info)
            organisms[taxon] = organism_info

    for doc in doc_grps['Fish']['doc']:
        name = doc.get('name')
        path = doc.reference.path.split('/')
        if len(path) > 2:
            org = path[1]
            branch = path[3]
        else:
            org = None
        if name not in organisms:
            if name == "Crown-of-thorn Sea Star":
                continue
            elif name == "Turtle":
                org_type = 'reptile'
            else:
                org_type = 'fish'
            function = doc.get('group')
            organism_info = {'organism_type': org_type,
                             'description': doc.get('description'),
                             'ecological_role': {'function': function},
                             'name': name,
                             'locations': [{'orgID': org, 'branchID': branch}] if org is not None else []
                             }
            set_metadata(organism_info)
            organisms[name] = organism_info
        elif org is not None:
            organism_info = organisms[name]
            organism_info['locations'].append({'orgID': org, 'branchID': branch})

    for doc in doc_grps['Invertebrates']['doc']:
        name = doc.get('name')
        if name not in organisms:
            org_type = 'invertebrate'
            function = doc.get('group')
            organism_info = {'organism_type': org_type,
                             'description': doc.get('description'),
                             'ecological_role': {'function': function},
                             'name': name,
                             'locations': []
                             }
            set_metadata(organism_info)
            organisms[name] = organism_info
    return organisms


def get_orgs(doc_grps):
    orgs = {}
    for doc in doc_grps['Orgs']['doc']:
        org = {
            'name': doc.get('name'),
            }
        set_metadata(org)
        orgs[doc.id] = org
    return orgs


def get_branches(doc_grps):
    sites = {}
    for doc in doc_grps['Branches']['doc']:
        path = doc.reference.path.split('/')
        doc = doc.to_dict()
        branch = {
            'siteType': st.branch.value,
            'location': {'orgID': path[1]},
            'name': doc['name'],
            'geolocation': doc['geoLocation']
            }
        set_metadata(branch)
        sites[path[3]] = branch
#        set_restosite_branch_id(doc['name'], path[1], path[3])
    return sites


def get_users(doc_grps, ignore_org='xperience'):
    users = {}
    for doc in doc_grps['Invites']['doc']:
        doc = doc.to_dict()
        if doc['orgID'] != ignore_org and doc['branchID'] != 'sandbox':
            user = {
                'location': {'branchID': doc['branchID'],
                             'orgID': doc['orgID']},
                'email': doc['email'],
                'dates': {'expires': doc['expiresAt']},
                'role': 'invited'
                }
            set_metadata(user)
            users[doc['email']] = user
    for doc in doc_grps['Users']['doc']:
        uid = doc.id
        doc = doc.to_dict()
        if doc['orgID'] != ignore_org and doc['branchID'] != 'sandbox':
            user = {
                'location': {'branchID': doc['branchID'],
                             'orgID': doc['orgID']},
                'firstname': doc['firstName'],
                'lastname': doc['lastName'],
                'email': doc['email'],
                'role': doc['role'],
                'dates': {},
                }
            set_metadata(user)
            users[uid] = user
    return users


def get_controlsites(doc_grps):
    sites = {}
    for doc in doc_grps['ControlSites']['doc']:
        path = doc.reference.path.split('/')
        doc = doc.to_dict()
        control_site = {
            'siteType': st.control.value,
            'location': {'orgID': path[1],
                         'branchID': path[3],
#                         'restoSiteID': get_restosite(path[3], 'control',
#                                                      doc['name'], path[5],
#                                                      doc['geoLocation'])
                         },
            'name': doc['name'],
            'geolocation': doc['geoLocation'],
            'perimeter': doc.get('perimeter'),
            'siteData': {'reefos_id': doc.get('reefos_id'),
                         'outplantID': doc.get('outplantSiteID')},
            }
        set_metadata(control_site, createdAt=doc.get('created'))
        sites[path[5]] = control_site
    return sites


def _get_coral_taxon(coral):
    genus = coral['genus'].capitalize()
    species = coral['species']
    taxon = genus + ' ' + species
    if taxon in taxon_errors:
        taxon = taxon_errors[taxon]
    return taxon


def get_mothercolonies(doc_grps):
    mcs = {}
    for doc in doc_grps['MotherColonies']['doc']:
        path = doc.reference.path.split('/')
        doc = doc.to_dict()
        mc = {
            'siteType': st.donorcolony.value,
            'location': {'orgID': path[1],
                         'branchID': path[3]},
            'name': doc['name'],
            'geolocation': doc.get('geoLocation', {}),
            'siteData': {'organismID': _get_coral_taxon(doc['coral']), 'images': doc['images']}
            }
        set_metadata(mc, createdAt=doc.get('created'), createdBy=doc.get('userID'))
        mcs[path[5]] = mc
    return mcs


def get_nurseries(doc_grps):
    sites = {}
    for doc in doc_grps['Nurseries']['doc']:
        path = doc.reference.path.split('/')
        doc = doc.to_dict()
        nursery = {
            'siteType': st.nursery.value,
            'location': {'orgID': path[1],
                         'branchID': path[3],
#                         'restoSiteID': get_restosite(path[3], 'nursery',
#                                                      doc['name'], path[5],
#                                                      doc['geoLocation'])
                         },
            'name': doc['name'],
            'geolocation': doc['geoLocation'],
            'siteData': {'image': doc.get('cover_image'),
                         'goals': doc.get('goals'),
                         'schedules': doc.get('schedules')},
            }
        set_metadata(nursery, createdAt=doc.get('created'))
        sites[path[5]] = nursery
    return sites


def get_structures(doc_grps):
    sites = {}
    for doc in doc_grps['Structures']['doc']:
        path = doc.reference.path.split('/')
        doc = doc.to_dict()
        loc = {'orgID': path[1],
               'branchID': path[3],
               'nurseryID': path[5],
               '_0': doc['position']}
        structure = {
            'siteType': st.structure.value,
            'location': loc,
            'siteData': {
                'child_count': doc['substructureCount'],
                'structuretype': doc['type'],
                }
            }
        set_metadata(structure, createdAt=doc.get('created'))
        sites[path[7]] = structure
    return sites


def get_nursery_monitoring_events(doc_grps):
    events = {}
    for doc in doc_grps['MonitoringLogs']['doc']:
        path = doc.reference.path.split('/')
        doc = doc.to_dict()
        location = {'orgID': path[1],
                    'branchID': path[3],
                    'nurseryID': doc['nurseryID']}
        mt = (et.full_nursery_monitoring if doc['monitoringType'] == 'full'
              else et.bleaching_nursery_monitoring)
        event = {
            'eventType': mt.value,
            'location': location,
            'name': doc['name'],
            'completedAt': doc.get('completedAt')
        }
        set_metadata(event, createdAt=doc.get('scheduled'))
        events[path[5]] = event
    return events


def get_outplantsites(doc_grps):
    sites = {}
    for doc in doc_grps['OutplantSites']['doc']:
        path = doc.reference.path.split('/')
        doc = doc.to_dict()
        monitoring_settings = doc.get('montioringSettings', {})
        outplant = {
            'siteType': st.outplant.value,
            'location': {'orgID': path[1],
                         'branchID': path[3],
#                         'restoSiteID': get_restosite(path[3], st.outplant.value,
#                                                      doc['name'], path[5],
#                                                      doc['geoLocation'])
                         },
            'name': doc['name'],
            'geolocation': doc['geoLocation'],
            'perimeter': doc['perimeter'],
            'siteData': {'controlSiteID': doc.get('controlSiteID'),
                         'monitoringSettings': monitoring_settings,
                         'restorationGoals': doc.get('restorationGoals'),
                         'schedules': doc.get('schedules'),
                         'reefos_id': doc.get('reefos_id')
                         },
            }
        set_metadata(outplant, createdAt=doc.get('created'))
        sites[path[5]] = outplant
    return sites


def get_opcell_events(cell_doc, loc):
    event_list = []
    for health in cell_doc['healths']:
        if 'cellMonitoring' in health and 'percentBleach' in health['cellMonitoring']:
            event = {
                'location': loc.copy(),
                'eventType': et.outplant_cell_monitoring.value,
                'eventData': health['cellMonitoring'],
                'monitoredAt': health['timestamp']
                }
            event['eventData']['logID'] = health['logID']
            set_metadata(event, createdAt=health.get('timestamp'), createdBy=health.get('userID'))
            event_list.append(event)
    return event_list


def get_outplantcells(doc_grps):
    cells = {}
    event_list = []
    for doc in doc_grps['OutplantCells']['doc']:
        path = doc.reference.path.split('/')
        loc = {'orgID': path[1],
               'branchID': path[3],
               'outplantID': path[5]
               }
        doc = doc.to_dict()
        monitoring_settings = doc.get('monitoringSettings', {})
        monitoring_settings['reefos_id'] = doc.get('reefos_id')
        cell = {
            'location': loc,
            'siteType': st.outplantcell.value,
            'celltype': doc['type'],
            'name': doc['name'],
            'geolocation': doc['geoLocation'],
            }
        set_metadata(cell, createdAt=doc.get('created'), createdBy=doc.get('userID'))
        cells[path[7]] = cell
        if 'healths' in doc:
            loc['outplantCellID'] = path[7]
            event_list.extend(get_opcell_events(doc, loc))
    events = {f'{path[7]}_{idx}': ev for idx, ev in enumerate(event_list)}
    return cells, events


def get_outplant_cell_monitoring(doc_grps, cell_monitoring):
    cell_event_data = {(ev['location']['outplantCellID'], ev['eventData']['logID']):
                       (ev['eventData'].copy(), ev['monitoredAt']) for ev in cell_monitoring.values()}
    events = {}
    for doc in doc_grps['OutplantCellMonitoringLogs']['doc']:
        path = doc.reference.path.split('/')
        log_id = path[5]
        doc = doc.to_dict()
        location = {'orgID': path[1],
                    'branchID': path[3],
                    'outplantID': doc['siteID']
                    }
        data = []
        for cell_id in doc['completedIDs']:
            k = (cell_id, log_id)
            if k in cell_event_data:
                ced = cell_event_data[k]
                ced[0]['monitoredAt'] = ced[1]
                data.append(ced[0])
        event = {
            'eventType': et.visual_survey.value,
            'location': location,
            'eventData': {'outplant_monitoring_name': doc['name'],
                          'scheduled': doc['scheduled'],
                          'ocIDs': doc['completedIDs']
                          }
            }
        if len(data) > 0:
            df = pd.DataFrame(data)
            event['percentCoralCover'] =  df.percentCoralCover.mean()
            event['percentSurvival'] =  df.percentSurvival.mean()
            event['percentBleach'] =  df.percentBleach.mean()
            event['monitoredAt'] =  df.monitoredAt.max()
        set_metadata(event, createdAt=doc.get('scheduled'))
        events[log_id] = event
    return events


def get_surveysites(doc_grps):
    sites = {}
    for doc in doc_grps['SurveySites']['doc']:
        path = doc.reference.path.split('/')
        doc = doc.to_dict()
        survey = {
            'siteType': st.surveysite.value,
            'location': {'orgID': path[1],
                         'branchID': path[3]},
            'name': doc['name'],
            'geolocation': doc['geoLocation'],
            'siteData': {'referenceSiteID': doc.get('referenceSiteID'),
                         }
            }
        set_metadata(survey, createdAt=doc.get('created'))
        sites[path[5]] = survey
    return sites


def get_survey_events(cell_doc, cellID):
    event_list = []
    for health in cell_doc['healths']:
        event = {
            'eventType': 'survey_site_monitoring',
            'cellID': cellID,
            'eventData': health
            }
        set_metadata(event, createdAt=cell_doc.get('timestamp'), createdBy=cell_doc.get('userID'))
        event_list.append(event)
    return event_list


def get_surveycells(doc_grps):
    event_list = []
    cells = {}
    for doc in doc_grps['SurveyCells']['doc']:
        path = doc.reference.path.split('/')
        doc = doc.to_dict()
        cell = {
            'location': {'orgID': path[1],
                         'branchID': path[3],
                         'surveyID': path[5]},
            'siteType': st.surveycell.value,
            'name': doc['name'],
            'geolocation': doc['geoLocation'],
            'surveytype': doc['type']
            }
        set_metadata(cell, createdAt=doc.get('created'))
        cells[path[7]] = cell
        if 'healths' in doc:
            event_list.extend(get_survey_events(doc, path[7]))
    events = {f'{path[7]}_{idx}': ev for idx, ev in enumerate(event_list)}
    return cells, events


def get_events_of_fragment(frag_doc, location, locations, nm_events, frag_id):

    # find first (latest) previous location which matches the nursery ID in the log
    def get_event_location(location, locations, log_nurseryID):
        new_loc = location.copy()
        if location.get('nurseryID') != log_nurseryID:
            for loc in locations:
                if loc['nurseryID'] == log_nurseryID:
#                    new_loc['restoSiteID'] = loc.get('restoSiteID')
                    new_loc['nurseryID'] = loc['nurseryID']
                    new_loc['_0'] = loc['structure']
                    new_loc['_1'] = loc['substructure']
                    new_loc['_2'] = loc['position']
                    break
        return new_loc

    event_list = []
    event = {
        'eventType': et.frag_creation.value,
        'location': location.copy(),
        'fragmentID': frag_id,
        }
    set_metadata(event, createdAt=frag_doc.get('created'))
    event_list.append(event)

    if 'outplantInfo' in frag_doc:
        op_info = frag_doc['outplantInfo']
        if 'measuredAt' in op_info:     # fiji fragments have been outplanted without measuring
            event = {
                'eventType': et.frag_outplant_monitor.value,
                'location': location.copy(),
                'fragmentID': frag_id,
                'eventData': {
                    'size': op_info['size'],
                    'associatedOrganisms': op_info.get('associateOrganisms')
                    }
                }
            set_metadata(event, createdAt=op_info['measuredAt'])
            event_list.append(event)
        if 'plantedAt' in op_info:     # fiji fragments have been outplanted without measuring
            event = {
                'eventType': et.frag_outplant.value,
                'location': location.copy(),
                'fragmentID': frag_id,
                'eventData': {
                    'depth': op_info.get('depth'),
                    'temperature': op_info.get('temperature'),
                    }
                }
            set_metadata(event, createdAt=op_info['plantedAt'], createdBy=op_info['userID'])
            event_list.append(event)
    for health in frag_doc['healths']:
        logID = health.get('logID')
        if logID in nm_events:
            log_nurseryID = nm_events[logID]['location']['nurseryID']
        else:
            log_nurseryID = location.get('nurseryID')
        # see if the current nursery is the same as the log nursery and if it is not then
        # fix the location data for this event
        current_loc = get_event_location(location, locations, log_nurseryID)
        event = {
            'eventType': et.frag_monitor.value,
            'location': current_loc,
            'fragmentID': frag_id,
            'eventData': {
                'logID': health.get('logID'),
                'bleach': health.get('bleach'),
                'health': health.get('health'),
                'size': health.get('size'),
                'predations': health.get('predations'),
                'images': health.get('images', [])
                }
            }
        set_metadata(event, createdAt=health['created'], createdBy=health['userID'])
        event_list.append(event)
    return event_list


# when a fragment is dead, it's health is not recorded in a monitoring, but it still exists as a dead fragment
# add a monitoring record for it with health 6 but no bleach or size values as these are only valid for live fragments
def add_events_for_dead_fragment(frag_id, fragment, events, monitoring_events):
    # see if fragment has died
    new_events = []
    is_dead = False
    for ev in events:
        if ev['eventType'] == et.frag_monitor and ev['eventData'].get('health') == 6:
            is_dead = True
            dead_date = ev['metadata']['createdAt']
            break
    if is_dead:
        # if fragment is dead, make sure all monitorings between death and removal/replacement have an event
        last_date = fragment['completedAt']
        for logID, ev in monitoring_events.items():
            ev_date = ev['metadata']['createdAt']
            if ev_date > dead_date and (last_date is None or ev_date < last_date):
                # make measurement event for dead not yet removed fragment
                event = {
                    'eventType': et.frag_monitor.value,
                    'location': fragment['location'].copy(),
                    'fragmentID': frag_id,
                    'eventData': {
                        'logID': logID,
                        'health': 6,
                        }
                    }
                set_metadata(event, createdAt=ev_date)
                new_events.append(event)
    return new_events


def get_fragments(doc_grps, nurseries, outplants, outplant_cells, nm_events):
#    rmap = get_restosites_of_sites()
    # get all locations the fragment has been at
    def get_frag_locations(doc):
        if 'location' in doc and doc['location'] is not None:
            locations = [doc['location']]
        else:
            locations = []
        if 'previousLocations' in doc:
            prev = doc['previousLocations']
            if prev is not None and len(prev) > 0:
                locations = locations + prev
        return locations

    # fill location with most recent location plus outplant location if any
    def fill_location(location, doc, locations):
        loc = locations[0] if len(locations) > 0 else None
        if loc is not None:
#            nursery = nurseries.get(loc['nurseryID'])
#            location['restoSiteID'] = rmap.get(nursery['name']) if nursery is not None else None
            location['nurseryID'] = loc['nurseryID']
            location['_0'] = loc['structure']
            location['_1'] = loc['substructure']
            location['_2'] = loc['position']
        if 'outplantInfo' in doc:
            if 'outplantCellID' in doc['outplantInfo']:
                op_cell_id = doc['outplantInfo']['outplantCellID']
                if op_cell_id not in outplant_cells:
                    return False
                op_id = outplant_cells[op_cell_id]['location']['outplantID']
#                location['restoSiteID'] = rmap.get(outplants[op_id]['name'])
                location['outplantID'] = op_id
                location['outplantCellID'] = op_cell_id
                if loc is not None:
                    location['nurseryID'] = loc['nurseryID']
                    location['_0'] = loc['structure']
                    location['_1'] = loc['substructure']
                    location['_2'] = loc['position']
            else:
                location['outplantID'] = None
                location['outplantCellID'] = None
        elif loc is not None:
            location['outplantID'] = None
            location['outplantCellID'] = None
        else:
            return False
        return True

    def get_fragment_state(doc):
        if 'location' in doc and doc['location'] is not None:
            return fs.in_nursery
        elif 'outplantInfo' in doc and 'plantedAt' in doc['outplantInfo']:
            return fs.outplanted
        elif 'previousLocations' in doc and doc['previousLocations'] is not None and len(doc['previousLocations']) > 0:
            return fs.removed
        return fs.unknown

    def hashable_location(loc):
        return tuple((k, v) for k, v in loc.items())

    fragments = {}
    events = {}
    location_fragids = defaultdict(list)
    frag_events = {}
    for idx, doc in enumerate(doc_grps['Fragments']['doc']):
        if idx % 10000 == 0:
            print(f"Processing fragment {idx} {doc.reference.path}")
        path = doc.reference.path.split('/')
        frag_id = path[5]
        doc = doc.to_dict()
        locations = get_frag_locations(doc)
        location = {'orgID': path[1],
                    'branchID': path[3]
                    }
        if not fill_location(location, doc, locations):
            prev = doc['previousLocations'] if 'previousLocations' in doc else None
            if prev is None or len(prev) == 0:
                print(f"Error in fragment {path}")
            else:
                print(f"Error in fragment {path} {prev[0]['nurseryID']}")
            continue
        if 'outplantInfo' in doc:
            op_info = doc['outplantInfo']
            completedAt = op_info.get('measuredAt')
            if completedAt is None:
                completedAt = op_info.get('plantedAt')
        else:
            completedAt = None
        fragment = {
            'donorID': doc['colonyID'],
            'organismID': _get_coral_taxon(doc['coral']),
            'monitored': doc['monitoring']['monitored'],
            'location': location,
            'state': get_fragment_state(doc).value,
            'completedAt': completedAt
            }
        set_metadata(fragment, createdAt=doc['created'])
        fragments[frag_id] = fragment
        location_fragids[hashable_location(location)].append(frag_id)
        # get fragment events - creation, monitoring, move, outplanting
        event_list = get_events_of_fragment(doc, location, locations, nm_events, frag_id)
        events.update({f'{frag_id}_{idx}': ev for idx, ev in enumerate(event_list)})
        frag_events[frag_id] = event_list
    # fill completedAt of fragments that died and then a frag was later seeded at that location
    # set the previous fragment end to the new fragment start
    for loc, frag_ids in location_fragids.items():
        if len(frag_ids) > 1:
            frags = [fragments[fid] for fid in frag_ids]
            frags.sort(key=lambda x: x['metadata']['createdAt'])
            for idx, frag in enumerate(frags[1:]):
                prev_frag = frags[idx]
                if prev_frag['completedAt'] is None:
                    prev_frag['completedAt'] = frag['metadata']['createdAt']
                    prev_frag['state'] = 'removed'
    # update event list for dead fragments
    for frag_id, frag in fragments.items():
        if frag['monitored'] and 'nurseryID' in frag['location']:
            monitorings = {logID: ev for logID, ev in nm_events.items()
                           if ev['location']['nurseryID'] == frag['location']['nurseryID']}
            event_list = add_events_for_dead_fragment(frag_id, frag, frag_events[frag_id], monitorings)
            events.update({f'{frag_id}_dead_{idx}': ev for idx, ev in enumerate(event_list)})
    return fragments, events


# %%
# read the existing database documents and build documents for the new database
# the new documents all contain ids from the previous database
organisms = get_organisms(doc_grps)         # organisms are indexed by name
orgs = get_orgs(doc_grps)
branches = get_branches(doc_grps)
control_sites = get_controlsites(doc_grps)
outplants = get_outplantsites(doc_grps)
outplant_cells, outplant_cell_events = get_outplantcells(doc_grps)
survey_sites = get_surveysites(doc_grps)
survey_cells, survey_events = get_surveycells(doc_grps)
users = get_users(doc_grps)
mcs = get_mothercolonies(doc_grps)
outplant_monitoring_events = get_outplant_cell_monitoring(doc_grps, outplant_cell_events)
nurseries = get_nurseries(doc_grps)
structures = get_structures(doc_grps)
nm_events = get_nursery_monitoring_events(doc_grps)
# %%
fragments, frag_events = get_fragments(doc_grps, nurseries, outplants, outplant_cells, nm_events)
# %%
#resto_sites = get_restosites()

# %%
# write the new documents to firestore and as this happens build mappings between the old docs and the new docs
# use the doc mappings to patch the new docs before writing them to firestore

# cred_file = 'reefapp-ios-997c0615c701.json'


def delete_collection(db, coll_name):
    prefix = '_'
    # prefix = ''
    coll_name = prefix + coll_name
    print(f"Deleting collection {coll_name}")
    cf.delete_collection(db, coll_name)


def add_collection(db, coll_name, documents, keep_id=False, batchsize=1000):
    prefix = '_'
    # prefix = ''
    coll_name = prefix + coll_name
    print(f"Adding {len(documents)} documents to collection {coll_name}")
    if db is not None:
        id_map = {}
        idx = 0
        batch = db.batch()
        for old_id, doc in documents.items():
            coll_ref = db.collection(coll_name)
            doc_ref = coll_ref.document(old_id if keep_id else None)
            if limit is None or idx < limit:
                batch.set(doc_ref, doc)
            id_map[old_id] = doc_ref.id      # map old doc id to new doc id
            idx += 1
            if idx % batchsize == 0:
                print(f"Committing batch {idx // batchsize} {coll_name}")
                batch.commit()
                batch = db.batch()
        batch.commit()
    else:
        id_map = {old_id: f'{coll_name}_{idx}' for idx, old_id in enumerate(documents.keys())}
    return id_map


def prepare_collection(db, coll_name, documents, keep_id=False):
    prefix = '_'
    # prefix = ''
    coll_name = prefix + coll_name
    print(f"Preparing {len(documents)} documents for collection {coll_name}")
    new_docs = []
    if db is not None:
        id_map = {}
        for old_id, doc in documents.items():
            coll_ref = db.collection(coll_name)
            doc_ref = coll_ref.document(old_id if keep_id else None)
            new_docs.append((doc_ref, doc))
            id_map[old_id] = doc_ref.id      # map old doc id to new doc id
    else:
        id_map = {old_id: f'{coll_name}_{idx}' for idx, old_id in enumerate(documents.keys())}
    return id_map, new_docs


def write_collection(db, documents, batchsize=1000):
    if db is not None:
        print(f"Writing {len(documents)} documents")
        batch = db.batch()
        for idx, doc in enumerate(documents):
            if limit is None or idx < limit:
                batch.set(doc[0], doc[1])
                if (idx + 1) % batchsize == 0:
                    print(f"Committing batch {idx // batchsize}")
                    batch.commit()
                    batch = db.batch()
        batch.commit()


def set_document_metadata(documents, user_ids):
    if type(documents) is dict:
        documents = documents.values()
    for doc in documents:
        if doc['metadata'].get('createdBy') is not None:
            doc['metadata']['createdBy'] = user_ids.get(doc['metadata'].get('createdBy'))


def set_org_branch(documents, org_ids, branch_ids):
    for doc_id, doc in documents.items():
        org_id = doc['location']['orgID']
        branch_id = doc['location']['branchID']
        doc['location']['orgID'] = org_ids[org_id]
        doc['location']['branchID'] = branch_ids[branch_id]


#def set_org_branch_site(documents, org_ids, branch_ids, restosite_ids):
#    for doc_id, doc in documents.items():
#        org_id = doc['location']['orgID']
#        branch_id = doc['location']['branchID']
#        restosite_id = doc['location']['restoSiteID']
#        doc['location']['orgID'] = org_ids[org_id]
#        doc['location']['branchID'] = branch_ids[branch_id]
#        if restosite_id is not None:
#            doc['location']['restoSiteID'] = restosite_ids[restosite_id]


def set_org_branch_nursery(documents, org_ids, branch_ids, nursery_ids):
    for doc_id, doc in documents.items():
        org_id = doc['location']['orgID']
        branch_id = doc['location']['branchID']
        nursery_id = doc['location']['nurseryID']
        doc['location']['orgID'] = org_ids[org_id]
        doc['location']['branchID'] = branch_ids[branch_id]
        doc['location']['nurseryID'] = nursery_ids.get(nursery_id, 'Missing')


def set_org_branch_outplant(documents, org_ids, branch_ids, outplant_ids):
    for doc_id, doc in documents.items():
        org_id = doc['location']['orgID']
        branch_id = doc['location']['branchID']
        outplant_id = doc['location']['outplantID']
        doc['location']['orgID'] = org_ids[org_id]
        doc['location']['branchID'] = branch_ids[branch_id]
        doc['location']['outplantID'] = outplant_ids.get(outplant_id)


def set_org_branch_survey(documents, org_ids, branch_ids, survey_ids):
    for doc_id, doc in documents.items():
        org_id = doc['location']['orgID']
        branch_id = doc['location']['branchID']
        survey_id = doc['location']['surveyID']
        doc['location']['orgID'] = org_ids[org_id]
        doc['location']['branchID'] = branch_ids[branch_id]
        doc['location']['surveyID'] = survey_ids.get(survey_id)


def set_organism_org_branch(organisms, org_ids, branch_ids):
    for doc_id, organism in organisms.items():
        for loc in organism['locations']:
            org_id = loc.get('orgID')
            branch_id = loc.get('branchID')
            if org_id is not None:
                loc['orgID'] = org_ids[org_id]
            if branch_id is not None:
                loc['branchID'] = branch_ids[branch_id]


def set_group(documents, group_ids):
    for doc_id, doc in documents.items():
        if 'group' in doc:
            doc['group'] = group_ids.get(doc['group'])


def set_mc_organismIDs(mcs, organism_ids):
    for mc in mcs.values():
        mc['siteData']['organismID'] = organism_ids.get(mc['siteData']['organismID'])


def set_opcell_monitoring_events(documents, outplant_cell_ids):
    for opce in documents.values():
        eventData = opce.get('eventData')
        if eventData is not None:
            ocids = eventData.get('ocIDs')
            if ocids is not None:
                opce['eventData']['ocIDs'] = [outplant_cell_ids[ocid] for ocid in ocids]


def set_fragment_ids(documents, org_ids, branch_ids, nursery_ids, outplant_ids,
                     organism_ids, mc_ids, op_cell_ids):
    for doc_id, doc in documents.items():
        # set ids in location
        org_id = doc['location']['orgID']
        branch_id = doc['location']['branchID']
        doc['location']['orgID'] = org_ids[org_id]
        doc['location']['branchID'] = branch_ids[branch_id]
#        if 'restoSiteID' in doc['location'] and doc['location']['restoSiteID'] is not None:
#            restosite_id = doc['location']['restoSiteID']
#            doc['location']['restoSiteID'] = resto_ids.get(restosite_id)
        if 'nurseryID' in doc['location']:
            nursery_id = doc['location']['nurseryID']
            doc['location']['nurseryID'] = nursery_ids.get(nursery_id)
        if 'outplantID' in doc['location']:
            outplant_id = doc['location']['outplantID']
            doc['location']['outplantID'] = outplant_ids.get(outplant_id)
        # set organism id
        doc['organismID'] = organism_ids[doc['organismID']]
        # set donor colony id
        doc['donorID'] = mc_ids.get(doc['donorID'], doc['donorID'])


def set_fragment_event_ids(documents, org_ids, branch_ids, nursery_ids,
                           outplant_ids, fragment_ids, nm_event_ids):
    for doc_id, doc in documents.items():
        # set fragmentID
        doc['fragmentID'] = fragment_ids[doc['fragmentID']]
        # set ids in location field
        doc['location']['orgID'] = org_ids[doc['location']['orgID']]
        doc['location']['branchID'] = branch_ids[doc['location']['branchID']]
#        if 'restoSiteID' in doc['location']:
#            restosite_id = doc['location']['restoSiteID']
#            doc['location']['restoSiteID'] = nursery_ids.get(restosite_id)
        if 'nurseryID' in doc['location']:
            nursery_id = doc['location']['nurseryID']
            doc['location']['nurseryID'] = nursery_ids.get(nursery_id)
        if 'outplantID' in doc['location']:
            outplant_id = doc['location']['outplantID']
            doc['location']['outplantID'] = outplant_ids.get(outplant_id)
        if 'eventData' in doc:
            event_data = doc['eventData']
            if 'logID' in event_data:
                event_data['logID'] = nm_event_ids.get(event_data['logID'])


def set_cell_ids(documents, cell_ids):
    for doc in documents.values():
        cell_id = doc['location']['outplantCellID']
        doc['location']['outplantCellID'] = cell_ids[cell_id]


def set_outplant_monitoring_event_ids(documents, log_ids):
    for doc in documents.values():
        log_id = doc['eventData']['logID']
        doc['eventData']['logID'] = log_ids[log_id]


def set_outplant_ctrl(documents, ctrl_site_id_map):
    for doc in documents.values():
        doc['siteData']['controlSiteID'] = ctrl_site_id_map.get(doc['siteData']['controlSiteID'])


def set_ctrl_outplant(docs, outplant_id_map):
    for doc in docs:
        doc[1]['siteData']['outplantID'] = outplant_id_map.get(doc[1]['siteData']['outplantID'])


# %%
include_surveys = False

collections = ["organisms", "orgs", "sites", "users", "fragments", "events", "statistics"]
if limit != 0:
    print("Initializing output database")
    fsu.cleanup_firestore()
    app, db = fsu.init_firebase_db(cred_file=new_firestore, db_id='reefapp')
    for coll in collections:
        delete_collection(db, coll)
else:
    app = db = None

# add organizations to firestore
print("Writing organizations")
org_id_map = add_collection(db, "orgs", orgs)

print("Writing branches")
# set branch orgIDs
for doc_id, branch in branches.items():
    branch['location']['orgID'] = org_id_map[branch['location']['orgID']]
# add branch sites to firestore
branch_id_map = add_collection(db, "sites", branches)

# add organisms to firestore
print("Writing organisms")
# set branch orgIDs
set_organism_org_branch(organisms, org_id_map, branch_id_map)
organism_id_map = add_collection(db, "organisms", organisms)

#print("Writing restoration sites")
## set resto site orgID and branchID
#set_org_branch(resto_sites, org_id_map, branch_id_map)
## add outplant sites to firestore
#resto_id_map = add_collection(db, "sites", resto_sites)

print("Writing nurseries")
# set nursery org and branch
#set_org_branch_site(nurseries, org_id_map, branch_id_map, resto_id_map)
set_org_branch(nurseries, org_id_map, branch_id_map)
# add nursery sites to firestore
nursery_id_map = add_collection(db, "sites", nurseries)

print("Writing control sites")
# set control site orgID and branchID
set_org_branch(control_sites, org_id_map, branch_id_map)
#set_org_branch_site(control_sites, org_id_map, branch_id_map, resto_id_map)
# add control sites to firestore
ctrl_site_id_map, new_ctrlsite_docs = prepare_collection(db, "sites", control_sites)

print("Writing outplants")
# set outplant orgID and branchID
set_org_branch(outplants, org_id_map, branch_id_map)
#set_org_branch_site(outplants, org_id_map, branch_id_map, resto_id_map)
set_outplant_ctrl(outplants, ctrl_site_id_map)
# add outplant sites to firestore
outplant_id_map = add_collection(db, "sites", outplants)

set_ctrl_outplant(new_ctrlsite_docs, outplant_id_map)
write_collection(db, new_ctrlsite_docs)

print("Writing users")
# set user org and branch
set_org_branch(users, org_id_map, branch_id_map)
# add users to firestore
user_id_map = add_collection(db, "users", users, keep_id=True)

print("Writing mother colonies")
# set mother colony org and branch ids
set_org_branch(mcs, org_id_map, branch_id_map)
# set mother colony coral organismID
set_mc_organismIDs(mcs, organism_id_map)
# add mcs to firestore
mc_id_map = add_collection(db, "sites", mcs)

print("Writing nursery monitoring events")
# set nursery monitoring event org and branch ids
set_org_branch_nursery(nm_events, org_id_map, branch_id_map, nursery_id_map)
# add nursery monitoring events to firestore
nm_event_id_map = add_collection(db, "events", nm_events)

print("Writing nursery structures")
# set nursery monitoring event org and branch ids
set_org_branch_nursery(structures, org_id_map, branch_id_map, nursery_id_map)
# add nursery monitoring events to firestore
structure_id_map = add_collection(db, "sites", structures)

print("Writing outplant cells")
# set outplant cell org branch outplant ids
set_org_branch_outplant(outplant_cells, org_id_map, branch_id_map, outplant_id_map)
set_document_metadata(outplant_cells, user_id_map)
# add outplant cells to firestore
op_cell_id_map = add_collection(db, "sites", outplant_cells)

print("Writing outplant cell monitoring events")
# set outplant cell monitoring event org branch outplant opcell ids
set_org_branch_outplant(outplant_monitoring_events, org_id_map, branch_id_map, outplant_id_map)
set_opcell_monitoring_events(outplant_monitoring_events, op_cell_id_map)
# add outplant cell monitoring events to firestore
op_monitoring_event_id_map = add_collection(db, "events", outplant_monitoring_events)

if include_surveys:
    print("Writing survey sites")
    # set survey site orgID and branchID
    set_org_branch(survey_sites, org_id_map, branch_id_map)
    # add survey sites to firestore
    survey_id_map = add_collection(db, "sites", survey_sites)

    print("Writing survey cells")
    # set outplant cell org branch outplant ids
    set_org_branch_survey(survey_cells, org_id_map, branch_id_map, survey_id_map)
    set_document_metadata(survey_cells, user_id_map)
    # add outplant cells to firestore
    survey_cell_id_map = add_collection(db, "sites", survey_cells)

print("Writing fragments")
# set doc ids in fragments
set_fragment_ids(fragments, org_id_map, branch_id_map, nursery_id_map,
                 outplant_id_map, organism_id_map,
                 mc_id_map, op_cell_id_map)
# add fragments to firestore
fragment_id_map = add_collection(db, "fragments", fragments)

print("Writing fragment events")
# set doc ids in fragment events
set_fragment_event_ids(frag_events, org_id_map, branch_id_map, nursery_id_map,
                       outplant_id_map,
                       fragment_id_map, nm_event_id_map)
set_document_metadata(frag_events, user_id_map)
# add fragment events to firestore
frag_event_id_map = add_collection(db, "events", frag_events)

print("Writing outplant cell events")
# set doc ids in fragment events
set_org_branch_outplant(outplant_cell_events, org_id_map, branch_id_map, outplant_id_map)
set_cell_ids(outplant_cell_events, op_cell_id_map)
set_outplant_monitoring_event_ids(outplant_cell_events, op_monitoring_event_id_map)
set_document_metadata(outplant_cell_events, user_id_map)
# add fragment events to firestore
op_cell_event_id_map = add_collection(db, "events", outplant_cell_events)

if include_surveys:
    print("Writing survey cell events")
    # set doc ids in fragment events
    set_cell_ids(survey_events, survey_cell_id_map)
    set_document_metadata(survey_events, user_id_map)
    # add fragment events to firestore
    survey_event_id_map = add_collection(db, "events", survey_events)

# all done...
fsu.cleanup_firestore()

# %%
qf = qq.QueryFirestore(project_id=new_project_id, creds=new_firestore)
cs.compute_statistics(qf, save=True, limit=None)
