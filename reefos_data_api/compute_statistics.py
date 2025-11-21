# compute current firestore fragment stats and add them to firestore

import datetime as dt
import numpy as np
import pandas as pd

import reefos_data_api.query_firestore as qq
import reefos_data_api.endpoints as ep
from reefos_data_api.firestore_constants import EventType as et
from reefos_data_api.firestore_constants import FragmentState as fs
# %%
# get the event data of an org


def get_location_monitoring_data(qf, loc):
    print(f"Getting raw frag_monitor events for {loc}")
    events = qf.get_docs(qf.query_events(loc, {'eventType': et.frag_monitor.value}))
    print(f"Got {len(events)}")
    print("Get logs")
    full_logs = qf.get_docs(qf.query_events(loc, {'eventType': et.full_nursery_monitoring.value}))
    bleach_logs = qf.get_docs(qf.query_events(loc, {'eventType': et.bleaching_nursery_monitoring.value}))
    return events, full_logs + bleach_logs


def explode_dict(df, dictname, keep):
    df = pd.concat([df, pd.DataFrame(df[dictname].tolist())], axis=1)
    if not keep:
        df = df.drop(dictname, axis=1)
    return df


def get_taxon_map(qf):
    corals = qf.get_organisms(org_type='coral')
    return {doc[0]: doc[1]['genus'] + ' ' + doc[1]['species'] for doc in corals}


# make event data into a dataframe ready for aggregation
def documents_to_dataframe(docs, to_explode, keep):
    df = pd.DataFrame([doc[1] for doc in docs])
    df['doc_id'] = [doc[0] for doc in docs]
    for attr in to_explode:
        df = explode_dict(df, attr, attr in keep)
    df['createdAt'] = explode_dict(df[['metadata']], 'metadata', False)['createdAt']
    return df


def add_evi(df):
    if 'size' in df and (~df['size'].isna()).sum() > 0:
        lwh_df = df[~df['size'].isna()]['size'].apply(pd.Series)
        r = (lwh_df['length'] + lwh_df['width']) / 4
        lwh_df['EVI'] = np.pi * r * r * lwh_df['height']
        # check for l, w, h valid values
        valid = (((lwh_df.length / lwh_df.width).between(0.1, 10)) &
                 ((lwh_df.length / lwh_df.height).between(0.1, 10)) &
                 ((lwh_df.width / lwh_df.height).between(0.1, 10)))
        lwh_df.loc[~valid, 'EVI'] = None  # np.nan
        df = pd.concat([df, lwh_df], axis=1).drop(columns=['size']).reset_index()
    return df


def aggregate_monitorings(qf, id_attrs, events, logs, fragids):
    taxon_map = get_taxon_map(qf)
    # make fragment monitoring event data into a dataframe ready for aggregation
    events_df = documents_to_dataframe(events, ['eventData', 'location'], {'location'})
    events_df = add_evi(events_df)
    events_df['organismID'] = events_df.fragmentID.map(fragids)
    # frag is alive if it has health less than 6 or if health is not measured and bleaching is
    events_df['alive'] = (events_df.health < 6) | (events_df.health.isna() & ~events_df.bleach.isna())
    # make log events into a dataframe
    log_df = documents_to_dataframe(logs, ['location'], {})[
        ['doc_id', 'name', 'eventType']].rename(columns={'doc_id': 'logID'})

    attrs = ['bleach', 'health', 'alive', 'EVI']
    # aggregate by monitoring, org and nursery
    grp = events_df.groupby(id_attrs + ['logID', 'nurseryID'])
    by_nursery = grp[attrs].agg(['mean', 'count'])
    by_nursery['monitoredAt'] = grp['createdAt'].max()
    by_nursery.reset_index(inplace=True)
    by_nursery.columns = ['_'.join(col).strip('_') for col in by_nursery.columns.values]
    by_nursery['agg_type'] = 'by_nursery'

    # aggregate by monitoring, org, nursery and structure 0
    grp = events_df.groupby(id_attrs + ['logID', 'nurseryID', '_0'])
    by_L1 = grp[attrs].agg(['mean', 'count'])
    by_L1['monitoredAt'] = grp['createdAt'].max()
    by_L1.columns = ['_'.join(col).strip('_') for col in by_L1.columns.values]
    by_L1.reset_index(inplace=True)
    by_L1['agg_type'] = 'by_L1'

    # aggregate by monitoring, org, nursery and species
    grp = events_df.groupby(id_attrs + ['logID', 'nurseryID', 'organismID'])
    by_spp = grp[attrs].agg(['mean', 'count'])
    by_spp['monitoredAt'] = grp['createdAt'].max()
    by_spp.reset_index(inplace=True)
    by_spp.columns = ['_'.join(col).strip('_') for col in by_spp.columns.values]
    by_spp['agg_type'] = 'by_species'
    by_spp['taxon'] = by_spp.organismID.map(taxon_map)

    re_order = (id_attrs + ['logID', 'nurseryID', '_0', 'taxon', 'monitoredAt', 'agg_type'] +
                [attr + '_mean' for attr in attrs] +
                [attr + '_count' for attr in attrs])
    agg_df = pd.concat([by_nursery, by_L1, by_spp])
    agg_df = agg_df[re_order].rename(columns={attr + '_mean': attr for attr in attrs})
    agg_df['monitoring_name'] = agg_df.logID.map(log_df.set_index('logID')['name'])
    agg_df['stat_type'] = agg_df.logID.map(log_df.set_index('logID')['eventType'])

    return agg_df, events_df


def make_into_stats_list(df, loc_attrs, stat_type):
    def to_native(val):
        if isinstance(val, np.integer):
            return int(val)
        elif isinstance(val, np.float64):
            return float(val)
        else:
            return val

    if len(df) == 0:
        return []
    df = df.reset_index(drop=True)
    df.replace([np.nan], [None], inplace=True)

    _df = df.join(df[loc_attrs].agg(dict, axis=1).to_frame('location')).drop(loc_attrs, axis=1)
    if 'stat_type' not in df:
        _df['stat_type'] = stat_type
    data_attrs = [attr for attr in _df.columns if attr not in ['location', 'stat_type']]
    data_dict = _df[data_attrs].apply(lambda x: {k: to_native(v) for k, v in dict(x).items()}, axis=1)
    _df = _df.join(data_dict.to_frame('data')).drop(data_attrs, axis=1)
    return _df.to_dict('records')


# get events and logs of a branch
def get_location_monitoring_event_info(qf, loc, frags):
    fragspp = {frag[0]: frag[1]['organismID'] for frag in frags}
    events, logs = get_location_monitoring_data(qf, loc)
    # get time series stats
    if len(events) > 0:
        agg_df, events_df = aggregate_monitorings(qf, list(loc.keys()), events, logs, fragspp)
        for id_name, id_val in loc.items():
            agg_df[id_name] = id_val
    else:
        agg_df = events_df = pd.DataFrame()
    return agg_df, events_df


# get latest value and aggregate over nursery or structure to get current state
def get_current_stats(qf, loc, edf, frags, outplanted):
    year = dt.datetime.now().year
    taxon_map = get_taxon_map(qf)
    frag_ids = set([frag[0] for frag in frags])
    fragspp = {frag[0]: frag[1]['organismID'] for frag in frags}
    fdf = documents_to_dataframe(frags, ['location'], {})
    ytd_df = fdf[fdf.createdAt.dt.year == year]
    opfdf = documents_to_dataframe(outplanted, ['location'], {}) if len(outplanted) > 0 else pd.DataFrame()
    # get events associated with current fragments
    edf = edf[edf.fragmentID.isin(frag_ids)].copy()
    # add fragment specie id
    edf['organismID'] = edf.fragmentID.map(fragspp)
    # turn location into a tuple to groupby
    edf['loc'] = edf.location.apply(lambda x: tuple((k, v) for k, v in x.items()))
    edf.sort_values('createdAt', inplace=True)

    attrs = ['health', 'bleach', 'EVI', 'alive', 'images']

    latest = {}
    for attr in attrs:
        ldf = (edf[['loc', 'location', 'createdAt', 'organismID', 'fragmentID', attr]]
               .dropna(subset=attr).groupby('loc').last().reset_index(drop=True))
        ldf = explode_dict(ldf, 'location', False)
        ldf['attr'] = attr
        ldf['value'] = ldf[attr]
        latest[attr] = ldf

    # get raw fragment data
    all_frags = fdf.drop(columns=['outplantID', 'outplantCellID', 'completedAt', 'metadata'])\
        .rename(columns={'doc_id': 'fragmentID'})
    keep = ['createdAt', 'organismID', 'fragmentID', 'orgID', 'branchID',
            'nurseryID', '_0', '_1', '_2', 'attr', 'value']
    cols = ['orgID', 'branchID', 'nurseryID', '_0', '_1', '_2']
    frag_df = pd.concat([ldf[keep] for ldf in latest.values()])
    latest_times = frag_df.groupby('fragmentID')['createdAt'].max().astype(str)
    frag_df = frag_df.pivot(columns='attr', values='value', index=['fragmentID', 'organismID'] + cols).reset_index()
    frag_df = all_frags.merge(frag_df[['fragmentID', 'EVI', 'alive', 'bleach', 'health', 'images']],
                              on='fragmentID', how='left')
    taxa = frag_df.organismID.map(taxon_map)
    frag_df['genus'] = taxa.str.split().str[0]
    frag_df['species'] = taxa.str.split().str[1]
    frag_df['measuredAt'] = frag_df.fragmentID.map(latest_times)
    fragment_stats = make_into_stats_list(frag_df, cols, 'fragment_stats')
    del latest['images']

    branch_stats = loc.copy()
    branch_stats.update({attr: ldf[attr].mean() for attr, ldf in latest.items()})
    branch_stats.update({attr + '_count': ldf[attr].count() for attr, ldf in latest.items()})
    branch_stats['n_species'] = fdf.organismID.nunique()
    branch_stats['n_fragments'] = len(frags)
    branch_stats['n_outplanted'] = len(outplanted)
    branch_stats['ytd_seeded'] = len(ytd_df)
    branch_stats['ytd_outplanted'] = (opfdf.completedAt.dt.year == year).sum() if len(opfdf) > 0 else 0
    branch_stats['createdAt'] = edf.createdAt.max()
    branch_stats = make_into_stats_list(pd.DataFrame([branch_stats]), ['orgID', 'branchID'], 'branch_stats')

    # get stats by restosite
    #restosite_stats = [ldf.groupby('restoSiteID')[attr]
    #                 .agg(['mean', 'count'])
    #                 .rename(columns={'mean': attr, 'count': attr + '_count'})
    #                 for attr, ldf in latest.items()]
    #restosite_stats.append(fdf.groupby('restoSiteID')['organismID'].nunique().rename('n_species'))
    #restosite_stats.append(fdf.groupby('restoSiteID')['doc_id'].count().rename('n_fragments'))
    #restosite_stats = pd.concat(restosite_stats, axis=1).reset_index().set_index('restoSiteID')
    #restosite_stats['orgID'] = loc['orgID']
    #restosite_stats['branchID'] = loc['branchID']
    #restosite_stats['createdAt'] = edf.createdAt.max()
    #restosite_stats['ytd_seeded'] = ytd_df.groupby(['restoSiteID'])['doc_id'].count()
    #restosite_stats['ytd_seeded'] = restosite_stats['ytd_seeded'].fillna(0)
    #restosite_stats = restosite_stats.reset_index()
    #restosite_stats = make_into_stats_list(restosite_stats, ['orgID', 'branchID', 'restoSiteID'], 'restosite_stats')

    # get stats by nursery
    nursery_stats = [ldf.groupby('nurseryID')[attr]
                     .agg(['mean', 'count'])
                     .rename(columns={'mean': attr, 'count': attr + '_count'})
                     for attr, ldf in latest.items()]
    nursery_stats.append(fdf.groupby('nurseryID')['organismID'].nunique().rename('n_species'))
    nursery_stats.append(fdf.groupby('nurseryID')['doc_id'].count().rename('n_fragments'))
    nursery_stats = pd.concat(nursery_stats, axis=1).reset_index().set_index('nurseryID')
    nursery_stats['orgID'] = loc['orgID']
    nursery_stats['branchID'] = loc['branchID']
    nursery_stats['createdAt'] = edf.createdAt.max()
    nursery_stats['ytd_seeded'] = ytd_df.groupby(['nurseryID'])['doc_id'].count()
    nursery_stats['ytd_seeded'] = nursery_stats['ytd_seeded'].fillna(0)
    nursery_stats = nursery_stats.reset_index()
    nursery_stats = make_into_stats_list(nursery_stats, ['orgID', 'branchID', 'nurseryID'], 'nursery_stats')

    # get stats by structure
    structure_stats = [ldf.groupby(['nurseryID', '_0'])[attr]
                       .agg(['mean', 'count'])
                       .rename(columns={'mean': attr, 'count': attr + '_count'})
                       for attr, ldf in latest.items()]
    structure_stats.append(fdf.groupby(['nurseryID', '_0'])['organismID'].nunique().rename('n_species'))
    structure_stats.append(fdf.groupby(['nurseryID', '_0'])['doc_id'].count().rename('n_fragments'))
    structure_stats = pd.concat(structure_stats, axis=1).reset_index()
    structure_stats['orgID'] = loc['orgID']
    structure_stats['branchID'] = loc['branchID']
    structure_stats['createdAt'] = edf.createdAt.max()
    structure_stats = make_into_stats_list(structure_stats,
                                           ['orgID', 'branchID', 'nurseryID', '_0'],
                                           'structure_stats')

    # get stats by species and nursery
    spp_nursery_stats = [ldf.groupby(['nurseryID', 'organismID'])[attr]
                         .agg(['mean', 'count'])
                         .rename(columns={'mean': attr, 'count': attr + '_count'})
                         for attr, ldf in latest.items()]
    spp_nursery_stats.append(edf.groupby(['nurseryID', 'organismID'])['organismID'].nunique().rename('n_species'))
    spp_nursery_stats.append(edf.groupby(['nurseryID', 'organismID'])['fragmentID'].nunique().rename('n_fragments'))
    spp_nursery_stats = pd.concat(spp_nursery_stats, axis=1).reset_index()
    spp_nursery_stats['orgID'] = loc['orgID']
    spp_nursery_stats['branchID'] = loc['branchID']
    spp_nursery_stats['createdAt'] = edf.createdAt.max()
    spp_nursery_stats['taxon'] = spp_nursery_stats.organismID.map(taxon_map)
    spp_nursery_stats = make_into_stats_list(spp_nursery_stats,
                                             ['orgID', 'branchID', 'nurseryID', 'organismID'],
                                             'spp_nursery_stats')

    all_stats = branch_stats.copy()
    #all_stats.extend(restosite_stats)
    all_stats.extend(nursery_stats)
    all_stats.extend(structure_stats)
    all_stats.extend(spp_nursery_stats)
    all_stats.extend(fragment_stats)
    return all_stats


def get_donor_stats(qf, location, frags):
    keep = ['doc_id', 'name', 'latitude', 'longitude', 'createdAt']
    taxon_map = get_taxon_map(qf)
    donors = qf.get_docs(qf.query_donorcolonies(location))
    if len(donors) > 0:
        donors_df = (documents_to_dataframe(donors, ['location', 'geolocation'], keep)
                     .rename(columns={'doc_id': 'donorID'}))
        fdf = documents_to_dataframe(frags, ['location'], {})
        summary = fdf.groupby(['orgID', 'branchID', 'organismID']).agg(fragments=('donorID', 'count'),
                                                                       colonies=('donorID', 'nunique')).reset_index()
        nursery_details = fdf.groupby(['orgID', 'branchID',
                                       'nurseryID', 'donorID']).agg(organismID=('organismID', 'first'),
                                                                    fragments=('organismID', 'count')).reset_index()
        details = fdf.groupby(['orgID', 'branchID',
                               'donorID']).agg(organismID=('organismID', 'first'),
                                               fragments=('organismID', 'count')).reset_index()
        summary['taxon'] = summary.organismID.map(taxon_map)
        nursery_details['taxon'] = nursery_details.organismID.map(taxon_map)
        details['taxon'] = details.organismID.map(taxon_map)
        nursery_details = nursery_details.merge(donors_df, on=['orgID', 'branchID', 'donorID']
                                                ).drop(columns=['metadata', 'siteType'])
        details = details.merge(donors_df, on=['orgID', 'branchID', 'donorID']).drop(columns=['metadata', 'siteType'])
        summary_list = make_into_stats_list(summary, ['orgID', 'branchID'], 'donor_summary_stats')
        nursery_details_list = make_into_stats_list(nursery_details,
                                                    ['orgID', 'branchID', 'nurseryID', 'donorID'],
                                                    'donor_nursery_details_stats')
        details_list = make_into_stats_list(details, ['orgID', 'branchID', 'donorID'], 'donor_details_stats')
        return summary_list + details_list + nursery_details_list
    return []


# aggregate outplanted fragments over outplant and cells
def get_outplanted_stats(loc, outplanted):
    if len(outplanted) > 0:
        loc_attrs = list(loc.keys())
        year = dt.datetime.now().year
        fdf = documents_to_dataframe(outplanted, ['location'], {})
        grps = fdf.groupby(loc_attrs + ['outplantID'])
        op_stats = grps.agg(species=('organismID', 'nunique'),
                            mother_colonies=('donorID', 'nunique'),
                            fragments=('doc_id', 'count')).reset_index()
        year = dt.datetime.now().year
        ytd_df = fdf[fdf.createdAt.dt.year == year]
        op_stats['ytd_outplanted'] = ytd_df.groupby(['outplantID'])['doc_id'].count()
        op_stats['ytd_outplanted'] = op_stats['ytd_outplanted'].fillna(0)
        op_stats = make_into_stats_list(op_stats, ['orgID', 'branchID', 'outplantID'], 'outplant_stats')
        grps = fdf.groupby(loc_attrs + ['outplantID', 'outplantCellID'])
        cell_stats = grps.agg(n_species=('organismID', 'nunique'),
                              n_mothercolonies=('donorID', 'nunique'),
                              n_outplanted=('doc_id', 'count')).reset_index()
        cell_stats = make_into_stats_list(cell_stats,
                                          loc_attrs + ['outplantID', 'outplantCellID'],
                                          'outplantcell_stats')
        return op_stats + cell_stats
    return []


def get_stats_by_year(loc, edf, frags):
    fdf = documents_to_dataframe(frags, ['location'], {})
    fdf['created_year'] = fdf.createdAt.dt.year
    fdf['completed_year'] = fdf.completedAt.dt.year
    created_grp = fdf.groupby(['created_year'])
    completed_grp = fdf.groupby(['completed_year', 'state'])
    created_by_year = (pd.DataFrame(
        [created_grp['monitored'].count().rename("count"),
         created_grp['organismID'].nunique().rename("n_species")])
        .T.reset_index().rename(columns={'created_year': 'year'}))
    created_by_year['state'] = 'created'
    completed_by_year = (pd.DataFrame(
        [completed_grp['monitored'].count().rename("count"),
         completed_grp['organismID'].nunique().rename("n_species")])
        .T.reset_index().rename(columns={'completed_year': 'year'}))
    df = pd.concat([created_by_year, completed_by_year])
    for id_name, id_val in loc.items():
        df[id_name] = id_val
    return make_into_stats_list(df, list(loc.keys()), 'by_year')


# get species counts by nursery or outplant
def get_fragment_stats(qf, location, frags, site_id_attr, stat_type):
    def _get_fragment_stats(location, fdf, corals):
        spp_info_df = pd.DataFrame(fdf.organismID.value_counts())
        spp_info_df['Colonies'] = fdf.groupby('organismID')['donorID'].nunique()
        spp_info_df['genus'] = spp_info_df.index.map(genus_map)
        spp_info_df['species'] = spp_info_df.index.map(species_map)
        spp_info_df = spp_info_df.reset_index()
        for k, v in location.items():
            spp_info_df[k] = v
        return make_into_stats_list(spp_info_df, location.keys(), stat_type)

    if len(frags) > 0:
        corals = qf.get_organisms(org_type='coral')
        genus_map = {doc[0]: doc[1]['genus'] for doc in corals}
        species_map = {doc[0]: doc[1]['species'] for doc in corals}
        fdf = documents_to_dataframe(frags, ['location'], {})
        # donor_df = pd.DataFrame(fdf.groupby('organismID')['doc_id'].count()).reset_index()
        frag_stats = _get_fragment_stats(location, fdf, corals)
        for sid, sdf in fdf.groupby(site_id_attr):
            loc = location.copy()
            loc[site_id_attr] = sid
            site_stats = _get_fragment_stats(loc, sdf, corals)
            frag_stats.extend(site_stats)
    else:
        frag_stats = []
    return frag_stats


# return branch_stats, nursery_stats, structure_stats
def get_stats_of_location(qf, loc):
    loc_attrs = list(loc.keys())
    frags = qf.get_docs(qf.query_fragments(loc))
    in_nursery_frags = [(nid, info) for (nid, info) in frags if info['state'] == fs.in_nursery.value]
    outplanted_frags = [(nid, info) for (nid, info) in frags if info['state'] == fs.outplanted.value]
    agg_df, events_df = get_location_monitoring_event_info(qf, loc, frags)
    all_stats = make_into_stats_list(agg_df, loc_attrs + ['nurseryID', '_0'], None)
    if len(events_df) > 0:
        current_stats = get_current_stats(qf, loc, events_df, in_nursery_frags, outplanted_frags)
        all_stats.extend(current_stats)
        by_year_stats = get_stats_by_year(loc, events_df, frags)
        all_stats.extend(by_year_stats)
        op_stats = get_outplanted_stats(loc, outplanted_frags)
        all_stats.extend(op_stats)
        frag_stats = get_fragment_stats(qf, loc, in_nursery_frags, 'nurseryID', 'fragment_donor')
        all_stats.extend(frag_stats)
        op_frag_stats = get_fragment_stats(qf, loc, outplanted_frags, 'outplantID', 'outplant_fragment_donor')
        all_stats.extend(op_frag_stats)
        donor_stats = get_donor_stats(qf, loc, in_nursery_frags)
        all_stats.extend(donor_stats)
    return all_stats


def set_metadata(doc, createdAt=None, createdBy=None):
    doc['metadata'] = {'createdAt': createdAt,
                       'createdBy': createdBy,
                       'deleted': False}


def delete_collection(db, coll_name, limit=5000):
    doc_counter = 0
    commit_counter = 0
    batch_size = 500
    while True:
        docs = []
        print('Getting docs to delete')
        docs = [snapshot for snapshot in db.collection(coll_name)
                .limit(limit).stream()]
        batch = db.batch()
        for doc in docs:
            doc_counter = doc_counter + 1
            if doc_counter % batch_size == 0:
                commit_counter += 1
                print(f'Deleting batch {batch_size * commit_counter}')
                batch.commit()
            batch.delete(doc.reference)
        batch.commit()
        if len(docs) == limit:
            continue
        break


def control_stats(qf, control_id, control=None):
    _control = control or qf.get_site_by_id(control_id)
    # make the block of control data
    control_data = {
        "controlID": control_id,
        "name": _control['name'],
        "lat": _control['geolocation']['latitude'],
        "lon": _control['geolocation']['longitude'],
        "perimeter": control["perimeter"]
        }
    return control_data


def all_nursery_stats(qf, stats_df, loc):
    results = []
    df = stats_df[stats_df.stat_type == 'nursery_stats'].dropna(axis=1, how='all')
    if len(df) > 0:
        ddf = stats_df[stats_df.stat_type == 'fragment_donor']
        ddf = ddf.dropna(subset='nurseryID', axis=0).dropna(axis=1, how='all')
        donor_df = stats_df[stats_df.stat_type == 'donor_nursery_details_stats']
        donor_df = donor_df.dropna(subset='nurseryID', axis=0).dropna(axis=1, how='all')
        for nid, stats in df.set_index('nurseryID').iterrows():
            nddf = ddf[ddf.nurseryID == nid].copy()
            ndonor_df = donor_df[donor_df.nurseryID == nid].copy()
            res = ep._nursery_stats_helper(qf, nid, stats.to_dict(), nddf, ndonor_df)
            results.append(res)
    # add empty records for nurseries with no fragments (either new or fully outplanted)
    nids = qf.get_docs(qf.query_nurseries(loc))
    nids = {val[0]: val[1] for val in nids}
    for nid, nursery in nids.items():
        if len(ddf) == 0 or nid not in ddf.nurseryID:
            nursery_data = {
                "nurseryID": nid,
                "name": nursery['name'],
                "lat": nursery['geolocation']['latitude'],
                "lon": nursery['geolocation']['longitude'],
                'orgID': nursery['location']['orgID'],
                'branchID': nursery['location']['branchID'],
                # 'restoSiteID': nursery['location'].get('restoSiteID'),
                'n_fragments': 0,
                "ytd_seeded": 0,
                "Species": 0,
                "Species_Frac": 0,
                "Genera": 0,
                "Genus_Frac": 0,
                'Colonies': 0,
                'Mother_Colony_Frac': 0,
                'age': 0,
                "nurseryCreatedAt": nursery['metadata']['createdAt'],
                'stat_type': 'nursery_stats',
                }
            results.append(nursery_data)
    return results


def all_outplant_stats(qf, stats_df, loc):
    results = []
    oids = qf.get_docs(qf.query_outplantsites(loc))
    oids = {val[0]: val[1] for val in oids}
    op_df = stats_df[stats_df.stat_type == 'outplant_stats'].dropna(axis=1, how='all')
    if len(op_df) > 0:
        cdf = stats_df[stats_df.stat_type == 'outplantcell_stats'].dropna(axis=1, how='all')
        for oid, op_stat in op_df.set_index('outplantID').iterrows():
            op_cdf = cdf[cdf.outplantID == oid]
            res = ep._outplant_stats_helper(qf, oid, oids[oid],
                                            op_stat.to_dict(),
                                            op_cdf, False)
            results.append(res)
    # add empty records for outplants with no fragments planted yet
    for oid, op in oids.items():
        if len(op_df) == 0 or oid not in op_df.outplantID:
            outplant_data = {
                "outplantID": oid,
                "name": op['name'],
                "lat": op['geolocation']['latitude'],
                "lon": op['geolocation']['longitude'],
                'orgID': op['location']['orgID'],
                'branchID': op['location']['branchID'],
                # 'restoSiteID': op['location'].get('restoSiteID'),
                "cells": 0,
                "ytd_outplanted": 0,
                "species": 0,
                'stat_type': 'outplant_stats',
                'mother_colonies': 0,
                'fragments': 0,
                "species_per_cell": 0,
                "corals_per_cell": 0,
                "outplantCreatedAt": op['metadata']['createdAt']
                }
            results.append(outplant_data)
    return results


def _get_fragment_species_stats(qf, loc, stats_df, data):
    # get species stats for fragments in this branch
    fdf = stats_df[stats_df.stat_type == 'fragment_donor']
    fdf = fdf.dropna(subset='nurseryID', axis=0).dropna(axis=1, how='all')
    if len(fdf) > 0:
        donor_df = stats_df[stats_df.stat_type == 'donor_nursery_details_stats'].copy()
        ep._donor_stats_helper(fdf, donor_df, data)


#def summary_restosite_stats(qf, loc, restosite_stats):
#    # get stats for this branch
#    _restosite = qf.get_site_by_id(loc['restoSiteID'])
#    stats = [(idx, doc) for idx, doc in enumerate(restosite_stats)]
#    stats_df = qf.documents_to_dataframe(stats, ['data', 'location'])
#    bdf = stats_df[stats_df.stat_type == 'restosite_stats'].dropna(axis=1, how='all')
#    # make the block of restoration site data
#    restosite_data = {
#        "restoSiteID": loc['restoSiteID'],
#        "Resoration Site": _restosite['name'],
#        "lat": _restosite['geolocation']['latitude'],
#        "lon": _restosite['geolocation']['longitude'],
#        'Nurseries': qf.get_count(qf.query_nurseries(loc)),
#        'Outplants': qf.get_count(qf.query_outplantsites(loc)),
#        'Controls': qf.get_count(qf.query_controlsites(loc)),
#        'Fragments': int(bdf.n_fragments.sum()),
#        'Fraction Alive': qq.wtd_mean(bdf, 'alive', 'alive_count'),
#        'Outplanted': bdf.n_outplanted.sum(),
#        'YTD Seeded': bdf.ytd_seeded.sum(),
#        'YTD Outplanted': bdf.ytd_outplanted.sum()
#    }
#    restosite_data["nursery_info"] = all_nursery_stats(qf, stats_df, loc)
#    restosite_data["outplant_info"] = all_outplant_stats(qf, stats_df, loc)
#    restosite_data["control_info"] = ep.all_control_stats(qf, loc)
#    _get_fragment_species_stats(qf, loc, stats_df, restosite_data)
#    return restosite_data


def summary_branch_stats(qf, loc, branch_stats):
    # get stats for this branch
    _branch = qf.get_site_by_id(loc['branchID'])
#    restosites = qf.get_docs(qf.query_restosites(loc))
    stats = [(idx, doc) for idx, doc in enumerate(branch_stats)]
    stats_df = qf.documents_to_dataframe(stats, ['data', 'location'])
    bdf = stats_df[stats_df.stat_type == 'branch_stats'].dropna(axis=1, how='all')
    # make the block of global data
    branch_data = {
        "branchID": loc['branchID'],
        "Branch": _branch['name'],
        "lat": _branch['geolocation']['latitude'],
        "lon": _branch['geolocation']['longitude'],
#        'Restoration Sites': len(restosites),
        'Nurseries': qf.get_count(qf.query_nurseries(loc)),
        'Outplants': qf.get_count(qf.query_outplantsites(loc)),
        'Controls': qf.get_count(qf.query_controlsites(loc)),
        'Fragments': int(bdf.n_fragments.sum()),
        'Fraction Alive': qq.wtd_mean(bdf, 'alive', 'alive_count'),
        'Outplanted': bdf.n_outplanted.sum(),
        'YTD Seeded': bdf.ytd_seeded.sum(),
        'YTD Outplanted': bdf.ytd_outplanted.sum()
    }
    branch_data["nursery_info"] = all_nursery_stats(qf, stats_df, loc)
    branch_data["outplant_info"] = all_outplant_stats(qf, stats_df, loc)
    branch_data["control_info"] = ep.all_control_stats(qf, loc)
    _get_fragment_species_stats(qf, loc, stats_df, branch_data)
#    if len(restosites) > 0:
#        # get stats for each resto site
#        restosite_stats = []    
#        for restosite in restosites:
#            restosite_id = restosite[0]
#            print(f"Getting stats for {_branch['name']} {restosite[1]['name']}")
#            loc['restoSiteID'] = restosite_id
#            current_stats = get_stats_of_location(qf, loc)
#            if len(current_stats) > 0:
#                restosite_stats.append(current_stats)
#                summary = summary_restosite_stats(qf, loc, current_stats)
#                restosite_stats.append({
#                    'stat_type': 'restosite_summary',
#                    'location': loc,
#                    'data': summary
#                    })
#        # branch_data["restosite_info"] = ep.all_restosite_stats(qf, branch_id)
#        branch_data["restosite_info"] = restosite_stats
    return branch_data


# add documents to a collection
# documents are a list of dicts
def add_collection(db, coll_name, documents, limit=None, batchsize=1000):
    print(f"Adding {len(documents)} documents to collection {coll_name}")
    now = dt.datetime.now()
    prefix = '_'
    # prefix = ''
    coll_name = prefix + coll_name
    if limit != 0:
        print(f"Deleting collection {coll_name} prior to writing new data")
        delete_collection(db, coll_name)
        batch = db.batch()
        idx = 0
        for doc in documents:
            set_metadata(doc, createdAt=now)
            coll_ref = db.collection(coll_name)
            doc_ref = coll_ref.document(None)
            if limit is None or idx < limit:
                batch.set(doc_ref, doc)
            idx += 1
            if idx % batchsize == 0:
                print(f"Committing batch {idx // batchsize} {coll_name}")
                batch.commit()
                batch = db.batch()
        batch.commit()


# %%

# compute stats and save to Firestoire collection
def compute_statistics(qf, save=False, limit=None):
    results = []
    # get summaries for each branch in each org
    orgs = qf.get_orgs()
    for org in orgs:
        org_id = org[0]
        branches = qf.get_branches(org_id)
        for branch in branches:
            loc = {'orgID': org_id, 'branchID': branch[0]}
            print(f"Getting stats for {org[1]['name']} {branch[1]['name']}")
            branch_stats = get_stats_of_location(qf, loc)
            summary = summary_branch_stats(qf, loc, branch_stats)
            branch_stats.append({
                'stat_type': 'branch_summary',
                'location': loc,
                'data': summary
                })
            results.extend(branch_stats)
    # add aggregate stats to Firestore
    if save:
        add_collection(qf.db, "statistics", results, limit=limit)


# %%
if __name__ == "__main__":
    creds = '../restoration-ios-firebase-adminsdk-wg0a4-a59664d92f.json'
    devcreds = '../restoration-app---dev-6df41-firebase-adminsdk-fbsvc-37ee88f0d4.json'
    qf = qq.QueryFirestore(creds=devcreds)
    compute_statistics(qf, save=True, limit=None)
