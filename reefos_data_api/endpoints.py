# implement functions to reproduce data explorer API endpoints using the new FireStore database
import pandas as pd
import json
import datetime as dt

import reefos_data_api.query_firestore as qq
from reefos_data_api.firestore_constants import EventType as et


fp = 'French Polynesia'
fp_pre_database = {'outplanted': 96961, 'seeded': 17116}
# data from impact reports, outplanted is cumulative, includes some frags in the database
moorea_historic_data = [
    {'Branch': fp, 'year': 2020, 'outplanted': 15000, 'seeded': 3598},
    {'Branch': fp, 'year': 2021, 'outplanted': 15755, 'seeded': 8945},
    {'Branch': fp, 'year': 2022, 'outplanted': 30980, 'seeded': 9450},
    {'Branch': fp, 'year': 2023, 'outplanted': 100870, 'seeded': 26159},
    ]


def api_help():
    strs = [
        "Hello data seeker",
        "Data Explorer API Endpoints",
        "",
        "Get org list",
        "data.coralgardeners.org/data/orgs?pwd=showmethedata",
        "",
        "Get global overview",
        "data.coralgardeners.org/data/global?org=coral-gardeners&pwd=showmethedata",
        "",
        "Get by-year overview",
        "data.coralgardeners.org/data/byyear?org=coral-gardeners&pwd=showmethedata",
        "",
        "Get branches summary",
        "data.coralgardeners.org/data/branches?org=coral-gardeners&pwd=showmethedata",
        "",
        "Get branch overview using branch ID or name",
        "data.coralgardeners.org/data/branch?org=coral-gardeners&branch=moorea&pwd=showmethedata",
        "",
        "Get nursery data using nursery ID. Add details=True to get monitoring details",
        "data.coralgardeners.org/data/nursery?org=coral-gardeners&nursery=xxx&pwd=showmethedata",
        "",
        "Get nursery data using branch and nursery name. Add details=True to get monitoring details",
        "data.coralgardeners.org/data/nursery?org=coral-gardeners&branch=xxx&nursery=xxx&pwd=showmethedata",
        "",
        "Get outplant data using outplant ID",
        "data.coralgardeners.org/data/outplant?org=coral-gardeners&outplant=xxx&pwd=showmethedata",
        "",
        "Get outplant data using branch and outplant name",
        "data.coralgardeners.org/data/outplant?org=coral-gardeners&branch=xxx&outplant=xxx&pwd=showmethedata",
        "",
        "Get branch mothercolonies. Add details=True to get individual colony detils",
        "data.coralgardeners.org/data/mothercolonies?org=coral-gardeners&branch=moorea&pwd=showmethedata",
        "",
        "Reefcam Data Endpoints",
        "All use the parameters:",
        "org=coral-gardeners&device=xxx&start=2023-12-1&end=2024-03-31&pwd=showmethedata",
        "end is optional, if not given then one day of data is retrieved",
        "data.coralgardeners.org/data/images?org=coral-gardeners&device=reefos-01&start=02/21/2024&pwd=showmethedata",
        "",
        "data.coralgardeners.org/data/images",
        "data.coralgardeners.org/data/audio",
        "data.coralgardeners.org/data/measurement/temperature",
        "data.coralgardeners.org/data/measurement/bioacoustics",
        "data.coralgardeners.org/data/measurement/fishcommunity",
        "data.coralgardeners.org/data/measurement/fishdiversity",
        "",
        "Get a file",
        "data.coralgardeners.org/file?branch=moorea&filename=filepath&pwd=showmethedata",
        "",
        "Add new site data (siteType=nursery, outplant, control) to a branch",
        "data.coralgardeners.org/refresh?org=coral-gardeners&branch=moorea&siteType=nursery&pwd=showmethedata",
        "",
        "Refresh cached data for a branch (threaded)",
        "data.coralgardeners.org/refreshbranch?org=coral-gardeners&branch=moorea&pwd=showmethedata",
        "",
        "Refresh all cached data for an org (threaded)",
        "data.coralgardeners.org/refreshall?org=coral-gardeners&pwd=showmethedata",
        "",
        "Status of refresh (available or running)",
        "data.coralgardeners.org/refreshstatus",
        ]
    return json.dumps(strs)


def _donor_stats_helper(stats_df, donor_df, data):
    spp_df = stats_df.groupby(['species', 'genus'])['count'].sum().reset_index()
    # compute summary stats for genus and species
    spp_df['taxon'] = spp_df.genus + ' ' + spp_df.species
    spp_df['frac'] = spp_df['count'] / spp_df['count'].sum()
    spp_info = spp_df.set_index('taxon')['frac'].sort_values(ascending=False).to_dict()
    genus_info = spp_df.groupby('genus')['frac'].sum().sort_values(ascending=False).to_dict()
    donor_df['frac'] = donor_df['fragments'] / donor_df['fragments'].sum()
    donor_stats = donor_df.groupby('taxon').frac.sum()
    donor_info = donor_stats.sort_values(ascending=False).to_dict()
    spp_data = {
        'Species': len(spp_info),
        'Species_Frac': spp_info,
        "shannon_diversity_spp": qq.shannon(spp_df['frac']),
        'Genera': spp_df.genus.nunique(),
        'Genus_Frac': genus_info,
        'Colonies': len(donor_df),
        'Mother_Colony_Frac': donor_info,
        "shannon_diversity_mc": qq.shannon(donor_stats),
        }
    data.update(spp_data)


def _get_fragment_species_stats(qf, location, data):
    # get species stats for fragments in this location
    _stats = qf.get_docs(qf.query_statistics(location, 'fragment_donor'))
    if len(_stats) > 0:
        stats_df = qf.documents_to_dataframe(_stats, ['data', 'location'])
        donor_stats = qf.get_docs(qf.query_statistics(location, 'donor_nursery_details_stats'))
        donor_df = qf.documents_to_dataframe(donor_stats, ['data', 'location'])
        _donor_stats_helper(stats_df, donor_df, data)


def get_orgs(qf):
    orgs = qf.get_orgs()
    results = []
    for org in orgs:
        res = org[1]
        res['orgID'] = org[0]
        results.append(res)
    return results


def get_orgbyname(qf, name):
    res = qf.get_org_by_name(name)
    if len(res) == 0:
        return None
    return res[0]


def org_exists(qf, org_id):
    return org_id in qf.get_org_ids()


def site_exists(qf, site_id):
    return qf.get_site_by_id(site_id) is not None


def global_stats(qf, org_id):
    loc = {'orgID': org_id}
    # get stats for branches in this org
    stats = qf.get_docs(qf.query_statistics(loc, 'branch_stats'))
    bdf = qf.documents_to_dataframe(stats, ['data', 'location'])
    # make the block of global data
    global_data = {
        'Branches': len(bdf),
        'Nurseries': qf.get_count(qf.query_nurseries(loc)),
        'Outplants': qf.get_count(qf.query_outplantsites(loc)),
        'Fragments': int(bdf.n_fragments.sum()) if len(bdf) > 0 else 0,
        'Fraction Alive': qq.wtd_mean(bdf, 'alive', 'alive_count'),
        'Outplanted': int(bdf.n_outplanted.sum()) if len(bdf) > 0 else 0,
        }
    _get_fragment_species_stats(qf, loc, global_data)
    return global_data


def by_year_stats(qf, org_id):    
    loc = {'orgID': org_id}
    # get the by year stats from FireStore
    stats = qf.get_docs(qf.query_statistics(loc, 'by_year'))
    df = qf.documents_to_dataframe(stats, ['location', 'data'])
    # filter to keep data about seeding and outplanting
    df = df[df.state.isin(['created', 'outplanted'])][['year', 'branchID', 'count', 'state', 'n_species']]
    # merge the data together
    by_year_df = df[df.state == 'created'].set_index(['year', 'branchID']).rename(columns={'count': 'seeded'})
    outplanted_df = df[df.state == 'outplanted'].set_index(['year', 'branchID'])
    by_year_df['outplanted'] = outplanted_df['count']
    by_year_df['outplanted'] = by_year_df['outplanted'].fillna(0)
    by_year_df = by_year_df.reset_index().drop('state', axis=1).sort_values('year')
    # outplanted is cumulative sum
    by_year_df['outplanted'] = by_year_df.groupby('branchID')['outplanted'].cumsum()
    by_year_df.set_index('year', inplace=True)
    # get the branch names
    branches = qf.get_branches(org_id)
    branch_names = {br[0]: br[1]['name'] for br in branches}
    by_year_df['Branch'] = by_year_df.branchID.map(branch_names)
    # add historic data to 'French Polynesia' branch
    if (by_year_df['Branch'] == fp).sum() > 0:
        # get branch id of fp branch
        fp_id = None
        for bid, bname in branch_names.items():
            if bname == fp:
                fp_id = bid
                break
        # make dataframe of historic data
        mby_df = by_year_df[by_year_df.branchID == fp_id]
        mdf = pd.DataFrame(moorea_historic_data).set_index('year')
        mdf['branchID'] = fp_id
        mdf['n_species'] = mby_df.n_species
        offset = mdf.loc[2023, ['outplanted', 'seeded']] - mby_df.loc[2023, ['outplanted', 'seeded']]
        by_year_df.loc[by_year_df.index > 2023, 'outplanted'] += offset.loc['outplanted']
        by_year_df.loc[by_year_df.index > 2023, 'seeded'] += offset.loc['seeded']
        by_year_df = pd.concat([mdf, by_year_df.loc[by_year_df.index > 2023]]).reset_index()
    return by_year_df.to_dict('records')


def get_nursery_monitoring_history(qf, nursery_id, agg_type='by_nursery'):
    loc = {'nurseryID': nursery_id}
    monitoring_stats = qf.get_docs(qf.query_statistics(loc, ['fullNurseryMonitoring', 'bleachingNurseryMonitoring'],
                                                       filter_fields={'data.agg_type': agg_type}
                                                       ))
    mdf = qf.documents_to_dataframe(monitoring_stats, ['data', 'location'])
    # set non-bleaching attributes in bleaching monitoring to nan as they are incomplete
    if len(mdf) > 0:
        mdf.loc[mdf.stat_type == 'bleachingNurseryMonitoring', ['EVI', 'alive', 'health']] = None  # np.nan
        mdf = mdf.sort_values('monitoredAt')
    return mdf


def get_nursery_monitoring_trends(monitoring_df):
    trend_results = {}
    attrs = ['bleach', 'EVI', 'alive', 'health']
    for attr in attrs:
        _df = monitoring_df[['monitoredAt', attr]].dropna()
        if len(_df) > 1:
            _df['diff'] = _df[attr].diff()
            res = {
                'diff': _df['diff'].iloc[-1],
                'first': _df['monitoredAt'].iloc[-2],
                'last': _df['monitoredAt'].iloc[-1],
                'mean': _df[attr].iloc[-2:].mean(),
                }
            trend = res['diff'] / res['mean']
            res['trend'] = 'down' if trend < -0.1 else ('up' if trend > 0.1 else 'same')
            trend_results[attr.split('_')[0] + '_trend'] = _df.iloc[-2:].to_dict('records')
    return trend_results


def _nursery_stats_helper(qf, nursery_id, nursery_data, ddf=None, donor_df=None):
    nursery = qf.get_site_by_id(nursery_id)
    branch_id = nursery['location']['branchID']
    drop = ['stat_type', 'doc_id']
    for attr in drop:
        del nursery_data[attr]
    nursery_data['nurseryID'] = nursery_id
    nursery_data['name'] = nursery['name']
    nursery_data["lat"] = nursery['geolocation']['latitude']
    nursery_data["lon"] = nursery['geolocation']['longitude']
    if ddf is None:
        _get_fragment_species_stats(qf, {'branchID': branch_id, 'nurseryID': nursery_id}, nursery_data)
    else:
        _donor_stats_helper(ddf, donor_df, nursery_data)
    monitoring_df = get_nursery_monitoring_history(qf, nursery_id, agg_type='by_nursery')
    if monitoring_df is not None and len(monitoring_df) > 1:
        trends = get_nursery_monitoring_trends(monitoring_df)
        nursery_data.update(trends)
    nursery_data['nurseryCreatedAt'] = nursery['metadata']['createdAt']
    nursery_data['age'] = (dt.datetime.now(dt.timezone.utc) - nursery_data['nurseryCreatedAt']).days
    return nursery_data


def nursery_stats(qf, nursery_id, nursery=None):
    stats = qf.get_docs(qf.query_statistics({'nurseryID': nursery_id}, 'nursery_stats'))
    nursery_stats = qf.documents_to_dataframe(stats, ['data', 'location']).iloc[0].to_dict()
    nursery_stats['age'] = (dt.datetime.now(dt.timezone.utc) - nursery_stats['createdAt']).days
    return _nursery_stats_helper(qf, nursery_id, nursery_stats)


def get_nursery_history(qf, nursery_id):
    nursery_df = get_nursery_monitoring_history(qf, nursery_id, agg_type='by_nursery')
    species_df = get_nursery_monitoring_history(qf, nursery_id, agg_type='by_species')
    # trends = get_monitoring_trends(nursery_df)
    return nursery_df, species_df # , trends


def get_outplant_monitoring_history(qf, outplant_id):
    loc = {'outplantID': outplant_id}
    monitoring_events = qf.get_docs(qf.query_events(loc, {'eventType': et.outplant_cell_monitoring.value}))
    mdf = qf.documents_to_dataframe(monitoring_events, ['eventData', 'location'])
    # set non-bleaching attributes in bleaching monitoring to nan as they are incomplete
    if len(mdf) > 0:
        mdf = mdf.sort_values('monitoredAt')
    return mdf


def _outplant_stats_helper(qf, outplant_id, outplant, op_stats, cdf, details):
    _outplant = outplant or qf.get_site_by_id(outplant_id)
    # make the block of outplant data
    n_cells = len(cdf)
    outplant_data = {
        "outplantID": outplant_id,
        "name": _outplant['name'],
        "cells": n_cells,
        "lat": _outplant['geolocation']['latitude'],
        "lon": _outplant['geolocation']['longitude'],
        }
    outplant_data.update(op_stats)
    outplant_data['species_per_cell'] = float(cdf.n_species.mean()) if n_cells > 0 else 0
    outplant_data['corals_per_cell'] = float(cdf.n_outplanted.mean()) if n_cells > 0 else 0
    if details and n_cells > 0:
        keep = ['n_mothercolonies', 'n_species', 'n_outplanted', 'outplantID', 'outplantCellID', 'monitoredAt']
        outplant_data['cells'] = cdf[keep].to_dict('records')
    monitoring_df = get_outplant_monitoring_history(qf, outplant_id)
    if monitoring_df is not None and len(monitoring_df) > 0:
        history_keep = ['logID', 'branchID', 'outplantID', 'outplantCellID', 'percentBleach',
                        'percentCoralCover', 'percentSurvival', 'monitoredAt']
        mdf = monitoring_df[history_keep]
        grp = mdf.groupby(['logID', 'branchID', 'outplantID'])
        mon_events = grp.agg({'percentCoralCover': 'mean', 'percentBleach': 'mean',
                              'percentSurvival': 'mean', 'monitoredAt': 'max'}).reset_index()
        mon_events['monitoredAt'] = mon_events['monitoredAt'].astype(str)
        outplant_data['monitoring'] = mon_events.to_dict('records')
    outplant_data['outplantCreatedAt'] = _outplant['metadata']['createdAt']
    outplant_data['age'] = (dt.datetime.now(dt.timezone.utc) - outplant_data['outplantCreatedAt']).days
    return outplant_data


def outplant_stats(qf, outplant_id, outplant=None, details=False):
    loc = {'outplantID': outplant_id}
    op_stats = qf.get_docs(qf.query_statistics(loc, 'outplant_stats'))
    if len(op_stats) > 0:
        cell_stats = qf.get_docs(qf.query_statistics(loc, 'outplantcell_stats'))
        cdf = qf.documents_to_dataframe(cell_stats, ['data', 'location'])
        op_stats = op_stats[0][1]
    else:
        op_stats = {'data': []}
        cdf = pd.DataFrame()
    return _outplant_stats_helper(qf, outplant_id, outplant, op_stats['data'], cdf, details)


def control_stats(qf, control_id, control=None):
    _control = control or qf.get_site_by_id(control_id)
    # make the block of global data
    control_data = {
        "controlID": control_id,
        "outplantID": _control['siteData']['outplantID'],
        "reefod_id": _control['siteData']['reefos_id'],
        "name": _control['name'],
        "lat": _control['geolocation']['latitude'],
        "lon": _control['geolocation']['longitude'],
        "perimeter": control["perimeter"],
        'controlCreatedAt': _control['metadata']['createdAt']
        }
    return control_data


def all_control_stats(qf, loc):
    results = []
    cids = qf.get_docs(qf.query_controlsites(loc))
    for cid, doc in cids:
        results.append(control_stats(qf, cid, doc))
    return results


def branch_stats(qf, loc):
    # get stats for brach or org
    stats = qf.get_docs(qf.query_statistics(loc, 'branch_summary'))
    return [stat[1]['data'] for stat in stats]


def full_nursery_stats(qf, nursery_id):
    keep = ['EVI_count', 'n_fragments', 'bleach', 'EVI', 'alive', 'health', 'nurseryID']
    history_keep = ['nurseryID', 'bleach', 'EVI', 'alive',
                    'health', 'stat_type', 'monitoring_name', 'monitoredAt']

    loc = {'nurseryID': nursery_id}
    structure_stats = qf.get_docs(qf.query_statistics(loc, 'structure_stats'))
    struct_df = qf.documents_to_dataframe(structure_stats, ['data', 'location'])
    spp_nursery_stats = qf.get_docs(qf.query_statistics(loc, 'spp_nursery_stats'))
    spp_df = qf.documents_to_dataframe(spp_nursery_stats, ['data', 'location'])
    stats = nursery_stats(qf, nursery_id)
    nursery_history, species_history = get_nursery_history(qf, nursery_id)
    # stats.update(trends)
    if len(nursery_history) > 0:
        hdf = nursery_history[history_keep].set_index('monitoredAt')
        hdf.index = hdf.index.astype(str)
    else:
        hdf = nursery_history
    shdf = species_history[history_keep + ['taxon']] if len(species_history) > 0 else species_history
    all_stats = {
        'summary': stats,
        'species_summary': spp_df[keep + ['taxon']].to_dict('records'),
        'structures': struct_df[keep + ['_0']].to_dict('records'),
        'history': hdf.to_dict(),
        'spp_history': shdf.to_dict('records')
        }
    return all_stats


def full_outplant_stats(qf, outplant_id):
    return outplant_stats(qf, outplant_id, details=True)


def get_donor_stats(qf, branch_id, details=True):
    loc = {'branchID': branch_id}
    summary_stats = qf.get_docs(qf.query_statistics(loc, 'donor_summary_stats'))
    summary_df = qf.documents_to_dataframe(summary_stats, ['data', 'location'])
    details_stats = qf.get_docs(qf.query_statistics(loc, 'donor_details_stats'))
    details_df = qf.documents_to_dataframe(details_stats, ['data', 'location'])
    all_stats = {
        'summary': summary_df.to_dict('records'),
        'details': details_df.to_dict('records')
        }
    return all_stats


def nursery_fragment_stats(qf, nursery_id):
    frag_stats = qf.get_docs(qf.query_statistics({'nurseryID': nursery_id}, 'fragment_stats'))
    frags_df = qf.documents_to_dataframe(frag_stats, [])
    return frags_df.to_dict('records')


def download_firestore_file(qf, org, branch, blobname):
    parts = blobname.split('/')
    fname = parts[-1]
    qf.download_blob(org, branch, blobname, fname)
