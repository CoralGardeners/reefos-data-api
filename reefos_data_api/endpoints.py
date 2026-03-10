# implement functions to reproduce data explorer API endpoints using the new FireStore database
import pandas as pd
import json
import datetime as dt

import reefos_data_api.query_firestore as qq
from reefos_data_api.firestore_constants import EventType as et
from reefos_data_api.firestore_constants import StatType as st


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
    help_data = {
        "description": "Coral Gardeners Data API",
        "base_url": "dataapi.coralgardeners.org",
        "auth": "All endpoints require ?pwd=showmethedata",
        "endpoints": {
            "orgs": {
                "description": "List all organizations",
                "url": "/data/orgs?pwd=showmethedata",
                "params": {},
            },
            "global": {
                "description": "Global overview for an organization",
                "url": "/data/global?org={orgID}&pwd=showmethedata",
                "params": {"org": "Organization ID"},
            },
            "byyear": {
                "description": "Annual seeding and outplanting stats by branch",
                "url": "/data/byyear?org={orgID}&pwd=showmethedata",
                "params": {"org": "Organization ID"},
            },
            "branches": {
                "description": "Summary of all branches in an organization",
                "url": "/data/branches?org={orgID}&pwd=showmethedata",
                "params": {"org": "Organization ID"},
            },
            "branch": {
                "description": "Overview of a single branch",
                "url": "/data/branch?branch={branchID}&pwd=showmethedata",
                "params": {"branch": "Branch ID"},
            },
            "restosites": {
                "description": "All restoration sites in a branch",
                "url": "/data/restosites?branch={branchID}&pwd=showmethedata",
                "params": {"branch": "Branch ID"},
            },
            "restosite": {
                "description": "Overview of a single restoration site",
                "url": "/data/restosite?restosite={restositeID}&pwd=showmethedata",
                "params": {"restosite": "Restoration site ID"},
            },
            "nursery": {
                "description": "Nursery data. Add details=True for monitoring history",
                "url": "/data/nursery?nursery={nurseryID}&pwd=showmethedata",
                "params": {"nursery": "Nursery ID", "details": "True (optional)"},
            },
            "outplant": {
                "description": "Outplant site data. Add details=True for cell and species breakdown",
                "url": "/data/outplant?outplant={outplantID}&pwd=showmethedata",
                "params": {"outplant": "Outplant site ID", "details": "True (optional)"},
            },
            "mothercolonies": {
                "description": "Donor colony stats for a branch. Add details=True for per-colony data",
                "url": "/data/mothercolonies?branch={branchID}&pwd=showmethedata",
                "params": {"branch": "Branch ID", "details": "True (optional)"},
            },
            "reefcam": {
                "description": "Reefcam data endpoints. end is optional; if omitted, one day of data is returned.",
                "common_params": {
                    "org": "Organization slug (e.g. coral-gardeners)",
                    "device": "Device ID (e.g. reefos-01)",
                    "start": "Start date (YYYY-MM-DD)",
                    "end": "End date (YYYY-MM-DD, optional)",
                    "pwd": "API password",
                },
                "urls": {
                    "images": "/data/images",
                    "audio": "/data/audio",
                    "temperature": "/data/measurement/temperature",
                    "bioacoustics": "/data/measurement/bioacoustics",
                    "fishcommunity": "/data/measurement/fishcommunity",
                    "fishdiversity": "/data/measurement/fishdiversity",
                },
            },
            "file": {
                "description": "Download a file from Cloud Storage",
                "url": "/file?branch={branch}&filename={filepath}&pwd=showmethedata",
                "params": {"branch": "Branch name (e.g. moorea)", "filename": "File path in storage"},
            },
            "object": {
                "description": "Fetch a single document by collection name and ID. "
                               "The leading underscore in the collection name is optional.",
                "url": "/data/object?collection={collection}&id={doc_id}&pwd=showmethedata",
                "params": {
                    "collection": "Collection name (e.g. 'fragments' or '_fragments')",
                    "id": "Document ID",
                },
            },
            "fragments": {
                "description": "Query the _fragments collection with optional filtering",
                "url": "/data/fragments",
                "params": {
                    "org": "orgID (optional)",
                    "branch": "branchID (optional)",
                    "nursery": "nurseryID (optional)",
                    "outplant": "outplantID (optional)",
                    "state": "Fragment state or comma-separated list "
                             "(inNursery | outplanted | refragmented | removed | completed | unknown)",
                    "start": "Start date filter on metadata.createdAt (YYYY-MM-DD, optional)",
                    "end": "End date filter on metadata.createdAt (YYYY-MM-DD, optional)",
                    "pwd": "API password",
                },
            },
            "sites": {
                "description": "Query the _sites collection with optional filtering",
                "url": "/data/sites",
                "params": {
                    "org": "orgID (optional)",
                    "branch": "branchID (optional)",
                    "type": "siteType or comma-separated list "
                            "(branch | nursery | structure | restoSite | outplant | "
                            "outplantCell | donorColony | control | surveySite | surveyCell | surveyPlot)",
                    "start": "Start date filter on metadata.createdAt (YYYY-MM-DD, optional)",
                    "end": "End date filter on metadata.createdAt (YYYY-MM-DD, optional)",
                    "pwd": "API password",
                },
            },
            "events": {
                "description": "Query the _events collection with optional filtering",
                "url": "/data/events",
                "params": {
                    "org": "orgID (optional)",
                    "branch": "branchID (optional)",
                    "nursery": "nurseryID (optional)",
                    "outplant": "outplantID (optional)",
                    "restosite": "restositeID (optional)",
                    "type": "eventType or comma-separated list "
                            "(fragmentCreation | fragmentMonitor | fragmentOutplantMonitor | "
                            "fragmentMove | fragmentOutplant | fragmentRefragment | "
                            "outplantCellMonitoring | visualSurvey | coralSurvey | fishSurvey | "
                            "fullNurseryMonitoring | bleachingNurseryMonitoring | "
                            "nurseryCleaning | nurseryPredatorSweep | donorColonyMonitoring)",
                    "start": "Start date filter on metadata.createdAt (YYYY-MM-DD, optional)",
                    "end": "End date filter on metadata.createdAt (YYYY-MM-DD, optional)",
                    "pwd": "API password",
                },
            },
        },
        "database": {
            "description": "Firestore database structure",
            "collections": {
                "_orgs": {
                    "description": "Organizations",
                    "fields": {"name": "string"},
                },
                "_sites": {
                    "description": "All site types (polymorphic). Discriminated by siteType.",
                    "common_fields": {
                        "siteType": "string — branch | nursery | structure | restoSite | outplant | "
                                    "outplantCell | donorColony | control | surveySite | surveyCell | surveyPlot",
                        "name": "string",
                        "location.orgID": "string",
                        "location.branchID": "string",
                        "location.nurseryID": "string (where applicable)",
                        "location.restositeID": "string (where applicable)",
                        "location.outplantID": "string (where applicable)",
                        "geolocation.latitude": "number",
                        "geolocation.longitude": "number",
                        "metadata.createdAt": "datetime",
                        "metadata.deleted": "boolean",
                    },
                    "hierarchy": "org → branch → nursery/restoSite/outplant → structure/outplantCell",
                },
                "_fragments": {
                    "description": "Individual coral fragments tracked through their lifecycle",
                    "fields": {
                        "state": "string — inNursery | outplanted | refragmented | removed | completed | unknown",
                        "organismID": "string — ref to _organisms",
                        "donorID": "string — ref to _sites (donorColony)",
                        "location.orgID": "string",
                        "location.branchID": "string",
                        "location.nurseryID": "string",
                        "location.outplantID": "string",
                        "location.outplantCellID": "string",
                        "createdAt": "datetime",
                        "completedAt": "datetime (optional)",
                        "metadata.deleted": "boolean",
                    },
                },
                "_events": {
                    "description": "All operational events (polymorphic). Discriminated by eventType.",
                    "fields": {
                        "eventType": "string — see event_types below",
                        "location": "object — same structure as _sites location",
                        "eventData": "object — event-specific data",
                        "metadata.createdAt": "datetime",
                        "metadata.deleted": "boolean",
                    },
                    "event_types": {
                        "fragment_lifecycle": [
                            "fragmentCreation", "fragmentMonitor", "fragmentOutplantMonitor",
                            "fragmentMove", "fragmentOutplant", "fragmentRefragment",
                        ],
                        "outplant": ["outplantCellMonitoring"],
                        "survey": ["visualSurvey", "coralSurvey", "fishSurvey"],
                        "nursery": [
                            "fullNurseryMonitoring", "bleachingNurseryMonitoring",
                            "nurseryCleaning", "nurseryPredatorSweep",
                        ],
                        "donor": ["donorColonyMonitoring"],
                    },
                },
                "_organisms": {
                    "description": "Coral species reference data",
                    "fields": {
                        "genus": "string",
                        "species": "string",
                        "organism_type": "string (e.g. coral)",
                    },
                },
                "_statistics": {
                    "description": "Pre-computed aggregated statistics for fast retrieval",
                    "fields": {
                        "statType": "string — see stat_types below",
                        "location": "object — location context for this stat",
                        "data": "object — aggregated data (schema varies by statType)",
                        "metadata.createdAt": "datetime",
                        "metadata.deleted": "boolean",
                    },
                    "stat_types": {
                        "branch": ["branch_stats", "branch_summary"],
                        "restosite": ["restosite_stats"],
                        "nursery": ["nursery_stats", "structure_stats", "spp_nursery_stats",
                                    "fullNurseryMonitoring", "bleachingNurseryMonitoring"],
                        "outplant": ["outplant_stats", "outplantcell_stats", "outplant_species_stats",
                                     "outplant_fragment_donor"],
                        "donor": ["donor_summary_stats", "donor_details_stats",
                                  "donor_site_summary_stats", "donor_nursery_details_stats"],
                        "fragment": ["fragment_stats", "fragment_donor"],
                        "annual": ["by_year"],
                    },
                },
            },
        },
    }
    return json.dumps(help_data)


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
    _stats = qf.get_docs(qf.query_statistics(location, st.fragment_donor.value))
    if len(_stats) > 0:
        stats_df = qf.documents_to_dataframe(_stats, ['data', 'location'])
        donor_stats = qf.get_docs(qf.query_statistics(location, st.donor_nursery_details_stats.value))
        donor_df = qf.documents_to_dataframe(donor_stats, ['data', 'location'])
        _donor_stats_helper(stats_df, donor_df, data)


def get_orgs(qf):
    """Return a list of all organizations, each as a dict with an added 'orgID' key."""
    orgs = qf.get_orgs()
    results = []
    for org in orgs:
        res = org[1]
        res['orgID'] = org[0]
        results.append(res)
    return results


def get_orgbyname(qf, name):
    """Return the first organization matching the given name, or None if not found."""
    res = qf.get_org_by_name(name)
    if len(res) == 0:
        return None
    return res[0]


def org_exists(qf, org_id):
    """Return True if the given org_id exists in Firestore."""
    return org_id in qf.get_org_ids()


def site_exists(qf, site_id):
    """Return True if the given site_id exists in Firestore."""
    return qf.get_site_by_id(site_id) is not None


def global_stats(qf, org_id):
    """Return organization-level summary statistics.

    Args:
        qf: QueryFirestore instance
        org_id: Organization document ID

    Returns:
        dict with keys: Branches, Nurseries, Outplants, Fragments, Fraction Alive,
        Outplanted, Species, Species_Frac, Genera, Genus_Frac, Colonies,
        Mother_Colony_Frac, shannon_diversity_spp, shannon_diversity_mc
    """
    loc = {'orgID': org_id}
    # get stats for branches in this org
    stats = qf.get_docs(qf.query_statistics(loc, st.branch_stats.value))
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
    """Return annual seeding and outplanting counts per branch.

    For the French Polynesia branch, pre-database impact report data (2020–2023)
    is merged in to provide a complete historical record.

    Args:
        qf: QueryFirestore instance
        org_id: Organization document ID

    Returns:
        list of dicts with keys: year, branchID, Branch, seeded, outplanted (cumulative), n_species
    """
    loc = {'orgID': org_id}
    # get the by year stats from FireStore
    stats = qf.get_docs(qf.query_statistics(loc, st.by_year.value))
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


def _restosite_stats_helper(qf, restosite_id, restosite_data, ddf=None, donor_df=None):
    restosite = qf.get_site_by_id(restosite_id)
    branch_id = restosite['location']['branchID']
    drop = ['statType', 'doc_id']
    for attr in drop:
        del restosite_data[attr]
    restosite_data['restositeID'] = restosite_id
    restosite_data['name'] = restosite['name']
    restosite_data["lat"] = restosite['geolocation']['latitude']
    restosite_data["lon"] = restosite['geolocation']['longitude']
    if ddf is None:
        _get_fragment_species_stats(qf, {'branchID': branch_id, 'restositeID': restosite_id}, restosite_data)
    else:
        _donor_stats_helper(ddf, donor_df, restosite_data)
    restosite_data['restositeCreatedAt'] = restosite['metadata']['createdAt']
    restosite_data['age'] = (dt.datetime.now(dt.timezone.utc) - restosite_data['restositeCreatedAt']).days
    return restosite_data


def get_nursery_monitoring_history(qf, nursery_id, agg_type='by_nursery'):
    """Return a DataFrame of monitoring events for a nursery, sorted by date.

    Args:
        qf: QueryFirestore instance
        nursery_id: Nursery document ID
        agg_type: Aggregation level — 'by_nursery' (default) or 'by_L1' (per structure)

    Returns:
        DataFrame with columns including monitoredAt, bleach, EVI, alive, health, statType.
        EVI/alive/health are null for bleachingNurseryMonitoring rows.
    """
    loc = {'nurseryID': nursery_id}
    monitoring_stats = qf.get_docs(qf.query_statistics(loc, [st.full_nursery_monitoring.value, st.bleaching_nursery_monitoring.value],
                                                       filter_fields={'data.agg_type': agg_type}
                                                       ))
    mdf = qf.documents_to_dataframe(monitoring_stats, ['data', 'location'])
    # set non-bleaching attributes in bleaching monitoring to nan as they are incomplete
    if len(mdf) > 0:
        mdf.loc[mdf.statType == st.bleaching_nursery_monitoring.value, ['EVI', 'alive', 'health']] = None  # np.nan
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
    drop = ['statType', 'doc_id']
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
    """Return summary statistics for a single nursery.

    Args:
        qf: QueryFirestore instance
        nursery_id: Nursery document ID
        nursery: Pre-fetched nursery site document (optional, avoids a Firestore read)

    Returns:
        dict with nursery health metrics (alive, bleach, EVI, health), age in days,
        location (lat/lon), species diversity, and recent monitoring trends.
    """
    stats = qf.get_docs(qf.query_statistics({'nurseryID': nursery_id}, st.nursery_stats.value))
    nursery_stats = qf.documents_to_dataframe(stats, ['data', 'location']).iloc[0].to_dict()
    nursery_stats['age'] = (dt.datetime.now(dt.timezone.utc) - nursery_stats['createdAt']).days
    return _nursery_stats_helper(qf, nursery_id, nursery_stats)


def get_nursery_history(qf, nursery_id):
    nursery_df = get_nursery_monitoring_history(qf, nursery_id, agg_type='by_nursery')
    species_df = get_nursery_monitoring_history(qf, nursery_id, agg_type='by_species')
    # trends = get_monitoring_trends(nursery_df)
    return nursery_df, species_df # , trends


def get_outplant_monitoring_history(qf, outplant_id):
    """Return a DataFrame of outplant cell monitoring events, sorted by date.

    Args:
        qf: QueryFirestore instance
        outplant_id: Outplant site document ID

    Returns:
        DataFrame with columns including monitoredAt, outplantCellID, percentBleach,
        percentCoralCover, percentSurvival.
    """
    loc = {'outplantID': outplant_id}
    monitoring_events = qf.get_docs(qf.query_events(loc, {'eventType': et.outplant_cell_monitoring.value}))
    mdf = qf.documents_to_dataframe(monitoring_events, ['eventData', 'location'])
    # set non-bleaching attributes in bleaching monitoring to nan as they are incomplete
    if len(mdf) > 0:
        mdf = mdf.sort_values('monitoredAt')
    return mdf


def _outplant_stats_helper(qf, outplant_id, outplant, op_stats, cdf, sdf, details):
    _outplant = outplant or qf.get_site_by_id(outplant_id)
    # make the block of outplant data
    n_cells = len(cdf)
    del op_stats['doc_id']
    outplant_data = {
        "outplantID": outplant_id,
        "name": _outplant['name'],
        "lat": _outplant['geolocation']['latitude'],
        "lon": _outplant['geolocation']['longitude'],
        }
    outplant_data.update(op_stats)
    outplant_data['species_per_cell'] = float(cdf.n_species.mean()) if n_cells > 0 else 0
    outplant_data['corals_per_cell'] = float(cdf.n_outplanted.mean()) if n_cells > 0 else 0
    if details and n_cells > 0:
        keep = ['n_mothercolonies', 'n_species', 'n_outplanted', 'outplantID', 'outplantCellID']
        outplant_data['cells'] = cdf[keep].to_dict('records')
    if details:
        keep = ['taxon', 'n_mothercolonies', 'n_outplanted', 'outplantID', 'organismID']
        outplant_data['outplant_species'] = sdf[keep].to_dict('records')        
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
    """Return statistics for a single outplant site.

    Args:
        qf: QueryFirestore instance
        outplant_id: Outplant site document ID
        outplant: Pre-fetched outplant site document (optional, avoids a Firestore read)
        details: If True, include per-cell stats ('cells') and species breakdown ('outplant_species')

    Returns:
        dict with outplantID, name, lat, lon, fragment counts, age in days,
        species_per_cell, corals_per_cell, monitoring history, and optionally
        cells and outplant_species lists.
    """
    loc = {'outplantID': outplant_id}
    op_stats = qf.get_docs(qf.query_statistics(loc, st.outplant_stats.value))
    if len(op_stats) > 0:
        if details:
            cell_stats = qf.get_docs(qf.query_statistics(loc, st.outplantcell_stats.value))
            cdf = qf.documents_to_dataframe(cell_stats, ['data', 'location'])
            spp_stats = qf.get_docs(qf.query_statistics(loc, st.outplant_species_stats.value))
            sdf = qf.documents_to_dataframe(spp_stats, ['data', 'location'])
        else:
            cdf = pd.DataFrame()
            sdf = pd.DataFrame()
        op_stats = op_stats[0][1]
    else:
        op_stats = {'data': []}
        cdf = pd.DataFrame()
        sdf = pd.DataFrame()
    return _outplant_stats_helper(qf, outplant_id, outplant, op_stats['data'], cdf, sdf, details)


def control_stats(qf, control_id, control=None):
    """Return statistics for a single control site.

    Args:
        qf: QueryFirestore instance
        control_id: Control site document ID
        control: Pre-fetched control site document (optional)

    Returns:
        dict with controlID, outplantID, reefod_id, name, lat, lon, perimeter, controlCreatedAt
    """
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
    """Return statistics for all control sites matching a location filter.

    Args:
        qf: QueryFirestore instance
        loc: Location filter dict (e.g. {'branchID': '...'})

    Returns:
        list of control site stat dicts (see control_stats)
    """
    results = []
    cids = qf.get_docs(qf.query_controlsites(loc))
    for cid, doc in cids:
        results.append(control_stats(qf, cid, doc))
    return results


def branch_stats(qf, loc):
    """Return summary statistics for branches matching a location filter.

    Args:
        qf: QueryFirestore instance
        loc: Location filter dict (e.g. {'orgID': '...'} or {'branchID': '...'})

    Returns:
        list of branch summary dicts
    """
    # get stats for brach or org
    stats = qf.get_docs(qf.query_statistics(loc, st.branch_summary.value))
    return [stat[1]['data'] for stat in stats]


def restosite_stats(qf, loc):
    """Return statistics for restoration sites matching a location filter.

    Args:
        qf: QueryFirestore instance
        loc: Location filter dict (e.g. {'branchID': '...'})

    Returns:
        list of restoration site stat dicts
    """
    # get stats for brach or org
    stats = qf.get_docs(qf.query_statistics(loc, st.restosite_stats.value))
    return [stat[1]['data'] for stat in stats]


def full_nursery_stats(qf, nursery_id):
    """Return comprehensive statistics for a nursery including history and species breakdown.

    Args:
        qf: QueryFirestore instance
        nursery_id: Nursery document ID

    Returns:
        dict with keys:
            summary: nursery_stats dict
            species_summary: list of per-species stats (EVI, alive, health, taxon, etc.)
            structures: list of per-structure (L1) stats
            history: time-series monitoring dict keyed by monitoredAt timestamp
            spp_history: list of per-species monitoring records over time
    """
    keep = ['EVI_count', 'n_fragments', 'bleach', 'EVI', 'alive', 'health', 'nurseryID']
    history_keep = ['nurseryID', 'bleach', 'EVI', 'alive',
                    'health', 'statType', 'monitoring_name', 'monitoredAt']

    loc = {'nurseryID': nursery_id}
    structure_stats = qf.get_docs(qf.query_statistics(loc, st.structure_stats.value))
    struct_df = qf.documents_to_dataframe(structure_stats, ['data', 'location'])
    spp_nursery_stats = qf.get_docs(qf.query_statistics(loc, st.spp_nursery_stats.value))
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
        'structures': struct_df[keep + ['structureID']].to_dict('records'),
        'history': hdf.to_dict(),
        'spp_history': shdf.to_dict('records')
        }
    return all_stats


def get_donor_stats(qf, branch_id, details=True):
    """Return donor colony statistics for a branch.

    Args:
        qf: QueryFirestore instance
        branch_id: Branch document ID
        details: If True, include per-colony detail records (default True)

    Returns:
        dict with keys:
            summary: list of summary stat dicts (species counts, genus fractions, etc.)
            details: list of per-colony detail dicts
    """
    loc = {'branchID': branch_id}
    summary_stats = qf.get_docs(qf.query_statistics(loc, st.donor_summary_stats.value))
    summary_df = qf.documents_to_dataframe(summary_stats, ['data', 'location'])
    details_stats = qf.get_docs(qf.query_statistics(loc, st.donor_details_stats.value))
    details_df = qf.documents_to_dataframe(details_stats, ['data', 'location'])
    all_stats = {
        'summary': summary_df.to_dict('records'),
        'details': details_df.to_dict('records')
        }
    return all_stats


def nursery_fragment_stats(qf, nursery_id):
    """Return individual fragment statistics for a nursery.

    Args:
        qf: QueryFirestore instance
        nursery_id: Nursery document ID

    Returns:
        list of fragment stat dicts
    """
    frag_stats = qf.get_docs(qf.query_statistics({'nurseryID': nursery_id}, st.fragment_stats.value))
    frags_df = qf.documents_to_dataframe(frag_stats, [])
    return frags_df.to_dict('records')


_VALID_COLLECTIONS = frozenset({'fragments', 'sites', 'events', 'statistics', 'organisms', 'orgs'})


def get_object(qf, collection, doc_id):
    """Fetch a single document from any collection by ID.

    The leading underscore in the collection name is optional:
    'fragments' and '_fragments' are equivalent.

    Args:
        qf: QueryFirestore instance
        collection: Collection name (e.g. 'fragments' or '_fragments')
        doc_id: Document ID

    Returns:
        dict of document fields, or None if not found
    """
    if not collection.startswith('_'):
        collection = '_' + collection
    return qf.get_document(collection, doc_id)


def get_fragments(qf, location, states=None, start_date=None, end_date=None):
    """Query the _fragments collection with optional filters.

    Args:
        qf: QueryFirestore instance
        location: dict of location fields to filter on (e.g. {'orgID': ..., 'branchID': ...})
        states: fragment state string or list of states
                (e.g. 'inNursery' or ['inNursery', 'outplanted'])
        start_date: inclusive lower bound on metadata.createdAt (datetime)
        end_date: exclusive upper bound on metadata.createdAt (datetime)

    Returns:
        list of dicts, each containing all document fields plus 'doc_id'
    """
    query = qf.query_fragments_filtered(location, states=states,
                                        start_date=start_date, end_date=end_date)
    docs = qf.get_docs(query)
    return [{'doc_id': doc_id, **doc} for doc_id, doc in docs]


def get_sites(qf, location, site_types=None, start_date=None, end_date=None):
    """Query the _sites collection with optional filters.

    Args:
        qf: QueryFirestore instance
        location: dict of location fields to filter on (e.g. {'orgID': ..., 'branchID': ...})
        site_types: siteType string or list of siteTypes
                    (e.g. 'nursery' or ['nursery', 'outplant'])
        start_date: inclusive lower bound on metadata.createdAt (datetime)
        end_date: exclusive upper bound on metadata.createdAt (datetime)

    Returns:
        list of dicts, each containing all document fields plus 'doc_id'
    """
    query = qf.query_sites_filtered(location, site_types=site_types,
                                    start_date=start_date, end_date=end_date)
    docs = qf.get_docs(query)
    return [{'doc_id': doc_id, **doc} for doc_id, doc in docs]


def get_events(qf, location, event_types=None, start_date=None, end_date=None):
    """Query the _events collection with optional filters.

    Args:
        qf: QueryFirestore instance
        location: dict of location fields to filter on (e.g. {'orgID': ..., 'branchID': ...})
        event_types: eventType string or list of eventTypes
                     (e.g. 'fragmentMonitor' or ['fragmentMonitor', 'fragmentCreation'])
        start_date: inclusive lower bound on metadata.createdAt (datetime)
        end_date: exclusive upper bound on metadata.createdAt (datetime)

    Returns:
        list of dicts, each containing all document fields plus 'doc_id'
    """
    query = qf.query_events_filtered(location, event_types=event_types,
                                     start_date=start_date, end_date=end_date)
    docs = qf.get_docs(query)
    return [{'doc_id': doc_id, **doc} for doc_id, doc in docs]


def download_firestore_file(qf, org, branch, blobname):
    """Download a file from Cloud Storage to the current directory.

    Args:
        qf: QueryFirestore instance
        org: Organization slug (e.g. 'coral-gardeners')
        branch: Branch name (e.g. 'moorea')
        blobname: Full path of the file in Cloud Storage
    """
    parts = blobname.split('/')
    fname = parts[-1]
    qf.download_blob(org, branch, blobname, fname)
