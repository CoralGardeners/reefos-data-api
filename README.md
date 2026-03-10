# reefos-data-api

Python package for querying and analyzing coral restoration data stored in Google Cloud Firestore.
Install locally to access data from python scripts. Package is also used by the REST API for data access

## Installation

```bash
pip  install -e .
```

## Quick Start

```python
from reefos_data_api.query_firestore import QueryFirestore
import reefos_data_api.endpoints as ep

# Connect to Firestore (use your project ID)
qf = QueryFirestore(project_id="your-project-id")

# Get all organizations
orgs = ep.get_orgs(qf)

# Get global stats for an org
stats = ep.global_stats(qf, org_id="your-org-id")

# Clean up
qf.destroy()
```

## Database Structure

The database is hosted on Google Cloud Firestore. All main collections use underscore-prefixed names.

### Site Hierarchy

Sites are organized in a tree structure stored in a single `_sites` collection, distinguished by the `siteType` field:

```
Organization (_orgs)
└── Branch (_sites, siteType: branch)
    ├── Nursery (_sites, siteType: nursery)
    │   └── Structure (_sites, siteType: structure)   ← L1 aggregation unit
    ├── Restoration Site (_sites, siteType: restoSite)
    ├── Outplant Site (_sites, siteType: outplant)
    │   └── Outplant Cell (_sites, siteType: outplantCell)
    ├── Donor Colony (_sites, siteType: donorColony)
    ├── Control Site (_sites, siteType: control)
    └── Survey Site (_sites, siteType: surveySite)
        ├── Survey Cell (_sites, siteType: surveyCell)
        └── Survey Plot (_sites, siteType: surveyPlot)
```

---

### `_orgs` — Organizations

| Field  | Type   | Description       |
|--------|--------|-------------------|
| `name` | string | Organization name |

---

### `_sites` — All Site Types

All sites share a common structure. The `siteType` field determines the type.

**Common fields:**

| Field                    | Type     | Description                               |
|--------------------------|----------|-------------------------------------------|
| `siteType`               | string   | Site type (see values below)              |
| `name`                   | string   | Human-readable name                       |
| `location.orgID`         | string   | Parent organization ID                    |
| `location.branchID`      | string   | Parent branch ID                          |
| `location.nurseryID`     | string   | Parent nursery ID (where applicable)      |
| `location.restositeID`   | string   | Parent restoration site ID (where applicable) |
| `location.outplantID`    | string   | Parent outplant site ID (where applicable)|
| `geolocation.latitude`   | number   | GPS latitude                              |
| `geolocation.longitude`  | number   | GPS longitude                             |
| `metadata.createdAt`     | datetime | Creation timestamp                        |
| `metadata.deleted`       | boolean  | Soft delete flag                          |

**`siteType` values:**

| Value         | Description                                      |
|---------------|--------------------------------------------------|
| `branch`      | Branch-level geographic division                 |
| `nursery`     | Coral nursery operation                          |
| `structure`   | Structure within a nursery (L1 aggregation unit) |
| `restoSite`   | Restoration site grouping outplant sites         |
| `outplant`    | Outplant/restoration deployment site             |
| `outplantCell`| Cell within an outplant site                     |
| `donorColony` | Donor colony (source of coral fragments)         |
| `control`     | Control/reference monitoring site                |
| `surveySite`  | Survey location                                  |
| `surveyCell`  | Survey cell                                      |
| `surveyPlot`  | Survey plot                                      |

---

### `_fragments` — Coral Fragments

Individual coral fragments tracked through their full lifecycle.

| Field                    | Type     | Description                                  |
|--------------------------|----------|----------------------------------------------|
| `location.orgID`         | string   | Organization ID                              |
| `location.branchID`      | string   | Branch ID                                    |
| `location.nurseryID`     | string   | Nursery ID                                   |
| `location.restositeID`   | string   | Restoration site ID                          |
| `location.outplantID`    | string   | Outplant site ID                             |
| `location.outplantCellID`| string   | Outplant cell ID                             |
| `state`                  | string   | Current fragment state (see values below)    |
| `organismID`             | string   | Reference to coral species in `_organisms`   |
| `donorID`                | string   | Reference to donor colony in `_sites`        |
| `createdAt`              | datetime | Fragment creation timestamp                  |
| `completedAt`            | datetime | Completion timestamp (optional)              |
| `metadata.createdAt`     | datetime | Record creation timestamp                    |
| `metadata.deleted`       | boolean  | Soft delete flag                             |

**`state` values:**

| Value          | Description                                          |
|----------------|------------------------------------------------------|
| `inNursery`    | Fragment currently in nursery                        |
| `outplanted`   | Fragment outplanted to reef                          |
| `refragmented` | Fragment was split into more fragments               |
| `removed`      | Fragment removed from nursery                        |
| `completed`    | Fragment marked complete (e.g. retired nursery outplant) |
| `unknown`      | State unknown                                        |

---

### `_events` — Activity Events

All operational events use a single polymorphic collection keyed by `eventType`.

| Field              | Type     | Description                                   |
|--------------------|----------|-----------------------------------------------|
| `eventType`        | string   | Event type (see values below)                 |
| `location`         | object   | Location identifiers (same structure as `_sites`) |
| `eventData`        | object   | Event-specific data fields                    |
| `metadata.createdAt` | datetime | Event creation timestamp                    |
| `metadata.deleted` | boolean  | Soft delete flag                              |

**`eventType` values:**

| Value                      | Category          | Description                          |
|----------------------------|-------------------|--------------------------------------|
| `fragmentCreation`         | Fragment lifecycle | New fragment created                |
| `fragmentMonitor`          | Fragment lifecycle | Fragment health check in nursery    |
| `fragmentOutplantMonitor`  | Fragment lifecycle | Fragment health check post-outplant |
| `fragmentMove`             | Fragment lifecycle | Fragment moved between locations    |
| `fragmentOutplant`         | Fragment lifecycle | Fragment outplanted to reef         |
| `fragmentRefragment`       | Fragment lifecycle | Fragment split into more fragments  |
| `outplantCellMonitoring`   | Outplant          | Outplant cell monitoring event      |
| `visualSurvey`             | Survey            | Visual reef survey                  |
| `coralSurvey`              | Survey            | Detailed coral survey               |
| `fishSurvey`               | Survey            | Fish community survey               |
| `fullNurseryMonitoring`    | Nursery           | Full nursery health monitoring      |
| `bleachingNurseryMonitoring` | Nursery         | Bleaching-focused monitoring        |
| `nurseryCleaning`          | Nursery           | Nursery cleaning event              |
| `nurseryPredatorSweep`     | Nursery           | Predator removal sweep              |
| `donorColonyMonitoring`    | Donor colony      | Donor colony health check           |

---

### `_organisms` — Coral Species

| Field           | Type   | Description               |
|-----------------|--------|---------------------------|
| `genus`         | string | Coral genus               |
| `species`       | string | Coral species             |
| `organism_type` | string | Type (e.g. `coral`)      |

---

### `_statistics` — Pre-computed Statistics

Aggregated statistics are pre-computed and stored for fast retrieval.

| Field              | Type     | Description                             |
|--------------------|----------|-----------------------------------------|
| `statType`         | string   | Statistic type (see values below)       |
| `location`         | object   | Location context for this stat          |
| `data`             | object   | Aggregated data (schema varies by type) |
| `metadata.createdAt` | datetime | Computation timestamp                 |
| `metadata.deleted` | boolean  | Soft delete flag                        |

**`statType` values:**

| Value                        | Description                                          |
|------------------------------|------------------------------------------------------|
| `branch_stats`               | Aggregated fragment counts and survival rates per branch |
| `branch_summary`             | Summary stats for a branch                           |
| `restosite_stats`            | Aggregated stats per restoration site                |
| `nursery_stats`              | Nursery-level health and fragment statistics          |
| `structure_stats`            | Stats aggregated by nursery structure (L1)           |
| `spp_nursery_stats`          | Species distribution within a nursery                |
| `fragment_stats`             | Individual fragment statistics                       |
| `outplant_stats`             | Outplant site aggregated statistics                  |
| `outplantcell_stats`         | Per-cell statistics within an outplant               |
| `outplant_species_stats`     | Species breakdown within an outplant                 |
| `donor_summary_stats`        | Donor colony summary by branch                       |
| `donor_details_stats`        | Per-colony donor statistics                          |
| `donor_site_summary_stats`   | Donor stats grouped by site                          |
| `donor_nursery_details_stats`| Donor stats broken down per nursery                  |
| `by_year`                    | Annual seeding and outplanting counts by branch      |
| `fragment_donor`             | Fragment-to-donor colony mapping                     |
| `outplant_fragment_donor`    | Outplanted fragment donor breakdown                  |
| `fullNurseryMonitoring`      | Time-series full nursery monitoring data             |
| `bleachingNurseryMonitoring` | Time-series bleaching monitoring data                |

---

## Python API Reference

All functions take a `QueryFirestore` instance (`qf`) as their first argument.

### Organizations

| Function | Parameters | Returns |
|----------|------------|---------|
| `get_orgs(qf)` | — | List of all organization dicts |
| `get_orgbyname(qf, name)` | `name`: org name string | Org dict or `None` |
| `org_exists(qf, org_id)` | `org_id` | `bool` |

### Global & Annual

| Function | Parameters | Returns |
|----------|------------|---------|
| `global_stats(qf, org_id)` | `org_id` | Dict: `Branches`, `Nurseries`, `Outplants`, `Fragments`, `Fraction Alive`, `Outplanted`, species diversity |
| `by_year_stats(qf, org_id)` | `org_id` | List of dicts with yearly seeding/outplanting counts per branch |

### Branches

| Function | Parameters | Returns |
|----------|------------|---------|
| `branch_stats(qf, loc)` | `loc`: location filter dict (e.g. `{'orgID': ...}`) | List of branch summary dicts |

### Restoration Sites

| Function | Parameters | Returns |
|----------|------------|---------|
| `restosite_stats(qf, loc)` | `loc`: location filter dict | List of restoration site stat dicts |

### Nurseries

| Function | Parameters | Returns |
|----------|------------|---------|
| `nursery_stats(qf, nursery_id)` | `nursery_id` | Dict with health metrics, age, monitoring trends |
| `full_nursery_stats(qf, nursery_id)` | `nursery_id` | Dict with `summary`, `species_summary`, `structures`, `history`, `spp_history` |
| `nursery_fragment_stats(qf, nursery_id)` | `nursery_id` | List of fragment stat dicts |
| `get_nursery_monitoring_history(qf, nursery_id, agg_type)` | `agg_type`: `'by_nursery'` (default) or `'by_L1'` | DataFrame of monitoring events |

### Outplants

| Function | Parameters | Returns |
|----------|------------|---------|
| `outplant_stats(qf, outplant_id, details=False)` | `details=True` adds cell and species breakdown | Dict with location, counts, monitoring history |
| `get_outplant_monitoring_history(qf, outplant_id)` | `outplant_id` | DataFrame of monitoring events |

### Donor Colonies

| Function | Parameters | Returns |
|----------|------------|---------|
| `get_donor_stats(qf, branch_id, details=True)` | `details=True` for per-colony data | Dict with `summary` and `details` lists |

### Control Sites

| Function | Parameters | Returns |
|----------|------------|---------|
| `control_stats(qf, control_id)` | `control_id` | Dict with location and perimeter |
| `all_control_stats(qf, loc)` | `loc`: location filter dict | List of control site dicts |

### Raw Collection Queries

These functions query the raw Firestore collections directly, bypassing pre-computed statistics. All filters are optional; combine them freely.

| Function | Parameters | Returns |
|----------|------------|---------|
| `get_object(qf, collection, doc_id)` | `collection`: name with or without `_` prefix | Document dict or `None` |
| `get_fragments(qf, location, states, start_date, end_date)` | `location`: dict of location fields; `states`: string or list; dates: UTC datetime | List of fragment dicts with `doc_id` |
| `get_sites(qf, location, site_types, start_date, end_date)` | `site_types`: string or list of `siteType` values | List of site dicts with `doc_id` |
| `get_events(qf, location, event_types, start_date, end_date)` | `event_types`: string or list of `eventType` values | List of event dicts with `doc_id` |

### Files

| Function | Parameters | Returns |
|----------|------------|---------|
| `download_firestore_file(qf, org, branch, blobname)` | `blobname`: Cloud Storage path | Downloads file to current directory |

---

## REST API Endpoints

The REST API is served at `dataapi.coralgardeners.org`. All endpoints require `?pwd=showmethedata`.

> **This section is auto-generated.** Run `python scripts/update_readme.py` after modifying endpoints in `endpoints.py`.

<!-- BEGIN_REST_API -->
Base URL: `dataapi.coralgardeners.org`
Auth: All endpoints require ?pwd=showmethedata

| Endpoint | URL | Description | Parameters |
|----------|-----|-------------|------------|
| `orgs` | `/data/orgs?pwd=showmethedata` | List all organizations | — |
| `global` | `/data/global?org={orgID}&pwd=showmethedata` | Global overview for an organization | `org`: Organization ID |
| `byyear` | `/data/byyear?org={orgID}&pwd=showmethedata` | Annual seeding and outplanting stats by branch | `org`: Organization ID |
| `branches` | `/data/branches?org={orgID}&pwd=showmethedata` | Summary of all branches in an organization | `org`: Organization ID |
| `branch` | `/data/branch?branch={branchID}&pwd=showmethedata` | Overview of a single branch | `branch`: Branch ID |
| `restosites` | `/data/restosites?branch={branchID}&pwd=showmethedata` | All restoration sites in a branch | `branch`: Branch ID |
| `restosite` | `/data/restosite?restosite={restositeID}&pwd=showmethedata` | Overview of a single restoration site | `restosite`: Restoration site ID |
| `nursery` | `/data/nursery?nursery={nurseryID}&pwd=showmethedata` | Nursery data. Add details=True for monitoring history | `nursery`: Nursery ID<br>`details`: True (optional) |
| `outplant` | `/data/outplant?outplant={outplantID}&pwd=showmethedata` | Outplant site data. Add details=True for cell and species breakdown | `outplant`: Outplant site ID<br>`details`: True (optional) |
| `mothercolonies` | `/data/mothercolonies?branch={branchID}&pwd=showmethedata` | Donor colony stats for a branch. Add details=True for per-colony data | `branch`: Branch ID<br>`details`: True (optional) |
| `reefcam` | `/data/images`, `/data/audio`, `/data/measurement/temperature`, `/data/measurement/bioacoustics`, `/data/measurement/fishcommunity`, `/data/measurement/fishdiversity` | Reefcam data endpoints. end is optional; if omitted, one day of data is returned. | `org`: Organization slug (e.g. coral-gardeners)<br>`device`: Device ID (e.g. reefos-01)<br>`start`: Start date (YYYY-MM-DD)<br>`end`: End date (YYYY-MM-DD, optional) |
| `file` | `/file?branch={branch}&filename={filepath}&pwd=showmethedata` | Download a file from Cloud Storage | `branch`: Branch name (e.g. moorea)<br>`filename`: File path in storage |
| `object` | `/data/object?collection={collection}&id={doc_id}&pwd=showmethedata` | Fetch a single document by collection name and ID. The leading underscore in the collection name is optional. | `collection`: Collection name (e.g. 'fragments' or '_fragments')<br>`id`: Document ID |
| `fragments` | `/data/fragments` | Query the _fragments collection with optional filtering | `org`: orgID (optional)<br>`branch`: branchID (optional)<br>`nursery`: nurseryID (optional)<br>`outplant`: outplantID (optional)<br>`state`: Fragment state or comma-separated list (inNursery \| outplanted \| refragmented \| removed \| completed \| unknown)<br>`start`: Start date filter on metadata.createdAt (YYYY-MM-DD, optional)<br>`end`: End date filter on metadata.createdAt (YYYY-MM-DD, optional) |
| `sites` | `/data/sites` | Query the _sites collection with optional filtering | `org`: orgID (optional)<br>`branch`: branchID (optional)<br>`type`: siteType or comma-separated list (branch \| nursery \| structure \| restoSite \| outplant \| outplantCell \| donorColony \| control \| surveySite \| surveyCell \| surveyPlot)<br>`start`: Start date filter on metadata.createdAt (YYYY-MM-DD, optional)<br>`end`: End date filter on metadata.createdAt (YYYY-MM-DD, optional) |
| `events` | `/data/events` | Query the _events collection with optional filtering | `org`: orgID (optional)<br>`branch`: branchID (optional)<br>`nursery`: nurseryID (optional)<br>`outplant`: outplantID (optional)<br>`restosite`: restositeID (optional)<br>`type`: eventType or comma-separated list (fragmentCreation \| fragmentMonitor \| fragmentOutplantMonitor \| fragmentMove \| fragmentOutplant \| fragmentRefragment \| outplantCellMonitoring \| visualSurvey \| coralSurvey \| fishSurvey \| fullNurseryMonitoring \| bleachingNurseryMonitoring \| nurseryCleaning \| nurseryPredatorSweep \| donorColonyMonitoring)<br>`start`: Start date filter on metadata.createdAt (YYYY-MM-DD, optional)<br>`end`: End date filter on metadata.createdAt (YYYY-MM-DD, optional) |
<!-- END_REST_API -->
