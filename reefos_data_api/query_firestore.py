import numpy as np
import pandas as pd

from google.api_core import exceptions as core_exceptions
from google.api_core import retry as retries

from google.cloud.firestore_v1.base_query import FieldFilter
from google.cloud.firestore_v1 import aggregation
import reefos_data_api.firestore_util as fsu

from reefos_data_api.firestore_constants import EventType as et
from reefos_data_api.firestore_constants import SiteType as st
from reefos_data_api.firestore_constants import FragmentState as fs

# from https://github.com/googleapis/python-firestore/issues/939
custom_retry = retries.Retry(
    initial=0.1,  # seconds
    maximum=10_000.0,  # seconds
    multiplier=1.3,
    predicate=retries.if_exception_type(
        core_exceptions.DeadlineExceeded,
        core_exceptions.ServiceUnavailable,
    ),
    timeout=10_000.0,  # seconds
)


# %%
def wtd_mean(df, attr, count_attr):
    if len(df) == 0:
        return 0
    return (df[attr] * df[count_attr]).sum() / df[count_attr].sum()


def shannon(p):
    p = p[p > 0]
    return float((-p * np.log(p)).sum())


class QueryFirestore:

    def __init__(self, project_id="restoration-app---dev-6df41", creds=None):
        self.app, self.db = fsu.init_firestore_db(project_id=project_id,
                                                  db_id='reefapp',
                                                  creds=creds)

    def destroy(self):
        fsu.cleanup_firestore()

    #@staticmethod
    #def _get_doc_info(query):
    #    def get_doc_info(doc):
    #        parts = doc.reference.path.split('/')
    #        return {'doc_type': parts[-2], 'doc_id': parts[-1], 'doc': doc}
    #    return [get_doc_info(doc) for doc in query.stream(retry=custom_retry)]

    @staticmethod
    def download_blob(org, branch, fname, dest_file):
        blob = fsu.get_blob(org, branch, fname)
        blob.download_to_filename(dest_file)

    @staticmethod
    def get_blob_url(org, branch, fname):
        blob = fsu.get_blob(org, branch, fname)
        return blob.public_url

    # make event data into a dataframe ready for aggregation
    @staticmethod
    def documents_to_dataframe(docs, to_explode, keep=None):
        def explode_dict(df, dictname, keep):
            df = pd.concat([df, pd.DataFrame(df[dictname].tolist())], axis=1)
            if not keep:
                df = df.drop(dictname, axis=1)
            return df
        df = pd.DataFrame([doc[1] for doc in docs])
        if len(docs) > 0:
            df['doc_id'] = [doc[0] for doc in docs]
            for attr in to_explode:
                df = explode_dict(df, attr, keep is not None and attr in keep)
            if 'metadata' not in to_explode and 'metadata' in df:
                df['createdAt'] = explode_dict(df[['metadata']], 'metadata', False)['createdAt']
        return df

    @staticmethod
    def get_docs(query, fields=None):
        return [(doc.id, doc.to_dict() if fields is None else {field: doc.get(field) for field in fields})
                for doc in query.where(filter=FieldFilter("metadata.deleted", "==", False)).stream(retry=custom_retry)]

    @staticmethod
    def get_doc_ids(query, fields=None):
        return {doc.id for doc in
                query.where(filter=FieldFilter("metadata.deleted", "==", False)).stream(retry=custom_retry)}

    @staticmethod
    def add_filter(query, filter_fields):
        for field, val in filter_fields.items():
            op = 'in' if type(val) is list else "=="
            query = query.where(filter=FieldFilter(field, op, val))
        return query

    @staticmethod
    def add_location_filter(query, location):
        for field, val in location.items():
            query = query.where(filter=FieldFilter(f"location.{field}", "==", val))
        return query

    @staticmethod
    def add_date_filter(query, start_date=None, end_date=None):
        if start_date is not None:
            query = query.where(filter=FieldFilter("metadata.createdAt", ">=", start_date))
        if end_date is not None:
            query = query.where(filter=FieldFilter("metadata.createdAt", "<", end_date))
        return query

    @staticmethod
    def created_at(doc):
        return doc.get('metadata')['createdAt']

    @staticmethod
    def get_count(query):
        return query.count().get()[0][0].value

    def get_organisms(self, org_type=None):
        coll = self.db.collection("_organisms")
        if org_type is not None:
            coll = coll.where(filter=FieldFilter("organism_type", "==", org_type))
        return self.get_docs(coll)

    def get_organisms_by_id(self, organism_ids):
        coll = self.db.collection("_organisms")
        return [(doc.id, doc.get().to_dict()) for doc in coll.list_documents() if doc.id in organism_ids]

    def get_orgs(self):
        coll = self.db.collection("_orgs")
        return self.get_docs(coll)

    def get_org_ids(self):
        coll = self.db.collection("_orgs")
        return self.get_doc_ids(coll)

    def get_org_by_name(self, name):
        coll = self.db.collection("_orgs").where(filter=FieldFilter("name", "==", name))
        return self.get_docs(coll)

    def get_org_by_id(self, org_id):
        doc = self.db.collection("_orgs").document(org_id)
        return doc.get().to_dict()

    def get_branches(self, org_id):
        coll = (self.db.collection("_sites")
                .where(filter=FieldFilter("siteType", "==", st.branch.value))
                .where(filter=FieldFilter("location.orgID", "==", org_id)))
        return self.get_docs(coll)

    def get_branch_by_name(self, org_id, name):
        query = (self.db.collection("_sites")
                 .where(filter=FieldFilter("siteType", "==", st.branch.value))
                 .where(filter=FieldFilter("location.orgID", "==", org_id))
                 .where(filter=FieldFilter("name", "==", name)))
        return self.get_docs(query)[0]

    def get_site_by_id(self, site_id):
        doc = self.db.collection("_sites").document(site_id)
        return doc.get().to_dict()

    def query_sites(self, location, siteType):
        query = (self.db.collection("_sites")
                 .where(filter=FieldFilter("siteType", "==", siteType)))
        return self.add_location_filter(query, location)

    def query_nurseries(self, location):
        return self.query_sites(location, st.nursery.value)

    def query_donorcolonies(self, location):
        return self.query_sites(location, st.donorcolony.value)

    def query_outplantsites(self, location):
        return self.query_sites(location, st.outplant.value)

    def query_restosites(self, location):
        return self.query_sites(location, st.restosite.value)

    def query_outplantcells(self, location):
        return self.query_sites(location, st.outplantcell.value)

    def query_controlsites(self, location):
        return self.query_sites(location, st.control.value)

    def query_fragments(self, location):
        query = self.db.collection("_fragments")
        return self.add_location_filter(query, location)

    def query_location_fragments_in_nursery(self, loc, filter_fields={}):
        query = (self.db.collection("_fragments")
                 .where(filter=FieldFilter("state", "==", fs.in_nursery.value)))
        query = self.add_filter(query, filter_fields)
        return self.add_location_filter(query, loc)

#    def query_all_outplanted_fragments(self, loc):
#        query = (self.db.collection("_fragments")
#                 .where(filter=FieldFilter("state", "==", fs.outplanted.value)))
#        return self.add_location_filter(query, loc)

#    def query_fragments_in_location(self, loc):
#        query = self.db.collection("_fragments")
#        return self.add_location_filter(query, loc)

    def query_events(self, location, filter_fields={}):
        query = self.db.collection("_events")
        query = self.add_filter(query, filter_fields)
        return self.add_location_filter(query, location)

    def query_statistics(self, location, stat_type=[], filter_fields={}):
        query = self.db.collection("_statistics")
        if len(stat_type) > 0:
            op = 'in' if type(stat_type) is list else "=="
            query = query.where(filter=FieldFilter('stat_type', op, stat_type))
        query = self.add_filter(query, filter_fields)
        return self.add_location_filter(query, location)
        

# %%
if __name__ == "__main__":
    qf = QueryFirestore()

    orgs = qf.get_orgs()
    
    cg = qf.get_org_by_name('Coral Gardeners')
    cg_org_id = cg[0][0]
    
    branches = qf.get_branches(cg_org_id)
    fp_branch = qf.get_branch_by_name(cg_org_id, 'French Polynesia')
    fp_branch_id = fp_branch[0]
    
    loc = {'branchID': fp_branch_id}
    outplants = qf.get_docs(qf.query_outplantsites(loc))
    controls = qf.get_docs(qf.query_controlsites(loc))
    nurseries = qf.get_docs(qf.query_nurseries(loc))

    for (nid, info) in nurseries:
        if info['name'] == 'Tiaia':
            tiaia_id = nid
            break
    
    for (oid, info) in outplants:
        if info['name'] == 'Tiaia Experiment':
            tiaia_expt_id = oid
            break
    
    tiaia_frag_query = qf.query_fragments_in_nursery(tiaia_id)
    
    print("Getting Tiaia fragments")
    tiaia_frags = qf.get_docs(tiaia_frag_query)
    
    print("Getting Tiaia fragment count")
    tiaia_frag_count = tiaia_frag_query.count(alias='frag_count').get()
    print(f"{tiaia_frag_count[0][0].alias}: {tiaia_frag_count[0][0].value}")

    # %%
    def get_outplant_monitoring_history(qf, outplant_id):
        keep = ['eventType', 'logID', 'cellMonitoring',
               'branchID', 'orgID', 'outplantID', 'outplantCellID', 'createdAt']
        loc = {'outplantID': outplant_id}
        monitoring_events = qf.get_docs(qf.query_events(loc, {'eventType': et.outplant_cell_monitoring.value}))
    
        mdf = qf.documents_to_dataframe(monitoring_events, ['eventData', 'location', 'metadata'])
        mdf = mdf.dropna(subset='cellMonitoring')
        if len(mdf) > 0:
            mdf = mdf[keep].sort_values('createdAt')
        return mdf
    
    get_outplant_monitoring_history(qf, tiaia_expt_id)

    # %%
    # fragment current state
    # get monitoring events in
    # get most recent bleaching measurement
    
    
    def get_monitoring_data(location, monitoring_type):
        print(f"Getting {monitoring_type} events")
        events = qf.get_docs(qf.query_events(location, {'eventType': monitoring_type}), fields=['name'])
        # get aggregate restuls for each monitoring event
        results = []
        for event in events:
            print(f"Getting events of monitoring {event[1]['name']}")
            q = qf.query_events({'nurseryID': tiaia_id}, {'eventType': et.frag_monitor.value,
                                                          'eventData.logID': event[0]})
            print("Getting mean bleach")
            agg = aggregation.AggregationQuery(q).avg("eventData.bleach")
            mean_bleach = agg.get()[0][0].value
            if monitoring_type == 'full_nursery_monitoring':
                print("Getting mean health")
                agg = aggregation.AggregationQuery(q).avg("eventData.health")
                mean_health = agg.get()[0][0].value
            else:
                mean_health = None
            print("Get monitoring time")
            mt = qf.created_at(q.order_by("metadata.createdAt").limit_to_last(1).get()[0])
            # print("Get count")
            # cnt = q.count().get()[0][0].value
            results.append({
                'name': event[1]['name'],
                'time': mt,
                'bleach': mean_bleach,
                'health': mean_health,
                # 'count': cnt
                })
        return results
    
    
    tiaia_loc = {'nurseryID': tiaia_id}
    # full_results = get_monitoring_data(tiaia_loc, "full_nursery_monitoring")
    all_results = qf.get_monitoring_data(tiaia_loc, ["bleaching_nursery_monitoring", "full_nursery_monitoring"])
