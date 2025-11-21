# load current firestore
# run with cwd set to CG/ReefAppAnalytics

import pandas as pd
import reefos_analysis.dbutils.firestore_util as fsu
from google.api_core.retry import Retry

def _get_current_database_docs(app, db, ignore_org='xperience'):
    subcollections = {"Org.Branches", "Org.Branches.ControlSites", "Org.Branches.Fish", "Org.Branches.Fragments",
                      "Org.Branches.MonitoringLogs", "Org.Branches.MotherColonies",
                      "Org.Branches.MotherColonyMonitoringLogs", "Org.Branches.Nurseries",
                      "Org.Branches.Nurseries.Structures", "Org.Branches.OutplantCellMonitoringLogs",
                      "Org.Branches.OutplantSites", "Org.Branches.OutplantSites",
                      "Org.Branches.OutplantSites.OutplantCells", "Org.Branches.SurveySites",
                      "Org.Branches.SurveySites.SurveyCells", "Org.Corals"}

    def get_collection_group(group_id):
        return db.collection_group(group_id).stream(retry=Retry())

    def get_doc_info(doc):
        parts = doc.reference.path.split('/')
        return {'doc_type': parts[-2], 'doc_id': parts[-1], 'doc': doc}

    # get the root collections
    def get_collection_docs(coll, all_docs):
        print(f"Getting docs in collection {coll.id}")
        docs = coll.stream(retry=Retry())
        for doc in docs:
            parts = doc.reference.path.split('/')
            if not (parts[0] == 'Orgs' and parts[1] == ignore_org):
                all_docs.append(get_doc_info(doc))
            else:
                print(f"Ignoring {parts}")

    # get the subcollection documents
    def get_subcollection_docs(sub_coll, all_docs):
        coll_id = sub_coll.split('.')[-1]
        docs = get_collection_group(coll_id)
        for idx, doc in enumerate(docs):
            if idx % 5000 == 0:
                print(f"Getting docs in subcollections {sub_coll} {idx}")
            parts = doc.reference.path.split('/')
            if not (parts[0] == 'Orgs' and parts[1] == ignore_org):
                all_docs.append(get_doc_info(doc))
            else:
                print(f"Ignoring {parts}")

    all_documents = []

    for coll in db.collections():
        get_collection_docs(coll, all_documents)

    for sub_coll in subcollections:
        get_subcollection_docs(sub_coll, all_documents)

    docs_df = pd.DataFrame(all_documents)
    return docs_df


def get_current_firestore(creds=None):
    app, db = fsu.init_firebase_db(cred_file=creds)
    docs_df = _get_current_database_docs(app, db)
    fsu.cleanup_firestore()
    return docs_df


def delete_collection(db, coll_name, limit=5000):
    doc_counter = 0
    commit_counter = 0
    batch_size = 500
    while True:
        docs = []
        print('Getting docs to delete')
        docs = [snapshot for snapshot in db.collection(coll_name)
                .limit(limit).stream(retry=Retry())]
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
