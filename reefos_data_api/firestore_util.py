import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from firebase_admin import storage


def init_firestore_db(project_id="restoration-app---dev-6df41", db_id=None, creds=None):
    try:
        app = firebase_admin.get_app()
    except ValueError:
        # Initialize the Firebase Admin SDK
        if creds is not None:
            cred = credentials.Certificate(creds)
        else:
            cred = credentials.ApplicationDefault()
        print("Initializing Firestore")
        app = firebase_admin.initialize_app(cred, {'projectId': project_id})
    db = firestore.client(database_id=db_id)
    return app, db


def cleanup_firestore():
    try:
        app = firebase_admin.get_app()
    except ValueError:
        app = None
    # cleanup - remove the app
    if app is not None:
        print("Cleanup Firestore")
        firebase_admin.delete_app(app)
        app = None


def get_bucket(bucket_name):
    return storage.bucket(bucket_name)

def get_blob(org, branch, fname, bucket_name):
    if fname[0] == '/':
        fname = fname[1:]
    source_blob = f"{org}/{branch}/{fname}"
    bucket = get_bucket(bucket_name)
    return bucket.blob(source_blob)


if __name__ == '__main__':
    creds = '../restoration-ios-firebase-adminsdk-wg0a4-a59664d92f.json'
    proj = "restoration-ios"
    devcreds = "../restoration-app---dev-6df41-firebase-adminsdk-fbsvc-37ee88f0d4.json"
    devproj = "restoration-app---dev-6df41"

    current_app, current_db = init_firestore_db(project_id=proj,
                                                creds=creds,
                                                db_id='reefapp',
                                                )
    coll = current_db.collection('_orgs')
    collection = coll.get()
