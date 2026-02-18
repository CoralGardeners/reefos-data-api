import reefos_data_api.query_firestore as qq
import reefos_data_api.compute_statistics as cs

new_firestore = 'restoration-ios-firebase-adminsdk-wg0a4-18ff398018.json'
new_project_id="restoration-ios"

qf = qq.QueryFirestore(project_id=new_project_id, creds=new_firestore)
cs.compute_statistics(qf, save=True, limit=None)

