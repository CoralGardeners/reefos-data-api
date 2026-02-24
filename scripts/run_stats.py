import argparse

import reefos_data_api.query_firestore as qq
import reefos_data_api.compute_statistics as cs

parser = argparse.ArgumentParser(description="Run full statistics computation")
parser.add_argument("--workers", type=int, default=1,
                    help="Branches to process in parallel (default: 1)")
parser.add_argument("--no-save", action="store_true",
                    help="Compute only, don't write to Firestore")
args = parser.parse_args()

new_firestore = 'restoration-ios-firebase-adminsdk-wg0a4-18ff398018.json'
new_project_id = "restoration-ios"

qf = qq.QueryFirestore(project_id=new_project_id, creds=new_firestore)
cs.compute_statistics(qf, save=not args.no_save, limit=None,
                      max_workers=args.workers)

