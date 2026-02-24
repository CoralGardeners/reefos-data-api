"""Fix frag_monitor events with null location.restositeID.

Some fragmentMonitor events have location.restositeID set to null.
The correct value can be recovered from the nursery site document:
each nursery (_sites doc with siteType='nursery') stores its
restositeID in its own location dict.

Algorithm:
  1. Iterate over all branches.
  2. Load all nurseries for the branch and build a nurseryID -> restositeID
     lookup from their location records.
  3. Load all frag_monitor events for the branch.
  4. For each event whose location.restositeID is null, look up the
     restositeID from the event's location.nurseryID via the nursery lookup.
  5. Collect the fixes and (if --save) apply them in batches.

Usage:
    python scripts/fix_event_restositeID.py                # production, dry-run
    python scripts/fix_event_restositeID.py --save          # production, apply fixes
    python scripts/fix_event_restositeID.py --dev           # dev database, dry-run
    python scripts/fix_event_restositeID.py --dev --save    # dev database, apply fixes
"""

import argparse

import reefos_data_api.query_firestore as qq
from reefos_data_api.firestore_constants import EventType as et


def loc_field(doc, field):
    """Safely get a location sub-field, returning None if absent."""
    loc = doc.get('location') or {}
    return loc.get(field)


def build_nursery_restosite_map(nurseries):
    """Build a dict mapping nurseryID -> restositeID from nursery site docs."""
    mapping = {}
    for nid, doc in nurseries:
        rsid = loc_field(doc, 'restositeID')
        if rsid:
            mapping[nid] = rsid
    return mapping


def find_events_to_fix(qf, branch_id, nursery_map):
    """Find frag_monitor events with null restositeID that can be fixed.

    Returns
    -------
    to_fix : list[tuple(str, str)]
        (event_doc_id, correct_restositeID) pairs.
    skipped_no_nursery : int
        Events skipped because nurseryID was also null.
    skipped_nursery_unknown : int
        Events skipped because the nurseryID wasn't in the nursery map.
    """
    loc = {'branchID': branch_id}
    events = qf.get_docs(
        qf.query_events(loc, event_type=et.frag_monitor.value)
    )

    to_fix = []
    skipped_no_nursery = 0
    skipped_nursery_unknown = 0

    for eid, doc in events:
        rsid = loc_field(doc, 'restositeID')
        if rsid is not None:
            continue  # already has a restositeID — nothing to fix

        nid = loc_field(doc, 'nurseryID')
        if not nid:
            skipped_no_nursery += 1
            continue

        correct_rsid = nursery_map.get(nid)
        if not correct_rsid:
            skipped_nursery_unknown += 1
            continue

        to_fix.append((eid, correct_rsid))

    return to_fix, skipped_no_nursery, skipped_nursery_unknown


def apply_fixes(db, to_fix, batchsize=500):
    """Write the corrected restositeID to each event using merge writes."""
    batch = db.batch()
    ops = 0
    coll_ref = db.collection('_events')

    for eid, correct_rsid in to_fix:
        doc_ref = coll_ref.document(eid)
        # merge=True ensures we only update location.restositeID and
        # leave every other field on the document untouched.
        batch.set(doc_ref, {'location': {'restositeID': correct_rsid}}, merge=True)
        ops += 1
        if ops >= batchsize:
            batch.commit()
            batch = db.batch()
            ops = 0

    if ops > 0:
        batch.commit()


def run(qf, save=False):
    """Scan all branches and fix frag_monitor events with null restositeID."""
    orgs = qf.get_orgs()
    grand_fixed = 0
    grand_skipped_no_nursery = 0
    grand_skipped_unknown = 0

    for org_id, org_doc in orgs:
        org_name = org_doc.get('name', org_id)
        branches = qf.get_branches(org_id)
        print(f"\nOrg: {org_name} ({len(branches)} branches)")

        for branch_id, branch_doc in branches:
            branch_name = branch_doc.get('name', branch_id)
            loc = {'branchID': branch_id}

            # Build nurseryID -> restositeID lookup for this branch.
            nurseries = qf.get_docs(qf.query_nurseries(loc))
            nursery_map = build_nursery_restosite_map(nurseries)

            # Find broken events.
            to_fix, skip_no_n, skip_unk = find_events_to_fix(
                qf, branch_id, nursery_map
            )

            if to_fix or skip_no_n or skip_unk:
                print(f"\n  Branch: {branch_name}")
                print(f"    {len(nursery_map)} nurseries with restositeID")
                print(f"    {len(to_fix)} events to fix")
                if skip_no_n:
                    print(f"    {skip_no_n} skipped (event nurseryID is also null)")
                if skip_unk:
                    print(f"    {skip_unk} skipped (nurseryID not found in nursery map)")

                # Show a few examples.
                for eid, rsid in to_fix[:5]:
                    print(f"      {eid}  -> restositeID={rsid}")
                if len(to_fix) > 5:
                    print(f"      ... and {len(to_fix) - 5} more")

                if save and to_fix:
                    apply_fixes(qf.db, to_fix)
                    print(f"    Applied {len(to_fix)} fixes")
            else:
                print(f"  Branch: {branch_name} — no issues")

            grand_fixed += len(to_fix)
            grand_skipped_no_nursery += skip_no_n
            grand_skipped_unknown += skip_unk

    # Summary
    print(f"\n{'=' * 60}")
    mode = "LIVE" if save else "DRY RUN"
    print(f"Summary ({mode}):")
    print(f"  Events {'fixed' if save else 'to fix'}:  {grand_fixed}")
    print(f"  Skipped (no nurseryID):        {grand_skipped_no_nursery}")
    print(f"  Skipped (nursery not in map):  {grand_skipped_unknown}")
    print(f"{'=' * 60}")


def main():
    parser = argparse.ArgumentParser(
        description="Fix frag_monitor events with null location.restositeID"
    )
    parser.add_argument("--dev", action="store_true",
                        help="Use the dev database (default: production)")
    parser.add_argument("--save", action="store_true",
                        help="Apply fixes to Firestore (default: dry-run)")
    args = parser.parse_args()

    if args.dev:
        creds = "restoration-app---dev-6df41-firebase-adminsdk-fbsvc-37ee88f0d4.json"
        project_id = "restoration-app---dev-6df41"
        print("Connecting to DEV database")
    else:
        creds = 'restoration-ios-firebase-adminsdk-wg0a4-18ff398018.json'
        project_id = "restoration-ios"
        print("Connecting to PRODUCTION database")

    if not args.save:
        print("DRY RUN — no changes will be written (use --save to apply)\n")
    else:
        print("LIVE MODE — fixes will be written to Firestore\n")

    qf = qq.QueryFirestore(project_id=project_id, creds=creds)
    try:
        run(qf, save=args.save)
    finally:
        qf.destroy()


if __name__ == "__main__":
    main()
