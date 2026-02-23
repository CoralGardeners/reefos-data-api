"""Fix missing metadata.deleted flag in Firestore documents.

Every document in the database should have a metadata record with
metadata.deleted set to False.  Some documents are missing this flag,
which causes them to be silently excluded from all standard queries
(get_docs, get_docrefs, get_doc_ids all filter on metadata.deleted == False).

This script scans all core collections, finds documents where
metadata.deleted is missing, and patches them using Firestore's
set-with-merge so that only missing fields are added — existing
fields are never overwritten.

Two cases are handled:
  - metadata missing entirely: writes the full metadata block
    {deleted: False, createdAt: None, createdBy: None}
  - metadata exists but deleted flag missing: adds only
    {deleted: False}, leaving createdAt and createdBy untouched

Collections scanned:
    _orgs, _sites, _organisms, _fragments, _events, _statistics, _users

Usage:
    python scripts/fix_metadata.py                  # production, dry-run
    python scripts/fix_metadata.py --save           # production, apply fixes
    python scripts/fix_metadata.py --dev            # dev database, dry-run
    python scripts/fix_metadata.py --dev --save     # dev database, apply fixes
"""

import argparse

from google.api_core import retry as retries
from google.api_core import exceptions as core_exceptions

import reefos_data_api.query_firestore as qq

# All collections that should have metadata.deleted on every document.
COLLECTIONS = [
    '_orgs',
    '_sites',
    '_organisms',
    '_fragments',
    '_events',
    '_statistics',
    '_users',
]

# Retry policy for Firestore reads (same as query_firestore.py).
custom_retry = retries.Retry(
    initial=0.1,
    maximum=10_000.0,
    multiplier=1.3,
    predicate=retries.if_exception_type(
        core_exceptions.DeadlineExceeded,
        core_exceptions.ServiceUnavailable,
    ),
    timeout=10_000.0,
)


def needs_fix(doc_dict):
    """Check whether a document is missing metadata.deleted.

    Returns a string describing the issue, or None if the document is OK.
    Possible issues:
      - 'no metadata'         — the metadata field is missing entirely
      - 'no deleted flag'     — metadata exists but has no 'deleted' key
    """
    metadata = doc_dict.get('metadata')
    if metadata is None:
        return 'no metadata'
    if not isinstance(metadata, dict):
        return 'no metadata'
    if 'deleted' not in metadata:
        return 'no deleted flag'
    return None


def scan_collection(db, collection_name):
    """Scan a single collection and return doc IDs that need fixing.

    Streams ALL documents (no metadata.deleted filter) so we can see
    documents that are currently invisible to normal queries.

    Returns
    -------
    total : int
        Total documents scanned.
    to_fix : list[tuple(str, str)]
        List of (doc_id, issue_description) for documents needing a fix.
    """
    print(f"  Scanning {collection_name}...", end="", flush=True)
    coll_ref = db.collection(collection_name)
    to_fix = []
    total = 0

    for doc in coll_ref.stream(retry=custom_retry):
        total += 1
        doc_dict = doc.to_dict()
        issue = needs_fix(doc_dict)
        if issue is not None:
            to_fix.append((doc.id, issue))

    print(f" {total} docs, {len(to_fix)} need fixing")
    return total, to_fix


def apply_fixes(db, collection_name, to_fix, batchsize=500):
    """Patch documents to add missing metadata fields.

    Uses Firestore set-with-merge so that only the specified metadata
    fields are written.  All other fields on the document are left
    untouched.

    Two cases:
      - 'no metadata': the entire metadata record is missing, so we
        write the full metadata block (deleted, createdAt, createdBy).
      - 'no deleted flag': metadata exists with createdAt/createdBy
        already present, so we only add the deleted flag.
    """
    coll_ref = db.collection(collection_name)
    batch = db.batch()
    ops = 0

    for doc_id, issue in to_fix:
        doc_ref = coll_ref.document(doc_id)
        if issue == 'no metadata':
            # Metadata is missing entirely — write the full block.
            patch = {'metadata': {'deleted': False,
                                  'createdAt': None,
                                  'createdBy': None}}
        else:
            # Metadata exists but is just missing the deleted flag.
            patch = {'metadata': {'deleted': False}}
        # merge=True means: only write the fields we specify, leave
        # everything else on the document as-is.
        batch.set(doc_ref, patch, merge=True)
        ops += 1
        if ops >= batchsize:
            batch.commit()
            batch = db.batch()
            ops = 0

    # Commit any remaining operations.
    if ops > 0:
        batch.commit()

    print(f"    Fixed {len(to_fix)} docs in {collection_name}")


def run(qf, save=False):
    """Scan all collections and optionally fix missing metadata.deleted."""
    db = qf.db

    grand_total = 0
    grand_fixed = 0

    for coll_name in COLLECTIONS:
        total, to_fix = scan_collection(db, coll_name)
        grand_total += total

        if to_fix:
            # Show examples of what's broken.
            for doc_id, issue in to_fix[:5]:
                print(f"    {doc_id}: {issue}")
            if len(to_fix) > 5:
                print(f"    ... and {len(to_fix) - 5} more")

            if save:
                apply_fixes(db, coll_name, to_fix)
            else:
                print(f"    (dry run — use --save to fix)")

            grand_fixed += len(to_fix)

    # Summary
    print(f"\n{'=' * 50}")
    mode = "LIVE" if save else "DRY RUN"
    print(f"Summary ({mode}):")
    print(f"  Total documents scanned: {grand_total}")
    print(f"  Documents {'fixed' if save else 'needing fix'}: {grand_fixed}")
    print(f"  Documents OK: {grand_total - grand_fixed}")
    print(f"{'=' * 50}")


def main():
    parser = argparse.ArgumentParser(
        description="Fix missing metadata.deleted flag in Firestore documents"
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
