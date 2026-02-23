"""Incremental statistics update for the ReefOS Firestore database.

Computes the same statistics as compute_statistics.py but only writes
documents that have actually changed, avoiding the costly delete-all /
write-all cycle. This dramatically reduces Firestore writes when only
a few nurseries or outplants have new data.

How it works:
  1. Compute all stats for every org/branch (same logic as compute_statistics).
  2. For each branch + statType, load existing stats from _statistics.
  3. Diff new vs existing stats using a canonical key (statType + location).
     - Unchanged docs are skipped (no write).
     - Changed docs are updated in place.
     - New docs are added.
     - Docs that exist in Firestore but not in the new set are deleted.
  4. Report per-branch counts of added / updated / deleted / unchanged.

Where the savings come from:
  - fragment_stats: thousands of docs, but only a handful of fragments
    change daily, so the vast majority are skipped.
  - donor_* stats: change infrequently, almost always skipped.
  - monitoring time-series: only change when new monitoring events are added.
  - nursery_stats, restosite_stats, branch_stats: small count, always cheap.

Usage:
    python scripts/update_stats.py                  # production, dry-run
    python scripts/update_stats.py --save           # production, write changes
    python scripts/update_stats.py --dev            # dev database, dry-run
    python scripts/update_stats.py --dev --save     # dev database, write changes
"""

import argparse
import datetime as dt
import math

import reefos_data_api.compute_statistics as cs
import reefos_data_api.query_firestore as qq


# ---------------------------------------------------------------------------
# Canonical key generation
# ---------------------------------------------------------------------------

def _normalize_value(v):
    """Normalize a value for comparison.

    Rounds floats to 6 decimal places to avoid false positives from
    floating-point noise.  Converts datetimes to ISO strings so they
    are JSON-serializable and comparable.
    """
    if isinstance(v, float):
        if math.isnan(v):
            return None
        return round(v, 6)
    if isinstance(v, dt.datetime):
        return v.isoformat()
    if isinstance(v, dict):
        return _normalize_dict(v)
    if isinstance(v, list):
        return [_normalize_value(item) for item in v]
    return v


def _normalize_dict(d):
    """Recursively normalize all values in a dict."""
    return {k: _normalize_value(v) for k, v in d.items()}


def _canonical_key(stat):
    """Build a hashable key from statType + location fields.

    This uniquely identifies a statistics document so we can match
    newly computed stats against existing ones in Firestore.  The key
    is a tuple of (statType, sorted-location-items) which is hashable
    and order-independent.
    """
    stat_type = stat.get('statType', '')
    location = stat.get('location', {})
    # Sort location items so the key is deterministic regardless of
    # dict ordering.  Convert values to strings for hashability.
    loc_items = tuple(sorted((k, str(v)) for k, v in location.items()))
    return (stat_type, loc_items)


def _comparable_data(stat):
    """Extract and normalize the 'data' payload for comparison.

    Two stats are considered identical if their normalized data dicts
    match.  We strip metadata since it contains timestamps that always
    differ between the old and new versions.
    """
    data = stat.get('data', {})
    return _normalize_dict(data)


# ---------------------------------------------------------------------------
# Diff engine
# ---------------------------------------------------------------------------

def diff_stats(new_stats, existing_stats):
    """Compare newly computed stats against existing Firestore stats.

    Parameters
    ----------
    new_stats : list[dict]
        Stats just computed (no metadata yet).
    existing_stats : list[tuple(str, dict)]
        Stats from Firestore as (doc_id, doc_dict) tuples.

    Returns
    -------
    to_add : list[dict]
        New stat docs that don't exist in Firestore.
    to_update : list[tuple(str, dict)]
        (doc_id, new_stat) pairs where the data has changed.
    to_delete : list[str]
        Doc IDs that exist in Firestore but are no longer produced.
    unchanged : int
        Count of docs that match exactly.
    """
    # Index existing Firestore stats by their canonical key so we can
    # look up each newly computed stat in O(1).
    # canonical_key -> (firestore_doc_id, normalized_data_dict)
    existing_by_key = {}
    for doc_id, doc in existing_stats:
        key = _canonical_key(doc)
        existing_by_key[key] = (doc_id, _comparable_data(doc))

    to_add = []       # new stats with no matching existing doc
    to_update = []    # existing docs whose data has changed
    matched_keys = set()  # track which existing docs were matched
    unchanged = 0

    # Walk through each newly computed stat and classify it.
    for stat in new_stats:
        key = _canonical_key(stat)
        new_data = _comparable_data(stat)

        if key in existing_by_key:
            # A doc with the same statType+location already exists in
            # Firestore.  Compare the data payloads to decide whether
            # it needs updating.
            matched_keys.add(key)
            doc_id, old_data = existing_by_key[key]
            if new_data == old_data:
                # Data is identical — no write needed.
                unchanged += 1
            else:
                # Data has changed — schedule an in-place update so we
                # reuse the existing Firestore doc ID.
                to_update.append((doc_id, stat))
        else:
            # No matching existing doc — this is a brand new stat
            # (e.g. a new fragment was seeded today).
            to_add.append(stat)

    # Existing docs that were NOT matched by any new stat have been
    # removed (e.g. a fragment was outplanted and is no longer
    # in-nursery, so its fragment_stats doc should be deleted).
    to_delete = [
        doc_id for key, (doc_id, _) in existing_by_key.items()
        if key not in matched_keys
    ]

    return to_add, to_update, to_delete, unchanged


# ---------------------------------------------------------------------------
# Firestore write helpers
# ---------------------------------------------------------------------------

def write_changes(db, to_add, to_update, to_delete, batchsize=500):
    """Apply adds, updates, and deletes to _statistics in batches.

    Each stat doc gets fresh metadata with the current timestamp before
    being written.
    """
    now = dt.datetime.now()
    coll_ref = db.collection('_statistics')

    # Firestore batches can hold up to 500 operations.  We accumulate
    # adds, updates, and deletes into a single batch and commit when
    # it reaches the limit.
    batch = db.batch()
    ops = 0

    def flush():
        """Commit the current batch and start a new one."""
        nonlocal batch, ops
        if ops > 0:
            batch.commit()
            batch = db.batch()
            ops = 0

    # --- Add new documents (auto-generated Firestore IDs) ---
    for stat in to_add:
        cs.set_metadata(stat, createdAt=now)
        doc_ref = coll_ref.document(None)
        batch.set(doc_ref, stat)
        ops += 1
        if ops >= batchsize:
            flush()

    # --- Update changed documents (reuse existing Firestore doc ID) ---
    for doc_id, stat in to_update:
        cs.set_metadata(stat, createdAt=now)
        doc_ref = coll_ref.document(doc_id)
        batch.set(doc_ref, stat)
        ops += 1
        if ops >= batchsize:
            flush()

    # --- Delete documents that are no longer in the computed stats ---
    for doc_id in to_delete:
        doc_ref = coll_ref.document(doc_id)
        batch.delete(doc_ref)
        ops += 1
        if ops >= batchsize:
            flush()

    # Commit any remaining operations
    flush()


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def update_stats_for_branch(qf, org_id, branch_id, branch_name, save=False):
    """Compute stats for a branch and write only the changes.

    Returns a dict with counts: {added, updated, deleted, unchanged}
    for this branch.
    """
    loc = {'orgID': org_id, 'branchID': branch_id}

    # --- Step 1: Compute all stats for this branch ---
    # This calls the same computation functions as compute_statistics.py:
    #   get_stats_of_location  -> fragment, nursery, structure, restosite,
    #                             outplant, donor, monitoring, by_year stats
    #   summary_branch_stats   -> the top-level branch summary
    # The result is a list of stat dicts, each with {statType, location, data}.
    print(f"  Computing stats for {branch_name}...")
    branch_stats = cs.get_stats_of_location(qf, loc)
    summary = cs.summary_branch_stats(qf, loc, branch_stats)
    branch_stats.append({
        'statType': 'branch_summary',
        'location': loc,
        'data': summary
    })

    # --- Step 2: Group new stats by statType ---
    # We process each statType separately so we can load and diff only
    # the corresponding existing docs from Firestore.
    new_by_type = {}
    for stat in branch_stats:
        st = stat['statType']
        new_by_type.setdefault(st, []).append(stat)

    # --- Step 3: Load existing stats and diff per statType ---
    totals = {'added': 0, 'updated': 0, 'deleted': 0, 'unchanged': 0}

    # We need the union of stat types from both new and existing data.
    # If an entire statType disappears from the new computation
    # (unlikely but possible), we need to delete the old docs.
    existing_stat_types = set()
    all_existing = qf.get_docs(qf.query_statistics({'branchID': branch_id}))
    for _, doc in all_existing:
        existing_stat_types.add(doc.get('statType'))

    all_stat_types = set(new_by_type.keys()) | existing_stat_types

    for stat_type in sorted(all_stat_types):
        # Load existing Firestore docs for this branch + statType.
        # For large stat types like fragment_stats this may be thousands
        # of docs, but they are small and load quickly.
        existing = qf.get_docs(
            qf.query_statistics({'branchID': branch_id}, stat_type=stat_type)
        )
        new_stats = new_by_type.get(stat_type, [])

        # Compare new vs existing and classify each doc.
        to_add, to_update, to_delete, unchanged = diff_stats(new_stats, existing)

        # Print per-statType summary.
        # +added  ~updated  -deleted  =unchanged
        if to_add or to_update or to_delete:
            print(f"    {stat_type}: +{len(to_add)} ~{len(to_update)} "
                  f"-{len(to_delete)} ={unchanged}")
        else:
            print(f"    {stat_type}: no changes ({unchanged} docs)")

        # Only write to Firestore if --save was specified and there
        # are actual changes to make.
        if save and (to_add or to_update or to_delete):
            write_changes(qf.db, to_add, to_update, to_delete)

        totals['added'] += len(to_add)
        totals['updated'] += len(to_update)
        totals['deleted'] += len(to_delete)
        totals['unchanged'] += unchanged

    return totals


def run_update(qf, save=False):
    """Iterate over all orgs and branches, updating statistics incrementally."""
    grand_totals = {'added': 0, 'updated': 0, 'deleted': 0, 'unchanged': 0}

    orgs = qf.get_orgs()
    for org_id, org_doc in orgs:
        org_name = org_doc.get('name', org_id)
        branches = qf.get_branches(org_id)
        print(f"\nOrg: {org_name} ({len(branches)} branches)")

        for branch_id, branch_doc in branches:
            branch_name = branch_doc.get('name', branch_id)
            print(f"\n  Branch: {branch_name}")

            totals = update_stats_for_branch(
                qf, org_id, branch_id, branch_name, save=save
            )

            for k in grand_totals:
                grand_totals[k] += totals[k]

    # --- Final summary across all orgs and branches ---
    print("\n" + "=" * 60)
    mode = "LIVE" if save else "DRY RUN"
    print(f"Overall ({mode}):")
    print(f"  Added:     {grand_totals['added']}")
    print(f"  Updated:   {grand_totals['updated']}")
    print(f"  Deleted:   {grand_totals['deleted']}")
    print(f"  Unchanged: {grand_totals['unchanged']}")
    # Show what percentage of docs were skipped (i.e. how much write
    # cost was avoided compared to the full delete-all/write-all approach).
    total_writes = grand_totals['added'] + grand_totals['updated'] + grand_totals['deleted']
    total_docs = total_writes + grand_totals['unchanged']
    if total_docs > 0:
        pct_saved = 100 * grand_totals['unchanged'] / total_docs
        print(f"  Writes saved: {pct_saved:.1f}% "
              f"({grand_totals['unchanged']}/{total_docs} docs unchanged)")
    print("=" * 60)


def update_statistics(save):
    creds = 'restoration-ios-firebase-adminsdk-wg0a4-18ff398018.json'
    project_id = "restoration-ios"
    print("Connecting to PRODUCTION database")

    qf = qq.QueryFirestore(project_id=project_id, creds=creds)
    try:
        run_update(qf, save=save)
    finally:
        qf.destroy()

# %%
if __name__ == "__main__":
    save = False
    update_statistics(save)
