"""Query donor colony statistics from the ReefOS Firestore database.

Reports fragment counts per donor colony, filterable by year.
Iterates per-org/per-branch to keep memory bounded.

Usage:
    python scripts/query_donors.py                          # 2025, production
    python scripts/query_donors.py --year 2024              # specific year
    python scripts/query_donors.py --dev                    # dev database
    python scripts/query_donors.py --csv donors_2025.csv    # export to CSV
"""

import argparse
import datetime as dt

import pandas as pd
from google.cloud.firestore_v1.base_query import FieldFilter

import reefos_data_api.query_firestore as qq
from reefos_data_api.firestore_constants import SiteType as st


def get_donor_fragment_counts(qf, year=None, branch=None):
    """Count fragments per donor colony, optionally filtered by creation year.

    Iterates per-org/per-branch. For each branch:
      1. Load all donor colonies (to get colony name and species).
      2. Load fragments, optionally filtered to those created in the
         given year.
      3. Group by donorID and count.

    Parameters
    ----------
    qf : QueryFirestore
        Connected Firestore query object.
    year : int or None
        If given, only count fragments with metadata.createdAt in this year.
        If None, count all fragments.

    Returns
    -------
    pd.DataFrame
        Columns: org, branch, donorID, donor_name, organismID, taxon,
                 fragment_count
    """
    # Build a taxon lookup: organismID -> "Genus species"
    organisms = qf.get_organisms(org_type='coral')
    taxon_map = {doc[0]: doc[1]['genus'] + ' ' + doc[1]['species']
                 for doc in organisms}

    # Define the date range for filtering if a year was specified.
    if year is not None:
        start = dt.datetime(year, 1, 1)
        end = dt.datetime(year + 1, 1, 1)

    rows = []
    orgs = qf.get_orgs()

    for org_id, org_doc in orgs:
        org_name = org_doc.get('name', org_id)
        branches = qf.get_branches(org_id)

        for branch_id, branch_doc in branches:
            branch_name = branch_doc.get('name', branch_id)
            if branch is not None and branch_name != branch:
                continue
            loc = {'branchID': branch_id}
            print(f"  {org_name} / {branch_name}...", end="", flush=True)

            # Load donor colonies for this branch to get names and species.
            donors = qf.get_docs(qf.query_donorcolonies(loc))
            donor_info = {}
            for did, ddoc in donors:
                donor_info[did] = {
                    'name': ddoc.get('name', ''),
                    'organismID': (ddoc.get('siteData') or {}).get('organismID', ''),
                }

            # Load fragments for this branch, optionally filtered by year.
            frag_query = qf.query_fragments(loc)
            if year is not None:
                frag_query = qf.add_date_filter(frag_query, start_date=start,
                                                end_date=end)
            fragments = qf.get_docs(frag_query)
            print(f" {len(fragments)} fragments")

            # Count fragments per donor colony.
            donor_counts = {}
            for fid, fdoc in fragments:
                did = fdoc.get('donorID')
                if did:
                    donor_counts[did] = donor_counts.get(did, 0) + 1

            # Build result rows, including donor colonies with zero fragments
            # (so we can see which colonies were not used).
            seen_donors = set()
            for did, count in donor_counts.items():
                info = donor_info.get(did, {})
                organism_id = info.get('organismID', '')
                rows.append({
                    'org': org_name,
                    'branch': branch_name,
                    'donorID': did,
                    'donor_name': info.get('name', '(unknown)'),
                    'organismID': organism_id,
                    'taxon': taxon_map.get(organism_id, organism_id),
                    'fragment_count': count,
                })
                seen_donors.add(did)

            # Add donor colonies that had no fragments in this period.
            for did, info in donor_info.items():
                if did not in seen_donors:
                    organism_id = info.get('organismID', '')
                    rows.append({
                        'org': org_name,
                        'branch': branch_name,
                        'donorID': did,
                        'donor_name': info.get('name', '(unknown)'),
                        'organismID': organism_id,
                        'taxon': taxon_map.get(organism_id, organism_id),
                        'fragment_count': 0,
                    })

    df = pd.DataFrame(rows)
    if len(df) > 0:
        df = df.sort_values(['org', 'branch', 'taxon', 'fragment_count'],
                            ascending=[True, True, True, False])
    return df


def print_report(df, year, all=True):
    """Print a formatted summary of donor colony fragment counts."""
    period = str(year) if year else "all time"
    print(f"\n{'=' * 70}")
    print(f"Fragments per donor colony — {period}")
    print(f"{'=' * 70}")

    if len(df) == 0:
        print("  No data found.")
        return

    # Print totals
    active = df[df.fragment_count > 0]
    total_frags = df.fragment_count.sum()
    total_donors = len(active)
    total_species = active.taxon.nunique() if len(active) > 0 else 0
    unused = len(df) - len(active)
    print(f"  Total fragments: {total_frags}")
    print(f"  Active donor colonies: {total_donors}")
    print(f"  Species represented: {total_species}")
    print(f"  Unused donor colonies: {unused}")

    # Print per-branch breakdown
    for (org, branch), bdf in df.groupby(['org', 'branch']):
        bactive = bdf[bdf.fragment_count > 0]
        print(f"\n  {org} / {branch}: "
              f"{bdf.fragment_count.sum()} fragments from "
              f"{len(bactive)} donors")
        if all:
            for _, row in bactive.iterrows():
                print(f"    {row.fragment_count:5d}  {row.donor_name:30s}  {row.taxon}")
        else:
            # Show top donors
            for _, row in bactive.head(20).iterrows():
                print(f"    {row.fragment_count:5d}  {row.donor_name:30s}  {row.taxon}")
            if len(bactive) > 20:
                print(f"    ... and {len(bactive) - 20} more donors")

    print(f"\n{'=' * 70}")


def main():
    parser = argparse.ArgumentParser(
        description="Query fragment counts per donor colony"
    )
    parser.add_argument("--year", type=int, default=2025,
                        help="Filter to fragments created in this year "
                             "(default: 2025, use 0 for all time)")
    parser.add_argument("--dev", action="store_true",
                        help="Use the dev database (default: production)")
    parser.add_argument("--csv", type=str, default=None,
                        help="Export results to a CSV file")
    args = parser.parse_args()

    year = args.year if args.year != 0 else 2025
    branch = "French Polynesia"

    if args.dev:
        creds = "restoration-app---dev-6df41-firebase-adminsdk-fbsvc-37ee88f0d4.json"
        project_id = "restoration-app---dev-6df41"
        print("Connecting to DEV database")
    else:
        creds = 'restoration-ios-firebase-adminsdk-wg0a4-18ff398018.json'
        project_id = "restoration-ios"
        print("Connecting to PRODUCTION database")

    qf = qq.QueryFirestore(project_id=project_id, creds=creds)
    try:
        df = get_donor_fragment_counts(qf, year=year, branch=branch)
        print_report(df, year)

        if args.csv and len(df) > 0:
            df.to_csv(args.csv, index=False)
            print(f"\nExported to {args.csv}")
    finally:
        qf.destroy()


if __name__ == "__main__":
    main()
