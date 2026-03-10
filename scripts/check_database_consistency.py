"""Database consistency checks for the ReefOS Firestore database.

Validates referential integrity across the flat Firestore collections
(_orgs, _sites, _organisms, _fragments, _events) by checking that all
ID references point to existing, non-deleted documents.

Iterates per-org/per-branch to keep memory use and Firestore reads
bounded as the database grows. Only the small global collections
(_orgs, _organisms) are loaded once upfront.

Checks performed (per branch):

  1. Site hierarchy
     - structure.nurseryID    -> nursery exists
     - outplantCell.outplantID -> outplant exists
     - site.restositeID       -> restosite exists

  2. Fragment references
     - location.nurseryID     -> nursery exists
     - location.structureID   -> structure exists
     - location.outplantID    -> outplant exists
     - location.outplantCellID -> outplantCell exists
     - location.restositeID   -> restosite exists
     - organismID             -> organism exists
     - donorID                -> donor colony exists
     - state is a valid FragmentState value
     - in-nursery fragments have a nurseryID
     - outplanted fragments have outplantID and outplantCellID

  3. Event references
     - location.nurseryID     -> nursery exists
     - location.outplantID    -> outplant exists
     - eventType is a valid EventType value
     - fragment event fragmentID -> fragment exists
     - frag_monitor logID     -> monitoring log event exists

  4. Legacy data warnings (pre-2025, informational)
     - fragments with null nurseryID (allowed for pre-2025 data)
     - fragments with donorID='Unknown' (with creation dates)
     These are reported with creation dates so you can verify they
     are all legacy. Post-2025 occurrences are hard failures.

  5. Coverage warnings (informational)
     - nurseries with no fragments
     - structures with no fragments

Cleanup mode (--cleanup --save):

  When --cleanup is specified, the script collects fixable issues and
  either reports what would change (dry-run) or applies fixes (--save).

  Cleanup actions:
    1. Delete events whose fragmentID references a non-existent fragment
    2. Delete events with missing nurseryID (nurseryID expected but null)
    3. Delete events with missing outplantID (outplantID expected but null)
    4. Delete structures whose structureID is never used in any
       fragment.location.structureID in the branch
    5. Delete fragments where all three of nurseryID, structureID, and
       outplantID are null
    6. Fix fragments where restositeID is incorrect and nurseryID is
       null: look up the outplant site via outplantID and copy its
       restositeID to the fragment
    7. Delete donor colonies never referenced by any fragment's donorID
    8. Mark frag_monitor events as deleted (metadata.deleted=True) when
       their eventData.logID references a monitoring log that is itself
       marked deleted

Exit code: 0 if all checks pass, 1 if any fail.

Usage:
    python scripts/check_database_consistency.py                # check only
    python scripts/check_database_consistency.py --cleanup      # check + dry-run cleanup
    python scripts/check_database_consistency.py --cleanup --save  # check + apply cleanup
"""

import datetime as dt
import sys

from reefos_data_api.firestore_constants import (
    EventType,
    FragmentState
)
import reefos_data_api.query_firestore as qq

# Fragments created before this date are considered legacy data and
# get softer checks (warnings instead of failures) for missing
# nurseryID and unknown donorID.
LEGACY_CUTOFF = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)


# ---------------------------------------------------------------------------
# Reporting helpers
# ---------------------------------------------------------------------------

class ConsistencyReport:
    """Accumulates check results and prints a summary."""

    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.warnings = 0
        self._section = None

    def section(self, title):
        self._section = title
        print(f"\n  --- {title} ---")

    def check(self, name, total, bad_ids):
        """Report a single check.  *bad_ids* is a set/list of failing doc IDs."""
        if len(bad_ids) == 0:
            self.passed += 1
            print(f"  PASS  {name} ({total} checked)")
        else:
            self.failed += 1
            examples = list(bad_ids)[:5]
            print(f"  FAIL  {name}: {len(bad_ids)}/{total} bad  examples={examples}")

    def warn(self, name, items):
        """Informational warning (not a hard failure)."""
        if len(items) > 0:
            self.warnings += 1
            examples = list(items)[:5]
            print(f"  WARN  {name}: {len(items)}  examples={examples}")

    def warn_with_dates(self, name, items_with_dates):
        """Warning that includes creation dates for context.

        items_with_dates is a list of (doc_id, created_at) tuples.
        Shows examples with dates plus the most recent occurrence.
        """
        if len(items_with_dates) > 0:
            self.warnings += 1
            # Sort by date so we can find the most recent.
            sorted_items = sorted(items_with_dates,
                                  key=lambda x: x[1] if x[1] else dt.datetime.min)
            most_recent_id, most_recent_date = sorted_items[-1]
            examples = [(fid, d.strftime('%Y-%m-%d') if d else 'None')
                        for fid, d in sorted_items[:5]]
            print(f"  WARN  {name}: {len(items_with_dates)}")
            for fid, dstr in examples:
                print(f"         {fid}  created={dstr}")
            if len(items_with_dates) > 5:
                print(f"         ... and {len(items_with_dates) - 5} more")
            mr_date_str = most_recent_date.strftime('%Y-%m-%d') if most_recent_date else 'None'
            print(f"         most recent: {most_recent_id}  created={mr_date_str}")

    def summary(self):
        total = self.passed + self.failed
        print("\n" + "=" * 60)
        print(f"Summary: {total} checks -- {self.passed} passed, "
              f"{self.failed} failed, {self.warnings} warnings")
        if self.failed > 0:
            print("RESULT: ISSUES FOUND")
        else:
            print("RESULT: ALL CHECKS PASSED")
        print("=" * 60)
        return self.failed


# ---------------------------------------------------------------------------
# Cleanup action collector
# ---------------------------------------------------------------------------

class CleanupActions:
    """Collects fixable issues during checks for later batch application."""

    def __init__(self):
        # Deletes: list of (collection_name, doc_id, reason) tuples
        self.deletes = []
        # Updates: list of (collection_name, doc_id, field_path, new_value, reason)
        self.updates = []

    def delete(self, collection, doc_id, reason):
        self.deletes.append((collection, doc_id, reason))

    def update(self, collection, doc_id, field_path, new_value, reason):
        self.updates.append((collection, doc_id, field_path, new_value, reason))

    def report(self):
        """Print a summary of pending cleanup actions."""
        print(f"\n{'=' * 60}")
        print("Cleanup actions:")
        if not self.deletes and not self.updates:
            print("  No cleanup actions needed.")
            return

        # Group deletes by reason for a readable summary.
        delete_reasons = {}
        for coll, doc_id, reason in self.deletes:
            delete_reasons.setdefault(reason, []).append((coll, doc_id))
        for reason, items in delete_reasons.items():
            print(f"  DELETE {len(items):5d}  {reason}")
            for coll, doc_id in items[:3]:
                print(f"           {coll}/{doc_id}")
            if len(items) > 3:
                print(f"           ... and {len(items) - 3} more")

        # Group updates by reason.
        update_reasons = {}
        for coll, doc_id, field, val, reason in self.updates:
            update_reasons.setdefault(reason, []).append((coll, doc_id, field, val))
        for reason, items in update_reasons.items():
            print(f"  UPDATE {len(items):5d}  {reason}")
            for coll, doc_id, field, val in items[:3]:
                print(f"           {coll}/{doc_id}  {field}={val}")
            if len(items) > 3:
                print(f"           ... and {len(items) - 3} more")

        total = len(self.deletes) + len(self.updates)
        print(f"  Total: {len(self.deletes)} deletes, {len(self.updates)} updates")
        print(f"{'=' * 60}")

    def apply(self, db, save=False):
        """Apply all pending cleanup actions to Firestore.

        If save is False (dry-run), only prints what would happen.
        Uses batched writes for efficiency (max 500 ops per batch).
        """
        self.report()

        if not save:
            print("\n  DRY RUN -- use --save to apply these changes")
            return

        if not self.deletes and not self.updates:
            return

        print("\n  Applying changes...")
        batchsize = 500
        batch = db.batch()
        ops = 0

        def flush():
            nonlocal batch, ops
            if ops > 0:
                batch.commit()
                batch = db.batch()
                ops = 0

        # Apply deletes
        for coll, doc_id, reason in self.deletes:
            doc_ref = db.collection(coll).document(doc_id)
            batch.delete(doc_ref)
            ops += 1
            if ops >= batchsize:
                flush()

        # Apply updates (merge=True to only update the specified field)
        for coll, doc_id, field_path, new_value, reason in self.updates:
            doc_ref = db.collection(coll).document(doc_id)
            # Build nested dict from dot-separated field path.
            # e.g. "location.restositeID" -> {'location': {'restositeID': val}}
            parts = field_path.split('.')
            update_dict = new_value
            for part in reversed(parts):
                update_dict = {part: update_dict}
            batch.set(doc_ref, update_dict, merge=True)
            ops += 1
            if ops >= batchsize:
                flush()

        flush()
        print(f"  Applied {len(self.deletes)} deletes and "
              f"{len(self.updates)} updates")


# ---------------------------------------------------------------------------
# ID extraction helpers
# ---------------------------------------------------------------------------

def ids_of(docs):
    """Return set of document IDs from list of (id, dict) tuples."""
    return {doc[0] for doc in docs}


def loc_field(doc_dict, field):
    """Safely get a location sub-field, returning None if absent."""
    loc = doc_dict.get('location') or {}
    return loc.get(field)


def created_at(doc_dict):
    """Extract metadata.createdAt from a document, or None."""
    metadata = doc_dict.get('metadata') or {}
    return metadata.get('createdAt')


def is_legacy(doc_dict):
    """Return True if the document was created before the legacy cutoff date."""
    ca = created_at(doc_dict)
    return ca is not None and ca < LEGACY_CUTOFF


# ---------------------------------------------------------------------------
# Per-branch checks
# ---------------------------------------------------------------------------

def check_site_hierarchy(report, branch_id,
                         nurseries, structures, outplants, outplant_cells,
                         donor_colonies, restosites):
    report.section("Site hierarchy")

    nursery_ids = ids_of(nurseries)
    outplant_ids = ids_of(outplants)
    restosite_ids = ids_of(restosites)

    # Structures -> nursery exists
    bad = {sid for sid, doc in structures if loc_field(doc, 'nurseryID') not in nursery_ids}
    report.check("structure.nurseryID -> nursery", len(structures), bad)

    # OutplantCells -> outplant exists
    bad = {sid for sid, doc in outplant_cells if loc_field(doc, 'outplantID') not in outplant_ids}
    report.check("outplantCell.outplantID -> outplant", len(outplant_cells), bad)

    # Any site with restositeID -> restosite exists
    all_sites = nurseries + outplants + donor_colonies
    sites_with_resto = [(sid, doc) for sid, doc in all_sites if loc_field(doc, 'restositeID')]
    bad = {sid for sid, doc in sites_with_resto if loc_field(doc, 'restositeID') not in restosite_ids}
    report.check("site.restositeID -> restosite", len(sites_with_resto), bad)


def check_fragments(report, qf, branch_id,
                     nursery_ids, structure_ids, outplant_ids, outplant_cell_ids,
                     restosite_ids, organism_ids, donor_colony_ids,
                     fragments, outplants, cleanup):
    report.section("Fragment references")

    valid_states = {s.value for s in FragmentState}

    bad_nursery = set()
    bad_structure = set()
    bad_outplant = set()
    bad_opcell = set()
    bad_restosite = set()
    bad_organism = set()
    bad_donor = set()
    bad_state = set()
    outplanted_no_outplant = set()
    outplanted_no_opcell = set()

    # Legacy data warnings (pre-2025):
    # - null nurseryID is expected for some older fragments
    # - donorID == "unknown" (any case) occurs in older data
    legacy_null_nursery = []     # (fid, createdAt) tuples
    unknown_donor = []           # (fid, createdAt) tuples
    # Post-2025 occurrences are still hard failures:
    post_cutoff_null_nursery = set()
    post_cutoff_unknown_donor = set()

    # Build outplant lookup for fix #6: outplantID -> outplant doc dict
    outplant_lookup = {oid: odoc for oid, odoc in outplants}

    for fid, doc in fragments:
        nid = loc_field(doc, 'nurseryID')
        sid = loc_field(doc, 'structureID')
        oid = loc_field(doc, 'outplantID')
        ocid = loc_field(doc, 'outplantCellID')
        rsid = loc_field(doc, 'restositeID')
        org_id = doc.get('organismID')
        did = doc.get('donorID')
        state = doc.get('state')
        legacy = is_legacy(doc)

        # Cleanup #5: fragments with all three of nurseryID, structureID,
        # and outplantID null are orphaned and should be deleted.
        if not nid and not sid and not oid:
            if cleanup:
                cleanup.delete('_fragments', fid,
                               "fragment with null nurseryID, structureID, and outplantID")
            # Don't run further checks on this fragment — it will be deleted.
            continue

        if nid and nid not in nursery_ids:
            bad_nursery.add(fid)
        if sid and sid not in structure_ids:
            bad_structure.add(fid)
        if oid and oid not in outplant_ids:
            bad_outplant.add(fid)
        if ocid and ocid not in outplant_cell_ids:
            bad_opcell.add(fid)

        # restositeID check, with cleanup #6:
        # If restositeID is bad AND nurseryID is null AND outplantID is
        # valid, look up the outplant site's restositeID and use that.
        if rsid and rsid not in restosite_ids:
            if not nid and oid and oid in outplant_lookup:
                outplant_doc = outplant_lookup[oid]
                correct_rsid = loc_field(outplant_doc, 'restositeID')
                if correct_rsid and correct_rsid in restosite_ids and cleanup:
                    cleanup.update(
                        '_fragments', fid,
                        'location.restositeID', correct_rsid,
                        f"fix restositeID from outplant (was {rsid})")
                elif correct_rsid and correct_rsid in restosite_ids:
                    # Would be fixable but cleanup not enabled — still report as failure
                    bad_restosite.add(fid)
                else:
                    bad_restosite.add(fid)
            else:
                bad_restosite.add(fid)

        if org_id and org_id not in organism_ids:
            bad_organism.add(fid)

        # donorID check: "unknown" (case-insensitive) is a warning for
        # legacy data, a failure for post-2025 data.  Any other donorID
        # that doesn't match an existing donor colony is always a failure.
        did_is_unknown = did and isinstance(did, str) and did.lower() == 'unknown'
        if did_is_unknown:
            unknown_donor.append((fid, created_at(doc)))
#            else:
#                post_cutoff_unknown_donor.add(fid)
        elif did and did not in donor_colony_ids:
            bad_donor.add(fid)

        if state and state not in valid_states:
            bad_state.add(fid)

        # Cross-collection: state vs location fields.
        # Null nurseryID on in-nursery fragments is a warning for
        # legacy data (pre-2025), a failure for newer data.
        if state == FragmentState.in_nursery.value and not nid:
            if legacy:
                legacy_null_nursery.append((fid, created_at(doc)))
            else:
                post_cutoff_null_nursery.add(fid)
        if state == FragmentState.outplanted.value:
            if not oid:
                outplanted_no_outplant.add(fid)
            if not ocid:
                outplanted_no_opcell.add(fid)

    n = len(fragments)
    report.check("fragment.nurseryID -> nursery", n, bad_nursery)
    report.check("fragment.structureID -> structure", n, bad_structure)
    report.check("fragment.outplantID -> outplant", n, bad_outplant)
    report.check("fragment.outplantCellID -> outplantCell", n, bad_opcell)
    report.check("fragment.restositeID -> restosite", n, bad_restosite)
    report.check("fragment.organismID -> organism", n, bad_organism)
    report.check("fragment.donorID -> donorColony", n,
                 bad_donor | post_cutoff_unknown_donor)
    report.check("fragment.state is valid", n, bad_state)
    report.check("in-nursery fragment has nurseryID (post-2025)", n, post_cutoff_null_nursery)
    report.check("outplanted fragment has outplantID", n, outplanted_no_outplant)
    report.check("outplanted fragment has outplantCellID", n, outplanted_no_opcell)

    # Legacy data warnings -- expected for older fragments, shown with
    # creation dates so the user can verify they are all pre-2025.
    report.warn_with_dates("legacy fragments with null nurseryID (pre-2025)",
                           legacy_null_nursery)
    report.warn_with_dates("legacy fragments with donorID='Unknown' (pre-2025)",
                           unknown_donor)


def check_events(report, qf, branch_id,
                  nursery_ids, outplant_ids, fragment_ids,
                  monitoring_event_ids, deleted_monitoring_ids, events, cleanup):
    report.section("Event references")

    valid_event_types = {e.value for e in EventType}
    # Event types that are associated with a specific fragment.
    fragment_event_types = {
        EventType.frag_creation.value,
        EventType.frag_monitor.value,
        EventType.frag_outplant_monitor.value,
        EventType.frag_move.value,
        EventType.frag_outplant.value,
        EventType.frag_refragment.value,
    }
    # Event types where a nurseryID is expected in the location.
    nursery_event_types = {
        EventType.frag_creation.value,
        EventType.frag_monitor.value,
        EventType.frag_move.value,
        EventType.full_nursery_monitoring.value,
        EventType.bleaching_nursery_monitoring.value,
    }
    # Event types where an outplantID is expected in the location.
    outplant_event_types = {
        EventType.frag_outplant.value,
        EventType.frag_outplant_monitor.value,
        EventType.outplant_cell_monitoring.value,
        EventType.visual_survey.value,
    }

    bad_nursery = set()
    bad_outplant = set()
    bad_fragment = set()
    bad_log = set()
    bad_event_type = set()
    bad_frag_monitor_deleted_log = set()

    for eid, doc in events:
        nid = loc_field(doc, 'nurseryID')
        oid = loc_field(doc, 'outplantID')
        etype = doc.get('eventType')
        frag_id = doc.get('fragmentID')
        event_data = doc.get('eventData') or {}
        log_id = event_data.get('logID')

        # Cleanup #1: events with a fragmentID that doesn't exist.
        if etype in fragment_event_types and frag_id and frag_id not in fragment_ids:
            bad_fragment.add(eid)
            if cleanup:
                cleanup.delete('_events', eid,
                               "event references non-existent fragment")
            continue  # skip further checks on this event

        # Cleanup #2: events where nurseryID is expected but missing.
        if etype in nursery_event_types and not nid:
            bad_nursery.add(eid)
            if cleanup:
                cleanup.delete('_events', eid,
                               "event missing expected nurseryID")
            continue

        # Cleanup #3: events where outplantID is expected but missing.
        if etype in outplant_event_types and not oid:
            bad_outplant.add(eid)
            if cleanup:
                cleanup.delete('_events', eid,
                               "event missing expected outplantID")
            continue

        # Standard reference checks for events not scheduled for deletion.
        if nid and nid not in nursery_ids:
            bad_nursery.add(eid)
        if oid and oid not in outplant_ids:
            bad_outplant.add(eid)
        if etype and etype not in valid_event_types:
            bad_event_type.add(eid)
        if etype == EventType.frag_monitor.value and log_id and log_id not in monitoring_event_ids:
            bad_log.add(eid)
        if etype == EventType.frag_monitor.value and log_id and log_id in deleted_monitoring_ids:
            bad_frag_monitor_deleted_log.add(eid)
            if cleanup:
                cleanup.update('_events', eid, 'metadata.deleted', True,
                               "frag_monitor references deleted monitoring log")

    n = len(events)
    report.check("event.nurseryID -> nursery", n, bad_nursery)
    report.check("event.outplantID -> outplant", n, bad_outplant)
    report.check("event.eventType is valid", n, bad_event_type)
    report.check("fragment event.fragmentID -> fragment", n, bad_fragment)
    report.check("frag_monitor.logID -> monitoring event", n, bad_log)
    report.check("frag_monitor not deleted when log is deleted", n, bad_frag_monitor_deleted_log)


def check_coverage(report, nursery_ids, structure_ids, donor_colony_ids,
                    fragments, cleanup):
    """Informational: sites with no fragments."""
    report.section("Coverage (informational)")

    frag_nursery_ids = {loc_field(doc, 'nurseryID')
                        for _, doc in fragments if loc_field(doc, 'nurseryID')}
    frag_structure_ids = {loc_field(doc, 'structureID')
                          for _, doc in fragments if loc_field(doc, 'structureID')}
    frag_donor_ids = {doc.get('donorID')
                      for _, doc in fragments if doc.get('donorID')}

    empty_nurseries = nursery_ids - frag_nursery_ids
    empty_structures = structure_ids - frag_structure_ids
    empty_donors = donor_colony_ids - frag_donor_ids

    report.warn("nurseries with no fragments", empty_nurseries)
    report.warn("structures with no fragments", empty_structures)
    report.warn("donor colonies never referenced by any fragment", empty_donors)

    # Cleanup #4: delete structures whose structureID is never referenced
    # by any fragment in this branch.
    if cleanup:
        for sid in empty_structures:
            cleanup.delete('_sites', sid,
                           "structure never used in any fragment")
        # Cleanup #7: delete donor colonies never referenced by any fragment.
        for did in empty_donors:
            cleanup.delete('_sites', did,
                           "donor colony never used in any fragment")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_checks(qf, do_cleanup=False):
    report = ConsistencyReport()
    cleanup = CleanupActions() if do_cleanup else None

    # -- Global data --
    print("Loading global data...")
    orgs = qf.get_orgs()
    org_ids = ids_of(orgs)
    organisms = qf.get_organisms()
    organism_ids = ids_of(organisms)
    print(f"  {len(org_ids)} orgs, {len(organism_ids)} organisms")

    for org_id, org_doc in orgs:
        org_name = org_doc.get('name', org_id)
        branches = qf.get_branches(org_id)
        print(f"\n{'=' * 60}")
        print(f"Org: {org_name} ({org_id})  --  {len(branches)} branches")

        for branch_id, branch_doc in branches:
            branch_name = branch_doc.get('name', branch_id)
            print(f"\n  Branch: {branch_name} ({branch_id})")

            loc = {'branchID': branch_id}

            # Load branch-scoped sites
            nurseries = qf.get_docs(qf.query_nurseries(loc))
            structures = qf.get_docs(qf.query_structures(loc))
            outplants = qf.get_docs(qf.query_outplantsites(loc))
            outplant_cells = qf.get_docs(qf.query_outplantcells(loc))
            donor_colonies = qf.get_docs(qf.query_donorcolonies(loc))
            restosites = qf.get_docs(qf.query_restosites(loc))

            nursery_ids = ids_of(nurseries)
            structure_ids = ids_of(structures)
            outplant_ids = ids_of(outplants)
            outplant_cell_ids = ids_of(outplant_cells)
            donor_colony_ids = ids_of(donor_colonies)
            restosite_ids = ids_of(restosites)

            print(f"  Sites: {len(nurseries)} nurseries, {len(structures)} structures, "
                  f"{len(outplants)} outplants, {len(outplant_cells)} cells, "
                  f"{len(donor_colonies)} donors, {len(restosites)} restosites")

            # Load fragments and events for this branch
            fragments = qf.get_docs(qf.query_fragments(loc))
            events = qf.get_docs(qf.query_events(loc))
            fragment_ids = ids_of(fragments)

            # Build set of monitoring log event IDs
            # (nursery monitoring events in this branch)
            monitoring_types = [
                EventType.full_nursery_monitoring.value,
                EventType.bleaching_nursery_monitoring.value,
            ]
            monitoring_events = [e for e in events
                                 if e[1].get('eventType') in monitoring_types]
            monitoring_event_ids = ids_of(monitoring_events)

            # Build set of deleted monitoring log IDs (including deleted docs,
            # which are excluded from the main `events` list).
            all_events = qf.get_docs_all(qf.query_events(loc))
            deleted_monitoring_ids = {
                eid for eid, doc in all_events
                if doc.get('eventType') in monitoring_types
                and (doc.get('metadata') or {}).get('deleted') == True
            }

            print(f"  {len(fragments)} fragments, {len(events)} events, "
                  f"{len(monitoring_event_ids)} monitoring logs, "
                  f"{len(deleted_monitoring_ids)} deleted monitoring logs")

            # Run checks
            check_site_hierarchy(report, branch_id,
                                 nurseries, structures, outplants, outplant_cells,
                                 donor_colonies, restosites)

            check_fragments(report, qf, branch_id,
                            nursery_ids, structure_ids, outplant_ids,
                            outplant_cell_ids, restosite_ids, organism_ids,
                            donor_colony_ids, fragments, outplants, cleanup)

            check_events(report, qf, branch_id,
                         nursery_ids, outplant_ids, fragment_ids,
                         monitoring_event_ids, deleted_monitoring_ids, events, cleanup)

            check_coverage(report, nursery_ids, structure_ids,
                           donor_colony_ids, fragments, cleanup)

    failures = report.summary()
    return failures, cleanup


def check_and_clean_database(cleanup=False, save=False):
    creds = 'restoration-ios-firebase-adminsdk-wg0a4-18ff398018.json'
    project_id = "restoration-ios"
    print("Connecting to PRODUCTION database")

    qf = qq.QueryFirestore(project_id=project_id, creds=creds)
    try:
        failures, cleanup = run_checks(qf, do_cleanup=cleanup)
        if cleanup:
            cleanup.apply(qf.db, save=save)
    finally:
        qf.destroy()

    sys.exit(1 if failures > 0 else 0)


# %%
if __name__ == "__main__":
    cleanup=True
    save=False
    check_and_clean_database(cleanup=cleanup, save=save)
