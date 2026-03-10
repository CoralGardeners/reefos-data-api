from enum import Enum


class EventType(Enum):
    # Fragment lifecycle events
    frag_creation = "fragmentCreation"
    frag_monitor = "fragmentMonitor"
    frag_outplant_monitor = "fragmentOutplantMonitor"
    frag_move = "fragmentMove"
    frag_outplant = "fragmentOutplant"
    frag_refragment = "fragmentRefragment"

    # Outplant monitoring events
    outplant_cell_monitoring = "outplantCellMonitoring"
    # outplant_monitoring = "outplantMonitoring"  # Deprecated - replaced by outplant_cell_monitoring

    # Survey events
    visual_survey = "visualSurvey"
    coral_survey = "coralSurvey"
    fish_survey = "fishSurvey"

    # Nursery monitoring events
    full_nursery_monitoring = "fullNurseryMonitoring"
    bleaching_nursery_monitoring = "bleachingNurseryMonitoring"
    nursery_cleaning = "nurseryCleaning"
    nursery_predator_sweep = "nurseryPredatorSweep"

    # Donor colony monitoring events
    donor_colony_monitoring = "donorColonyMonitoring"


class SiteType(Enum):
    branch = 'branch'
    control = "control"
    donorcolony = "donorColony"
    nursery = "nursery"
    structure = "structure"
    outplant = "outplant"
    outplantcell = "outplantCell"
    surveysite = "surveySite"
    surveycell = "surveyCell"
    surveyplot = "surveyPlot"
    restosite = "restoSite"


# dead or removed frags are set to completed when the nursery is outplanted
class FragmentState(Enum):
    in_nursery = "inNursery"
    outplanted = "outplanted"
    refragmented = "refragmented"
    removed = "removed"
    completed = "completed"
    unknown = "unknown"


class AggregationType(Enum):
    by_nursery = "by_nursery"
    by_structure = "by_L1"

class CellType(Enum):
    bommie = "bommie"
    region = "region"

class NurseryState(Enum):
    active = "active"
    seeding = "seeding"
    growing = "growing"
    outplanting = "outplanting"
    retired = "retired"


class StatType(Enum):
    fragment_stats = "fragment_stats"
    branch_stats = "branch_stats"
    branch_summary = "branch_summary"
    restosite_stats = "restosite_stats"
    nursery_stats = "nursery_stats"
    structure_stats = "structure_stats"
    spp_nursery_stats = "spp_nursery_stats"
    outplant_stats = "outplant_stats"
    outplantcell_stats = "outplantcell_stats"
    outplant_species_stats = "outplant_species_stats"
    donor_summary_stats = "donor_summary_stats"
    donor_site_summary_stats = "donor_site_summary_stats"
    donor_nursery_details_stats = "donor_nursery_details_stats"
    donor_details_stats = "donor_details_stats"
    monitoring_time_series_stats = "monitoring_time_series_stats"
    by_year = "by_year"
    fragment_donor = "fragment_donor"
    outplant_fragment_donor = "outplant_fragment_donor"
    full_nursery_monitoring = "fullNurseryMonitoring"
    bleaching_nursery_monitoring = "bleachingNurseryMonitoring"
