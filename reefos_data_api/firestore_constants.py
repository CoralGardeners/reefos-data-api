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


class FragmentState(Enum):
    in_nursery = "inNursery"
    outplanted = "outplanted"
    refragmented = "refragmented"
    removed = "removed"
    unknown = "unknown"


class AggregationType(Enum):
    by_nursery = "by_nursery"
    by_structure = "by_L1"
