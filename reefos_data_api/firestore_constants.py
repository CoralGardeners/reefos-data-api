from enum import Enum


class EventType(Enum):
    frag_creation = "fragmentCreation"
    frag_monitor = "fragmentMonitor"
    frag_outplant_monitor = "fragmentOutplantMonitor"
    frag_move = "fragmentMove"
    frag_outplant = "fragmentOutplant"
    outplant_cell_monitoring = "outplantCellMonitoring"
    # outplant_monitoring = "outplantMonitoring"
    visual_survey = "visualSurvey"
    coral_survey = "coralSurvey"
    full_nursery_monitoring = "fullNurseryMonitoring"
    bleaching_nursery_monitoring = "bleachingNurseryMonitoring"


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
    restosite = "restoSite"


class FragmentState(Enum):
    in_nursery = "inNursery"
    outplanted = "outplanted"
    removed = "removed"
    unknown = "unknown"


class AggregationType(Enum):
    by_nursery = "by_nursery"
    by_structure = "by_L1"
