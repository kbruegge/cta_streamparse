"""
Word count topology
"""

from streamparse import Grouping, Topology

from bolts.direction import DirectionBolt
from spouts.events import EventSpout


class WordCount(Topology):
    event_spout = EventSpout.spec()
    count_bolt = DirectionBolt.spec(inputs={event_spout: Grouping.fields('array_event_id')}, par=2)
