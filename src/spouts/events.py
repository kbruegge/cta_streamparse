from itertools import cycle
import pandas as pd
from streamparse import Spout
import numpy as np

types = {
    'array_event_id': np.int32,
    'telescope_type_name': np.str,
    'telescope_id': np.int32,
    'telescope_type_id': np.int32,
    'run_id_y': np.int32,
    'num_triggered_telescopes': np.int32,
    'run_id_x': np.int32,
    'camera_id': np.int32,
    'mc_corsika_primary_id': np.int32,
    'camera_name': np.str,
}

class EventSpout(Spout):
    outputs = ['array_event_id', 'telescope_event']
    rows = []
    df = []

    def initialize(self, stormconf, context):
        self.df = pd.read_csv('/Users/mackaiver/Development/wordcount/gamma_test.csv', dtype=types)
        self.rows = cycle(range(len(self.df)))

    def next_tuple(self):
        event = self.df.iloc[next(self.rows)]
        event.array_event_id
        self.emit([int(event.array_event_id), dict(event)])
