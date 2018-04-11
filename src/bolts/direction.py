from streamparse import TicklessBatchingBolt
from ctapipe.reco import HillasReconstructor
from astropy.utils.exceptions import AstropyDeprecationWarning
from collections import namedtuple
import numpy as np
import astropy.units as u
import pandas as pd
import pickle
import warnings


SubMomentParameters = namedtuple('SubMomentParameters', 'size,cen_x,cen_y,length,width,psi')


class DirectionBolt(TicklessBatchingBolt):
    outputs = ['array_event_id', 'direction']
    reco = HillasReconstructor()
    instrument = []

    # monkey patch this huansohn. this is super slow otherwise. who needs max h anyways
    def _dummy_function_h_max(self, hillas_dict, subarray, tel_phi):
        return -1

    def initialize(self, conf, ctx):
        self.reco.fit_h_max = self._dummy_function_h_max
        instrument_description = '/Users/mackaiver/Development/wordcount/instrument_description.pkl'
        self.instrument = pickle.load(open(instrument_description, 'rb'))
        # do some horrible things to silencece astropy warnings in ctapipe
        warnings.filterwarnings('ignore', category=AstropyDeprecationWarning, append=True)
        warnings.filterwarnings('ignore', category=FutureWarning, append=True)

    # def process(self, tup):
    #     row = tup.values[0]
    #     width = row.width
    #     self.emit([row, width])



    def group_key(self, tup):
        array_event_id = tup.values[0]
        return array_event_id  # collect batches of words

    def process_batch(self, key, tups):

        # emit the count of words we had per batch
        group = pd.DataFrame(tups)
        result = self.reconstruct_direction(key, group,)
        print(result)
        self.emit([key, result])


    def reconstruct_direction(self, array_event_id, group):

        params = {}
        pointing_azimuth = {}
        pointing_altitude = {}
        for index, row in group.iterrows():
            tel_id = row.telescope_id
            # the data in each event has to be put inside these namedtuples to call reco.predict
            moments = SubMomentParameters(size=row.intensity, cen_x=row.x * u.m, cen_y=row.y * u.m, length=row.length * u.m, width=row.width * u.m, psi=row.psi * u.rad)
            params[tel_id] = moments
            pointing_azimuth[tel_id] = row.pointing_azimuth * u.rad
            pointing_altitude[tel_id] = row.pointing_altitude * u.rad

        try:
            reconstruction = reco.predict(params, instrument, pointing_azimuth, pointing_altitude)
        except NameError:
            return {'alt_prediction': np.nan,
                    'az_prediction': np.nan,
                    'core_x_prediction': np.nan,
                    'core_y_prediction': np.nan,
                    'array_event_id': array_event_id,
            }

        if reconstruction.alt.si.value == np.nan:
            print('Not reconstructed')
            print(params)

        return {'alt_prediction': ((np.pi / 2) - reconstruction.alt.si.value),  # TODO srsly now? FFS
                'az_prediction': reconstruction.az.si.value,
                'core_x_prediction': reconstruction.core_x.si.value,
                'core_y_prediction': reconstruction.core_y.si.value,
                'array_event_id': array_event_id,
                # 'h_max_prediction': reconstruction.h_max.si.value
                }
