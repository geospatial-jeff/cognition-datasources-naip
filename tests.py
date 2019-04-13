from datasources import tests

from NAIP import NAIP



class NAIPTestCases(tests.BaseTestCases):

    def _setUp(self):

        self.datasource = NAIP
        self.spatial = {
        "type": "Polygon",
        "coordinates": [
          [
            [
              -119.46533203125,
              33.26624989076275
            ],
            [
              -116.56494140625001,
              33.26624989076275
            ],
            [
              -116.56494140625001,
              34.92197103616377
            ],
            [
              -119.46533203125,
              34.92197103616377
            ],
            [
              -119.46533203125,
              33.26624989076275
            ]
          ]
        ]
      }

        self.temporal = ("2012-06-01", "2012-06-30")
        self.properties = {'eo:instrument': {'eq': 'Leica ADS100'}}
        self.limit = 20