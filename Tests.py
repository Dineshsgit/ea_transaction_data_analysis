import unittest
import logging
import pandas as pd
import os, os.path

from Main import read_input_file, generate_hash, split_dataframe, find_top_suburbs, find_top_agents_by_suburb, get_config

class ETLTest(unittest.TestCase):

    def test_split_dataframe(self):
        data = {'Product': ['Desktop Computer', 'Tablet', 'Printer', 'Laptop', 'super Computer',
                            'mobile phone', 'tablet', 'smart watch', 'ear phones'],
                'Price': [850, 200, 150, 1300, 10000, 1000, 1200, 400, 50]
                }
        df = pd.DataFrame(data, columns=['Product', 'Price'])
        split_dataframe(df, 'test_output/', 2)
        path = os.path.dirname(os.path.realpath(__file__)) + '/' + 'test_output/'
        self.assertEqual(len(os.listdir(path)),  5)

if __name__ == '__main__':
    unittest.main()

