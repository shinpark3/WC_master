import unittest
import datetime as dt
import pandas as pd
import yaml
from pandas.util.testing import assert_frame_equal
from monthly_WC import main as wc_main

start_date = dt.date(2019, 7, 1)
end_date = dt.date(2019, 9, 30)
days = [(start_date + dt.timedelta(days=x)).strftime("%Y-%m-%d")
        for x in range((end_date - start_date).days + 1)]
main_df = pd.DataFrame(
    {'category_cluster': ['EL'] * 3 * 92 + ['FMCG'] * 3 * 92 + ['lifestyle'] * 3 * 92,
     'supplier_name': ['VN_Supplier_A'] * 3 * 92 + ['VN_Supplier_B'] * 3 * 92 + ['VN_Supplier_C'] * 3 * 92,
     'sku_id': ['A001'] * 92 + ['A002'] * 92 + ['A003'] * 92
               + ['B001'] * 92 + ['B002'] * 92 + ['B003'] * 92
               + ['C001'] * 92 + ['C002'] * 92 + ['C003'] * 92,
     'sourcing_status': [float('nan')] * 828,
     'cogs_usd': [0] * 92  # A001
                 + [0] * 7 + [400] + [0] * 30 + [100] + [0] * 53  # A002
                 + [0] * 4 + [90] + [0] * 58 + [90, 120] + [0] * 27  # A003
                 + [0, 900] + [0] * 7 + [300] + [0] * 82  # B001
                 + [0] * 19 + [1500] + [0] * 4 + [200] + [0] * 6 + [300] + [0] * 60  # B002
                 + [0] * 71 + [200] + [0] * 20  # B003
                 + [0] * 21 + [250] + [0] * 11 + [400, 100] + [0] * 57  # C001
                 + [0] * 50 + [800] + [0] * 41  # C002
                 + [0] * 71 + [300] + [0] * 14 + [300] + [0] * 5,  # C003
     'stock_on_hand': [0] * 25 + [5] * 2 + [10] * 65  # A001
                      + [0] * 3 + [20] * 4 + [0] * 29 + [25] * 2 + [20] * 54  # A002
                      + [0] + [5] * 3 + [2] * 57 + [17] * 2 + [14] + [10] * 28  # A003
                      + [30] + [0] * 7 + [10] * 2 + [0] * 82  # B001
                      + [0] * 7 + [20] * 12 + [5] * 5 + [3] * 7 + [0] * 3 + [5] * 58  # B002
                      + [0] * 63 + [5] * 4 + [50] * 4 + [30] * 21  # B003
                      + [50] * 21 + [0] * 10 + [200] * 2 + [120] + [100] * 58  # C001
                      + [0] * 32 + [10] * 5 + [50] * 13 + [10] * 42  # C002
                      + [0] * 65 + [20] * 6 + [0] * 11 + [20] * 4 + [0] * 6,  # C003
     'inbound_value_usd': [0] * 25 + [50, 0, 50] + [0] * 64  # A001
                          + [0] * 3 + [400] + [0] * 32 + [500] + [0] * 55  # A002
                          + [0, 150] + [0] * 59 + [450] + [0] * 30  # A003
                          + [0] * 7 + [300] + [0] * 84  # B001
                          + [0] * 7 + [2000] + [0] * 26 + [500] + [0] * 57  # B002
                          + [0] * 63 + [50] + [0] * 3 + [450] + [0] * 24  # B003
                          + [0] * 31 + [1000] + [0] * 60  # C001
                          + [0] * 32 + [200] + [0] * 4 + [800] + [0] * 54  # C002
                          + [0] * 65 + [300] + [0] * 16 + [300] + [0] * 9,  # C003
     'acct_payables_usd': [0]*25 + [50, 50, 100] + [100]*27 + [50]*2 + [0]*35  # A001
                          + [0] * 3 + [400] * 30 + [0] * 3 + [500] * 30 + [0] * 26  # A002
                          + [0] + [150] * 30 + [0] * 30 + [450] * 30 + [0]  # A003
                          + [900] * 2 + [0] * 5 + [300] * 7 + [0] * 78  # B001
                          + [0] * 7 + [2000] * 7 + [0] * 20 + [500] * 7 + [0] * 51  # B002
                          + [0] * 63 + [50] * 4 + [500] * 3 + [450] * 4 + [0] * 18  # B003
                          + [250] * 21 + [0] * 10 + [1000] * 30 + [0] * 31  # C001
                          + [0] * 32 + [200] * 5 + [1000] * 25 + [800] * 5 + [0] * 25  # C002
                          + [0] * 65 + [300] * 17 + [600] * 10,  # C003
     'inventory_value_usd': [0] * 25 + [50] * 2 + [100] * 65  # A001
                            + [0] * 3 + [400] * 4 + [0] * 29 + [500] * 2 + [400] * 54  # A002
                            + [0] + [150] * 3 + [60] * 57 + [510] * 2 + [420] + [300] * 28  # A003
                            + [900] + [0] * 7 + [300] * 2 + [0] * 82  # B001
                            + [0] * 7 + [2000] * 12 + [500] * 5 + [300] * 7 + [0] * 3 + [500] * 58  # B002
                            + [0] * 63 + [50] * 4 + [500] * 4 + [300] * 21  # B003
                            + [250] * 21 + [0] * 10 + [1000] * 2 + [600] + [500] * 58  # C001
                            + [0] * 32 + [200] * 5 + [1000] * 13 + [200] * 42  # C002
                            + [0] * 65 + [300] * 6 + [0] * 11 + [300] * 4 + [0] * 6,
     'payment_terms': ['30 NET'] * 3 * 92 + ['7 NET'] * 3 * 92 + ['30 NET'] * 3 * 92,
     'brand': ['Abrand1'] * 92 + ['Abrand1'] * 92 + ['Abrand2'] * 92 +
              ['Bbrand1'] * 92 + ['Bbrand2'] * 92 + ['Bbrand3'] * 92 +
              ['Cbrand1'] * 92 + ['Cbrand2'] * 92 + ['Cbrand2'] * 92,
     'grass_date': days * 9})

result_df = wc_main('testing', end_date, main_df)


class TestPivotTables(unittest.TestCase):

    def test_payable(self):
        expected_payable = pd.read_csv('expected_output/acct_df.csv', encoding='utf-8-sig', index_col=False)
        expected_payable.columns = ['category_cluster', 'supplier_name'] + days
        result_payable = result_df.get('payable')
        result_payable1 = result_payable.sort_values(['supplier_name']).reset_index(drop=True)
        self.assertEqual(result_payable1.shape[0], expected_payable.shape[0],
                         "Number of suppliers is different")
        self.assertEqual(result_payable1.shape[1], expected_payable.shape[1],
                         "Number of days is different")
        assert_frame_equal(result_payable1, expected_payable)

    def test_inventory(self):
        expected_inv = pd.read_csv('expected_output/inv_df.csv', encoding='utf-8-sig', index_col=False)
        expected_inv.columns = ['category_cluster', 'supplier_name'] + days
        result_inv = result_df.get('inventory')
        result_inv1 = result_inv.sort_values(['supplier_name']).reset_index(drop=True)
        self.assertEqual(result_inv1.shape[0], expected_inv.shape[0],
                         "Number of suppliers is different")
        self.assertEqual(result_inv1.shape[1], expected_inv.shape[1],
                         "Number of days is different")
        assert_frame_equal(result_inv1, expected_inv)

    def test_cogs(self):
        expected_cogs = pd.read_csv('expected_output/cogs_df.csv', encoding='utf-8-sig', index_col=False)
        expected_cogs.columns = ['category_cluster', 'supplier_name'] + days
        result_cogs = result_df.get('cogs')
        result_cogs1 = result_cogs.sort_values(['supplier_name']).reset_index(drop=True)
        self.assertEqual(result_cogs1.shape[0], expected_cogs.shape[0],
                         "Number of suppliers is different")
        self.assertEqual(result_cogs1.shape[1], expected_cogs.shape[1],
                         "Number of days is different")
        assert_frame_equal(result_cogs1, expected_cogs)

    def test_inbounds(self):
        expected_inbounds = pd.read_csv('expected_output/inbounds_df.csv', encoding='utf-8-sig', index_col=False)
        expected_inbounds.columns = ['category_cluster', 'supplier_name'] + days
        result_inbounds = result_df.get('inbound')
        result_inbounds1 = result_inbounds.sort_values(['supplier_name']).reset_index(drop=True)
        self.assertEqual(result_inbounds1.shape[0], expected_inbounds.shape[0],
                         "Number of suppliers is different")
        self.assertEqual(result_inbounds1.shape[1], expected_inbounds.shape[1],
                         "Number of days is different")
        assert_frame_equal(result_inbounds1, expected_inbounds)

    def test_tracking(self):
        expected_tracking = pd.read_csv('expected_output/tracking_df.csv', encoding='utf-8-sig', index_col=False)
        result_tracking = result_df.get('tracking')
        result_tracking1 = result_tracking.sort_values(['supplier_name']).reset_index(drop=True)
        self.assertEqual(result_tracking1.shape[0], expected_tracking.shape[0],
                         "Number of suppliers is different")
        assert_frame_equal(result_tracking1, expected_tracking)

    def test_yaml_reading(self):
        result_yaml_reading = result_df.get('tracking_yaml')
        stream = open('./suppliers_testing.yaml', 'r')
        dictionary = yaml.load(stream, Loader=yaml.FullLoader)
        supplier_dict = {}
        for key, value in dictionary.items():
            my_key = str(key)
            supplier_dict[my_key] = value

        print(result_yaml_reading['supplier_name'])
        print(supplier_dict[my_key])
        self.assertEqual(len(result_yaml_reading['supplier_name']), len(supplier_dict[my_key]),
                         "Number of suppliers highlighted is different")


if __name__ == '__main__':
    unittest.main()
