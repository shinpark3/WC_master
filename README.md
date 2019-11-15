# WC_master
> This project generates a weekly/monthly working capital (WC) report.

## Background
* Working capital days = Inventory days - Payable days
* Inventory days = Average daily Inventory / COGS * 30
* Payable days = Average daily Payables / COGS * 30

## Data source 
shopee_bi_sbs_mart

## Setup
The script monthly_WC_v3.py runs on the server to pull the data and generate a report together. 
            
![tree] images/tree.jpg

* Template: wc_template_<country>_v3.xlsx
![alt test] link to screenshot
* Supplier list: suppliers.yaml
![alt test] link to screenshot

## Running
To generate a WC report, the only thing you need to do is typing the following command:
```
$spark-submit monthly_WC_v3.py -c [countries] -d [date] -q [queried]
```
- Countries: none, one or more countries. Default: ID, MY, TH, VN, TW, PH
- Date: the date by which the data will be pulled. Default: yesterday 
- Queried: set to True if the data has already been queried and saved. Default: False

Examples:
```
$spark-submit monthly_WC_v3.py -c ID VN TH PH MY -d 20191114 
$spark-submit monthly_WC_v3.py -c ID -d 20191114 -q True
```

## Walkthrough
### Pull data
This function queries the database with pyspark, saves the data in csv format and returns a pandas dataframe.
If data_queried = True, we will directly read from the csv file main_df.csv
```python
def get_main_df(country, today_date, data_queried):
    '''
    Get the data for the specific country between date start_of_period (sop) and today_date
    :return: main_df as pandas dataframe
    '''
    country_folder_path = './' + country + '/'
    if not os.path.exists(country_folder_path):
        os.makedirs(country_folder_path)
    input_file_path = "{}input_files".format(country_folder_path)
    if not os.path.exists(input_file_path):
        os.makedirs(input_file_path)
    if not data_queried:
        m2_sop = get_sop(today_date).strftime("%Y-%m-%d")
        today_date = today_date.strftime("%Y-%m-%d")
        query = '''
            SELECT category_cluster, supplier_name, sku_id, cdate, color,
                    cogs_usd, stock_on_hand, inbound_value_usd, acct_payables_usd, 
                    inventory_value_usd, payment_terms, brand, grass_date 
            FROM shopee_bi_sbs_mart
            WHERE
                supplier_name is NOT NULL
            AND
                grass_date >= date('{start_date}')
            AND
                grass_date <= date('{end_date}')
            AND
                purchase_type = 'Outright'
            AND 
                grass_region = '{cntry}'
            '''.format(start_date=m2_sop, end_date=today_date, cntry=country)
        print(query)
        main_df = SPARK.sql(query)
        write_to_csv(main_df, 'main_df', input_file_path + '/main_df.csv')
        main_df = main_df.toPandas()
    else:
        main_df = pd.read_csv(input_file_path + '/main_df.csv',
                              encoding='utf-8-sig', index_col=False)
    return main_df
```

### Process data
* __Inbound replenishment__: Out of total inbound values, how much are of the skus that we already selling (sku created before the start of the current month)
* __Green replenishment__: Out of inbound replenishment, how much skus are Green
```python
df_repln = main_df[['supplier_name', 'inbound_value_usd', 'cdate', 'grass_date', 'color']]
df_repln['is_replenishment'] = np.where(df_repln['cdate']
                                        < df_repln['grass_date'].astype('datetime64[M]'), 1, 0)
df_repln['inb_repln_usd'] = df_repln['inbound_value_usd'] * df_repln['is_replenishment']
df_repln['is_green_repln'] = np.where((df_repln['color'] == 'Green')
                                      & (df_repln['is_replenishment'] == 1), 1, 0)
df_repln['green_repln_usd'] = df_repln['inbound_value_usd'] * df_repln['is_green_repln']
```

* look back __90 days__ for three end_of_period (eop) dates 
```python
df_repln1 = df_repln[['supplier_name']].drop_duplicates()
df_green_repln = df_repln[['supplier_name']].drop_duplicates()
for i in range(3):
    start_date = eops[i] - dt.timedelta(days=89)
    df_total_temp = df_repln[(df_repln['grass_date'] >= start_date) & (df_repln['grass_date'] <= eops[i])]
    df_repln_mi = df_total_temp.groupby(['supplier_name'])[['inb_repln_usd']].sum()
    df_repln1 = df_repln1.merge(df_repln_mi, on=['supplier_name'], how='left')
    df_green_repln_mi = df_total_temp.groupby(['supplier_name'])[['green_repln_usd']].sum()
    df_green_repln = df_green_repln.merge(df_green_repln_mi, on=['supplier_name'], how='left')
```

* __Last 30d inbound replenishment__, similarly for last 30d green inbound replenishment
```python
df_repln1_m0 = df_repln[df_repln['grass_date'] >= today_date - dt.timedelta(days=29)]
df_repln_m0 = df_repln1_m0.groupby(['supplier_name'])[['inb_repln_usd']].sum()
df_repln1 = df_repln1.merge(df_repln_m0, on=['supplier_name'], how='left')
df_repln1.columns = ['supplier_name', 'inb_repln_m2', 'inb_repln_m1', 'inb_repln_m0', 'inb_repln_m0_30d']
```

* __End of month black stock value__: how much of the total inventory values are black at the end of month
```python
df_eop_inv = main_df[['supplier_name', 'inventory_value_usd', 'grass_date', 'color']]
df_eop_inv0 = df_eop_inv[(df_eop_inv['grass_date']).isin(eops)]
df_eop_inv0['is_black_inv'] = np.where(df_eop_inv0['color'] == 'Black', 1, 0)
df_eop_inv0['black_inv_usd'] = df_eop_inv0['is_black_inv'] * df_eop_inv0['inventory_value_usd']

df_black_inv0 = df_eop_inv0.groupby(['supplier_name', 'grass_date']) \
        [['black_inv_usd']].sum() \
        .pivot_table(values='black_inv_usd', index='supplier_name', columns='grass_date') \
        .reset_index()
df_black_inv0.columns = ['supplier_name', 'black_inv_m2', 'black_inv_m1', 'black_inv_m0']

df_eop_inv1 = df_eop_inv0.groupby(['supplier_name', 'grass_date']) \
        [['inventory_value_usd']].sum()\
        .pivot_table(values='inventory_value_usd', index='supplier_name', columns='grass_date')\
        .reset_index()
df_eop_inv1.columns = ['supplier_name', 'eop_inv_m2', 'eop_inv_m1', 'eop_inv_m0']
```

* __Pivot tables for Daily COGS__. Similarly for Daily inbound, Average daily accounts payable and Average daily inventory value
```python
df_total_sum = main_df.groupby(['supplier_name', 'grass_date']).sum().reset_index()
df_cogs = pd.pivot_table(df_total_sum, values='cogs_usd', index='supplier_name', columns='grass_date').reset_index()
```

* __Append data frame to template__: row by row, cell by cell appending
```python
path = country_folder_path + 'template/wc_template_' + country + '_v3.xlsx'
wb_obj = openpyxl.load_workbook(path)
main_ws = wb_obj['Tracking']
main_ws['B1'] = today_date
for df_index, row in df_info5.iterrows():
    for col_index in range(9):
        cell_index = get_column_letter(col_index + 1) + str(df_index + 4)
        main_ws[cell_index] = row[col_index]
    for col_index in range(9, 23):
        cell_index = get_column_letter(col_index + 8 + 1) + str(df_index + 4)
        main_ws[cell_index] = row[col_index]
```

## Tests

