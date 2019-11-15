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

![alt test] link to tree structure image

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
**df_repln**:
* Inbound replenishment: Out of total inbound values, how much are of the skus that we already selling (sku created before the start of the current month)
* Green replenishment: Out of inbound replenishment, how much skus are Green
```python
df_repln = main_df[['supplier_name', 'inbound_value_usd', 'cdate', 'grass_date', 'color']]
df_repln['is_replenishment'] = np.where(df_repln['cdate']
                                        < df_repln['grass_date'].astype('datetime64[M]'), 1, 0)
df_repln['inb_repln_usd'] = df_repln['inbound_value_usd'] * df_repln['is_replenishment']
df_repln['is_green_repln'] = np.where((df_repln['color'] == 'Green')
                                      & (df_repln['is_replenishment'] == 1), 1, 0)
df_repln['green_repln_usd'] = df_repln['inbound_value_usd'] * df_repln['is_green_repln']
```

## Tests

