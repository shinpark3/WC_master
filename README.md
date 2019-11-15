# WC_master
> This project generates a weekly/monthly working capital (WC) report.

## Background
* Working capital days = Inventory days - Payable days
* Inventory days = Average daily Inventory / COGS * 30
* Payable days = Average daily Payables / COGS * 30

## Data source 
shopee_bi_sbs_mart

## Setup
The script monthly_WC_v3.py runs on the server to pull the data and generate a report. 

![alt test] link to tree structure image

## Running
To generate a WC report, the only thing you need to do is typing the following command in the server:
```
$spark-submit monthly_WC_v3.py -c [countries] -d [date] -q [queried]
```
Examples:
```
$spark-submit monthly_WC_v3.py -c ID VN TH PH MY -d 20191114 
$spark-submit monthly_WC_v3.py -c ID -d 20191114 -q True
```

## Utils


## Tests

