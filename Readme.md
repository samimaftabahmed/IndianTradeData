# Indian Trade Data Analysis

Map Reduce Program to analyse trade statistics of India for export and import of commodities.

The analysis can either be done to find out the overall trade about a particular commodity in a particular year with a country, or just to find out the trade on a particular year with a country.


#### Usage
- To analyse overall data on the basis of Country and Year
-- hadoop jar IndianTradeData com.samhad.YearlyAnalysis.CYApp </export-file> <import-file> <outputDirectory>

- To analyse overall data on the basis of Commodity, Country and Year
-- hadoop jar IndianTradeData com.samhad.YearlyCommodityAnalysis.CYCApp </export-file> <import-file> <outputDirectory>

For dataset visit: https://www.kaggle.com/lakshyaag/india-trade-data
