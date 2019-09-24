# Indian Trade Data Analysis

Map Reduce Program to analyse trade statistics of India for export and import of commodities.

The analysis can either be done on overall trade based on commodity in a particular year with a country, or just on a particular year with a country



#### Usage
- To analyse overall data on the basis of Country and Year
-- hadoop jar IndianTradeData com.samhad.YearlyAnalysis.CYApp </export-file> <import-file> <outputDirectory>

- To analyse overall data on the basis of Commodity, Country and Year
-- hadoop jar IndianTradeData com.samhad.YearlyCommodityAnalysis.CYCApp </export-file> <import-file> <outputDirectory>

For dataset visit: https://www.kaggle.com/lakshyaag/india-trade-data
