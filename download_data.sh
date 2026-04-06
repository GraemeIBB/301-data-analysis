#!/bin/bash

BASE="http://301.graeme.fun"

mkdir -p data/spending_by_foreign_tourists
mkdir -p data/tourists_entering_canada
mkdir -p data/tourism_supply_expenditure
mkdir -p data/interprovincial_tourism_expenditures

echo "Downloading 24100047.csv (spending by foreign tourists)..."
curl "$BASE/24100047.csv" -o data/spending_by_foreign_tourists/24100047.csv

echo "Downloading 24100050.csv (tourists entering Canada)..."
curl "$BASE/24100050.csv" -o data/tourists_entering_canada/24100050.csv

echo "Downloading 24100004.csv (tourism supply/demand)..."
curl "$BASE/24100004.csv" -o data/tourism_supply_expenditure/24100004.csv

echo "Downloading 24100044.csv (interprovincial tourism expenditures)..."
curl "$BASE/24100044.csv" -o data/interprovincial_tourism_expenditures/24100044.csv

echo "Done."
find data -name "*.csv"
