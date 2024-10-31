# Technical-Test-Phase-II
# HubSpot Contact Data Pipeline

This project implements a data pipeline to automate the extraction, transformation, and loading (ETL) of contact data into HubSpot. The pipeline retrieves contact information from HubSpot, processes the data, and updates or creates contacts in bulk.

## Requirements

- pandas
- requests
- hubspot-api-client
- phonenumbers
- matplotlib
- geopy
- pycountry
s
## Overview

The pipeline handles:
1. **Data Extraction**: Retrieves contact data marked for collection.
2. **Data Transformation**: Cleans and standardizes email domains, phone numbers, and duplicates.
3. **Data Loading**: Batch uploads or updates the contacts in HubSpot with the cleaned data.

## Features

- Extracts and sorts contacts based on specified fields.
- Cleans email domains, standardizes phone numbers, and removes duplicate entries.
- Loads new contacts in bulk or updates existing records in HubSpot.
- Visualizes contact data with `matplotlib` for analysis.

## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/yourusername/hubspot-data-pipeline.git
   cd hubspot-data-pipeline
