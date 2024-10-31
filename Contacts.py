import pandas as pd
import Tools
import hubspot as hubspot
from hubspot.crm.contacts import BatchInputSimplePublicObjectInputForCreate, ApiException
from hubspot.crm.contacts import BatchInputSimplePublicObjectBatchInput, ApiException

HubSpotDataPipeline=Tools.HubSpotDataPipeline
# Initialize the HubSpot data pipeline with API key
pipeline = HubSpotDataPipeline(api_key_to="HUBSPOT_API_KEY",api_key_from="HUBSPOT_API_KEY")

# Extract, transform, and load data
data_list = pipeline.extract()
data_frame = pd.DataFrame(data_list)

# Save the extracted data
data_frame.to_csv("contacts_data_collect.csv", index=False)
print("Data saved to contacts_data_collect.csv")


Tools.car(0)  # Indicate data import with car animation

Tools.car(2)  # Indicate data transform

# Transform the data and save the transformed DataFrame
data_frame = pipeline.transform(data_list)
data_frame.to_csv("contacts_data_result.csv", index=False)
print("Data saved to contacts_data_result.csv")

# Load data from a CSV file into a DataFrame
# data_frame = pd.read_csv("contacts_data_result.csv")

# Load the transformed data into HubSpot
pipeline.load(data_frame)
Tools.car(1)  # Indicate data export with car animation