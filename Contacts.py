import pandas as pd
import requests
import Tools
from datetime import datetime
from pprint import pprint
import hubspot as hubspot
from hubspot.crm.contacts import BatchInputSimplePublicObjectInputForCreate, ApiException
from hubspot.crm.contacts import BatchInputSimplePublicObjectBatchInput, ApiException

# Display full DataFrame without truncation in the console for debugging
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)

class HubSpotDataPipeline:
    def __init__(self, api_key_from,api_key_to):
        """
        Initialize the HubSpot Data Pipeline with the provided API key.
        
        Args:
            api_key (str): The HubSpot API key for authentication.
        """
        self.api_key = api_key_from
        self.client = hubspot.Client.create(access_token=api_key_to)
        self.headers = {
            "Authorization": f"Bearer {api_key_from}",
            "Content-Type": "application/json"
        }
        self.endpoint = "https://api.hubapi.com/crm/v3/objects/contacts/search"

    def extract(self):
        """
        Collects contacts marked as 'allowed_to_collect' = true from HubSpot,
        sorts them alphabetically by 'lastname', and returns a list of sorted contacts.
        
        Returns:
            list: A sorted list of dictionaries, each containing contact details.
        """
        # Define contact properties to retrieve
        properties = [
            "firstname", "lastname", "raw_email", "country", "phone",
            "technical_test___create_date", "industry", "address", "hs_object_id"
        ]

        # Define search request body with filtering criteria
        request_body = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "allowed_to_collect",
                            "operator": "EQ",
                            "value": "true"
                        }
                    ]
                }
            ],
            "properties": properties,
            "limit": 100  # Max contacts to fetch per request
        }

        # Make POST request to fetch contacts
        try:
            response = requests.post(self.endpoint, headers=self.headers, json=request_body)
            response.raise_for_status()  # Raise error for HTTP issues
            data = response.json()

            # Collect contact properties into a list of dictionaries
            contacts = data.get("results", [])
            collected_contacts = [
                {prop: contact["properties"].get(prop, None) for prop in properties} 
                for contact in contacts
            ]

            # Sort contacts by lastname alphabetically
            sorted_contacts = sorted(
                collected_contacts, key=lambda x: x.get("lastname", "").lower()
            )

            # Sort contacts by date if available
            sorted_contacts = sorted(
                sorted_contacts, 
                key=lambda x: datetime.strptime(
                    x.get('technical_test___create_date', "1900-01-01"), "%Y-%m-%d"
                )
            )
            return sorted_contacts

        except requests.exceptions.RequestException as e:
            print("Error fetching contacts:", e)
            return None

    def transform(self, sorted_contacts):
        """
        Transforms contact data, extracting and cleaning properties like emails,
        country codes, and phone numbers.
        
        Args:
            sorted_contacts (list): List of sorted contact dictionaries.
        
        Returns:
            pd.DataFrame: DataFrame of transformed contact data.
        """
        # Transform contact data based on Tools functions
        for row in sorted_contacts:
            if 'raw_email' in row:
                row['raw_email'] = Tools.extract_emails(row['raw_email'])[0]
            if 'country' in row:
                row['city'] = row['country']
                country = Tools.get_country_from_city(row['country'])[0]
                row['country'] = country
                row['phone'] = Tools.format_phone_number(row['phone'], Tools.get_country_code_from_city(country))

        # Convert list of dictionaries to DataFrame and save to CSV
        transformed_df = pd.DataFrame(sorted_contacts)
        transformed_df.to_csv("contacts_data_transformation.csv", index=False)
        print("Data saved to contacts_data_transformation.csv")

        # Reload the transformed data
        transformed_df = pd.read_csv("contacts_data_transformation.csv")
        transformed_df = Tools.merge_duplicates(transformed_df)
        print(transformed_df)
        return transformed_df

    def load(self, df):
        """
        Loads (creates) multiple contacts in HubSpot in batch from a DataFrame.

        Args:
            df (pd.DataFrame): DataFrame with columns for HubSpot contact properties.
        """
        batch_contacts = []

        for _, row in df.iterrows():
            # Convert creation date to timestamp in milliseconds
            create_date = pd.to_datetime(row.get("technical_test___create_date")).value // 10**6

            # Prepare each contact's data for batch creation
            contact_data = {
                "properties": {
                    "email": row.get("raw_email"),
                    "phone": row.get("phone"),
                    "country": row.get("country"),
                    "city": row.get("city"),
                    "firstname": row.get("firstname"),
                    "lastname": row.get("lastname"),
                    "address": row.get("address"),
                    "original_create_date": create_date,
                    "original_industry": ";" + ";".join(row.get("industry", "").split(";")),
                    "temporary_id": row.get("hs_object_id")
                }
            }
            batch_contacts.append(contact_data)

        # Batch input for creating contacts in HubSpot
        batch_input = BatchInputSimplePublicObjectInputForCreate(inputs=batch_contacts)

        try:
            api_response = self.client.crm.contacts.batch_api.create(
                batch_input_simple_public_object_input_for_create=batch_input
            )
            pprint(api_response)
            print("Batch creation completed successfully.")
        except ApiException as e:
            print("Exception when calling batch_api->create:", e)

    def batch_update_contacts(self, df):
        """
        Updates multiple contacts in HubSpot in batch.

        Args:
            df (pd.DataFrame): DataFrame with columns for HubSpot contact properties.
        """
        batch_contacts = []

        for _, row in df.iterrows():
            # Convert creation date to timestamp in milliseconds
            create_date = pd.to_datetime(row.get("technical_test___create_date")).value // 10**6

            # Prepare each contact's data for batch update
            contact_data = {
                "idProperty": "temporary_id",
                "id": row.get("hs_object_id"),
                "properties": {
                    "email": row.get("raw_email"),
                    "phone": row.get("phone"),
                    "country": row.get("country"),
                    "city": row.get("city"),
                    "firstname": row.get("firstname"),
                    "lastname": row.get("lastname"),
                    "address": row.get("address"),
                    "original_create_date": create_date,
                    "original_industry": ";" + ";".join(row.get("industry", "").split(";")),
                    "temporary_id": row.get("hs_object_id")
                }
            }
            batch_contacts.append(contact_data)

        # Batch input for updating contacts in HubSpot
        batch_input = BatchInputSimplePublicObjectBatchInput(inputs=batch_contacts)

        try:
            api_response = self.client.crm.contacts.batch_api.update(
                batch_input_simple_public_object_batch_input=batch_input
            )
            pprint(api_response)
            print("Batch update completed successfully.")
        except ApiException as e:
            print("Exception when calling batch_api->update:", e)


# Initialize the HubSpot data pipeline with API key
pipeline = HubSpotDataPipeline(api_key_to="HUBSPOT_API_KEY",api_key_from="HUBSPOT_API_KEY")

# Extract, transform, and load data
data_list = pipeline.extract()
data_frame = pd.DataFrame(data_list)

# Save the extracted data
data_frame.to_csv("contacts_data_collect.csv", index=False)
print("Data saved to contacts_data_collect.csv")

Tools.car(0)  # Indicate data import with car animation

# Transform the data and save the transformed DataFrame
data_frame = pipeline.transform(data_list)
data_frame.to_csv("contacts_data_result.csv", index=False)
print("Data saved to contacts_data_result.csv")

# Load the transformed data into HubSpot
pipeline.load(data_frame)
Tools.car(1)  # Indicate data export with car animation