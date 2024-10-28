import pandas as pd
import requests
import Tools
from datetime import datetime
#import test

from pprint import pprint
import hubspot as hubspot
from hubspot.crm.contacts import BatchInputSimplePublicObjectInputForCreate, ApiException

from hubspot.crm.contacts import BatchInputSimplePublicObjectBatchInput, ApiException

# Display the entire DataFrame without truncation
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)


# Define the API key and endpoint URL
api_key = "HUBSPOT_API_KEY"  # actual HubSpot API key
endpoint = "https://api.hubapi.com/crm/v3/objects/contacts/search"

# Set up headers for authentication and data format
headers = {
    "Authorization": f"Bearer {api_key}",
    "Content-Type": "application/json"
}

def Extract():
    """
    Collects all contact records marked as 'allowed_to_collect' = true
    and sorts them alphabetically by 'lastname'. Retrieves specified contact
    properties from HubSpot and returns a list of organized contacts.

    Returns:
        list: A sorted list of dictionaries, each containing contact details.
    """
    
    # Define the properties to be collected for each contact
    properties = [
        "firstname", "lastname", "raw_email", "country", "phone",
        "technical_test___create_date", "industry", "address", "hs_object_id"
    ]

    # Define the body of the search request with filter criteria
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
        "limit": 100  # Adjust the limit if necessary
    }

    # Make the POST request to fetch contacts
    try:
        response = requests.post(endpoint, headers=headers, json=request_body)
        response.raise_for_status()  # Check for HTTP errors
        data = response.json()

        # Extract and collect each contact's specified properties
        contacts = data.get("results", [])
        collected_contacts = []
        for contact in contacts:
            contact_details = {prop: contact["properties"].get(prop, None) for prop in properties}
            collected_contacts.append(contact_details)

        # Sort contacts alphabetically by 'lastname'
        sorted_contacts = sorted(collected_contacts, key=lambda x: x.get("lastname", "").lower())

        # Sort contacts by date (assuming date is in 'YYYY-MM-DD' format in the 'date' field)
        sorted_contacts = sorted(collected_contacts, key=lambda x: datetime.strptime(x.get('technical_test___create_date', "1900-01-01"), "%Y-%m-%d"))

        return sorted_contacts

    except requests.exceptions.RequestException as e:
        print("Error fetching contacts:", e)
        return None


def Transform(sorted_contacts):
    
    for row in sorted_contacts:
            if 'raw_email' in row:
                row['raw_email'] = Tools.extract_emails(row['raw_email'])[0]
            if 'country' in row:
                row['city'] = row['country']
                country = Tools.get_country_from_city(row['country'])[0]
                row['country'] = country
                row['phone'] = Tools.format_phone_number(row['phone'],Tools.get_country_code_from_city(country))

    result=pd.DataFrame(sorted_contacts)

    # Save the DataFrame to a CSV file
    result.to_csv("contacts_data_transformation.csv", index=False)
    print()
    print("Data saved to contacts_data_transformation.csv")
    print()


    # Load the CSV file into a DataFrame
    result = pd.read_csv("contacts_data_transformation.csv")

    result=Tools.merge_duplicates(result)
    print(result)
    return result

def Load(df):
    """
    Creates multiple contacts in HubSpot in batch.

    Args:
        df (pd.DataFrame): DataFrame containing contact data with columns such as 'email', 'phone_number', 'country', etc.
    """
    # HubSpot API base URL and endpoint for creating contacts
    BASE_URL = "https://app.hubspot.com/private-apps/47865434"
    CONTACTS_ENDPOINT = "/crm/v3/objects/contacts/batch/create"
    api_key = "HUBSPOT_API_KEY"  #  actual HubSpot API key

    #client = HubSpot(api_key=api_key)
    client = hubspot.Client.create(access_token=api_key)
    
        
    batch_contacts = []

    for _, row in df.iterrows():
        # Convert 'technical_test___create_date' to a timestamp in milliseconds
        create_date = pd.to_datetime(row.get("technical_test___create_date")).value // 10**6

        # Prepare each contact's data for batch input
        contact_data = {
            "properties": {
                "email": row.get("raw_email"),
                "phone": row.get("phone"),
                "country": row.get("country"),
                "city": row.get("city"),  # Ensure 'city' is taken from the correct field
                "firstname": row.get("firstname"),
                "lastname": row.get("lastname"),
                "address": row.get("address"),
                "original_create_date": create_date,  # Set as a timestamp in milliseconds
                "original_industry": ";" + ";".join(row.get("industry", "").split(";")),
                "temporary_id": row.get("hs_object_id")
            }
        }
        batch_contacts.append(contact_data)

    # Create a BatchInput object
    batch_input = BatchInputSimplePublicObjectInputForCreate(inputs=batch_contacts)

    # Perform the batch create request
    try:
        api_response = client.crm.contacts.batch_api.create(batch_input_simple_public_object_input_for_create=batch_input)
        pprint(api_response)
        print("Batch creation completed successfully.")
    except ApiException as e:
        print("Exception when calling batch_api->create: %s\n" % e)



def batch_update_contacts(df, id_property="hs_object_id"):
    """
    Updates multiple contacts in HubSpot in batch.

    Args:
        df (pd.DataFrame): DataFrame containing contact data with columns such as 'raw_email', 'phone', 'country', etc.
        id_property (str): The unique property name used to identify each contact in HubSpot.
    """

    # HubSpot API base URL and endpoint for creating contacts
    BASE_URL = "https://app.hubspot.com/private-apps/47865434"
    CONTACTS_ENDPOINT = "/crm/v3/objects/contacts/batch/create"
    api_key = "HUBSPOT_API_KEY"  #  actual HubSpot API key

    #client = HubSpot(api_key=api_key)
    client = hubspot.Client.create(access_token=api_key)
    
     # Prepare the batch data for contacts
    batch_contacts = []

    for _, row in df.iterrows():
        # Convert 'technical_test___create_date' to a timestamp in milliseconds
        create_date = pd.to_datetime(row.get("technical_test___create_date")).value // 10**6

        # Prepare each contact's data for batch input
        contact_data = {
            "idProperty": "temporary_id",  # Use 'hs_object_id' as the unique identifier for updating contacts
            "id": row.get("hs_object_id"),
            "properties": {
                "email": row.get("raw_email"),
                "phone": row.get("phone"),
                "country": row.get("country"),
                "city": row.get("city"),
                "firstname": row.get("firstname"),
                "lastname": row.get("lastname"),
                "address": row.get("address"),
                "original_create_date": create_date,  # Set as a timestamp in milliseconds
                "original_industry": ";" + ";".join(row.get("industry", "").split(";")),
                "temporary_id": row.get("hs_object_id")
            }
        }
        batch_contacts.append(contact_data)

    # Create a BatchInput object
    batch_input = BatchInputSimplePublicObjectBatchInput(inputs=batch_contacts)

    # Perform the batch update request
    try:
        api_response = client.crm.contacts.batch_api.update(batch_input_simple_public_object_batch_input=batch_input)
        pprint(api_response)
        print("Batch update completed successfully.")
    except ApiException as e:
        print("Exception when calling batch_api->update: %s\n" % e)


# Execute the function to collect and sort contacts
data_list = Extract()
data_frame=pd.DataFrame(data_list)

for contact in data_list:
    print(f"date: {contact['technical_test___create_date']} ,  email: {contact['raw_email']} , phone: {contact['phone']} ,  Name: {contact['firstname']} {contact['lastname']}  ,  Country:  {contact['country']}")

# Save the DataFrame to a CSV file
data_frame.to_csv("contacts_data_collect.csv", index=False)
print()
print("Data saved to contacts_data_collect.csv")

Tools.car(0)


data_frame=Transform(data_list)

# Save the DataFrame to a CSV file
data_frame.to_csv("contacts_data_result.csv", index=False)
print("Data saved to contacts_data_result.csv")

#batch_update_contacts(data_frame)
Load(data_frame)
Tools.car(1)

