
import re
import numpy as np
import time
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderUnavailable

import pandas as pd

import phonenumbers
from phonenumbers import phonenumberutil

import pycountry

def show_errors(response):
    """
    Displays errors in the response if they exist, otherwise confirms success.

    Args:
        response (BatchResponseSimplePublicObject): The response object returned by the API.
    """
    # Check if the response contains errors by looking at the 'status' attribute or similar
    try:
        # Accessing response details based on known properties of BatchResponseSimplePublicObject
        if hasattr(response, 'status') and response.status == 'error' and response.category != 'CONFLICT':
            print("Error:", response.message)
        else:
            print("Batch processed successfully.")
    except AttributeError:
        print("Response object does not have expected attributes.")


def merge_duplicates(df):
    """
    Merges duplicate contact records in a DataFrame based on email or full name, keeping the most recent record
    and merging data from older records according to specified criteria.

    Args:
        df (pd.DataFrame): DataFrame containing contact records with columns 'hs_object_id', "Full Name"('firstname'+'lastname'),
                           'raw_email', 'address', 'industry', and  'technical_test___create_date'.

    Returns:
        pd.DataFrame: A DataFrame with duplicates merged and only the most recent records retained.
    """

    # Ensure the create date is in datetime format for comparison
    df["technical_test___create_date"] = pd.to_datetime(df["technical_test___create_date"], format="%Y-%m-%d")


    # Second Step: Merge duplicates based on Email
    df = merge_by_key(df, "Email")

    # First Step: Merge duplicates based on Full Name
    df = merge_by_key(df, "Full Name")

    return df



def merge_by_key(df, key):
    """
    Helper function to merge duplicates based on a specified key.

    Args:
        df (pd.DataFrame): DataFrame to be processed.
        key (str): The column name to use as the grouping key (e.g., "Full Name" or "Email").

    Returns:
        pd.DataFrame: A DataFrame with duplicates merged based on the specified key.
    """

    # Sort by create date in descending order so the most recent record comes first
    df = df.sort_values(by="technical_test___create_date", ascending=False)

    # Create a unique identifier based on the specified key
    if "Full Name" in key:
        df["unique_id"] = (df['firstname'] + df['lastname']).fillna("")
        grouped = df.groupby("unique_id")
    elif "Email" in key:
        grouped = df.groupby("raw_email")

    # Initialize an empty list to store merged records
    merged_records = []

    # Process each group of duplicates
    for _, group in grouped:
        # Start with the most recent record (first in sorted group)
        primary_record = group.iloc[0].copy()

        # Collect unique industries
        industry_set = set(primary_record["industry"].split(";")) if pd.notna(primary_record["industry"]) else set()

        # Iterate through other records in the group (older records)
        for _, row in group.iloc[1:].iterrows():
            # Fill missing fields in the primary record only if the value in `row` is not null or empty
            for column in ['hs_object_id', 'firstname', 'lastname', 'raw_email', 'address', 'industry', 'technical_test___create_date']:
                if pd.isna(primary_record[column]) and pd.notna(row[column]) and row[column] != "":
                    primary_record[column] = row[column]

            # Concatenate unique industry values
            if pd.notna(row["industry"]):
                industry_set.update(row["industry"].split(";"))

        # Format the industry field as required
        primary_record["industry"] = ";" + ";".join(sorted(industry_set)).strip(";")

        # Append the processed primary record to the list of merged records
        merged_records.append(primary_record)

    # Create a DataFrame from the merged records list
    merged_df = pd.DataFrame(merged_records)

    return merged_df


def get_country_code_from_city(city_name):
    """
    Returns the ISO Alpha-2 country code based on the city name.

    Args:
        city_name (str): The name of the city.

    Returns:
        str: The ISO Alpha-2 country code if found, otherwise an error message.
    """
    # Initialize the geolocator
    geolocator = Nominatim(user_agent="city_to_country_code")

    # Geocode the city to find the country
    location = geolocator.geocode(city_name)
    
    if location:
        # Extract country name from the location
        
        country_name = location.address.split(",")[-1].strip()

        if "/" in country_name:
            country_name = country_name.split("/")[1].strip()
        
    
        # Use pycountry to find the ISO country code
        country = pycountry.countries.get(name=country_name)
        
        if country:
            return country.alpha_2  # Return the Alpha-2 country code
        else:
            return "Country code not found in pycountry"
    else:
        return "City not found"


def format_phone_number(phone, country_code):
    """
    Formats a phone number with the country code and ensures it adheres to a standard format.

    Args:
        phone (str): The phone number as a string.
        country_code (str): The ISO Alpha-2 country code to interpret the number.

    Returns:
        str: The formatted phone number or an error message if invalid.
    """
    # Check if the phone number is None or empty
    if not phone:
        return "Phone number not provided"

    # Clean the phone number by removing any non-numeric characters
    phone = ''.join(filter(str.isdigit, phone))

    # If the phone starts with '00', replace it with the country code
    if phone.startswith("00"):
        phone = phone[2:]

    if phone.startswith("0"):
        phone = phone[1:]

    # Prepend the country code if not already present
    try:
        parsed_number = phonenumbers.parse(phone, country_code)

        # Get the international dialing code and format the number
        code = str(parsed_number).split(" ")[2].strip()
        custom_format = f"(+{code}) {phone}"

        return custom_format

    except phonenumberutil.NumberParseException:
        return "Could not parse the phone number"



def extract_emails(text_input):
    """
    Extracts an email address from a single text string.

    Args:
        text_input (str): A single string potentially containing an email address.

    Returns:
        str: The first extracted email address found in the string, or None if no email is found.
    """
    # Check if text_input is None
    if text_input is None:
        return None

    # Regular expression pattern to match email addresses
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'

    # Find all matches in the input string
    emails = re.findall(email_pattern, text_input)
    
    # Return the first email found, or None if no emails were found
    return emails[0] if emails else None


def get_country_from_city(city_name_input, retries=3, delay=2):
    """
    Returns the country name based on a provided city name, with retry logic for handling timeouts.

    Args:
        city_name_input (str): A single city name as a string.
        retries (int): Number of retry attempts in case of a timeout or service unavailability.
        delay (int): Delay in seconds between retry attempts.

    Returns:
        str: The country name corresponding to the input city, or an error message if not found.
    """
    # Check if the input is valid (not None or empty)
    if not city_name_input:
        return "City not provided"

    # Initialize the Nominatim geolocator with an increased timeout
    geolocator = Nominatim(user_agent="city_to_country", timeout=5)

    # Remove any extra quotes from the city name if present
    city_name_input = city_name_input.strip("'")

    # Retry logic
    for attempt in range(retries):
        try:
            # Geocode the city name
            location = geolocator.geocode(city_name_input)
            
            # Check if location was found and extract country
            if location:
                country = location.address.split(",")[-1].strip()
                return country
            else:
                return "Country not found"
        
        except GeocoderUnavailable:
            print(f"Attempt {attempt + 1} failed. Retrying in {delay} seconds...")
            time.sleep(delay)

    return "Service unavailable after retries"

def car(op):
    if op == 0 :
        print()
        print("-------─────▄▌▐▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▌ ---------------------------------")
        print("-------───▄▄██▌█ ..Data Imported..      ---------------------------------")
        print("-------▄▄▄▌▐██▌█ ................       ---------------------------------")
        print("-------███████▌█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▌ ---------------------------------")
        print("-------▀(@)▀▀▀▀▀▀▀(@)(@)▀▀▀▀▀▀▀▀▀▀▀▀(@)▀---------------------------------")
        print()
    elif op == 1:
        print()
        print("--------------------  ▌▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▐▄─────-------")
        print("---------------------     ....Data Sent...  █▌██▄▄───-------")
        print("---------------------       ...........     █▌██ ▐▄▄▄-------")
        print("--------------------- ▌▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄█▌███████-------")
        print("--------------------- ▀(@)▀▀▀▀▀▀▀▀▀▀▀▀(@)(@)▀▀▀▀▀▀▀(@)▀-------")
        print()
    elif op == 2:
        print("\n" + "#" * 60)
        print("#" + " " * 20 + "Processing Information..." + " " * 13 + "#")
        print("#" + " " * 16 + "Please wait while we process the data." + " " * 4 + "#")
        print("#" * 60 + "\n")

import pandas as pd
import requests
import Tools
import hubspot as hubspot
from hubspot.crm.contacts import BatchInputSimplePublicObjectInputForCreate, ApiException

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
        Collects all contacts marked as 'allowed_to_collect' = true from HubSpot,
        sorts them alphabetically by 'lastname', and returns a list of sorted contacts.

        Returns:
            list: A sorted list of dictionaries, each containing contact details.
        """
        # Define contact properties to retrieve
        properties = [
            "firstname", "lastname", "raw_email", "country", "phone",
            "technical_test___create_date", "industry", "address", "hs_object_id"
        ]

        # Define initial request body with filter criteria
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
            "limit": 100,  # Set limit per page (adjust as needed)
            "after": 0     # Initial offset for pagination
        }

        all_contacts = []
        has_more = True

        while has_more:
            try:
                # Make POST request to fetch a page of contacts
                response = requests.post(self.endpoint, headers=self.headers, json=request_body)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Collect contact properties into a list of dictionaries
                contacts = data.get("results", [])
                collected_contacts = [
                    {prop: contact["properties"].get(prop, None) for prop in properties}
                    for contact in contacts
                ]
                all_contacts.extend(collected_contacts)

                # Check if there are more pages of data
                has_more = data.get("paging", {}).get("next", None)
                if has_more:
                    # Update request_body with the next page's offset
                    request_body["after"] = data["paging"]["next"]["after"]
                else:
                    break

            except requests.exceptions.RequestException as e:
                print("Error fetching contacts:", e)
                return None
    
        return all_contacts


    def transform(self, sorted_contacts):
        """
        Transforms contact data by extracting and cleaning properties such as emails,
        merging duplicates, and formatting phone numbers and country codes.
        
        Args:
            sorted_contacts (list): List of sorted contact dictionaries.
        
        Returns:
            pd.DataFrame: DataFrame of transformed contact data.
        """
        # Step 1: Extract and clean emails
        for row in sorted_contacts:
            if 'raw_email' in row and row['raw_email']:
                row['raw_email'] = Tools.extract_emails(row['raw_email'])
                
        # Step 2: Convert to DataFrame and merge duplicates
        transformed_df = pd.DataFrame(sorted_contacts)
        transformed_df = Tools.merge_duplicates(transformed_df)

        # Step 3: Further transformations for city-country, country code, and phone formatting
        for index, row in transformed_df.iterrows():
            # Set city and transform country/country code
            if pd.notna(row['country']):
                city = row['country']
                transformed_df.at[index, 'city'] = city
                country = Tools.get_country_from_city(city) if city else None
                transformed_df.at[index, 'country'] = country if country else "Country not found"
                
                # Get country code if country is valid
                country_code = Tools.get_country_code_from_city(country) if country else None
                transformed_df.at[index, 'country_code'] = country_code

                # Format phone number if country code is available
                if pd.notna(row['phone']) and country_code:
                    formatted_phone = Tools.format_phone_number(row['phone'], country_code)
                    transformed_df.at[index, 'phone'] = formatted_phone

        # Replace NaN values with an empty string to ensure compatibility with JSON
        transformed_df = transformed_df.replace({pd.NA: "", np.nan: ""})

        # Save the transformed data to CSV after all transformations
        transformed_df.to_csv("contacts_data_transformation.csv", index=False)
        print("Data saved to contacts_data_transformation.csv")

        return transformed_df



    def load(self, df):
        """
        Loads (creates) multiple contacts in HubSpot in batches of 100 from a DataFrame.

        Args:
            df (pd.DataFrame): DataFrame with columns for HubSpot contact properties.
        """
        batch_size = 100  # HubSpot's max limit for batch API calls
        total_contacts = len(df)

        # Replace NaN values in the DataFrame with empty strings before batching
        df = df.fillna("")

        # Iterate through the DataFrame in chunks of batch_size
        for start in range(0, total_contacts, batch_size):
            # Slice the DataFrame to get the current batch
            batch_df = df.iloc[start:start + batch_size]
            batch_contacts = []

            for _, row in batch_df.iterrows():
                # Convert creation date to timestamp in milliseconds if not empty
                create_date = pd.to_datetime(row.get("technical_test___create_date")).value // 10**6 if row.get("technical_test___create_date") else ""

                # Prepare each contact's data for batch creation
                contact_data = {
                    "properties": {
                        "email": row.get("raw_email", ""),
                        "phone": row.get("phone", ""),
                        "country": row.get("country", ""),
                        "city": row.get("city", ""),
                        "firstname": row.get("firstname", ""),
                        "lastname": row.get("lastname", ""),
                        "address": row.get("address", ""),
                        "original_create_date": create_date,
                        "original_industry": ";" + ";".join(row.get("industry", "").split(";")) if row.get("industry") else "",
                        "temporary_id": row.get("hs_object_id", ""),
                        "hs_country_region_code": row.get("country_code", "")
                    }
                }
                batch_contacts.append(contact_data)

            # Batch input for creating contacts in HubSpot
            batch_input = BatchInputSimplePublicObjectInputForCreate(inputs=batch_contacts)

            try:
                api_response = self.client.crm.contacts.batch_api.create(
                    batch_input_simple_public_object_input_for_create=batch_input
                )
                show_errors(api_response)
                print(f"Batch creation of {len(batch_contacts)} contacts completed successfully.")
            except ApiException as e:
                print("Exception when calling batch_api->create:", e)


    

