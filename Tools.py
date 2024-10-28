from geopy.geocoders import Nominatim
import re

import pandas as pd
from datetime import datetime

import phonenumbers
from phonenumbers import geocoder, carrier

import pycountry

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


    # First Step: Merge duplicates based on Full Name
    df = merge_by_key(df, "Full Name")

    # Second Step: Merge duplicates based on Email
    df = merge_by_key(df, "Email")

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

    if "Full Name" in key:
        # Create a unique identifier based on email and full name
        df["unique_id"] = (df['firstname']+df['lastname']).fillna("")
        # Group by the specified key to detect duplicates
        grouped = df.groupby("unique_id")
    elif "Email" in key:
        # Group by the specified key to detect duplicates
        grouped = df.groupby('raw_email')
    

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
            # Fill missing fields in the primary record
            for column in ["address", "industry"]:
                if pd.isna(primary_record[column]) and pd.notna(row[column]):
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

        code = str(parsed_number).split(" ")[2].strip()
        
        custom_format= f"(+{code}) {phone}"

        return custom_format

    except phonenumbers.phonenumberutil.NumberParseException:
        return "Could not parse the phone number"



def extract_emails(text_input):
    """
    Extracts email addresses from a string or list of text strings.

    Args:
        text_input (str or list): A single string or list of text strings potentially containing email addresses.

    Returns:
        list: A list of extracted email addresses.
    """
    # Ensure the input is a list
    if isinstance(text_input, str):
        text_input = [text_input]

    # Regular expression pattern to match email addresses
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'

    # Extract and collect emails that match the pattern
    extracted_emails = []
    for text in text_input:
        emails = re.findall(email_pattern, text)
        extracted_emails.extend(emails)

    return extracted_emails


def get_country_from_city(city_name_input):
    """
    Returns a list of countries based on provided city names.

    Args:
        city_name_input (str or list): A single city name or a list of city names as strings.

    Returns:
        list: A list of country names corresponding to the input cities.
    """
    # Ensure the input is a list
    if isinstance(city_name_input, str):
        city_name_input = [city_name_input]

    # Initialize the Nominatim geolocator (moved outside the loop to avoid redundant calls)
    geolocator = Nominatim(user_agent="city_to_country")

    # Initialize list to store extracted countries
    extracted_countries = []

    # Loop through each city name in the list
    for city in city_name_input:
        # Remove extra quotes from city names if present
        city = city.strip("'")

        # Geocode the city name
        location = geolocator.geocode(city)
        
        # Check if location was found and extract country
        if location:
            # Get the last part of the address (assumed to be the country)
            country = location.address.split(",")[-1].strip()
            extracted_countries.append(country)
        else:
            extracted_countries.append("Country not found")
        
    return extracted_countries

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
        print("#" + " " * 20 + "Processing Information..." + " " * 19 + "#")
        print("#" + " " * 16 + "Please wait while we process the data." + " " * 15 + "#")
        print("#" * 60 + "\n")