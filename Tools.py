from geopy.geocoders import Nominatim
import re

import phonenumbers
from phonenumbers import geocoder, carrier

import pycountry

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