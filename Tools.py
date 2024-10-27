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
    try:
        # Parse the phone number with the specified country code region
        parsed_number = phonenumbers.parse(phone, country_code)
        
        # Check if the number is valid
        if not phonenumbers.is_valid_number(parsed_number):
            return "Invalid phone number"

        # Format the number in international format
        international_format = phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.INTERNATIONAL)

        return international_format

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



# Example usage with a list of city names
cities = [
    "Winchester",
    "Oxford",
    "Waterford",
    "Cork"
]
countries = get_country_from_city(cities)
print("Extracted Countries:", countries)

# Example usage for email extraction with a list of strings
text_samples = [
    "'William <william_wilkinson881615937@dionrab.com> Contact Info.'",
    "'John Doe <john.doe123@example.com> More Info.'",
    "'Random text without email'",
    "'Alice <alice.smith456@anotherdomain.com> Contact'"
]
emails = extract_emails(text_samples)
print("Extracted Emails from list:", emails)



# Example usage with a single city string
single_city = "Dublin"
country = get_country_from_city(single_city)
print(f"The country for {single_city} is:", country)

# Example usage for email extraction with a single string
single_text = "'William <william_wilkinson498036762@cispeto.com> Contact Info.'"
emails = extract_emails(single_text)
print("Extracted Emails from single string:", emails)

# Example usage
country_code = get_country_code_from_city(single_city)
print(f"The country code for {single_city} is: {country_code}")

# Example usage
phone_number = "1161604327"  # An English-style number without country code
formatted_number = format_phone_number(phone_number, country_code)
print("Formatted Phone Number:", formatted_number)


"""
{'firstname': 'Wade', 'lastname': 'Thompson', 'raw_email': 'Wade <wade_thompson931319772@zorer.org> Contact Info.', 'country': 'Limerick', 'phone': '1-142-256-130', 'technical_test___create_date': '2021-08-29', 'industry': 'Animal feeds', 'address': 'West Grove, 8089', 'hs_object_id': '438151'}
{'firstname': 'Winnie', 'lastname': 'Walter', 'raw_email': 'Winnie <winnie_walter538064895@sheye.org> Contact Info.', 'country': 'Dublin', 'phone': '1-161-604-327', 'technical_test___create_date': '2021-02-10', 'industry': 'Dairy products', 'address': 'Chester      Crossroad, 7070', 'hs_object_id': '419852'}
{'firstname': 'Winnie', 'lastname': 'Walter', 'raw_email': 'Winnie <winnie_walter1068546463@naiker.biz> Contact Info.', 'country': 'Ireland', 'phone': '2-567-604-285', 'technical_test___create_date': '2021-10-10', 'industry': 'Dairy products', 'address': 'Collins  Lane, 3994', 'hs_object_id': '437452'}
"""