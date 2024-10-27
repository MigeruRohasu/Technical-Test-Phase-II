import requests

# Define the API key and endpoint URL
api_key = "pat-na1-3c7b0af9-bb66-40e7-a256-ce4c5eb27e81"  # actual HubSpot API key
endpoint = "https://api.hubapi.com/crm/v3/objects/contacts/search"

# Set up headers for authentication and data format
headers = {
    "Authorization": f"Bearer {api_key}",
    "Content-Type": "application/json"
}

def collect_and_sort_contacts():
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
        
        # Display organized contacts
        print("Organized Contacts:",sorted_contacts)
        for contact in sorted_contacts:
            print(contact)
        
        return sorted_contacts

    except requests.exceptions.RequestException as e:
        print("Error fetching contacts:", e)
        return None

# Execute the function to collect and sort contacts
sorted_contacts = collect_and_sort_contacts()
