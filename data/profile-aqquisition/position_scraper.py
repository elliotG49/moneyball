import requests
from bs4 import BeautifulSoup
from urllib.parse import quote
import argparse
import re

def get_wikipedia_player_page(full_name):
    # Replace spaces with underscores and URL-encode the full name.
    slug = quote(full_name.replace(" ", "_"))
    url = f"https://en.wikipedia.org/wiki/{slug}"
    return url

def extract_playing_position(html_content):
    soup = BeautifulSoup(html_content, "lxml")
    # Find the infobox table (it typically has class 'infobox vcard')
    infobox = soup.find("table", {"class": "infobox"})
    if not infobox:
        return None
    # Loop through the rows looking for the "Playing position" or similar key.
    for row in infobox.find_all("tr"):
        header = row.find("th")
        if header and "position" in header.text.lower():
            cell = row.find("td")
            if cell:
                # Remove all <sup> tags (which are typically used for references)
                for sup in cell.find_all("sup"):
                    sup.decompose()
                    
                position_text = cell.get_text(separator=", ").strip()
                # Clean up the position text by removing extra commas or spaces.
                position_text = re.sub(r"\s*,\s*", ", ", position_text)
                position_text = re.sub(r",\s*,", ",", position_text)
                
                # Split the text by commas and filter out any empty strings.
                positions = [pos.strip() for pos in position_text.split(",") if pos.strip()]
                if len(positions) > 1:
                    return ", ".join(positions)
                return positions[0]
    return None


def get_player_position(full_name):
    url = get_wikipedia_player_page(full_name)
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error: Received status code {response.status_code} for URL: {url}")
        return None
    position = extract_playing_position(response.content)
    return position


# Argument parsing for the full player name
parser = argparse.ArgumentParser(description='Get football player position from Wikipedia.')
parser.add_argument('name', type=str, help='Full player name (e.g. "Virgil van Dijk")')

args = parser.parse_args()

position = get_player_position(args.name)
if position:
    print(f"{args.name} plays as: {position}")
else:
    print(f"Could not retrieve position for {args.name}.")
