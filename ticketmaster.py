import os
import aiohttp
from datetime import datetime
from typing import List, Dict, Optional
from geopy.distance import geodesic

# Współrzędne Wrocławia jako punkt centralny
WROCLAW_COORDS = (51.1079, 17.0385)
MAX_DISTANCE_KM = 1000

class TicketmasterScraper:
    def __init__(self):
        self.api_key = os.getenv("TM_API_KEY")
        self.base_url = "https://app.ticketmaster.com/discovery/v2/events"
        
    async def scrape_events(self, artists: List[str]) -> List[Dict]:
        if not self.api_key:
            return []
            
        all_events = []
        
        async with aiohttp.ClientSession() as session:
            for artist in artists:
                events = await self.search_artist_events(session, artist)
                all_events.extend(events)
                
        return all_events
    
    async def search_artist_events(self, session: aiohttp.ClientSession, artist: str) -> List[Dict]:
        params = {
            "apikey": self.api_key,
            "keyword": artist,
            "size": 100,
            "countryCode": "PL,DE,CZ,SK",
            "classificationName": "Music",
            "sort": "date,asc"
        }
        
        try:
            async with session.get(self.base_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    print(f"DEBUG: Found {len(data.get('_embedded', {}).get('events', []))} events for {artist}")
                    return self.parse_events(data, artist)
                else:
                print(f"DEBUG: API returned status {response.status} for {artist}")
        except Exception as e:
            print(f"Error fetching events for {artist}: {e}")
            
        return []
    
    def parse_events(self, data: dict, artist: str) -> List[Dict]:
        events = []
        
        if "_embedded" not in data or "events" not in data["_embedded"]:
            return events
            
        for event in data["_embedded"]["events"]:
            try:
                # Podstawowe informacje o evencie
                name = event.get("name", "")
                url = event.get("url", "")
                
                # Data
                dates = event.get("dates", {}).get("start", {})
                date_str = dates.get("localDate", "")
                
                # Venue i lokalizacja
                venues = event.get("_embedded", {}).get("venues", [])
                if venues:
                    venue = venues[0]
                    city = venue.get("city", {}).get("name", "")
                    country = venue.get("country", {}).get("countryCode", "")
                    location = f"{city}, {country}"
                    
                    # Sprawdź odległość od Wrocławia
                    if venue.get("location"):
                        lat = float(venue["location"].get("latitude", 0))
                        lon = float(venue["location"].get("longitude", 0))
                        
                        if lat and lon:
                            distance = geodesic(WROCLAW_COORDS, (lat, lon)).kilometers
                            print(f"DEBUG: {city} distance: {distance}km")
                            if distance > MAX_DISTANCE_KM:
                                print(f"DEBUG: Skipping {city} - too far ({distance}km)")
                                continue
                            
                            coordinates = (lat, lon)
                        else:
                            coordinates = None
                    else:
                        coordinates = None
                else:
                    location = "Unknown"
                    coordinates = None
                
                events.append({
                    "title": name,
                    "artist": artist,
                    "date_str": date_str,
                    "location": location,
                    "source": "ticketmaster",
                    "ticket_link": url,
                    "coordinates": coordinates,
                    "scraped_at": datetime.now().isoformat()
                })
                
            except Exception as e:
                print(f"Error parsing event: {e}")
                continue
                

        return events

