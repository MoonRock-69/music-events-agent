from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import asyncpg
import os
from datetime import datetime, timedelta
import re
from typing import List, Optional, Dict
import json
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
import logging
from ticketmaster import TicketmasterScraper

# Konfiguracja logowania
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Music Events Agent", description="Agent dla eventów muzyki elektronicznej")

# Współrzędne Wrocławia jako punkt centralny
WROCLAW_COORDS = (51.1079, 17.0385)
MAX_DISTANCE_KM = 700

# Lista monitorowanych artystów
TARGET_ARTISTS = [
    "Tiësto", "David Guetta", "Martin Garrix", "Armin van Buuren",
    "Calvin Harris", "Hardwell", "Dimitri Vegas & Like Mike",
    "Fisher", "Charlotte de Witte", "Carl Cox", "Adam Beyer",
    "Amelie Lens", "Tale Of Us", "Boris Brejcha", "Kolsch"
]

# Konfiguracja bazy danych
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is required")

class EventScraper:
    def __init__(self):
        self.geolocator = Nominatim(user_agent="music-events-agent")
        self.session = None
    
    async def get_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession(
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                },
                timeout=aiohttp.ClientTimeout(total=30)
            )
        return self.session
    
    async def close_session(self):
        if self.session:
            await self.session.close()
            self.session = None
    
    def get_city_coordinates(self, city_name: str) -> Optional[tuple]:
        try:
            location = self.geolocator.geocode(city_name)
            if location:
                return (location.latitude, location.longitude)
        except Exception as e:
            logger.error(f"Błąd geokodowania dla {city_name}: {e}")
        return None
    
    def is_within_range(self, city_coords: tuple) -> bool:
        try:
            distance = geodesic(WROCLAW_COORDS, city_coords).kilometers
            return distance <= MAX_DISTANCE_KM
        except Exception:
            return False
    
    async def scrape_eventim_pl(self) -> List[Dict]:
        events = []
        session = await self.get_session()
        
        for artist in TARGET_ARTISTS:
            try:
                search_url = f"https://www.eventim.pl/search/?term={artist.replace(' ', '+')}"
                async with session.get(search_url) as response:
                    if response.status == 200:
                        html = await response.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        # Parsing eventów z Eventim.pl
                        event_elements = soup.find_all(['div', 'article'], class_=re.compile(r'event|item|card'))
                        
                        for element in event_elements[:5]:  # Maksymalnie 5 eventów na artystę
                            event_data = self.parse_eventim_event(element, artist, 'eventim.pl')
                            if event_data:
                                events.append(event_data)
                
                await asyncio.sleep(1)  # Rate limiting
            except Exception as e:
                logger.error(f"Błąd scraping Eventim.pl dla {artist}: {e}")
        
        return events
    
    async def scrape_eventim_de(self) -> List[Dict]:
        events = []
        session = await self.get_session()
        
        for artist in TARGET_ARTISTS:
            try:
                search_url = f"https://www.eventim.de/search/?term={artist.replace(' ', '+')}"
                async with session.get(search_url) as response:
                    if response.status == 200:
                        html = await response.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        event_elements = soup.find_all(['div', 'article'], class_=re.compile(r'event|item|card'))
                        
                        for element in event_elements[:5]:
                            event_data = self.parse_eventim_event(element, artist, 'eventim.de')
                            if event_data:
                                events.append(event_data)
                
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Błąd scraping Eventim.de dla {artist}: {e}")
        
        return events
    
    def parse_eventim_event(self, element, artist: str, source: str) -> Optional[Dict]:
        try:
            # Tytuł wydarzenia
            title_elem = element.find(['h1', 'h2', 'h3', 'h4'], class_=re.compile(r'title|name|heading'))
            title = title_elem.get_text(strip=True) if title_elem else f"{artist} - Event"
            
            # Data
            date_elem = element.find(['time', 'span', 'div'], class_=re.compile(r'date|time'))
            date_str = date_elem.get_text(strip=True) if date_elem else ""
            
            # Miasto
            location_elem = element.find(['span', 'div'], class_=re.compile(r'location|venue|city'))
            location = location_elem.get_text(strip=True) if location_elem else "Unknown"
            
            # Link do biletu
            link_elem = element.find('a', href=True)
            ticket_link = ""
            if link_elem:
                href = link_elem['href']
                if href.startswith('/'):
                    ticket_link = f"https://www.{source}{href}"
                else:
                    ticket_link = href
            
            # Sprawdzenie czy miasto jest w zasięgu
            city_coords = self.get_city_coordinates(location)
            if city_coords and not self.is_within_range(city_coords):
                return None
            
            return {
                'title': title,
                'artist': artist,
                'date_str': date_str,
                'location': location,
                'source': source,
                'ticket_link': ticket_link,
                'coordinates': city_coords,
                'scraped_at': datetime.now().isoformat()
            }
        
        except Exception as e:
            logger.error(f"Błąd parsowania eventu: {e}")
            return None

# Database functions
async def get_db_connection():
    return await asyncpg.connect(DATABASE_URL)

async def init_database():
    conn = await get_db_connection()
    try:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                title VARCHAR(500),
                artist VARCHAR(200),
                date_str VARCHAR(100),
                location VARCHAR(200),
                source VARCHAR(100),
                ticket_link TEXT,
                coordinates_lat FLOAT,
                coordinates_lon FLOAT,
                scraped_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        await conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_events_artist ON events(artist);
            CREATE INDEX IF NOT EXISTS idx_events_location ON events(location);
            CREATE INDEX IF NOT EXISTS idx_events_scraped_at ON events(scraped_at);
        ''')
        
        logger.info("Database initialized successfully")
    finally:
        await conn.close()

async def save_events_to_db(events: List[Dict]):
    if not events:
        return
    
    conn = await get_db_connection()
    try:
        # Wyczyść stare dane (starsze niż 7 dni)
        await conn.execute(
            "DELETE FROM events WHERE scraped_at < $1",
            datetime.now() - timedelta(days=7)
        )
        
        # Dodaj nowe eventy
        for event in events:
            coords_lat = event['coordinates'][0] if event['coordinates'] else None
            coords_lon = event['coordinates'][1] if event['coordinates'] else None
            
            await conn.execute('''
                INSERT INTO events (title, artist, date_str, location, source, ticket_link, 
                                  coordinates_lat, coordinates_lon, scraped_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ''', event['title'], event['artist'], event['date_str'], event['location'],
                event['source'], event['ticket_link'], coords_lat, coords_lon, 
                datetime.fromisoformat(event['scraped_at']))
        
        logger.info(f"Saved {len(events)} events to database")
    finally:
        await conn.close()

# API Endpoints
@app.on_event("startup")
async def startup_event():
    await init_database()

@app.get("/")
async def root():
    return FileResponse('static/index.html')

@app.get("/events")
async def get_events(
    artist: Optional[str] = Query(None, description="Filter by artist"),
    location: Optional[str] = Query(None, description="Filter by location"),
    limit: int = Query(50, le=100)
):
    conn = await get_db_connection()
    try:
        query = "SELECT * FROM events WHERE 1=1"
        params = []
        
        if artist:
            query += " AND LOWER(artist) LIKE LOWER($" + str(len(params) + 1) + ")"
            params.append(f"%{artist}%")
        
        if location:
            query += " AND LOWER(location) LIKE LOWER($" + str(len(params) + 1) + ")"
            params.append(f"%{location}%")
        
        query += " ORDER BY scraped_at DESC LIMIT $" + str(len(params) + 1)
        params.append(limit)
        
        rows = await conn.fetch(query, *params)
        
        events = []
        for row in rows:
            events.append({
                'id': row['id'],
                'title': row['title'],
                'artist': row['artist'],
                'date_str': row['date_str'],
                'location': row['location'],
                'source': row['source'],
                'ticket_link': row['ticket_link'],
                'coordinates': [row['coordinates_lat'], row['coordinates_lon']] if row['coordinates_lat'] else None,
                'scraped_at': row['scraped_at'].isoformat() if row['scraped_at'] else None
            })
        
        return {"events": events, "total": len(events)}
    
    finally:
        await conn.close()

@app.get("/artists")
async def get_artists():
    return {"artists": TARGET_ARTISTS}

@app.get("/stats")
async def get_stats():
    conn = await get_db_connection()
    try:
        total_events = await conn.fetchval("SELECT COUNT(*) FROM events")
        unique_artists = await conn.fetchval("SELECT COUNT(DISTINCT artist) FROM events")
        unique_locations = await conn.fetchval("SELECT COUNT(DISTINCT location) FROM events")
        
        return {
            "total_events": total_events,
            "unique_artists": unique_artists,
            "unique_locations": unique_locations,
            "monitored_artists": len(TARGET_ARTISTS)
        }
    finally:
        await conn.close()

@app.post("/scrape")
async def manual_scrape():
    scraper = EventScraper()
    try:
        logger.info("Starting manual scrape...")
        
        # Scraping z różnych źródeł
        eventim_pl_events = await scraper.scrape_eventim_pl()
        eventim_de_events = await scraper.scrape_eventim_de()

 	# Scraping z Ticketmaster
    tm_scraper = TicketmasterScraper()
    ticketmaster_events = await tm_scraper.scrape_events(TARGET_ARTISTS)

    all_events = eventim_pl_events + eventim_de_events + ticketmaster_events
      
        # Zapisz do bazy danych
        await save_events_to_db(all_events)
        
        return {
            "message": "Scraping completed successfully",
            "total_events_found": len(all_events),
            "eventim_pl": len(eventim_pl_events),
            "eventim_de": len(eventim_de_events)
        }
    
    except Exception as e:
        logger.error(f"Scraping error: {e}")
        raise HTTPException(status_code=500, detail=f"Scraping failed: {str(e)}")
    
    finally:
        await scraper.close_session()

# Serve static files
app.mount("/static", StaticFiles(directory="static"), name="static")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

