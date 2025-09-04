import pandas as pd
import time
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from geopy.exc import GeocoderTimedOut
from .database import get_db_connection
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# -----------------------------
# Helper functions
# -----------------------------

def get_user_books(user_id):
    """Haal alle boeken van een gebruiker op en zet prijzen/pagina's om naar juiste types"""
    conn = get_db_connection()
    try:
        df = pd.read_sql_query('SELECT * FROM books WHERE user_id = ?', conn, params=(user_id,))
        logger.debug(f"Retrieved {len(df)} books for user {user_id}")
    except Exception as e:
        logger.error(f"Database error in get_user_books: {e}")
        conn.close()
        return pd.DataFrame()

    if not df.empty:
        df['prijs'] = pd.to_numeric(df.get('prijs', 0), errors='coerce').fillna(0).astype(float)
        df['paginas'] = pd.to_numeric(df.get('paginas', 0), errors='coerce').fillna(0).astype(int)
        if 'auteur_voornaam' in df.columns and 'auteur_achternaam' in df.columns:
            df['auteur'] = df['auteur_voornaam'] + " " + df['auteur_achternaam']
    return df

def generate_charts(df):
    """Genereer data voor grafieken"""
    charts = {}
    if df.empty:
        return charts

    if 'genre' in df.columns:
        counts = df['genre'].value_counts()
        charts['genre'] = {'labels': counts.index.tolist(), 'data': counts.values.tolist()}

    if 'gelezen' in df.columns:
        counts = df['gelezen'].value_counts()
        charts['gelezen'] = {'labels': counts.index.tolist(), 'data': counts.values.tolist()}

    if 'taal' in df.columns:
        counts = df['taal'].value_counts()
        charts['taal'] = {'labels': counts.index.tolist(), 'data': counts.values.tolist()}

    if 'paginas' in df.columns:
        pages = df['paginas'].dropna()
        if not pages.empty:
            hist = pd.cut(pages, bins=20, include_lowest=True)
            counts = hist.value_counts().sort_index()
            charts['paginas'] = {
                'labels': [f"{int(interval.left)}-{int(interval.right)}" for interval in counts.index],
                'data': counts.tolist()
            }

    if 'auteur' in df.columns:
        counts = df['auteur'].value_counts().head(10)
        charts['auteur'] = {'labels': counts.index.tolist(), 'data': counts.values.tolist()}

    if 'genre' in df.columns and 'prijs' in df.columns:
        avg_price = df.groupby('genre')['prijs'].mean().round(2)
        charts['avg_price'] = {'labels': avg_price.index.tolist(), 'data': avg_price.values.tolist()}

    if 'land' in df.columns:
        counts = df[df['land'].notnull() & (df['land'] != '')]['land'].value_counts()
        charts['land'] = {'labels': counts.index.tolist(), 'data': counts.values.tolist()}

    return charts

def get_location_coords(df):
    """Geocode unieke landen en sla op in geocache"""
    location_coords = {}
    conn = get_db_connection()
    c = conn.cursor()
    geolocator = Nominatim(user_agent="boeken_app")

    if 'land' not in df.columns:
        return location_coords

    locations = df[df['land'].notnull() & (df['land'] != '')]['land'].unique()
    for loc in locations:
        loc_clean = loc.strip()
        if not loc_clean:
            continue

        # Check cache
        c.execute('SELECT lat, lon FROM geocache WHERE location = ?', (loc_clean,))
        result = c.fetchone()
        if result:
            location_coords[loc_clean] = (result[0], result[1])
        else:
            try:
                time.sleep(1)  # Rate limiting
                geo = geolocator.geocode(loc_clean, country_codes='nl,be,gb,it,de,at,ch', timeout=5)
                if geo:
                    location_coords[loc_clean] = (geo.latitude, geo.longitude)
                    c.execute('INSERT INTO geocache (location, lat, lon) VALUES (?, ?, ?)',
                              (loc_clean, geo.latitude, geo.longitude))
                    conn.commit()
                else:
                    logger.debug(f"No coordinates for {loc_clean}")
            except (GeocoderTimedOut, Exception) as e:
                logger.error(f"Geocoding error for {loc_clean}: {e}")
    conn.close()
    return location_coords

def generate_fun_facts(df, location_coords):
    """Genereer leuke feitjes over de boeken"""
    fun_facts = []

    if df.empty:
        return fun_facts

    # Dikste boek
    if 'paginas' in df.columns and df['paginas'].notna().any():
        dikste = df.loc[df['paginas'].idxmax()]
        fun_facts.append(f"Je dikste boek is '{dikste['titel']}' met {int(dikste['paginas'])} pagina's.")

    # Duurste boek
    if 'prijs' in df.columns and df['prijs'].notna().any():
        duurste = df.loc[df['prijs'].idxmax()]
        fun_facts.append(f"Het duurste boek is '{duurste['titel']}' voor €{round(duurste['prijs'], 2)}.")

    # Talen
    if 'taal' in df.columns:
        talen = df['taal'].nunique()
        if talen > 1:
            fun_facts.append(f"Je hebt boeken in {talen} verschillende talen!")

    # Totaal aantal boeken
    fun_facts.append(f"Totaal aantal boeken in je collectie: {len(df)}.")

    # Verste afstand tussen boeken
    if location_coords and len(location_coords) >= 2:
        max_distance = 0
        title_pair = (None, None)
        loc_list = list(location_coords.items())
        for i in range(len(loc_list)):
            loc1, coord1 = loc_list[i]
            book1 = df[df['land'] == loc1].iloc[0]
            for j in range(i + 1, len(loc_list)):
                loc2, coord2 = loc_list[j]
                book2 = df[df['land'] == loc2].iloc[0]
                distance = geodesic(coord1, coord2).kilometers
                if distance > max_distance:
                    max_distance = distance
                    title_pair = (book1['titel'], book2['titel'])
                    loc_pair = (loc1, loc2)
        if title_pair[0] and title_pair[1]:
            fun_facts.append(
                f"De verste afstand tussen twee boeken is {round(max_distance,2)} km, "
                f"tussen '{title_pair[0]}' ({loc_pair[0]}) en '{title_pair[1]}' ({loc_pair[1]})."
            )

    # Oudste boek
    if 'publicatie_jaar' in df.columns and df['publicatie_jaar'].notna().any():
        oldest = df.loc[df['publicatie_jaar'].idxmin()]
        fun_facts.append(f"Je oudste boek is '{oldest['titel']}' uit {int(oldest['publicatie_jaar'])}.")

    return fun_facts
