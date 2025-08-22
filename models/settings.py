from .database import get_db_connection

def get_settings():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT color, dark_mode FROM settings WHERE id = 1')
    result = c.fetchone()
    conn.close()
    return {'color': result[0], 'dark_mode': bool(result[1])}  # Tuple-indexatie

def update_settings(color=None, dark_mode=None):
    conn = get_db_connection()
    c = conn.cursor()
    if color:
        c.execute('UPDATE settings SET color = ? WHERE id = 1', (color,))
    if dark_mode is not None:
        c.execute('UPDATE settings SET dark_mode = ? WHERE id = 1', (int(dark_mode),))
    conn.commit()
    conn.close()

def get_color_from_position(x, canvas_width):
    """Bereken een hex-kleurcode op basis van de x-positie in een RGB-gradient."""
    # Normaliseer de x-positie naar 0-1 (relatief aan canvas-breedte)
    ratio = x / canvas_width if canvas_width > 0 else 0
    ratio = max(0, min(1, ratio))  # Beperk tot 0-1
    
    # Creëer een eenvoudige RGB-gradient (rood naar violet)
    r = int(255 * (1 - ratio))  # Rood vermindert van 255 naar 0
    g = 0  # Groen blijft 0 voor een simpele overgang
    b = int(255 * ratio)  # Blauw neemt toe van 0 naar 255
    
    # Converteer RGB naar hex
    return f'#{r:02x}{g:02x}{b:02x}'