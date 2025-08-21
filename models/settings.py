from .database import get_db_connection

def get_settings():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT color, dark_mode FROM settings WHERE id = 1')
    result = c.fetchone()
    conn.close()
    return {'color': result['color'], 'dark_mode': bool(result['dark_mode'])}

def update_settings(color=None, dark_mode=None):
    conn = get_db_connection()
    c = conn.cursor()
    if color:
        c.execute('UPDATE settings SET color = ? WHERE id = 1', (color,))
    if dark_mode is not None:
        c.execute('UPDATE settings SET dark_mode = ? WHERE id = 1', (int(dark_mode),))
    conn.commit()
    conn.close()