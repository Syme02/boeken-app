from .database import get_db_connection
import bcrypt
from flask import session
import logging

# Configureer logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def is_admin():
    role = session.get('role')
    logger.debug(f"is_admin check - Session role: {role}")
    return role == 'admin'

def register_user(form):
    username = form.get('username', '').strip()
    password = form.get('password', '').strip()
    
    if not username or not password:
        return False, 'Gebruikersnaam en wachtwoord zijn verplicht!'
    if len(password) < 8:
        return False, 'Wachtwoord moet minimaal 8 tekens lang zijn!'
    
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT id FROM users WHERE username = ?', (username,))
    if c.fetchone():
        conn.close()
        return False, 'Gebruikersnaam is al in gebruik!'
    
    hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    try:
        c.execute('INSERT INTO users (username, password, role) VALUES (?, ?, ?)', (username, hashed_password, 'user'))
        conn.commit()
        conn.close()
        return True, 'Gebruiker succesvol geregistreerd! Log nu in.'
    except Exception as e:
        conn.close()
        return False, f'Fout bij registreren: {str(e)}'

def login_user(form):
    username = form.get('username', '').strip()
    password = form.get('password', '').strip()
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT id, username, password, role FROM users WHERE username = ?', (username,))
    user = c.fetchone()
    conn.close()
    
    if user and bcrypt.checkpw(password.encode('utf-8'), user[2].encode('utf-8')):  # Index 2 voor password
        session['user_id'] = user[0]  # Index 0 voor id
        session['username'] = user[1]  # Index 1 voor username
        session['role'] = user[3]     # Index 3 voor role
        logger.debug(f"Login successful - Session: {session}")
        return True, 'Succesvol ingelogd!'
    logger.debug(f"Login failed - User: {user}, Password check failed")
    return False, 'Ongeldige gebruikersnaam of wachtwoord!'