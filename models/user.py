from .database import get_db_connection
import bcrypt
from flask import session

def is_admin():
    return session.get('role') == 'admin'

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
    
    if user and bcrypt.checkpw(password.encode('utf-8'), user['password'].encode('utf-8')):
        session['user_id'] = user['id']
        session['username'] = user['username']
        session['role'] = user['role']
        return True, 'Succesvol ingelogd!'
    return False, 'Ongeldige gebruikersnaam of wachtwoord!'