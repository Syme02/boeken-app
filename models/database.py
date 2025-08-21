import sqlite3
import bcrypt

def get_db_connection():
    conn = sqlite3.connect('books.db')
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    print("Initializing database...")
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS books
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  titel TEXT,
                  auteur_voornaam TEXT,
                  auteur_achternaam TEXT,
                  genre TEXT,
                  prijs REAL,
                  paginas INTEGER,
                  bindwijze TEXT,
                  edition TEXT,
                  isbn TEXT,
                  reeks_nr TEXT,
                  uitgeverij TEXT,
                  serie TEXT,
                  staat TEXT,
                  taal TEXT,
                  gesigneerd TEXT,
                  gelezen TEXT,
                  added_date TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS settings
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  color TEXT,
                  dark_mode INTEGER)''')
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  username TEXT UNIQUE,
                  password TEXT,
                  role TEXT)''')
    c.execute('SELECT * FROM settings')
    if not c.fetchone():
        c.execute('INSERT INTO settings (color, dark_mode) VALUES (?, ?)', ('#15d49b', 0))
    c.execute('SELECT * FROM users WHERE username = ?', ('admin',))
    if not c.fetchone():
        hashed_password = bcrypt.hashpw('admin123'.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        c.execute('INSERT INTO users (username, password, role) VALUES (?, ?, ?)', ('admin', hashed_password, 'admin'))
    conn.commit()
    conn.close()
    print("Database initialized successfully.")