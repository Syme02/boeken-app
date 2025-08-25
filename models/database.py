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

    # Create books table
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

    # Create users table
    c.execute('''CREATE TABLE IF NOT EXISTS users (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 username TEXT UNIQUE NOT NULL,
                 password TEXT NOT NULL,
                 role TEXT NOT NULL,
                 color TEXT DEFAULT '#e31c73',
                 dark_mode INTEGER DEFAULT 1
             )''')

    # Add color and dark_mode columns if they don't exist
    c.execute("PRAGMA table_info(users)")
    columns = [col['name'] for col in c.fetchall()]
    if 'color' not in columns:
        print("Adding 'color' column to users table...")
        c.execute("ALTER TABLE users ADD COLUMN color TEXT DEFAULT '#e31c73'")
    if 'dark_mode' not in columns:
        print("Adding 'dark_mode' column to users table...")
        c.execute("ALTER TABLE users ADD COLUMN dark_mode INTEGER DEFAULT 1")

    # Drop settings table if it exists (no longer needed)
    c.execute('DROP TABLE IF EXISTS settings')

    # Insert default admin user if not exists
    c.execute('SELECT * FROM users WHERE username = ?', ('admin',))
    if not c.fetchone():
        hashed_password = bcrypt.hashpw('admin123'.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        c.execute('INSERT INTO users (username, password, role, color, dark_mode) VALUES (?, ?, ?, ?, ?)', 
                  ('admin', hashed_password, 'admin', '#2563eb', 1))

    conn.commit()
    conn.close()
    print("Database initialized successfully.")