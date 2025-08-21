from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, session
import sqlite3
import pandas as pd
import os
import requests
from datetime import datetime
import json
from io import StringIO
import bcrypt

app = Flask(__name__)
app.secret_key = "your_secret_key"
app.config['SESSION_TYPE'] = 'filesystem'

# Database initialization
def init_db():
    print("Initializing database...")
    conn = sqlite3.connect('books.db')
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
    # Voeg standaard admin-gebruiker toe als deze niet bestaat
    c.execute('SELECT * FROM users WHERE username = ?', ('admin',))
    if not c.fetchone():
        hashed_password = bcrypt.hashpw('admin123'.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        c.execute('INSERT INTO users (username, password, role) VALUES (?, ?, ?)', ('admin', hashed_password, 'admin'))
    conn.commit()
    conn.close()
    print("Database initialized successfully.")

# Load CSV into SQLite
def load_csv_to_db(csv_source, overwrite=False):
    try:
        if not hasattr(csv_source, 'read'):
            raise ValueError("Alleen geüploade CSV-bestanden worden ondersteund.")
        
        encodings = ['utf-8-sig', 'iso-8859-1', 'windows-1252']
        df = None
        for encoding in encodings:
            try:
                csv_source.seek(0)
                df = pd.read_csv(StringIO(csv_source.read().decode(encoding)), sep=None, engine="python")
                print(f"Succes: CSV gelezen met encoding {encoding}")
                break
            except UnicodeDecodeError:
                print(f"Mislukt: Encoding {encoding} faalde")
                continue
        if df is None:
            raise ValueError("Geen geschikte encoding gevonden voor het geüploade CSV-bestand")

        print(f"Gelezen kolomnamen (voor verwerking): {list(df.columns)}")
        df.columns = [col.replace('\ufeff', '').strip() for col in df.columns]
        print(f"Kolomnamen na BOM-verwijdering: {list(df.columns)}")
        
        column_mapping = {
            'Titel': 'titel', 'titel': 'titel',
            'Auteur voornaam': 'auteur_voornaam', 'Auteur_voornaam': 'auteur_voornaam', 'auteur voornaam': 'auteur_voornaam',
            'Auteur achternaam': 'auteur_achternaam', 'Auteur_achternaam': 'auteur_achternaam', 'auteur achternaam': 'auteur_achternaam',
            'Genre': 'genre', 'genre': 'genre',
            'Prijs': 'prijs', 'prijs': 'prijs',
            "Pagina's": 'paginas', "pagina's": 'paginas', 'paginas': 'paginas',
            'Bindwijze': 'bindwijze', 'bindwijze': 'bindwijze',
            'Edition': 'edition', 'edition': 'edition',
            'ISBN': 'isbn', 'isbn': 'isbn',
            'Reeks nr': 'reeks_nr', 'Reeks_nr': 'reeks_nr', 'reeks nr': 'reeks_nr', 'reeks_nr': 'reeks_nr',
            'Uitgeverij': 'uitgeverij', 'uitgeverij': 'uitgeverij',
            'Serie': 'serie', 'serie': 'serie',
            'Staat': 'staat', 'staat': 'staat',
            'Taal': 'taal', 'taal': 'taal',
            'Gesigneerd': 'gesigneerd', 'gesigneerd': 'gesigneerd',
            'Gelezen': 'gelezen', 'gelezen': 'gelezen'
        }
        
        df.columns = [column_mapping.get(col.strip(), col.lower()) for col in df.columns]
        print(f"Genormaliseerde kolomnamen: {list(df.columns)}")
        
        required_columns = ['titel']
        missing_required = [col for col in required_columns if col not in df.columns]
        if missing_required:
            return False, f"Fout: Verplichte kolommen ontbreken in het CSV-bestand: {missing_required}"
        
        expected_columns = ['titel', 'auteur_voornaam', 'auteur_achternaam', 'genre', 'prijs', 'paginas', 
                            'bindwijze', 'edition', 'isbn', 'reeks_nr', 'uitgeverij', 'serie', 'staat', 
                            'taal', 'gesigneerd', 'gelezen']
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            print(f"Waarschuwing: De volgende optionele kolommen ontbreken in het CSV-bestand: {missing_columns}")
            for col in missing_columns:
                df[col] = ''
        
        df = df.drop_duplicates(subset=["titel", "isbn"], keep="first")
        print(f"Aantal boeken na duplicaatverwijdering: {len(df)}")
        
        if "prijs" in df.columns:
            df['prijs'] = df['prijs'].replace({r'€': '', r'\,': '.'}, regex=True)
            df['prijs'] = pd.to_numeric(df['prijs'], errors='coerce').fillna(0)
        if "paginas" in df.columns:
            df['paginas'] = pd.to_numeric(df['paginas'], errors='coerce').fillna(0)
        if "reeks_nr" in df.columns:
            df['reeks_nr'] = pd.to_numeric(df['reeks_nr'], errors='coerce').fillna(0)
        if 'added_date' not in df.columns:
            df['added_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        conn = sqlite3.connect('books.db')
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM books')
        existing_count = c.fetchone()[0]
        print(f"Aantal bestaande boeken in database: {existing_count}")
        
        if overwrite and existing_count > 0:
            c.execute('DELETE FROM books')
            print("Bestaande boeken verwijderd uit database")
            existing_count = 0
        
        if existing_count == 0 or overwrite:
            df.to_sql('books', conn, if_exists='append', index=False)
            print(f"Succes: {len(df)} boeken geïmporteerd")
            conn.commit()
            conn.close()
            return True, f"Succes: {len(df)} boeken geïmporteerd"
        else:
            for _, row in df.iterrows():
                c.execute('''SELECT COUNT(*) FROM books WHERE titel = ? AND isbn = ?''', (row['titel'], row['isbn']))
                if c.fetchone()[0] == 0:
                    c.execute('''INSERT INTO books (titel, auteur_voornaam, auteur_achternaam, genre, prijs, paginas, bindwijze, edition, isbn, reeks_nr, uitgeverij, serie, staat, taal, gesigneerd, gelezen, added_date)
                                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', tuple(row))
            conn.commit()
            conn.close()
            return True, f"Succes: Nieuwe boeken toegevoegd uit CSV"
    except Exception as e:
        return False, f"Fout bij importeren: {str(e)}"

# Initialize database
init_db()

# Get settings
def get_settings():
    conn = sqlite3.connect('books.db')
    c = conn.cursor()
    c.execute('SELECT color, dark_mode FROM settings WHERE id = 1')
    result = c.fetchone()
    conn.close()
    print(f"Settings geladen: {{'color': '{result[0]}', 'dark_mode': {bool(result[1])}}}")  # Debugging
    return {'color': result[0], 'dark_mode': bool(result[1])}

# Update settings
def update_settings(color=None, dark_mode=None):
    conn = sqlite3.connect('books.db')
    c = conn.cursor()
    if color:
        c.execute('UPDATE settings SET color = ? WHERE id = 1', (color,))
    if dark_mode is not None:
        c.execute('UPDATE settings SET dark_mode = ? WHERE id = 1', (int(dark_mode),))
    conn.commit()
    conn.close()

# Check if user is admin
def is_admin():
    return session.get('role') == 'admin'

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()
        role = request.form.get('role', 'user').strip()
        
        # Validaties
        if not username or not password:
            flash('Gebruikersnaam en wachtwoord zijn verplicht!', 'error')
            return redirect(url_for('register'))
        if len(password) < 8:
            flash('Wachtwoord moet minimaal 8 tekens lang zijn!', 'error')
            return redirect(url_for('register'))
        if role not in ['user', 'admin']:
            flash('Ongeldige rol geselecteerd!', 'error')
            return redirect(url_for('register'))
        if role == 'admin' and not is_admin():
            flash('Alleen admins kunnen nieuwe admins registreren!', 'error')
            return redirect(url_for('register'))
        
        # Controleer of gebruikersnaam al bestaat
        conn = sqlite3.connect('books.db')
        c = conn.cursor()
        c.execute('SELECT id FROM users WHERE username = ?', (username,))
        if c.fetchone():
            flash('Gebruikersnaam is al in gebruik!', 'error')
            conn.close()
            return redirect(url_for('register'))
        
        # Hash wachtwoord en sla gebruiker op
        hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        try:
            c.execute('INSERT INTO users (username, password, role) VALUES (?, ?, ?)', (username, hashed_password, role))
            conn.commit()
            flash('Gebruiker succesvol geregistreerd! Log nu in.', 'success')
            conn.close()
            return redirect(url_for('login'))
        except sqlite3.Error as e:
            flash(f'Fout bij registreren: {str(e)}', 'error')
            conn.close()
            return redirect(url_for('register'))
    
    return render_template('register.html', is_admin=is_admin())

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()
        conn = sqlite3.connect('books.db')
        c = conn.cursor()
        c.execute('SELECT id, username, password, role FROM users WHERE username = ?', (username,))
        user = c.fetchone()
        conn.close()
        
        if user and bcrypt.checkpw(password.encode('utf-8'), user[2].encode('utf-8')):
            session['user_id'] = user[0]
            session['username'] = user[1]
            session['role'] = user[3]
            flash('Succesvol ingelogd!', 'success')
            return redirect(url_for('index'))
        else:
            flash('Ongeldige gebruikersnaam of wachtwoord!', 'error')
            return redirect(url_for('login'))
    
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.pop('user_id', None)
    session.pop('username', None)
    session.pop('role', None)
    flash('Succesvol uitgelogd!', 'success')
    return redirect(url_for('index'))

@app.route('/', methods=['GET', 'POST'])
def index():
    if not session.get('user_id'):
        return redirect(url_for('login'))
    
    settings = get_settings()
    conn = sqlite3.connect('books.db')
    c = conn.cursor()
    filters = {}
    edit_book_data = {}
    
    if request.method == 'POST':
        if not is_admin() and request.form.get('action') in ['add', 'edit']:
            flash('Alleen admins kunnen boeken toevoegen of bewerken!', 'error')
            return redirect(url_for('index'))
        
        action = request.form.get('action', 'search')
        
        if action == 'add':
            data = {
                'titel': request.form.get('titel', ''),
                'auteur_voornaam': request.form.get('auteur_voornaam', ''),
                'auteur_achternaam': request.form.get('auteur_achternaam', ''),
                'genre': request.form.get('genre', ''),
                'prijs': float(request.form.get('prijs', 0)) if request.form.get('prijs', '') else 0.0,
                'paginas': int(request.form.get('paginas', 0)) if request.form.get('paginas', '') else 0,
                'bindwijze': request.form.get('bindwijze', ''),
                'edition': request.form.get('edition', ''),
                'isbn': request.form.get('isbn', ''),
                'reeks_nr': request.form.get('reeks_nr', ''),
                'uitgeverij': request.form.get('uitgeverij', ''),
                'serie': request.form.get('serie', ''),
                'staat': request.form.get('staat', ''),
                'taal': request.form.get('taal', ''),
                'gesigneerd': request.form.get('gesigneerd', ''),
                'gelezen': request.form.get('gelezen', ''),
                'added_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            if not data['titel']:
                flash("Titel is verplicht!", "error")
            else:
                try:
                    c.execute('''INSERT INTO books (titel, auteur_voornaam, auteur_achternaam, genre, prijs, paginas, bindwijze, edition, isbn, reeks_nr, uitgeverij, serie, staat, taal, gesigneerd, gelezen, added_date)
                                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                              tuple(data.values()))
                    conn.commit()
                    flash("Boek succesvol toegevoegd!", "success")
                except sqlite3.Error as e:
                    flash(f"Fout bij toevoegen boek: {str(e)}", "error")
        
        elif action == 'edit':
            book_id = request.form.get('book_id', '')
            if not book_id:
                flash("Geen boek-ID opgegeven!", "error")
            else:
                data = {
                    'titel': request.form.get('titel', ''),
                    'auteur_voornaam': request.form.get('auteur_voornaam', ''),
                    'auteur_achternaam': request.form.get('auteur_achternaam', ''),
                    'genre': request.form.get('genre', ''),
                    'prijs': float(request.form.get('prijs', 0)) if request.form.get('prijs', '') else 0.0,
                    'paginas': int(request.form.get('paginas', 0)) if request.form.get('paginas', '') else 0,
                    'bindwijze': request.form.get('bindwijze', ''),
                    'edition': request.form.get('edition', ''),
                    'isbn': request.form.get('isbn', ''),
                    'reeks_nr': request.form.get('reeks_nr', ''),
                    'uitgeverij': request.form.get('uitgeverij', ''),
                    'serie': request.form.get('serie', ''),
                    'staat': request.form.get('staat', ''),
                    'taal': request.form.get('taal', ''),
                    'gesigneerd': request.form.get('gesigneerd', ''),
                    'gelezen': request.form.get('gelezen', ''),
                    'added_date': request.form.get('added_date', '')
                }
                
                if not data['titel']:
                    flash("Titel is verplicht!", "error")
                else:
                    try:
                        c.execute('''UPDATE books SET titel = ?, auteur_voornaam = ?, auteur_achternaam = ?, genre = ?, prijs = ?, paginas = ?, bindwijze = ?, edition = ?, isbn = ?, reeks_nr = ?, uitgeverij = ?, serie = ?, staat = ?, taal = ?, gesigneerd = ?, gelezen = ?, added_date = ?
                                     WHERE id = ?''', tuple(data.values()) + (book_id,))
                        conn.commit()
                        flash("Boek succesvol bijgewerkt!", "success")
                    except sqlite3.Error as e:
                        flash(f"Fout bij bijwerken boek: {str(e)}", "error")
        
        query = 'SELECT * FROM books'
        params = []
        where_clauses = []
        for col in ['titel', 'auteur_voornaam', 'auteur_achternaam', 'genre', 'uitgeverij', 'isbn', 'serie', 'staat', 'taal', 'gesigneerd', 'gelezen', 'bindwijze', 'edition']:
            value = request.form.get(col, '').strip()
            if value:
                filters[col] = value
                where_clauses.append(f"{col} LIKE ?")
                params.append(f'%{value}%')
        
        if where_clauses:
            query += ' WHERE ' + ' AND '.join(where_clauses)
        
        try:
            c.execute(query + ' ORDER BY genre ASC, auteur_achternaam ASC, reeks_nr ASC', params)
            books = c.fetchall()
        except sqlite3.Error as e:
            flash(f"Databasefout bij ophalen boeken: {str(e)}", "error")
            books = []
    else:
        book_id = request.args.get('edit_book_id')
        if book_id:
            try:
                c.execute('SELECT * FROM books WHERE id = ?', (book_id,))
                book = c.fetchone()
                if book:
                    edit_book_data = {
                        'book_id': book[0],
                        'titel': book[1],
                        'auteur_voornaam': book[2],
                        'auteur_achternaam': book[3],
                        'genre': book[4],
                        'prijs': book[5],
                        'paginas': book[6],
                        'bindwijze': book[7],
                        'edition': book[8],
                        'isbn': book[9],
                        'reeks_nr': book[10],
                        'uitgeverij': book[11],
                        'serie': book[12],
                        'staat': book[13],
                        'taal': book[14],
                        'gesigneerd': book[15],
                        'gelezen': book[16],
                        'added_date': book[17]
                    }
                else:
                    flash("Boek niet gevonden!", "error")
            except sqlite3.Error as e:
                flash(f"Databasefout bij ophalen boek: {str(e)}", "error")
        
        try:
            c.execute('SELECT * FROM books ORDER BY genre ASC, auteur_achternaam ASC, reeks_nr ASC')
            books = c.fetchall()
        except sqlite3.Error as e:
            flash(f"Databasefout bij ophalen boeken: {str(e)}", "error")
            books = []
    
    conn.close()
    df = pd.DataFrame(books, columns=['id', 'titel', 'auteur_voornaam', 'auteur_achternaam', 'genre', 'prijs', 'paginas', 'bindwijze', 'edition', 'isbn', 'reeks_nr', 'uitgeverij', 'serie', 'staat', 'taal', 'gesigneerd', 'gelezen', 'added_date'])
    total_price = df['prijs'].sum() if not df.empty else 0
    total_pages = df['paginas'].sum() if not df.empty else 0
    
    return render_template('index.html', books=books, total_price=total_price, total_pages=total_pages, filters=filters, settings=settings, edit_book_data=edit_book_data, is_admin=is_admin())

@app.route('/search', methods=['POST'])
def search():
    filters = request.get_json() or {}
    conn = sqlite3.connect('books.db')
    c = conn.cursor()
    query = 'SELECT * FROM books'
    params = []
    where_clauses = []
    
    for col in ['titel', 'auteur_voornaam', 'auteur_achternaam', 'genre', 'uitgeverij', 'isbn', 'serie', 'staat', 'taal', 'gesigneerd', 'gelezen', 'bindwijze', 'edition']:
        value = filters.get(col, '').strip()
        if value:
            where_clauses.append(f"{col} LIKE ?")
            params.append(f'%{value}%')
    
    if where_clauses:
        query += ' WHERE ' + ' AND '.join(where_clauses)
    
    try:
        c.execute(query + ' ORDER BY genre ASC, auteur_achternaam ASC, reeks_nr ASC', params)
        books = c.fetchall()
    except sqlite3.Error as e:
        flash(f"Databasefout bij zoeken: {str(e)}", "error")
        books = []
    conn.close()
    
    return jsonify([{
        'id': book[0], 'titel': book[1], 'auteur_voornaam': book[2], 'auteur_achternaam': book[3],
        'genre': book[4], 'prijs': book[5], 'paginas': book[6], 'bindwijze': book[7],
        'edition': book[8], 'isbn': book[9], 'reeks_nr': book[10], 'uitgeverij': book[11],
        'serie': book[12], 'staat': book[13], 'taal': book[14], 'gesigneerd': book[15],
        'gelezen': book[16], 'added_date': book[17]
    } for book in books])

@app.route('/fetch_cover', methods=['POST'])
def fetch_cover():
    title = request.form.get('titel', '').strip()
    isbn = request.form.get('isbn', '').strip()
    if not title and not isbn:
        return jsonify({'cover_url': '', 'message': 'Vul een titel of ISBN in!', 'category': 'error'})
    
    query = f"isbn:{isbn.replace('-', '')}" if isbn else f"intitle:{title.replace(' ', '+')}"
    try:
        response = requests.get(f"https://www.googleapis.com/books/v1/volumes?q={query}", timeout=3)
        if response.status_code == 200:
            data = response.json()
            if data["totalItems"] > 0:
                book = data["items"][0]["volumeInfo"]
                cover_url = book.get("imageLinks", {}).get("thumbnail", "")
                return jsonify({
                    'cover_url': cover_url,
                    'message': 'Boekkaft opgehaald!' if cover_url else 'Geen boekkaft gevonden.',
                    'category': 'success' if cover_url else 'info'
                })
            return jsonify({'cover_url': '', 'message': 'Geen boek gevonden in Google Books API.', 'category': 'info'})
        return jsonify({'cover_url': '', 'message': f'Fout bij extern zoeken: HTTP {response.status_code}', 'category': 'error'})
    except requests.exceptions.RequestException as e:
        return jsonify({'cover_url': '', 'message': f'Fout bij extern zoeken: {str(e)}', 'category': 'error'})

@app.route('/edit/<int:book_id>', methods=['GET'])
def edit_book(book_id):
    if not session.get('user_id'):
        flash('Log in om een boek te bewerken!', 'error')
        return redirect(url_for('login'))
    if not is_admin():
        flash('Alleen admins kunnen boeken bewerken!', 'error')
        return redirect(url_for('index'))
    return redirect(url_for('index', edit_book_id=book_id))

@app.route('/delete/<int:book_id>', methods=['POST'])
def delete_book(book_id):
    if not session.get('user_id'):
        flash('Log in om een boek te verwijderen!', 'error')
        return redirect(url_for('login'))
    if not is_admin():
        flash('Alleen admins kunnen boeken verwijderen!', 'error')
        return redirect(url_for('index'))
    
    print(f"Delete route called with book_id: {book_id}")
    conn = sqlite3.connect('books.db')
    c = conn.cursor()
    try:
        c.execute('SELECT * FROM books WHERE id = ?', (book_id,))
        book = c.fetchone()
        if not book:
            flash(f"Boek met ID {book_id} niet gevonden!", "error")
        else:
            c.execute('DELETE FROM books WHERE id = ?', (book_id,))
            if c.rowcount == 0:
                flash(f"Geen boek verwijderd voor ID {book_id}!", "error")
            else:
                conn.commit()
                flash("Boek succesvol verwijderd!", "success")
    except sqlite3.Error as e:
        flash(f"Databasefout bij verwijderen: {str(e)}", "error")
    finally:
        conn.close()
    return redirect(url_for('index'))

@app.route('/upload_csv', methods=['POST'])
def upload_csv():
    if not session.get('user_id'):
        flash('Log in om een CSV te uploaden!', 'error')
        return redirect(url_for('login'))
    if not is_admin():
        flash('Alleen admins kunnen CSV-bestanden uploaden!', 'error')
        return redirect(url_for('settings'))
    
    if 'csv_file' not in request.files:
        flash("Geen bestand geselecteerd!", "error")
        return redirect(url_for('settings'))
    
    file = request.files['csv_file']
    if file.filename == '':
        flash("Geen bestand geselecteerd!", "error")
        return redirect(url_for('settings'))
    
    if not file.filename.endswith('.csv'):
        flash("Alleen CSV-bestanden zijn toegestaan!", "error")
        return redirect(url_for('settings'))
    
    overwrite = 'overwrite' in request.form
    print(f"CSV-upload gestart: Bestand={file.filename}, Overschrijven={overwrite}")
    success, message = load_csv_to_db(file, overwrite=overwrite)
    flash(message, "success" if success else "error")
    return redirect(url_for('settings'))

@app.route('/statistics')
def statistics():
    if not session.get('user_id'):
        flash('Log in om statistieken te bekijken!', 'error')
        return redirect(url_for('login'))
    
    settings = get_settings()
    conn = sqlite3.connect('books.db')
    try:
        df = pd.read_sql_query('SELECT * FROM books', conn)
    except sqlite3.Error as e:
        flash(f"Databasefout bij ophalen statistieken: {str(e)}", "error")
        conn.close()
        return render_template('statistics.html', charts={}, settings=settings, is_admin=is_admin())
    
    conn.close()
    charts = {}
    if not df.empty:
        if "genre" in df.columns:
            genre_counts = df["genre"].value_counts().to_dict()
            charts['genre'] = {'labels': list(genre_counts.keys()), 'data': list(genre_counts.values())}
        if "gelezen" in df.columns:
            gelezen_counts = df["gelezen"].value_counts().to_dict()
            charts['gelezen'] = {'labels': list(gelezen_counts.keys()), 'data': list(gelezen_counts.values())}
        if "taal" in df.columns:
            taal_counts = df["taal"].value_counts().to_dict()
            charts['taal'] = {'labels': list(taal_counts.keys()), 'data': list(taal_counts.values())}
        if "paginas" in df.columns:
            pages = df["paginas"].dropna()
            if not pages.empty:
                hist = pd.cut(pages, bins=20, include_lowest=True)
                counts = hist.value_counts().sort_index()
                charts['paginas'] = {
                    'labels': [f"{int(interval.left)}-{int(interval.right)}" for interval in counts.index],
                    'data': counts.tolist()
                }
        if "auteur_voornaam" in df.columns and "auteur_achternaam" in df.columns:
            df["auteur"] = df["auteur_voornaam"] + " " + df["auteur_achternaam"]
            auteur_counts = df["auteur"].value_counts().head(10).to_dict()
            charts['auteur'] = {'labels': list(auteur_counts.keys()), 'data': list(auteur_counts.values())}
        if "genre" in df.columns and "prijs" in df.columns:
            avg_price = df.groupby("genre")["prijs"].mean().to_dict()
            charts['avg_price'] = {'labels': list(avg_price.keys()), 'data': [round(v, 2) for v in avg_price.values()]}
    
    print(f"Charts data: {charts}")
    if not isinstance(charts, dict):
        flash("Fout: ongeldige grafiekdata!", "error")
        charts = {}

    return render_template('statistics.html', charts=charts, settings=settings, is_admin=is_admin())

@app.route('/settings', methods=['GET', 'POST'])
def settings():
    if not session.get('user_id'):
        flash('Log in om instellingen te bekijken!', 'error')
        return redirect(url_for('login'))
    if not is_admin():
        flash('Alleen admins kunnen instellingen wijzigen!', 'error')
        return render_template('settings.html', settings=get_settings(), is_admin=is_admin())
    
    settings = get_settings()
    if request.method == 'POST' and 'color' in request.form:
        color = request.form.get('color', settings['color']).strip()
        dark_mode = 'dark_mode' in request.form
        if color.startswith('#') and len(color) == 7 and all(c in '0123456789ABCDEFabcdef' for c in color[1:]):
            update_settings(color=color, dark_mode=dark_mode)
            flash("Instellingen opgeslagen!", "success")
        else:
            flash("Ongeldige kleurcode!", "error")
        return redirect(url_for('settings'))
    
    return render_template('settings.html', settings=settings, is_admin=is_admin())

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)