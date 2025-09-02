<<<<<<< HEAD
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, session, current_app
=======
<<<<<<< HEAD
#### comment zodat anders is
=======
>>>>>>> 7b27123b4ad630ecb94483bbc6c34d181b2a5b2a
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, session
>>>>>>> 7e0b705695751dbe97a337d88715a82050bc9b73
from flask_cors import CORS
from functools import wraps
from models.database import init_db, get_db_connection
from models.book import load_csv_to_db, search_books, add_book, edit_book as update_book, delete_book
from models.user import register_user, login_user, is_admin
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from werkzeug.utils import secure_filename
from geopy.distance import geodesic
import time
import os
import pandas as pd
import logging

# Configureer logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.urandom(24).hex()
app.config['SESSION_TYPE'] = 'filesystem'
CORS(app)

# Decorators
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('user_id'):
            flash('Log in om deze pagina te bekijken.', 'error')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not is_admin():
            flash('Alleen admins en supergebruikers hebben toegang tot deze functie.', 'error')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

def super_admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if session.get('role') != 'admin':
            flash('Alleen superadmins hebben toegang tot gebruikersbeheer.', 'error')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

# Context processor → maakt is_admin en is_super_admin beschikbaar in alle templates
@app.context_processor
def inject_roles():
    return dict(is_admin=is_admin(), is_super_admin=session.get('role') == 'admin')

# Initialize database
init_db()

def clean_geocache():
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute('''DELETE FROM geocache WHERE location NOT IN (SELECT DISTINCT land FROM books WHERE land IS NOT NULL AND land != '')''')

def get_user_settings(user_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT color, dark_mode FROM users WHERE id = ?', (user_id,))
    result = c.fetchone()
    conn.close()
    return {'color': result[0] if result else '#e31c73', 'dark_mode': result[1] if result else True}

def update_user_settings(user_id, color, dark_mode):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('UPDATE users SET color = ?, dark_mode = ? WHERE id = ?', (color, dark_mode, user_id))
    conn.commit()
    conn.close()

@app.route('/manage_users', methods=['GET', 'POST'])
@super_admin_required
def manage_users():
    conn = get_db_connection()
    c = conn.cursor()

    if request.method == 'POST':
        action = request.form.get('action')
        user_id = request.form.get('user_id')

        if action == 'delete':
            c.execute('DELETE FROM users WHERE id = ?', (user_id,))
            flash('Gebruiker verwijderd.', 'success')
        elif action == 'make_admin':
            c.execute('UPDATE users SET role = "admin" WHERE id = ?', (user_id,))
            flash('Gebruiker is nu admin.', 'success')
        elif action == 'remove_admin':
            c.execute('UPDATE users SET role = "super" WHERE id = ?', (user_id,))
            flash('Adminstatus verwijderd; gebruiker is nu supergebruiker.', 'success')

        conn.commit()

    c.execute('SELECT id, username, role FROM users')
    users = c.fetchall()
    conn.close()
    return render_template('manage_users.html', users=users, settings=get_user_settings(session.get('user_id')))

@app.route('/admin/users/delete/<int:user_id>', methods=['POST'])
@super_admin_required
def delete_user(user_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("DELETE FROM users WHERE id = ?", (user_id,))
    conn.commit()
    conn.close()
    flash("Gebruiker verwijderd!")
    return redirect(url_for('manage_users'))

@app.route('/admin/users/promote/<int:user_id>', methods=['POST'])
@super_admin_required
def promote_user(user_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("UPDATE users SET role = 'admin' WHERE id = ?", (user_id,))
    conn.commit()
    conn.close()
    flash("Gebruiker is nu admin!")
    return redirect(url_for('manage_users'))

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        success, message = register_user(request.form)
        flash(message, 'success' if success else 'error')
        return redirect(url_for('login') if success else 'register')
    return render_template('register.html', settings=get_user_settings(session.get('user_id', 0)))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        success, message = login_user(request.form)
        if success:
            flash(message, 'success')
            return redirect(url_for('index'))
        flash(message, 'error')
        return redirect(url_for('login'))
    return render_template('login.html', settings=get_user_settings(session.get('user_id', 0)))

@app.route('/logout')
def logout():
    session.pop('user_id', None)
    session.pop('username', None)
    session.pop('role', None)
    flash('Succesvol uitgelogd!', 'success')
    return redirect(url_for('index'))

    
# Nieuwe welkomstpagina (default root)
@app.route("/")
def index():
    user_id = session.get('user_id', 0)
    settings = get_user_settings(user_id)
    return render_template("index.html", settings=settings)   # dit is jouw nieuwe welkomspagina
    
@app.route("/over-mij")
def over_mij():
    user_id = session.get("user_id", 0)
    if not user_id:
        flash("Log in om je profiel te bekijken.", "error")
        return redirect(url_for("login"))

    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM books WHERE user_id = ?", conn, params=(user_id,))
    conn.close()

    boek_count = len(df)
    gelezen_count = df[df["gelezen"] == "ja"].shape[0] if "gelezen" in df.columns else 0
    wishlist_count = df[df["status"] == "wishlist"].shape[0] if "status" in df.columns else 0
    fav_genre = df["genre"].mode()[0] if "genre" in df.columns and not df["genre"].empty else "Onbekend"

    # user info uit je users tabel ophalen
    conn = get_db_connection()
    user = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    conn.close()


    return render_template(
        "over_mij.html",
        boek_count=boek_count,
        gelezen_count=gelezen_count,
        wishlist_count=wishlist_count,
        fav_genre=fav_genre,
        user=user
    )


@app.route("/edit-profile", methods=["GET", "POST"])
@login_required
def edit_profile():
    user_id = session.get("user_id", 0)
    if not user_id:
        flash("Log in om je profiel te bekijken.", "error")
        return redirect(url_for("login"))

    conn = get_db_connection()
    user = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()

    if request.method == "POST":
        updates = []
        params = []

        # 📖 Bio opslaan
        bio = request.form.get("bio", "").strip()
        if bio:
            updates.append("bio = ?")
            params.append(bio)

        # 📸 Foto uploaden
        if "profile_pic" in request.files:
            file = request.files["profile_pic"]
            if file and file.filename.strip() != "":
                filename = secure_filename(file.filename)
                upload_folder = os.path.join(current_app.root_path, "static", "images")
                os.makedirs(upload_folder, exist_ok=True)
                filepath = os.path.join(upload_folder, filename)
                file.save(filepath)

                updates.append("profile_pic = ?")
                params.append(filename)

        # 🚀 Update uitvoeren als er iets gewijzigd is
        if updates:
            sql = f"UPDATE users SET {', '.join(updates)} WHERE id = ?"
            params.append(user_id)
            conn.execute(sql, tuple(params))
            conn.commit()
            flash("Profiel bijgewerkt!", "success")

        conn.close()
        return redirect(url_for("over_mij"))

    conn.close()
    return render_template("edit_profile.html", user=user)


@app.route('/dashboard', methods=['GET', 'POST'])
def dashboard():
    user_id = session.get('user_id', 0)
    if not user_id:
        flash('Log in om boeken te bekijken.', 'error')
        return redirect(url_for('login'))
    
    logger.debug(f"Index route - User ID: {user_id}, Role: {session.get('role')}")
    settings = get_user_settings(user_id)
    conn = get_db_connection()
    filters = {}
    edit_book_data = {}
    books = []

    if request.method == 'POST':
        if not is_admin() and request.form.get('action') in ['add', 'edit']:
            flash('Alleen admins en supergebruikers kunnen boeken toevoegen of bewerken!', 'error')
            return redirect(url_for('index'))
        
        action = request.form.get('action', 'search')

        if action == 'add':
            form_data = request.form.to_dict()
            form_data['prijs'] = form_data.get('min_prijs', '')
            form_data['paginas'] = form_data.get('min_paginas', '')
            form_data['user_id'] = str(user_id)
            logger.debug(f"Add book form data: {form_data}")
            success, message = add_book(form_data)
            flash(message, 'success' if success else 'error')
        
        elif action == 'edit':
            book_id = request.form.get('book_id', '')
            form_data = request.form.to_dict()
            form_data['prijs'] = form_data.get('min_prijs', '')
            form_data['paginas'] = form_data.get('min_paginas', '')
            form_data['user_id'] = str(user_id)
            logger.debug(f"Edit book form data: {form_data}")
            success, message = update_book(book_id, form_data)
            clean_geocache()
            flash(message, 'success' if success else 'error')
        
        filters = {col: request.form.get(col, '').strip() for col in 
                   ['titel', 'auteur_voornaam', 'auteur_achternaam', 'genre', 'uitgeverij', 'isbn', 
                    'serie', 'staat', 'taal', 'gesigneerd', 'gelezen', 'bindwijze', 'edition', 'land',
                    'min_prijs', 'max_prijs', 'min_paginas', 'max_paginas'] if request.form.get(col, '').strip()}
        books = search_books(filters, user_id=user_id)
        logger.debug(f"Books retrieved for user {user_id}: {len(books)} books")

    else:
        book_id = request.args.get('edit_book_id')
        if book_id and is_admin():
            c = conn.cursor()
            c.execute('SELECT * FROM books WHERE id = ? AND user_id = ?', (book_id, user_id))
            book = c.fetchone()
            if book:
                edit_book_data = {
                    'book_id': book['id'],
                    'titel': book['titel'],
                    'auteur_voornaam': book['auteur_voornaam'],
                    'auteur_achternaam': book['auteur_achternaam'],
                    'genre': book['genre'],
                    'min_prijs': book['prijs'],
                    'min_paginas': book['paginas'],
                    'bindwijze': book['bindwijze'],
                    'edition': book['edition'],
                    'isbn': book['isbn'],
                    'reeks_nr': book['reeks_nr'],
                    'uitgeverij': book['uitgeverij'],
                    'serie': book['serie'],
                    'staat': book['staat'],
                    'taal': book['taal'],
                    'gesigneerd': book['gesigneerd'],
                    'gelezen': book['gelezen'],
                    'land': book['land'],
                    'added_date': book['added_date']
                }
                logger.debug(f"Edit book data loaded: {edit_book_data}")
            else:
                flash("Boek niet gevonden!", "error")
                logger.debug(f"Book ID {book_id} not found for user {user_id}")
        
        books = search_books({}, user_id=user_id)
        logger.debug(f"Books retrieved for user {user_id}: {len(books)} books")

    conn.close()
    df = pd.DataFrame(books, columns=['user_id','id', 'titel', 'auteur_voornaam', 'auteur_achternaam', 'genre', 'prijs', 
                                     'paginas', 'bindwijze', 'edition', 'isbn', 'reeks_nr', 'uitgeverij', 'serie', 
                                     'staat', 'taal', 'gesigneerd', 'gelezen', 'added_date', 'land'])
    logger.debug(f"DataFrame dtypes: {df.dtypes}")
    if not df.empty:
        df['prijs'] = pd.to_numeric(df['prijs'], errors='coerce').fillna(0).astype(float)
        df['paginas'] = pd.to_numeric(df['paginas'], errors='coerce').fillna(0).astype(int)
        logger.debug(f"Sample paginas values: {df['paginas'].head().tolist()}")
    total_price = df['prijs'].sum() if not df.empty else 0
    total_pages = df['paginas'].sum() if not df.empty else 0
    
    return render_template('dashboard.html', 
                       books=books, 
                       total_price=total_price, 
                       total_pages=total_pages,
                       filters=filters, 
                       settings=settings, 
                       edit_book_data=edit_book_data)

@app.route('/search', methods=['POST'])
def search():
    user_id = session.get('user_id', 0)
    is_admin_val = session.get("role") in ["admin", "super"]
    filters = request.get_json() or {}
    books = search_books(filters, user_id=user_id)
    logger.debug(f"Search route - Retrieved {len(books)} books for user {user_id}")
    return jsonify([{
        'id': book[0], 'titel': book[1], 'auteur_voornaam': book[2], 'auteur_achternaam': book[3],
        'genre': book[4], 'prijs': book[5], 'paginas': book[6], 'bindwijze': book[7],
        'edition': book[8], 'isbn': book[9], 'reeks_nr': book[10], 'uitgeverij': book[11],
        'serie': book[12], 'staat': book[13], 'taal': book[14], 'gesigneerd': book[15],
        'gelezen': book[16], 'added_date': book[17], 'land': book[18], 'is_admin': is_admin_val
    } for book in books])

@app.route('/fetch_cover', methods=['POST'])
def fetch_cover():
    import requests
    title = request.form.get('titel', '').strip()
    isbn = request.form.get('isbn', '').strip()
    if not title and not isbn:
        return jsonify({'cover_url': '', 'message': 'Vul een titel of ISBN in!', 'category': 'error'})
    
    query = f"isbn:{isbn.replace('-', '')}" if isbn else f"intitle:{title.replace(' ', '+')}"
    try:
        response = requests.get(f"https://www.googleapis.com/books/v1/volumes?q={query}", timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data["totalItems"] > 0:
                book = data["items"][0]["volumeInfo"]
                cover_url = book.get("imageLinks", {}).get("thumbnail", "")
                country = data["items"][0].get("saleInfo", {}).get("country", "")
                country_map = {'NL': 'Nederland', 'BE': 'België', 'DE': 'Duitsland', 'FR': 'Frankrijk', 'ES': 'Spanje', 'IT': 'Italië'}
                country_name = country_map.get(country, country or 'Onbekend')
                return jsonify({
                    'cover_url': cover_url,
                    'land': country_name,
                    'message': 'Boekkaft opgehaald!' if cover_url else 'Geen boekkaft gevonden.',
                    'category': 'success' if cover_url else 'info'
                })
            return jsonify({'cover_url': '', 'land': '', 'message': 'Geen boek gevonden in Google Books API.', 'category': 'info'})
        return jsonify({'cover_url': '', 'land': '', 'message': f'Fout bij extern zoeken: HTTP {response.status_code}', 'category': 'error'})
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching cover: {str(e)}")
        return jsonify({'cover_url': '', 'land': '', 'message': f'Fout bij extern zoeken: {str(e)}', 'category': 'error'})

@app.route('/edit/<int:book_id>', methods=['GET'], endpoint='edit_book')
@admin_required
def edit_book_view(book_id):
    return redirect(url_for('dashboard', edit_book_id=book_id))

@app.route('/delete/<int:book_id>', methods=['POST'])
@admin_required
def delete_book_route(book_id):
    success, message = delete_book(book_id)
    clean_geocache()
    flash(message, 'success' if success else 'error')
    return redirect(url_for('dashboard'))

@app.route('/upload_csv', methods=['POST'])
@admin_required
def upload_csv():
    user_id = session.get('user_id')
    logger.debug(f"CSV upload initiated by user {user_id}")
    if 'csv_file' not in request.files:
        logger.error("No file selected for CSV upload")
        flash("Geen bestand geselecteerd!", "error")
        return redirect(url_for('index'))
    
    file = request.files['csv_file']
    if file.filename == '':
        logger.error("Empty filename for CSV upload")
        flash("Geen bestand geselecteerd!", "error")
        return redirect(url_for('index'))
    
    if not file.filename.endswith('.csv'):
        logger.error(f"Invalid file extension: {file.filename}")
        flash("Alleen CSV-bestanden zijn toegestaan!", "error")
        return redirect(url_for('dashboard'))
    
    overwrite = 'overwrite' in request.form
    success, message = load_csv_to_db(file, overwrite=overwrite, user_id=user_id)
    logger.debug(f"CSV upload result: success={success}, message={message}")
    flash(message, "success" if success else "error")
    
    # Verify imported books
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM books WHERE user_id = ?', (user_id,))
    book_count = c.fetchone()[0]
    logger.debug(f"Books for user {user_id} after upload: {book_count}")
    conn.close()
    
    return redirect(url_for('dashboard'))

@app.route('/statistics')
def statistics():
    user_id = session.get('user_id', 0)
    if not user_id:
        flash('Log in om boeken te bekijken.', 'error')
        return redirect(url_for('login'))
    user_id = session.get('user_id', 0)
    settings = get_user_settings(user_id)
    conn = get_db_connection()
    try:
        df = pd.read_sql_query('SELECT * FROM books WHERE user_id = ?', conn, params=(user_id,))
        logger.debug(f"Statistics - Retrieved {len(df)} rows for user {user_id}")
        logger.debug(f"Columns in dataframe: {df.columns.tolist()}")
    except Exception as e:
        logger.error(f"Database error in statistics: {str(e)}")
        flash(f"Databasefout bij ophalen statistieken: {str(e)}", "error")
        conn.close()
        return render_template('statistics.html', charts={}, settings=settings, fun_facts=[], location_coords={})

    if not df.empty:
        df['prijs'] = pd.to_numeric(df['prijs'], errors='coerce').fillna(0).astype(float)
        df['paginas'] = pd.to_numeric(df['paginas'], errors='coerce').fillna(0).astype(int)
        logger.debug(f"Sample paginas values: {df['paginas'].head().tolist()}")

    charts = {}
    location_coords = {}
    fun_facts = []

    if not df.empty:
        if "genre" in df.columns:
            genre_counts = df["genre"].value_counts().to_dict()
            charts['genre'] = {'labels': list(genre_counts.keys()), 'data': list(genre_counts.values())}
            logger.debug(f"Genre chart: {charts['genre']}")
        if "gelezen" in df.columns:
            gelezen_counts = df["gelezen"].value_counts().to_dict()
            charts['gelezen'] = {'labels': list(gelezen_counts.keys()), 'data': list(gelezen_counts.values())}
            logger.debug(f"Gelezen chart: {charts['gelezen']}")
        if "taal" in df.columns:
            taal_counts = df["taal"].value_counts().to_dict()
            charts['taal'] = {'labels': list(taal_counts.keys()), 'data': list(taal_counts.values())}
            logger.debug(f"Taal chart: {charts['taal']}")
        if "paginas" in df.columns:
            pages = df["paginas"].dropna()
            if not pages.empty:
                hist = pd.cut(pages, bins=20, include_lowest=True)
                counts = hist.value_counts().sort_index()
                charts['paginas'] = {
                    'labels': [f"{int(interval.left)}-{int(interval.right)}" for interval in counts.index],
                    'data': counts.tolist()
                }
                logger.debug(f"Paginas chart: {charts['paginas']}")
        if "auteur_voornaam" in df.columns and "auteur_achternaam" in df.columns:
            df["auteur"] = df["auteur_voornaam"] + " " + df["auteur_achternaam"]
            auteur_counts = df["auteur"].value_counts().head(10).to_dict()
            charts['auteur'] = {'labels': list(auteur_counts.keys()), 'data': list(auteur_counts.values())}
            logger.debug(f"Auteur chart: {charts['auteur']}")
        if "genre" in df.columns and "prijs" in df.columns:
            avg_price = df.groupby("genre")["prijs"].mean().to_dict()
            charts['avg_price'] = {'labels': list(avg_price.keys()), 'data': [round(v, 2) for v in avg_price.values()]}
            logger.debug(f"Avg price chart: {charts['avg_price']}")
        if "land" in df.columns:
            land_counts = df[df["land"].notnull() & (df["land"] != '')]["land"].value_counts().to_dict()
            charts['land'] = {'labels': list(land_counts.keys()), 'data': list(land_counts.values())}
            logger.debug(f"Aankoop stad chart: {charts['land']}")

            c = conn.cursor()
            try:
                geolocator = Nominatim(user_agent="boeken_app")
                for loc in land_counts.keys():
                    if loc and loc.strip():
                        c.execute('SELECT lat, lon FROM geocache WHERE location = ?', (loc,))
                        result = c.fetchone()
                        if result:
                            location_coords[loc] = (result[0], result[1])
                            logger.debug(f"Cached coords for {loc}: {location_coords[loc]}")
                        else:
                            try:
                                time.sleep(1)
                                geo = geolocator.geocode(loc, country_codes='nl,be,gb,it,de,at,ch', timeout=5)
                                if geo:
                                    location_coords[loc] = (geo.latitude, geo.longitude)
                                    c.execute('INSERT INTO geocache (location, lat, lon) VALUES (?, ?, ?)', 
                                              (loc, geo.latitude, geo.longitude))
                                    conn.commit()
                                    logger.debug(f"Geocoded and cached {loc}: {location_coords[loc]}")
                                else:
                                    logger.debug(f"Geen coördinaten voor {loc}")
                            except (GeocoderTimedOut, Exception) as e:
                                logger.error(f"Geocoding fout voor {loc}: {e}")
            except Exception as e:
                logger.error(f"Geocoding initialisatie fout: {e}")
                flash("Geocoding niet beschikbaar; controleer geopy installatie.", "error")
            logger.debug(f"Location coords: {location_coords}")

        if "paginas" in df.columns and df["paginas"].notna().any():
            dikste = df.loc[df["paginas"].idxmax()]
            fun_facts.append(f"Je dikste boek is '{dikste['titel']}' met {int(dikste['paginas'])} pagina’s.")
        if "prijs" in df.columns and df["prijs"].notna().any():
            duurste = df.loc[df["prijs"].idxmax()]
            fun_facts.append(f"Het duurste boek in je collectie is '{duurste['titel']}' voor €{round(duurste['prijs'],2)}.")
        if "taal" in df.columns:
            talen = df["taal"].nunique()
            if talen > 1:
                fun_facts.append(f"Je hebt boeken in {talen} verschillende talen!")
        fun_facts.append(f"Totaal aantal boeken in je collectie: {len(df)}.")
        logger.debug(f"Fun facts: {fun_facts}")
        
        
                # New Fun Facts
        # 1. Farthest Distance Between Two Books
        if location_coords and len(location_coords) >= 2:
            max_distance = 0
            book_pair = (None, None)
            title_pair = (None, None)
            for loc1, coord1 in location_coords.items():
                book1 = df[df['land'] == loc1].iloc[0]
                for loc2, coord2 in location_coords.items():
                    if loc1 != loc2:
                        distance = geodesic(coord1, coord2).kilometers
                        if distance > max_distance:
                            max_distance = distance
                            book_pair = (loc1, loc2)
                            title_pair = (book1['titel'], df[df['land'] == loc2].iloc[0]['titel'])
            if book_pair[0] and book_pair[1]:
                fun_facts.append(
                    f"De verste afstand tussen twee boeken is {round(max_distance, 2)} km, "
                    f"tussen '{title_pair[0]}' ({book_pair[0]}) en '{title_pair[1]}' ({book_pair[1]})."
                )
                logger.debug(f"Max distance: {max_distance} km between {book_pair}")

        # 2. Most Prolific Publication Year
        if "publicatie_jaar" in df.columns and df["publicatie_jaar"].notna().any():
            most_common_year = df["publicatie_jaar"].value_counts().idxmax()
            year_count = df["publicatie_jaar"].value_counts().max()
            fun_facts.append(
                f"Je hebt {year_count} boeken uit {int(most_common_year)}, je meest populaire publicatiejaar!"
            )
            logger.debug(f"Most common publication year: {most_common_year} with {year_count} books")

        # 3. Average Book Price
        if "prijs" in df.columns and df["prijs"].notna().any():
            avg_price = df["prijs"].mean()
            fun_facts.append(f"De gemiddelde prijs van je boeken is €{round(avg_price, 2)}.")
            logger.debug(f"Average book price: €{avg_price}")

        # 4. Longest Author Name
        if "auteur" in df.columns and df["auteur"].notna().any():
            longest_author_book = df.loc[df["auteur"].str.len().idxmax()]
            longest_author = longest_author_book["auteur"]
            fun_facts.append(
                f"Het boek met de langste auteursnaam is '{longest_author_book['titel']}' "
                f"door '{longest_author}' ({len(longest_author)} karakters)."
            )
            logger.debug(f"Longest author name: {longest_author}")

        # 5. Most Diverse Genre
        if "genre" in df.columns and "auteur" in df.columns and df["genre"].notna().any():
            genre_author_counts = df.groupby("genre")["auteur"].nunique()
            most_diverse_genre = genre_author_counts.idxmax()
            author_count = genre_author_counts.max()
            fun_facts.append(
                f"Het genre '{most_diverse_genre}' heeft de meeste unieke auteurs: {author_count} verschillende auteurs."
            )
            logger.debug(f"Most diverse genre: {most_diverse_genre} with {author_count} authors")

        logger.debug(f"Fun facts: {fun_facts}")
        
                # New Additional Fun Facts
        # 1. Oldest Book in Collection
        if "publicatie_jaar" in df.columns and df["publicatie_jaar"].notna().any():
            oldest_book = df.loc[df["publicatie_jaar"].idxmin()]
            fun_facts.append(
                f"Je oudste boek is '{oldest_book['titel']}' uit {int(oldest_book['publicatie_jaar'])}."
            )
            logger.debug(f"Oldest book: {oldest_book['titel']} from {oldest_book['publicatie_jaar']}")

        # 2. Most Common Title First Letter
        if "titel" in df.columns and df["titel"].notna().any():
            first_letters = df["titel"].str[0].str.upper().value_counts()
            if not first_letters.empty:
                most_common_letter = first_letters.idxmax()
                letter_count = first_letters.max()
                fun_facts.append(
                    f"De meest voorkomende beginletter van je boektitels is '{most_common_letter}' "
                    f"({letter_count} boeken)."
                )
                logger.debug(f"Most common title first letter: {most_common_letter} with {letter_count} books")

        # 3. Total Reading Time Estimate
        if "paginas" in df.columns and df["paginas"].notna().any():
            total_pages = df["paginas"].sum()
            reading_speed = 80  # pages per hour
            total_hours = total_pages / reading_speed
            if total_hours < 24:
                fun_facts.append(
                    f"Het lezen van al je boeken zou ongeveer {round(total_hours, 1)} uur duren "
                    f"(bij 250 pagina’s per uur)."
                )
            else:
                total_days = total_hours / 24
                fun_facts.append(
                    f"Het lezen van al je boeken zou ongeveer {round(total_days, 1)} dagen duren "
                    f"(bij 250 pagina’s per uur)."
                )
            logger.debug(f"Total reading time: {total_hours} hours for {total_pages} pages")

        # 4. Most Expensive Genre
        if "genre" in df.columns and "prijs" in df.columns and df["prijs"].notna().any():
            genre_total_price = df.groupby("genre")["prijs"].sum()
            most_expensive_genre = genre_total_price.idxmax()
            total_price = genre_total_price.max()
            fun_facts.append(
                f"Het genre '{most_expensive_genre}' is het duurst, met een totale waarde van €{round(total_price, 2)}."
            )
            logger.debug(f"Most expensive genre: {most_expensive_genre} with total €{total_price}")

        # 5. Language Diversity by Author
        if "auteur" in df.columns and "taal" in df.columns and df["taal"].notna().any():
            author_language_counts = df.groupby("auteur")["taal"].nunique()
            if not author_language_counts.empty:
                most_diverse_author = author_language_counts.idxmax()
                language_count = author_language_counts.max()
                if language_count > 1:
                    fun_facts.append(
                        f"De auteur '{most_diverse_author}' heeft boeken in {language_count} verschillende talen."
                    )
                    logger.debug(f"Most diverse author: {most_diverse_author} with {language_count} languages")

        logger.debug(f"Fun facts: {fun_facts}")

    conn.close()
    return render_template('statistics.html', charts=charts, settings=settings, fun_facts=fun_facts, location_coords=location_coords)

    conn.close()
    return render_template('statistics.html', charts=charts, settings=settings, fun_facts=fun_facts, location_coords=location_coords)

    conn.close()
    return render_template('statistics.html', charts=charts, settings=settings, fun_facts=fun_facts, location_coords=location_coords)

@app.route('/settings', methods=['GET', 'POST'])
@login_required
def settings():
    user_id = session.get('user_id')
    settings = get_user_settings(user_id)
    if request.method == 'POST':
        if 'color' in request.form:
            color = request.form.get('color', settings['color']).strip()
            dark_mode = 'dark_mode' in request.form
            update_user_settings(user_id, color, dark_mode)
            flash("Instellingen opgeslagen!", "success")
        return redirect(url_for('settings'))
    
    return render_template('settings.html', settings=settings)
    
@app.route("/mijn_boekenlijst")
def mijn_boekenlijst():

	

    if "user_id" not in session:

	

        return redirect(url_for("login"))

	

	

    user_id = session["user_id"]

	

	

    liked_ids = get_user_likes(user_id)

	

    books = get_books_by_ids(liked_ids)  # lijst van tuples

	

	

    settings = get_user_settings(user_id)

	

	

    # Correct voor tuples

	

    total_price = sum(book[5] or 0 for book in books)

	

    total_pages = sum(book[6] or 0 for book in books)

	

	

    filters = {}

	

    is_admin = session.get("role") == "admin"

	

    edit_book_data = None

	

    user_likes = liked_ids

	

	

    return render_template(

	

        "mijn_boekenlijst.html",

	

        books=books,

	

        total_price=total_price,

	

        total_pages=total_pages,

	

        filters=filters,

	

        is_admin=is_admin,

        edit_book_data=edit_book_data,

        user_likes=user_likes,

        settings=settings

    ) 

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
<<<<<<< HEAD
    app.run(host="0.0.0.0", port=port, debug=False)
=======
    app.run(host="0.0.0.0", port=port, debug=True)
>>>>>>> 7b27123b4ad630ecb94483bbc6c34d181b2a5b2a
