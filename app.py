from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, session
from flask_cors import CORS
from functools import wraps
from models.database import init_db, get_db_connection
from models.book import load_csv_to_db, search_books, add_book, edit_book, delete_book
from models.user import register_user, login_user, is_admin
import os

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
            flash('Alleen admins hebben toegang tot deze functie.', 'error')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

# Context processor → maakt is_admin beschikbaar in alle templates
@app.context_processor
def inject_is_admin():
    return dict(is_admin=is_admin())

# Initialize database
init_db()

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
def manage_users():
    if not is_admin():
        flash('Je hebt geen toegang tot deze pagina.', 'error')
        return redirect(url_for('index'))

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
            c.execute('UPDATE users SET role = "user" WHERE id = ?', (user_id,))
            flash('Adminstatus verwijderd.', 'success')

        conn.commit()

    c.execute('SELECT id, username, role FROM users')
    users = c.fetchall()
    conn.close()
    return render_template('manage_users.html', users=users, settings=get_user_settings(session.get('user_id')))

@app.route('/admin/users/delete/<int:user_id>', methods=['POST'])
def delete_user(user_id):
    if not is_admin():
        return redirect(url_for('index'))

    conn = get_db_connection()
    c = conn.cursor()
    c.execute("DELETE FROM users WHERE id = ?", (user_id,))
    conn.commit()
    conn.close()
    flash("Gebruiker verwijderd!")
    return redirect(url_for('manage_users'))

@app.route('/admin/users/promote/<int:user_id>', methods=['POST'])
def promote_user(user_id):
    if not is_admin():
        return redirect(url_for('index'))

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

@app.route('/', methods=['GET', 'POST'])
def index():
    user_id = session.get('user_id', 0)
    settings = get_user_settings(user_id)
    conn = get_db_connection()
    filters = {}
    edit_book_data = {}
    books = []

    if request.method == 'POST':
        if not is_admin() and request.form.get('action') in ['add', 'edit']:
            flash('Alleen admins kunnen boeken toevoegen of bewerken!', 'error')
            return redirect(url_for('index'))
        
        action = request.form.get('action', 'search')
        
        if action == 'add':
            form_data = request.form.copy()
            form_data = form_data.to_dict()  # Convert to regular dict
            form_data['prijs'] = form_data.get('min_prijs', '')  # Use min_prijs for adding
            form_data['paginas'] = form_data.get('min_paginas', '')  # Use min_paginas for adding
            success, message = add_book(form_data)
            flash(message, 'success' if success else 'error')
        
        elif action == 'edit':
            book_id = request.form.get('book_id', '')
            form_data = request.form.copy()
            form_data = form_data.to_dict()  # Convert to regular dict
            form_data['prijs'] = form_data.get('min_prijs', '')  # Use min_prijs for editing
            form_data['paginas'] = form_data.get('min_paginas', '')  # Use min_paginas for editing
            success, message = edit_book(book_id, form_data)
            flash(message, 'success' if success else 'error')
        
        filters = {col: request.form.get(col, '').strip() for col in 
                   ['titel', 'auteur_voornaam', 'auteur_achternaam', 'genre', 'uitgeverij', 'isbn', 
                    'serie', 'staat', 'taal', 'gesigneerd', 'gelezen', 'bindwijze', 'edition',
                    'min_prijs', 'max_prijs', 'min_paginas', 'max_paginas'] if request.form.get(col, '').strip()}
        books = search_books(filters)
    
    else:
        book_id = request.args.get('edit_book_id')
        if book_id and is_admin():
            c = conn.cursor()
            c.execute('SELECT * FROM books WHERE id = ?', (book_id,))
            book = c.fetchone()
            if book:
                edit_book_data = {
                    'book_id': book[0], 'titel': book[1], 'auteur_voornaam': book[2], 'auteur_achternaam': book[3],
                    'genre': book[4], 'min_prijs': book[5], 'min_paginas': book[6], 'bindwijze': book[7],
                    'edition': book[8], 'isbn': book[9], 'reeks_nr': book[10], 'uitgeverij': book[11],
                    'serie': book[12], 'staat': book[13], 'taal': book[14], 'gesigneerd': book[15],
                    'gelezen': book[16], 'added_date': book[17]
                }
            else:
                flash("Boek niet gevonden!", "error")
        
        books = search_books({})

    conn.close()
    import pandas as pd
    df = pd.DataFrame(books, columns=['id', 'titel', 'auteur_voornaam', 'auteur_achternaam', 'genre', 'prijs', 
                                     'paginas', 'bindwijze', 'edition', 'isbn', 'reeks_nr', 'uitgeverij', 'serie', 
                                     'staat', 'taal', 'gesigneerd', 'gelezen', 'added_date'])
    total_price = df['prijs'].sum() if not df.empty else 0
    total_pages = df['paginas'].sum() if not df.empty else 0
    
    return render_template('index.html', books=books, total_price=total_price, total_pages=total_pages, 
                          filters=filters, settings=settings, edit_book_data=edit_book_data)

@app.route('/search', methods=['POST'])
def search():
    filters = request.get_json() or {}
    books = search_books(filters)
    return jsonify([{
        'id': book[0], 'titel': book[1], 'auteur_voornaam': book[2], 'auteur_achternaam': book[3],
        'genre': book[4], 'prijs': book[5], 'paginas': book[6], 'bindwijze': book[7],
        'edition': book[8], 'isbn': book[9], 'reeks_nr': book[10], 'uitgeverij': book[11],
        'serie': book[12], 'staat': book[13], 'taal': book[14], 'gesigneerd': book[15],
        'gelezen': book[16], 'added_date': book[17]
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
@admin_required
def edit_book(book_id):
    return redirect(url_for('index', edit_book_id=book_id))

@app.route('/delete/<int:book_id>', methods=['POST'])
@admin_required
def delete_book_route(book_id):
    success, message = delete_book(book_id)
    flash(message, 'success' if success else 'error')
    return redirect(url_for('index'))

@app.route('/upload_csv', methods=['POST'])
@admin_required
def upload_csv():
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
    success, message = load_csv_to_db(file, overwrite=overwrite)
    flash(message, "success" if success else "error")
    return redirect(url_for('settings'))

@app.route('/statistics')
def statistics():
    user_id = session.get('user_id', 0)
    settings = get_user_settings(user_id)
    conn = get_db_connection()
    try:
        import pandas as pd
        df = pd.read_sql_query('SELECT * FROM books', conn)
    except Exception as e:
        flash(f"Databasefout bij ophalen statistieken: {str(e)}", "error")
        conn.close()
        return render_template('statistics.html', charts={}, settings=settings)
    
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

    return render_template('statistics.html', charts=charts, settings=settings)

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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)