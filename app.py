from flask import Flask, request, jsonify
import os
import sqlite3
import pandas as pd
from io import StringIO
from datetime import datetime
import logging

# Configureer Flask-applicatie
app = Flask(__name__)

# Stel logging in
logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

# Controleer of de database bestaat, anders initialiseer deze
def init_db():
    conn = sqlite3.connect('books.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS books
                 (titel TEXT, auteur_voornaam TEXT, auteur_achternaam TEXT, genre TEXT, prijs REAL, 
                  paginas INTEGER, bindwijze TEXT, edition TEXT, isbn TEXT, reeks_nr INTEGER, 
                  uitgeverij TEXT, serie TEXT, staat TEXT, taal TEXT, gesigneerd TEXT, gelezen TEXT, 
                  added_date TEXT)''')
    conn.commit()
    conn.close()
    app.logger.info("Database 'books.db' geïnitialiseerd of gecontroleerd")

# Laad CSV naar database (alleen via endpoint)
def load_csv_to_db(csv_source, overwrite=False):
    try:
        app.logger.info("Starten van CSV-import")

        # Als csv_source een bestandspad is (voor initiële import)
        if isinstance(csv_source, str):
            if not os.path.exists(csv_source):
                app.logger.error(f"Fout: CSV-bestand niet gevonden op {csv_source}")
                return False, f"Fout: CSV-bestand niet gevonden op {csv_source}"
            encodings = ['utf-8-sig', 'iso-8859-1', 'windows-1252']
            df = None
            for encoding in encodings:
                try:
                    df = pd.read_csv(csv_source, sep=None, engine="python", encoding=encoding)
                    app.logger.info(f"Succes: CSV gelezen met encoding {encoding}")
                    break
                except UnicodeDecodeError:
                    app.logger.debug(f"Mislukt: Encoding {encoding} faalde")
                    continue
            if df is None:
                raise ValueError("Geen geschikte encoding gevonden voor het CSV-bestand")
        else:
            # Als csv_source een geüpload bestand is
            encodings = ['utf-8-sig', 'iso-8859-1', 'windows-1252']
            df = None
            for encoding in encodings:
                try:
                    csv_source.seek(0)  # Reset bestandspositie
                    df = pd.read_csv(StringIO(csv_source.read().decode(encoding)), sep=None, engine="python")
                    app.logger.info(f"Succes: CSV gelezen met encoding {encoding}")
                    break
                except UnicodeDecodeError:
                    app.logger.debug(f"Mislukt: Encoding {encoding} faalde")
                    continue
            if df is None:
                raise ValueError("Geen geschikte encoding gevonden voor het geüploade CSV-bestand")

        app.logger.debug(f"Gelezen kolomnamen (voor verwerking): {list(df.columns)}")
        
        # Verwijder BOM en normaliseer kolomnamen
        df.columns = [col.replace('\ufeff', '').strip() for col in df.columns]
        app.logger.debug(f"Kolomnamen na BOM-verwijdering: {list(df.columns)}")
        
        # Mapping van mogelijke CSV-kolomnamen naar databasekolommen
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
        
        # Hernoem kolommen naar de verwachte databasekolommen
        df.columns = [column_mapping.get(col.strip(), col.lower()) for col in df.columns]
        app.logger.debug(f"Genormaliseerde kolomnamen: {list(df.columns)}")
        
        # Controleer op verplichte kolommen
        required_columns = ['titel']
        missing_required = [col for col in required_columns if col not in df.columns]
        if missing_required:
            app.logger.error(f"Fout: Verplichte kolommen ontbreken: {missing_required}")
            return False, f"Fout: Verplichte kolommen ontbreken in het CSV-bestand: {missing_required}"
        
        # Controleer op ontbrekende optionele kolommen
        expected_columns = ['titel', 'auteur_voornaam', 'auteur_achternaam', 'genre', 'prijs', 'paginas', 
                            'bindwijze', 'edition', 'isbn', 'reeks_nr', 'uitgeverij', 'serie', 'staat', 
                            'taal', 'gesigneerd', 'gelezen']
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            app.logger.warning(f"De volgende optionele kolommen ontbreken: {missing_columns}")
            for col in missing_columns:
                df[col] = ''
        
        # Verwijder duplicaten (minder strikt, alleen op titel en isbn)
        df = df.drop_duplicates(subset=["titel", "isbn"], keep="first")
        app.logger.info(f"Aantal boeken na duplicaatverwijdering: {len(df)}")
        
        # Converteer prijs, pagina's en reeks_nr naar numeriek
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
        # Controleer of de database al gegevens bevat
        c.execute('SELECT COUNT(*) FROM books')
        existing_count = c.fetchone()[0]
        app.logger.info(f"Aantal bestaande boeken in database: {existing_count}")
        
        if overwrite and existing_count > 0:
            c.execute('DELETE FROM books')
            app.logger.info("Bestaande boeken verwijderd uit database")
            existing_count = 0
        
        if existing_count == 0 or overwrite:
            df.to_sql('books', conn, if_exists='append', index=False)
            app.logger.info(f"Succes: {len(df)} boeken geïmporteerd")
            conn.commit()
            conn.close()
            return True, f"Succes: {len(df)} boeken geïmporteerd"
        else:
            # Alleen nieuwe boeken toevoegen (controleer op duplicaten op titel en isbn)
            existing_books = pd.read_sql_query('SELECT titel, isbn FROM books', conn)
            new_books = df[~df[['titel', 'isbn']].apply(tuple, axis=1).isin(
                existing_books[['titel', 'isbn']].apply(tuple, axis=1))]
            app.logger.info(f"Aantal nieuwe boeken: {len(new_books)}")
            if not new_books.empty:
                new_books.to_sql('books', conn, if_exists='append', index=False)
                app.logger.info(f"Succes: {len(new_books)} nieuwe boeken toegevoegd")
                conn.commit()
                conn.close()
                return True, f"Succes: {len(new_books)} nieuwe boeken toegevoegd"
            else:
                app.logger.info("Geen nieuwe boeken om toe te voegen")
                conn.close()
                return True, "Geen nieuwe boeken om toe te voegen"
    except Exception as e:
        app.logger.error(f"Fout bij importeren CSV: {str(e)}")
        return False, f"Fout bij importeren CSV: {str(e)}"

# Endpoint voor CSV-upload
@app.route('/upload-csv', methods=['POST'])
def upload_csv():
    if 'file' not in request.files:
        return jsonify({"error": "Geen bestand geüpload"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "Geen bestandsnaam opgegeven"}), 400
    if file:
        success, message = load_csv_to_db(file, overwrite=False)
        if success:
            return jsonify({"message": message}), 200
        else:
            return jsonify({"error": message}), 400

# Voorbeeld endpoint om boeken uit de database te tonen
@app.route('/')
def home():
    conn = sqlite3.connect('books.db')
    df = pd.read_sql_query('SELECT * FROM books', conn)
    conn.close()
    return jsonify(df.to_dict(orient='records'))

# Initialiseer database bij opstart
init_db()

if __name__ == "__main__":
    app.run(debug=False)