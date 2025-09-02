from .database import get_db_connection
from datetime import datetime
import pandas as pd
from io import StringIO
import logging

# Configureer logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

DB_PATH = "books.db"  # Pas aan als je bestand anders heet

def load_csv_to_db(csv_source, overwrite=False, user_id=None):
    logger.debug(f"Starting CSV import for user_id: {user_id}, overwrite: {overwrite}")
    try:
        if not hasattr(csv_source, 'read'):
            logger.error("CSV source is not a file-like object")
            return False, "Alleen geüploade CSV-bestanden worden ondersteund."
        
        if user_id is None:
            logger.error("No user_id provided")
            return False, "Gebruiker-ID is verplicht voor CSV-import."
        
        # Try multiple encodings to handle various CSV formats
        encodings = ['utf-8-sig', 'iso-8859-1', 'windows-1252']
        df = None
        for encoding in encodings:
            try:
                csv_source.seek(0)
                df = pd.read_csv(StringIO(csv_source.read().decode(encoding)), sep=None, engine="python")
                logger.info(f"CSV successfully read with encoding {encoding}")
                logger.debug(f"CSV columns: {df.columns.tolist()}")
                logger.debug(f"First few rows: {df.head().to_dict()}")
                break
            except UnicodeDecodeError:
                logger.warning(f"Encoding {encoding} failed")
                continue
        if df is None:
            logger.error("No suitable encoding found for CSV")
            return False, "Geen geschikte encoding gevonden voor het geüploade CSV-bestand"

        # Clean column names
        df.columns = [col.replace('\ufeff', '').strip() for col in df.columns]
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
            'Gelezen': 'gelezen', 'gelezen': 'gelezen',
            'Land': 'land', 'land': 'land',
            'User ID': 'user_id', 'user_id': 'user_id'
        }
        
        df.columns = [column_mapping.get(col.strip(), col.lower()) for col in df.columns]
        logger.debug(f"DataFrame columns after mapping: {df.columns.tolist()}")

        # Check for required columns
        required_columns = ['titel']
        missing_required = [col for col in required_columns if col not in df.columns]
        if missing_required:
            logger.error(f"Missing required columns: {missing_required}")
            return False, f"Fout: Verplichte kolommen ontbreken in het CSV-bestand: {missing_required}"
        
        # Define expected columns for the books table
        expected_columns = ['user_id', 'titel', 'auteur_voornaam', 'auteur_achternaam', 'genre', 'prijs', 
                            'paginas', 'bindwijze', 'edition', 'isbn', 'reeks_nr', 'uitgeverij', 'serie', 
                            'staat', 'taal', 'gesigneerd', 'gelezen', 'added_date', 'land']
        
        # Add missing columns with default values
        for col in expected_columns:
            if col not in df.columns:
                df[col] = '' if col not in ['user_id', 'prijs', 'paginas', 'reeks_nr'] else 0
        logger.debug(f"DataFrame columns after adding missing: {df.columns.tolist()}")

        # Filter DataFrame to only include expected columns
        df = df[expected_columns]
        logger.debug(f"DataFrame columns after filtering: {df.columns.tolist()}")

        # Type conversions and data cleaning
        df = df.drop_duplicates(subset=["titel", "isbn"], keep="first")
        logger.debug(f"After deduplication, DataFrame has {len(df)} rows")
        if "prijs" in df.columns:
            df['prijs'] = df['prijs'].replace({r'€': '', r'\,': '.'}, regex=True)
            df['prijs'] = pd.to_numeric(df['prijs'], errors='coerce').fillna(0).astype(float)
        if "paginas" in df.columns:
            df['paginas'] = pd.to_numeric(df['paginas'], errors='coerce').fillna(0).astype(int)
        if "reeks_nr" in df.columns:
            df['reeks_nr'] = pd.to_numeric(df['reeks_nr'], errors='coerce').fillna(0).astype(int)
        if 'added_date' not in df.columns:
            df['added_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Always set user_id to the logged-in user's ID, ignoring any user_id in the CSV
        df['user_id'] = int(user_id)
        logger.debug(f"DataFrame dtypes: {df.dtypes}")
        logger.debug(f"Sample paginas values: {df['paginas'].head().tolist()}")
        logger.debug(f"Sample user_id values: {df['user_id'].head().tolist()}")

        conn = get_db_connection()
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM books WHERE user_id = ?', (user_id,))
        existing_count = c.fetchone()[0]
        logger.debug(f"Existing books for user {user_id}: {existing_count}")
        
        if overwrite and existing_count > 0:
            c.execute('DELETE FROM books WHERE user_id = ?', (user_id,))
            logger.info(f"Deleted {existing_count} existing books for user {user_id}")
            existing_count = 0
        
        if existing_count == 0 or overwrite:
            df.to_sql('books', conn, if_exists='append', index=False)
            conn.commit()
            logger.info(f"Inserted {len(df)} books for user {user_id}")
            conn.close()
            return True, f"Succes: {len(df)} boeken geïmporteerd"
        else:
            inserted_count = 0
            for _, row in df.iterrows():
                logger.debug(f"Processing row: {row.to_dict()}")
                c.execute('''SELECT COUNT(*) FROM books WHERE titel = ? AND isbn = ? AND user_id = ?''', 
                          (row['titel'], row['isbn'], int(row['user_id'])))
                if c.fetchone()[0] == 0:
                    c.execute('''INSERT INTO books (user_id, titel, auteur_voornaam, auteur_achternaam, genre, prijs, paginas, bindwijze, edition, isbn, reeks_nr, uitgeverij, serie, staat, taal, gesigneerd, gelezen, added_date, land)
                                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                                 (int(row['user_id']), row['titel'], row['auteur_voornaam'], row['auteur_achternaam'], 
                                  row['genre'], float(row['prijs']), int(row['paginas']), row['bindwijze'], 
                                  row['edition'], row['isbn'], int(row['reeks_nr']), row['uitgeverij'], row['serie'], 
                                  row['staat'], row['taal'], row['gesigneerd'], row['gelezen'], row['added_date'], 
                                  row['land']))
                    inserted_count += 1
            conn.commit()
            logger.info(f"Inserted {inserted_count} new books for user {user_id}")
            conn.close()
            return True, f"Succes: {inserted_count} nieuwe boeken toegevoegd uit CSV"
    except Exception as e:
        logger.error(f"Error during CSV import: {str(e)}")
        return False, f"Fout bij importeren: {str(e)}"

def search_books(filters, user_id=None):
    logger.debug(f"Searching books for user_id: {user_id}, filters: {filters}")
    if user_id is None:
        logger.error("No user_id provided for search")
        return []
    
    conn = get_db_connection()
    c = conn.cursor()
    query = 'SELECT * FROM books WHERE user_id = ?'
    params = [user_id]
    
    # Text-based filters
    for col in ['titel', 'auteur_voornaam', 'auteur_achternaam', 'genre', 'uitgeverij', 'isbn', 
                'serie', 'staat', 'taal', 'gesigneerd', 'gelezen', 'bindwijze', 'edition', 'land']:
        value = filters.get(col, '').strip()
        if value:
            query += f" AND {col} LIKE ?"
            params.append(f'%{value}%')
    
    # Numeric filter for reeks_nr
    if 'reeks_nr' in filters and filters['reeks_nr'].strip():
        try:
            query += " AND reeks_nr = ?"
            params.append(int(filters['reeks_nr']))
        except ValueError:
            logger.warning(f"Invalid reeks_nr value: {filters['reeks_nr']}")
            pass
    
    # Numeric range filters
    for range_col, col_name in [('min_prijs', 'prijs'), ('max_prijs', 'prijs'), ('min_paginas', 'paginas'), ('max_paginas', 'paginas')]:
        if range_col in filters and filters[range_col].strip():
            try:
                operator = '>=' if 'min' in range_col else '<='
                query += f" AND {col_name} {operator} ?"
                params.append(float(filters[range_col]) if col_name == 'prijs' else int(filters[range_col]))
            except ValueError:
                logger.warning(f"Invalid {range_col} value: {filters[range_col]}")
                pass
    
    try:
        c.execute(query + ' ORDER BY genre ASC, auteur_achternaam ASC, reeks_nr ASC', params)
        books = c.fetchall()
        logger.debug(f"Retrieved {len(books)} books for user {user_id}")
    except Exception as e:
        logger.error(f"Database error during search: {str(e)}")
        books = []
    conn.close()
    return books

def add_book(form):
    logger.debug(f"Adding book with form data: {form}")
    if not form.get('titel', '').strip():
        logger.error("Missing required field: titel")
        return False, "Titel is verplicht!"
    
    user_id = form.get('user_id')
    if not user_id:
        logger.error("Missing user_id in form")
        return False, "Gebruiker-ID is verplicht!"
    
    try:
        user_id = int(user_id)
    except (ValueError, TypeError):
        logger.error(f"Invalid user_id: {user_id}")
        return False, "Ongeldige Gebruiker-ID!"
    
    data = {
        'user_id': user_id,
        'titel': form.get('titel', ''),
        'auteur_voornaam': form.get('auteur_voornaam', ''),
        'auteur_achternaam': form.get('auteur_achternaam', ''),
        'genre': form.get('genre', ''),
        'prijs': float(form.get('prijs', 0)) if form.get('prijs', '') else 0.0,
        'paginas': int(form.get('paginas', 0)) if form.get('paginas', '') else 0,
        'bindwijze': form.get('bindwijze', ''),
        'edition': form.get('edition', ''),
        'isbn': form.get('isbn', ''),
        'reeks_nr': int(form.get('reeks_nr', 0)) if form.get('reeks_nr', '').strip() else 0,
        'uitgeverij': form.get('uitgeverij', ''),
        'serie': form.get('serie', ''),
        'staat': form.get('staat', ''),
        'taal': form.get('taal', ''),
        'gesigneerd': form.get('gesigneerd', ''),
        'gelezen': form.get('gelezen', ''),
        'added_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'land': form.get('land', '')
    }
    
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute('''INSERT INTO books (user_id, titel, auteur_voornaam, auteur_achternaam, genre, prijs, paginas, bindwijze, edition, isbn, reeks_nr, uitgeverij, serie, staat, taal, gesigneerd, gelezen, added_date, land)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  tuple(data.values()))
        conn.commit()
        logger.info(f"Book added successfully for user {user_id}: {data['titel']}")
        conn.close()
        return True, "Boek succesvol toegevoegd!"
    except Exception as e:
        logger.error(f"Error adding book: {str(e)}")
        conn.close()
        return False, f"Fout bij toevoegen boek: {str(e)}"

def edit_book(book_id, form):
    logger.debug(f"Editing book {book_id} with form data: {form}")
    if not book_id:
        logger.error("No book_id provided")
        return False, "Geen boek-ID opgegeven!"
    if not form.get('titel', '').strip():
        logger.error("Missing required field: titel")
        return False, "Titel is verplicht!"
    
    user_id = form.get('user_id')
    if not user_id:
        logger.error("Missing user_id in form")
        return False, "Gebruiker-ID is verplicht!"
    
    try:
        user_id = int(user_id)
    except (ValueError, TypeError):
        logger.error(f"Invalid user_id: {user_id}")
        return False, "Ongeldige Gebruiker-ID!"
    
    data = {
        'titel': form.get('titel', ''),
        'auteur_voornaam': form.get('auteur_voornaam', ''),
        'auteur_achternaam': form.get('auteur_achternaam', ''),
        'genre': form.get('genre', ''),
        'prijs': float(form.get('prijs', 0)) if form.get('prijs', '') else 0.0,
        'paginas': int(form.get('paginas', 0)) if form.get('paginas', '') else 0,
        'bindwijze': form.get('bindwijze', ''),
        'edition': form.get('edition', ''),
        'isbn': form.get('isbn', ''),
        'reeks_nr': int(form.get('reeks_nr', 0)) if form.get('reeks_nr', '').strip() else 0,
        'uitgeverij': form.get('uitgeverij', ''),
        'serie': form.get('serie', ''),
        'staat': form.get('staat', ''),
        'taal': form.get('taal', ''),
        'gesigneerd': form.get('gesigneerd', ''),
        'gelezen': form.get('gelezen', ''),
        'added_date': form.get('added_date', '') or datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'land': form.get('land', '')
    }
    
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute('SELECT * FROM books WHERE id = ? AND user_id = ?', (book_id, user_id))
        book = c.fetchone()
        if not book:
            logger.error(f"Book with ID {book_id} not found for user {user_id}")
            conn.close()
            return False, f"Boek met ID {book_id} niet gevonden of geen rechten!"
        c.execute('''UPDATE books SET titel = ?, auteur_voornaam = ?, auteur_achternaam = ?, genre = ?, prijs = ?, paginas = ?, bindwijze = ?, edition = ?, isbn = ?, reeks_nr = ?, uitgeverij = ?, serie = ?, staat = ?, taal = ?, gesigneerd = ?, gelezen = ?, added_date = ?, land = ?
                     WHERE id = ?''', tuple(data.values()) + (book_id,))
        conn.commit()
        logger.info(f"Book {book_id} updated successfully for user {user_id}")
        conn.close()
        return True, "Boek succesvol bijgewerkt!"
    except Exception as e:
        logger.error(f"Error updating book {book_id}: {str(e)}")
        conn.close()
        return False, f"Fout bij bijwerken boek: {str(e)}"

def delete_book(book_id):
    logger.debug(f"Deleting book {book_id}")
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute('SELECT * FROM books WHERE id = ?', (book_id,))
        book = c.fetchone()
        if not book:
            logger.error(f"Book with ID {book_id} not found")
            conn.close()
            return False, f"Boek met ID {book_id} niet gevonden!"
        c.execute('DELETE FROM books WHERE id = ?', (book_id,))
        if c.rowcount == 0:
            logger.error(f"No book deleted for ID {book_id}")
            conn.close()
            return False, f"Geen boek verwijderd voor ID {book_id}!"
        conn.commit()
        logger.info(f"Book {book_id} deleted successfully")
        conn.close()
        return True, "Boek succesvol verwijderd!"
    except Exception as e:
        logger.error(f"Database error during deletion: {str(e)}")
        conn.close()
        return False, f"Databasefout bij verwijderen: {str(e)}"