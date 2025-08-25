from .database import get_db_connection
from datetime import datetime
import pandas as pd
from io import StringIO

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
            'Gelezen': 'gelezen', 'gelezen': 'gelezen'
        }
        
        df.columns = [column_mapping.get(col.strip(), col.lower()) for col in df.columns]
        required_columns = ['titel']
        missing_required = [col for col in required_columns if col not in df.columns]
        if missing_required:
            return False, f"Fout: Verplichte kolommen ontbreken in het CSV-bestand: {missing_required}"
        
        expected_columns = ['titel', 'auteur_voornaam', 'auteur_achternaam', 'genre', 'prijs', 'paginas', 
                            'bindwijze', 'edition', 'isbn', 'reeks_nr', 'uitgeverij', 'serie', 'staat', 
                            'taal', 'gesigneerd', 'gelezen']
        for col in expected_columns:
            if col not in df.columns:
                df[col] = ''
        
        df = df.drop_duplicates(subset=["titel", "isbn"], keep="first")
        if "prijs" in df.columns:
            df['prijs'] = df['prijs'].replace({r'€': '', r'\,': '.'}, regex=True)
            df['prijs'] = pd.to_numeric(df['prijs'], errors='coerce').fillna(0)
        if "paginas" in df.columns:
            df['paginas'] = pd.to_numeric(df['paginas'], errors='coerce').fillna(0)
        if "reeks_nr" in df.columns:
            df['reeks_nr'] = pd.to_numeric(df['reeks_nr'], errors='coerce').fillna(0)
        if 'added_date' not in df.columns:
            df['added_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM books')
        existing_count = c.fetchone()[0]
        
        if overwrite and existing_count > 0:
            c.execute('DELETE FROM books')
            existing_count = 0
        
        if existing_count == 0 or overwrite:
            df.to_sql('books', conn, if_exists='append', index=False)
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

def search_books(filters):
    conn = get_db_connection()
    c = conn.cursor()
    query = 'SELECT * FROM books WHERE 1=1'
    params = []
    
    # Text-based filters
    for col in ['titel', 'auteur_voornaam', 'auteur_achternaam', 'genre', 'uitgeverij', 'isbn', 
                'serie', 'staat', 'taal', 'gesigneerd', 'gelezen', 'bindwijze', 'edition']:
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
            pass  # Ignore invalid numeric input for reeks_nr
    
    # Numeric range filters
    if 'min_prijs' in filters and filters['min_prijs'].strip():
        try:
            query += " AND prijs >= ?"
            params.append(float(filters['min_prijs']))
        except ValueError:
            pass  # Ignore invalid numeric input
    if 'max_prijs' in filters and filters['max_prijs'].strip():
        try:
            query += " AND prijs <= ?"
            params.append(float(filters['max_prijs']))
        except ValueError:
            pass  # Ignore invalid numeric input
    if 'min_paginas' in filters and filters['min_paginas'].strip():
        try:
            query += " AND paginas >= ?"
            params.append(int(filters['min_paginas']))
        except ValueError:
            pass  # Ignore invalid numeric input
    if 'max_paginas' in filters and filters['max_paginas'].strip():
        try:
            query += " AND paginas <= ?"
            params.append(int(filters['max_paginas']))
        except ValueError:
            pass  # Ignore invalid numeric input
    
    try:
        c.execute(query + ' ORDER BY genre ASC, auteur_achternaam ASC, reeks_nr ASC', params)
        books = c.fetchall()
    except Exception as e:
        print(f"Databasefout bij zoeken: {str(e)}")
        books = []
    conn.close()
    return books

def add_book(form):
    if not form.get('titel', '').strip():
        return False, "Titel is verplicht!"
    
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
        'added_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute('''INSERT INTO books (titel, auteur_voornaam, auteur_achternaam, genre, prijs, paginas, bindwijze, edition, isbn, reeks_nr, uitgeverij, serie, staat, taal, gesigneerd, gelezen, added_date)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  tuple(data.values()))
        conn.commit()
        conn.close()
        return True, "Boek succesvol toegevoegd!"
    except Exception as e:
        conn.close()
        return False, f"Fout bij toevoegen boek: {str(e)}"

def edit_book(book_id, form):
    if not book_id:
        return False, "Geen boek-ID opgegeven!"
    if not form.get('titel', '').strip():
        return False, "Titel is verplicht!"
    
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
        'added_date': form.get('added_date', '')
    }
    
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute('''UPDATE books SET titel = ?, auteur_voornaam = ?, auteur_achternaam = ?, genre = ?, prijs = ?, paginas = ?, bindwijze = ?, edition = ?, isbn = ?, reeks_nr = ?, uitgeverij = ?, serie = ?, staat = ?, taal = ?, gesigneerd = ?, gelezen = ?, added_date = ?
                     WHERE id = ?''', tuple(data.values()) + (book_id,))
        conn.commit()
        conn.close()
        return True, "Boek succesvol bijgewerkt!"
    except Exception as e:
        conn.close()
        return False, f"Fout bij bijwerken boek: {str(e)}"

def delete_book(book_id):
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute('SELECT * FROM books WHERE id = ?', (book_id,))
        book = c.fetchone()
        if not book:
            conn.close()
            return False, f"Boek met ID {book_id} niet gevonden!"
        c.execute('DELETE FROM books WHERE id = ?', (book_id,))
        if c.rowcount == 0:
            conn.close()
            return False, f"Geen boek verwijderd voor ID {book_id}!"
        conn.commit()
        conn.close()
        return True, "Boek succesvol verwijderd!"
    except Exception as e:
        conn.close()
        return False, f"Databasefout bij verwijderen: {str(e)}"