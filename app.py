import os
import random
import mysql.connector
from dotenv import load_dotenv
from flask import Flask, render_template, request, redirect, url_for, make_response, flash
from atproto import Client

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY") or "dev-default-key"
app.config['TEMPLATES_AUTO_RELOAD'] = True

# Load environment variables
load_dotenv()
PASSWORD = os.getenv("GATEKEEP_PASSWORD")
BLUESKY_HANDLE = os.getenv("BLUESKY_HANDLE")
BLUESKY_APP_PASSWORD = os.getenv("BLUESKY_APP_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")  # <-- this line is essential

#DB connection
def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv('DB_HOST') or 'localhost',
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME')
    )

def get_latest_post():
    try:
        client = Client()
        client.login(BLUESKY_HANDLE, BLUESKY_APP_PASSWORD)
        feed = client.app.bsky.feed.get_author_feed({'actor': BLUESKY_HANDLE, 'limit': 1})
        if not feed.feed:
            print("No posts found for this account.")
            return None
        post_view = feed.feed[0].post
        author = post_view.author
        record = post_view.record
        return {
            'display_name': getattr(author, 'display_name', None) or author.handle,
            'handle': author.handle,
            'avatar': author.avatar,
            'text': record.text,
            'timestamp': record.created_at,
            'uri': post_view.uri  # 👈 Add this
        }
    except Exception as e:
        print(f"Error fetching Bluesky post: {e}")
        return None

def get_all_signatories():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT name, role, url FROM Signatory")
    entries = cursor.fetchall()
    conn.close()
    return entries

def load_shuffled_signatures():
    all_signatures = get_all_signatories()
    names = [s['name'] for s in all_signatures]
    random.shuffle(names)
    return names

@app.context_processor
def inject_signatures():
    try:
        return dict(signatures=load_shuffled_signatures())
    except:
        return dict(signatures=[])

@app.route('/')
def home():
    if request.cookies.get("authenticated") != "true":
        return redirect(url_for('gate'))

    latest_post = get_latest_post()
    return render_template('index.html',
                           active_page='home',
                           latest_post=latest_post)

@app.route('/gate', methods=['GET', 'POST'])
def gate():
    error = None
    if request.method == 'POST':
        submitted = request.form.get('password')
        if submitted == PASSWORD:
            response = make_response(redirect(url_for('home')))
            response.set_cookie("authenticated", "true", max_age=60*60*24*7)  # 1 week
            return response
        else:
            error = "Incorrect password."
    return render_template('gate.html', error=error)

@app.route('/bestpractice')
def bestpractice():
    # Get query params
    search = request.args.get('search') or ''
    type_filter = request.args.get('type') or ''
    selected_id = request.args.get('id')

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    # Get all distinct types for dropdown
    cursor.execute("SELECT DISTINCT type FROM BestPractice ORDER BY type")
    types = [row['type'] for row in cursor.fetchall() if row['type']]

    # Build the filtered results query
    query = "SELECT id, title FROM BestPractice WHERE 1=1"
    params = []

    if search:
        query += " AND (LOWER(title) LIKE %s OR LOWER(description) LIKE %s)"
        params += [f"%{search.lower()}%"] * 2

    if type_filter.strip().lower() not in ('', 'any'):
        query += " AND type = %s"
        params.append(type_filter)

    query += " ORDER BY title"
    cursor.execute(query, params)
    results = cursor.fetchall()

    # Try to fetch selected item (with aliased keys)
    selected = None
    if selected_id:
        print(f"Selected ID from query string: {selected_id}")
        try:
            cursor.execute("""
                SELECT
                    id,
                    Title AS title,
                    Type AS type,
                    Description AS description,
                    URL AS url
                FROM BestPractice
                WHERE id = %s
            """, (selected_id,))
            selected = cursor.fetchone()
            print("Selected record:", selected)
        except Exception as e:
            print(f"Error fetching selected record: {e}")

    conn.close()

    return render_template(
        'bestpractice.html',
        active_page='bestpractice',
        results=results,
        selected=selected,
        search=search,
        type_filter=type_filter,
        types=types
    )

@app.route('/datavis')
def datavis():
    return render_template('datavis.html', active_page='datavis')

@app.route('/signatories')
def signatories():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT name, role, url FROM Signatory ORDER BY name")
    entries = cursor.fetchall()
    conn.close()
    return render_template('signatories.html',
                           active_page='signatories',
                           signatories=entries)

@app.route('/tbd')
def tbd():
    return render_template('tbd.html', active_page='tbd')

@app.route('/thank-you')
def thankyou():
    return render_template('thankyou.html', active_page=None)

@app.route('/silent-pebble-echo')
def admin():
    selected_table = request.args.get('table')
    search_query = request.args.get('search', '')

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    # Step 1: Get table list
    cursor.execute("SHOW TABLES")
    tables = [row[f'Tables_in_{os.getenv("DB_NAME")}'] for row in cursor.fetchall()]

    results = []
    columns = []
    column_names = []

    if selected_table:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(f"DESCRIBE `{selected_table}`")
        columns = cursor.fetchall()
        column_names = [col['Field'] for col in columns] if columns else []

        if search_query:
            search_columns = [col['Field'] for col in columns if col['Type'].startswith(('varchar', 'text'))]
            if search_columns:
                where_clause = " OR ".join([f"LOWER(`{col}`) LIKE %s" for col in search_columns])
                values = [f"%{search_query.lower()}%"] * len(search_columns)
                query = f"SELECT * FROM `{selected_table}` WHERE {where_clause}"
                cursor.execute(query, values)
                results = cursor.fetchall()
            else:
                results = []
        else:
            cursor.execute(f"SELECT * FROM `{selected_table}`")
            results = cursor.fetchall()

    conn.close()

    return render_template(
        'admin.html',
        tables=tables,
        selected_table=selected_table,
        columns=columns,
        column_names=column_names,
        results=results,
        search_query=search_query
    )

from flask import flash  # Add at top with other Flask imports

@app.route('/admin/delete/<table>/<int:record_id>')
def delete_record(table, record_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"DELETE FROM `{table}` WHERE id = %s", (record_id,))
        conn.commit()
        flash('Record deleted successfully.', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error deleting record: {e}', 'error')
    finally:
        conn.close()
    return redirect(url_for('admin', table=table))

@app.route('/admin/edit/<table>/<int:record_id>', methods=['GET', 'POST'])
def edit_record(table, record_id):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    # Get column info
    cursor.execute(f"DESCRIBE `{table}`")
    columns = [col['Field'] for col in cursor.fetchall()]

    if request.method == 'POST':
        updates = []
        values = []
        for col in columns:
            if col != 'id':
                updates.append(f"`{col}` = %s")
                values.append(request.form.get(col))
        values.append(record_id)
        update_query = f"UPDATE `{table}` SET {', '.join(updates)} WHERE id = %s"
        try:
            cursor.execute(update_query, values)
            conn.commit()
            flash('Record updated successfully.', 'success')
            return redirect(url_for('admin', table=table))
        except Exception as e:
            conn.rollback()
            flash(f'Error updating record: {e}', 'error')

    cursor.execute(f"SELECT * FROM `{table}` WHERE id = %s", (record_id,))
    row = cursor.fetchone()
    conn.close()
    return render_template('edit.html', table=table, row=row, columns=columns)

@app.route('/admin/add/<table>', methods=['GET', 'POST'])
def add_record(table):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    # Get column names (excluding 'id')
    cursor.execute(f"DESCRIBE `{table}`")
    columns = [col['Field'] for col in cursor.fetchall() if col['Field'] != 'id']

    if request.method == 'POST':
        fields = []
        values = []
        placeholders = []

        for col in columns:
            fields.append(f"`{col}`")
            values.append(request.form.get(col))
            placeholders.append("%s")

        insert_query = f"INSERT INTO `{table}` ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
        try:
            cursor.execute(insert_query, values)
            conn.commit()
            flash('Record added successfully.', 'success')
            return redirect(url_for('admin', table=table))
        except Exception as e:
            conn.rollback()
            flash(f'Error adding record: {e}', 'error')

    conn.close()
    return render_template('add.html', table=table, columns=columns)

@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html', active_page=None), 404

if __name__ == '__main__':
    app.run(debug=True)

