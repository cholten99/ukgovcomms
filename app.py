import os
import re
import random
import datetime as dt
import mysql.connector
from dotenv import load_dotenv
from flask import (
    Flask, render_template, request, redirect, url_for,
    make_response, flash, send_from_directory
)
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
DB_NAME = os.getenv("DB_NAME")

APP_ROOT = os.path.dirname(os.path.abspath(__file__))
ASSETS_DIR = os.path.join(APP_ROOT, "assets")

# ---------------- DB helpers ----------------
def get_db_connection():
    return mysql.connector.connect(
        host=DB_HOST or 'localhost',
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

def get_latest_post():
    try:
        if not (BLUESKY_HANDLE and BLUESKY_APP_PASSWORD):
            return None
        client = Client()
        client.login(BLUESKY_HANDLE, BLUESKY_APP_PASSWORD)
        feed = client.app.bsky.feed.get_author_feed({'actor': BLUESKY_HANDLE, 'limit': 1})
        if not feed.feed:
            return None
        post_view = feed.feed[0].post
        author = post_view.author
        record = post_view.record
        return {
            'display_name': getattr(author, 'display_name', None) or author.handle,
            'handle': author.handle,
            'avatar': getattr(author, 'avatar', None),
            'text': getattr(record, 'text', ''),
            'timestamp': getattr(record, 'created_at', ''),
            'uri': post_view.uri
        }
    except Exception:
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

# ---------------- Pretty date helpers ----------------
def _ordinal(n: int) -> str:
    n = int(n)
    if 11 <= (n % 100) <= 13:
        suf = "th"
    else:
        suf = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suf}"

def _pretty_date(d) -> str:
    if d is None or d == "":
        return "—"
    if isinstance(d, (dt.date, dt.datetime)):
        return f"{d.strftime('%B')} {_ordinal(d.day)}, {d.year}"
    try:
        x = dt.datetime.fromisoformat(str(d))
        return f"{x.strftime('%B')} {_ordinal(x.day)}, {x.year}"
    except Exception:
        return str(d)

# ---------------- Slug helper (to map to assets/sources/<slug>/...) ---------
_slug_collapse = re.compile(r"-{2,}")
def slugify(name: str) -> str:
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)     # non-alnum -> hyphen
    s = _slug_collapse.sub("-", s).strip("-")
    return s or "unnamed"

# ---------------- Queries ----------------
def fetch_global_stats():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT DATE(MIN(published_at)) AS first_dt,
               DATE(MAX(published_at)) AS last_dt,
               COUNT(*) AS total_posts
        FROM BlogPost
        WHERE published_at IS NOT NULL
    """)
    row = cur.fetchone()
    conn.close()
    first_dt, last_dt, total = (row or (None, None, 0))
    return {'first': _pretty_date(first_dt), 'last': _pretty_date(last_dt), 'total': int(total or 0)}

def fetch_leaderboard_rows():
    """Return list of {name, url, slug, last_raw, last_pretty, total} for all enabled blogs."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT name, url, last_post_date, total_posts
        FROM Source
        WHERE kind='Blog' AND is_enabled=1
        ORDER BY name ASC
    """)
    rows = []
    for r in cur.fetchall():
        nm = r['name']
        rows.append({
            'name': nm,
            'url': r['url'],
            'slug': slugify(nm),
            'last_raw': r['last_post_date'],          # e.g. 2025-09-04 (or None)
            'last_pretty': _pretty_date(r['last_post_date']),
            'total': int(r['total_posts'] or 0),
        })
    conn.close()
    return rows

@app.context_processor
def inject_signatures():
    try:
        return dict(signatures=load_shuffled_signatures())
    except Exception:
        return dict(signatures=[])

@app.route('/assets/<path:filename>')
def serve_assets(filename):
    return send_from_directory(ASSETS_DIR, filename, conditional=True)

# ---------------- Routes ----------------
@app.route('/')
def home():
    if request.cookies.get("authenticated") != "true":
        return redirect(url_for('gate'))
    latest_post = get_latest_post()
    return render_template('index.html', active_page='home', latest_post=latest_post)

@app.route('/gate', methods=['GET', 'POST'])
def gate():
    error = None
    if request.method == 'POST':
        submitted = request.form.get('password')
        if submitted == PASSWORD:
            response = make_response(redirect(url_for('home')))
            response.set_cookie("authenticated", "true", max_age=60*60*24*7)
            return response
        else:
            error = "Incorrect password."
    return render_template('gate.html', error=error)

@app.route('/bestpractice')
def bestpractice():
    search = request.args.get('search', '').strip()
    type_filter = request.args.get('type', '').strip()
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT DISTINCT Type FROM BestPractice WHERE Type IS NOT NULL ORDER BY Type")
    types = [row['Type'] for row in cursor.fetchall()]
    query = "SELECT id, Title, Type, Description, URL FROM BestPractice WHERE 1=1"
    params = []
    if search:
        query += " AND (Title LIKE %s OR Description LIKE %s)"
        like = f"%{search}%"; params.extend([like, like])
    if type_filter and type_filter.lower() != "any":
        query += " AND Type = %s"; params.append(type_filter)
    cursor.execute(query, params)
    results = cursor.fetchall()
    cursor.close(); conn.close()
    return render_template('bestpractice.html', results=results, search=search,
                           type_filter=type_filter, types=types)

@app.route('/datavis')
def datavis():
    stats = fetch_global_stats()
    leaderboard = fetch_leaderboard_rows()
    return render_template('datavis.html', active_page='datavis',
                           stats=stats, leaderboard=leaderboard)

@app.route('/signatories')
def signatories():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT name, role, url FROM Signatory ORDER BY name")
    entries = cursor.fetchall()
    conn.close()
    return render_template('signatories.html', active_page='signatories', signatories=entries)

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
    cursor.execute("SHOW TABLES")
    tables = [row[f'Tables_in_{DB_NAME}'] for row in cursor.fetchall()]
    results = []; columns = []; column_names = []
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
                cursor.execute(f"SELECT * FROM `{selected_table}` WHERE {where_clause}", values)
                results = cursor.fetchall()
        else:
            cursor.execute(f"SELECT * FROM `{selected_table}`")
            results = cursor.fetchall()
    conn.close()
    return render_template('admin.html', tables=tables, selected_table=selected_table,
                           columns=columns, column_names=column_names,
                           results=results, search_query=search_query)

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
    cursor.execute(f"DESCRIBE `{table}`")
    columns = [col['Field'] for col in cursor.fetchall()]
    if request.method == 'POST':
        updates, values = [], []
        for col in columns:
            if col != 'id':
                updates.append(f"`{col}` = %s")
                values.append(request.form.get(col))
        values.append(record_id)
        try:
            cursor.execute(f"UPDATE `{table}` SET {', '.join(updates)} WHERE id = %s", values)
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
    cursor.execute(f"DESCRIBE `{table}`")
    columns = [c['Field'] for c in cursor.fetchall() if c['Field'] != 'id']
    if request.method == 'POST':
        fields, values, ph = [], [], []
        for col in columns:
            fields.append(f"`{col}`")
            values.append(request.form.get(col))
            ph.append("%s")
        try:
            cursor.execute(f"INSERT INTO `{table}` ({', '.join(fields)}) VALUES ({', '.join(ph)})", values)
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

