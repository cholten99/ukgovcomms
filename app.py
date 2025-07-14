import os
import random
import mysql.connector
from dotenv import load_dotenv
from flask import Flask, render_template, request, redirect, url_for, make_response
from atproto import Client

app = Flask(__name__)
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
        }
    except Exception as e:
        print(f"Error fetching Bluesky post: {e}")
        return None

def parse_signatures():
    entries = []
    with open('signatures.txt', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split('|')
            name = parts[0].strip()
            role = parts[1].strip() if len(parts) > 1 and parts[1].strip() else None
            url = parts[2].strip() if len(parts) > 2 and parts[2].strip() else None
            entries.append({'name': name, 'role': role, 'url': url})
    return entries

def load_shuffled_signatures():
    all_signatures = parse_signatures()
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
    return render_template('bestpractice.html', active_page='bestpractice')

@app.route('/datavis')
def datavis():
    return render_template('datavis.html', active_page='datavis')

@app.route('/signatories')
def signatories():
    entries = parse_signatures()
    entries.sort(key=lambda x: x['name'].lower())
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
    columns = []

    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = conn.cursor(dictionary=True)

        # Get tables for dropdown
        cursor.execute("SHOW TABLES")
        tables = [list(row.values())[0] for row in cursor.fetchall()]

        # Get column info for selected table
        if selected_table in tables:
            cursor.execute(f"DESCRIBE {selected_table}")
            columns = cursor.fetchall()

        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(f"Error in admin interface: {err}")
        tables = []
        columns = []

    return render_template(
        'admin.html',
        active_page=None,
        databases=[DB_NAME],
        tables=tables,
        selected_table=selected_table,
        columns=columns
    )



@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html', active_page=None), 404

if __name__ == '__main__':
    app.run(debug=True)

