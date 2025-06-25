import os
import random
from dotenv import load_dotenv
from flask import Flask, render_template
from atproto import Client

app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True

# Load environment variables from .env file
load_dotenv()
BLUESKY_HANDLE = os.getenv("BLUESKY_HANDLE")
BLUESKY_APP_PASSWORD = os.getenv("BLUESKY_APP_PASSWORD")

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

        embed = {
            'display_name': getattr(author, 'display_name', None) or author.handle,
            'handle': author.handle,
            'avatar': author.avatar,
            'text': record.text,
            'timestamp': record.created_at,
        }

        return embed

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

def get_rotating_signature_name():
    signatures = parse_signatures()
    if not signatures:
        return None
    return random.choice(signatures)['name']

@app.context_processor
def inject_signatures():
    return {'signatures': load_shuffled_signatures()}

@app.route('/')
def home():
    latest_post = get_latest_post()
    return render_template('index.html',
                           active_page='home',
                           latest_post=latest_post)

@app.route('/bestpractice')
def bestpractice():
    return render_template('bestpractice.html',
                           active_page='bestpractice',
                           signature=get_rotating_signature_name())

@app.route('/video')
def video():
    return render_template('video.html',
                           active_page='video',
                           signature=get_rotating_signature_name())

@app.route('/dataviz')
def dataviz():
    return render_template('dataviz.html',
                           active_page='dataviz',
                           signature=get_rotating_signature_name())

@app.route('/signatories')
def signatories():
    entries = parse_signatures()
    entries.sort(key=lambda x: x['name'].lower())
    return render_template('signatories.html',
                           active_page='signatories',
                           signatories=entries,
                           signature=get_rotating_signature_name())

@app.route('/tbd')
def tbd():
    return render_template('tbd.html',
                           active_page='tbd',
                           signature=get_rotating_signature_name())

@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html',
                           active_page=None,
                           signature=get_rotating_signature_name()), 404

if __name__ == '__main__':
    app.run(debug=True)
