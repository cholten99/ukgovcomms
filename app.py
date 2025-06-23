import os
from dotenv import load_dotenv
import random
from flask import Flask, render_template
from atproto import Client

app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True

# Bluesky configuration
load_dotenv()  # Load environment variables from .env file

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

# Signatures config
def load_shuffled_signatures():
    with open('signatures.txt', encoding='utf-8') as f:
        lines = [line.strip() for line in f if line.strip()]
    random.shuffle(lines)
    return lines

# Page routes
@app.route('/')
def home():
    signatures = load_shuffled_signatures()
    latest_post = get_latest_post()
    return render_template('index.html',
                           active_page='home',
                           signatures=signatures,
                           latest_post=latest_post)

@app.route('/bestpractice')
def bestpractice():
    return render_template('bestpractice.html', active_page='bestpractice')

@app.route('/video')
def video():
    return render_template('video.html', active_page='video')

@app.route('/dataviz')
def dataviz():
    return render_template('dataviz.html', active_page='dataviz')
    
@app.route('/signatories')
def signatories():
    return render_template('signatories.html', active_page='signatories')

@app.route('/tbd')
def tbd():
    return render_template('tbd.html', active_page='tbd')
    
@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html', active_page=None), 404


if __name__ == '__main__':
    app.run(debug=True)
