import sys
import os

# Add the project directory to the path
sys.path.insert(0, '/var/www/ukgovcomms')

# Set the environment variable Flask uses
os.environ['FLASK_APP'] = 'app'

# Import and expose the Flask app
from app import app as application

