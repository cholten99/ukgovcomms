import sys
import os

# Add the project directory and site-packages from the virtual environment
sys.path.insert(0, '/var/www/ukgovcomms')
sys.path.insert(0, '/var/www/.venvs/ukgovcomms/lib/python3.6/site-packages')  # adjust version if needed

# Set environment variable so Flask knows where the app is
os.environ['FLASK_APP'] = 'app'

# Import and expose the Flask application
from app import app as application
