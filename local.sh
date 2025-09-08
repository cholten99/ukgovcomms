#/bin/bash
export FLASK_APP=wsgi:application
export FLASK_DEBUG=1            # optional, auto-reload + debugger
flask run --host 127.0.0.1 --port 8000
