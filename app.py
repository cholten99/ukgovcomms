from flask import Flask, render_template

app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True

@app.route('/')
def home():
    return render_template('index.html', active_page='home')

@app.route('/about')
def about():
    return render_template('about.html', active_page='about')

@app.route('/contact')
def contact():
    return render_template('contact.html', active_page='contact')
    
@app.route('/tbd')
def tbd():
    return render_template('tbd.html', active_page='tbd')
    
@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html', active_page=None), 404


if __name__ == '__main__':
    app.run(debug=True)
