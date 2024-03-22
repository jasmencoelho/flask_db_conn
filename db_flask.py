from flask import Flask, render_template, request, send_file, redirect, url_for, session
import psycopg2
import geopandas as gpd
from io import BytesIO

app = Flask(__name__)
app.secret_key = 'your_secret_key'  # Replace with a secure random string

# Database connection parameters
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'test_postgis'

# Function to establish a database connection using user-provided credentials
def connect_to_database(username, password):
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=username,
            password=password,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except psycopg2.Error as e:
        print("Error connecting to database:", e)
        return None

@app.route('/')
def index():
    # Check if user is already logged in
    if 'username' in session:
        # If user is logged in, redirect to the download page
        return redirect(url_for('download_page'))
    else:
        return render_template('login.html')

@app.route('/login', methods=['POST'])
def login():
    username = request.form['db_user']
    password = request.form['db_password']
    
    # Connect to the database using user-provided credentials
    conn = connect_to_database(username, password)
    if conn is not None:
        # Store username and password in session
        session['username'] = username
        session['password'] = password
        return redirect(url_for('download_page'))
    else:
        return 'Error: Unable to connect to the database.'

@app.route('/download')
def download_page():
    # Fetch table names after successful login
    username = session.get('username')
    password = session.get('password')
    conn = connect_to_database(username, password)
    if conn is not None:
        table_names = get_table_names(conn)
        if table_names:
            return render_template('index.html', table_names=table_names)
        else:
            return 'Error: Unable to fetch table names from the database.'
    else:
        return 'Error: You need to log in first.'

def get_table_names(conn):
    try:
        cur = conn.cursor()
        # Define the list of tables you want to include
        included_tables = ['stl_hom']  # Add your table names here
        # Fetch the table names from the database
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        all_table_names = [row[0] for row in cur.fetchall()]
        # Filter the table names based on the included_tables list
        table_names = [table_name for table_name in all_table_names if table_name in included_tables]
        cur.close()
        return table_names
    except psycopg2.Error as e:
        print("Error getting table names:", e)
        return None


@app.route('/download', methods=['POST'])
def download():
    username = session.get('username')
    password = session.get('password')
    table_name = request.form['table_name']
    
    # Connect to the database using user-provided credentials
    conn = connect_to_database(username, password)
    if conn is not None:
        try:
            query = f'SELECT * FROM {table_name};'
            gdf = gpd.read_postgis(query, conn) 
            if not gdf.empty:
                # Create a temporary directory to store the shapefile
                temp_dir = 'temp_shapefile'
                os.makedirs(temp_dir, exist_ok=True)
                
                # Save GeoDataFrame to shapefile
                temp_shapefile = os.path.join(temp_dir, table_name)
                gdf.to_file(temp_shapefile, driver='ESRI Shapefile')
                
                # Create a zip file containing the shapefile
                zip_file = f'{table_name}.zip'
                shutil.make_archive(os.path.join('temp_zip', table_name), 'zip', temp_dir)
                
                # Send the zip file as an attachment
                return send_file(os.path.join('temp_zip', zip_file), as_attachment=True, download_name=zip_file)
            else:
                return 'Error: GeoDataFrame is empty.'
        except psycopg2.Error as e:
            print("Error executing query:", e)
            return 'Error: Unable to fetch data from the database.'
        finally:
            # Clean up temporary files and directories
            shutil.rmtree(temp_dir, ignore_errors=True)
            shutil.rmtree('temp_zip', ignore_errors=True)
            # Close the connection
            conn.close()
    else:
        return 'Error: Unable to connect to the database.'
if __name__ == '__main__':
    app.run(debug=True)
