# Flask Database Connection App
This project demonstrates a simple Flask web application that connects to a PostgreSQL database, allowing user authentication and basic geospatial data handling using GeoPandas.
![](https://github.com/jasmencoelho/flask_db_conn/blob/main/demo.gif)

It includes:
- User login page with credential-based authentication
- Database connection to PostgreSQL using psycopg2
- Handling and downloading of spatial datasets
- Basic web interface with Flask templates

## 📋 Features

- **Login System**: Users can authenticate using their database credentials.
- **Database Connection**: Connects securely to a PostgreSQL database with user-supplied credentials.
- **Spatial Data Support**: Integration with GeoPandas to allow interaction with geospatial datasets.
- **Simple Web UI**: HTML templates for login and home page.
- **Session Management**: User sessions are maintained securely using Flask session cookies.

## 🛠️ Technologies Used

- Python (Flask Framework)
- PostgreSQL
- psycopg2 (PostgreSQL client library)
- GeoPandas (for spatial data)
- HTML, CSS (basic styling)
- Jupyter Notebook for visualization (demo.gif)
