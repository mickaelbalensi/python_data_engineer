import os
import logging
import time
import mysql.connector
from mysql.connector import Error
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define local HTML folder (use absolute path for reliability)
LOCAL_HTML_FOLDER = os.path.abspath("../../html_pages")  # Change to your actual path

def connect_to_mysql(max_retries=5, retry_delay=5):
    """
    Connects to MySQL with retry mechanism.
    
    Args:
        max_retries (int): Maximum number of connection attempts
        retry_delay (int): Delay in seconds between retries
    
    Returns:
        mysql.connector.connection.MySQLConnection: Database connection object or None
    """
    for attempt in range(max_retries):
        try:
            connection = mysql.connector.connect(
                host='localhost',  # Change 'mysql' to 'localhost'
                port=3306,  # Add port 3307
                database='wiki_scraper_db',
                user='debian-sys-maint',
                password='rootpassword',
                # auth_plugin='mysql_native_password'
            )
            if connection.is_connected():
                logger.info("Successfully connected to MySQL")
                return connection
        except Error as e:
            logger.warning(f"Connection attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Maximum retry attempts reached. Could not connect to MySQL.")
                return None

def insert_html_page(connection, url):
    """Inserts an HTML page into the database and returns its ID."""
    cursor = connection.cursor()
    insert_query = """INSERT INTO htmlpage_table (url) VALUES (%s)"""
    
    cursor.execute(insert_query, (url,))  # Pass a tuple (url,)
    connection.commit()
    
    logger.info(f"Inserted HTML page: {url}")
    return cursor.lastrowid

def insert_category(connection, category_name):
    """Inserts a category if not already in the database, then returns its ID."""
    cursor = connection.cursor()
    cursor.execute("SELECT id FROM categories_table WHERE category_name = %s", (category_name,))
    result = cursor.fetchone()
    
    if result:
        category_id = result[0]
        logger.debug(f"Category already exists: {category_name} (ID: {category_id})")
    else:
        insert_query = """INSERT INTO categories_table (category_name)
                          VALUES (%s)"""
        cursor.execute(insert_query, (category_name,))
        connection.commit()
        category_id = cursor.lastrowid
        logger.info(f"Inserted new category: {category_name} (ID: {category_id})")
    
    return category_id

def link_html_and_category(connection, htmlpage_id, category_id):
    """Links an HTML page to a category in the relational table."""
    cursor = connection.cursor()
    insert_query = """INSERT INTO html_categories_table (htmlpage_id, category_id)
                      VALUES (%s, %s)"""
    cursor.execute(insert_query, (htmlpage_id, category_id))
    connection.commit()
    logger.info(f"Linked HTML page ID {htmlpage_id} to category ID {category_id}")

def extract_categories_from_html(html_content):
    """Extracts categories from a Wikipedia HTML page."""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find the category section in Wikipedia HTML
    category_section = soup.find('div', {'id': 'mw-normal-catlinks'})
    
    categories = []
    if category_section:
        category_links = category_section.find_all('a')[1:]  # Skip "Categories"
        categories = [link.get_text().strip() for link in category_links]

    logger.debug(f"Extracted categories: {categories}")
    return categories

def process_html_folder(connection):
    """Processes all HTML files in the local folder and stores them in MySQL."""
    if not os.path.exists(LOCAL_HTML_FOLDER):
        logger.error(f"Directory does not exist: {LOCAL_HTML_FOLDER}")
        return

    for filename in os.listdir(LOCAL_HTML_FOLDER)[:1]:
        if filename.endswith('.html'):
            file_path = os.path.join(LOCAL_HTML_FOLDER, filename)
            logger.info(f"Processing file: {file_path}")

            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()

            logger.info(f"Extracting categories from HTML file: {filename}")
            # Extract categories from the HTML file
            categories = extract_categories_from_html(content)
            logger.info(f"categories: {categories}")
            
            # Insert HTML page into database
            page_id = insert_html_page(connection, filename)
            
            # Insert categories and link them to the page
            for category in categories:
                category_id = insert_category(connection, category)
                link_html_and_category(connection, page_id, category_id)
        else:
            logger.warning(f"Skipping non-HTML file: {filename}")

# Main script execution
conn = connect_to_mysql()
if conn:
    process_html_folder(conn)
    conn.close()
    logger.info("Database connection closed.")



