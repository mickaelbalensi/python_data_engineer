import os
import logging
import time
import mysql.connector
from mysql.connector import Error
from bs4 import BeautifulSoup
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define local HTML folder
LOCAL_HTML_FOLDER = os.path.abspath("../../html_pages")

def create_spark_session():
    """Creates and returns a Spark session"""
    return SparkSession.builder \
        .appName("WikiMetadataExtractor") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

def extract_metadata_from_html(file_path, content):
    """
    Extracts all metadata from an HTML file.
    
    Returns:
        dict: Dictionary containing all extracted metadata
    """
    soup = BeautifulSoup(content, 'html.parser')
    file_stat = os.stat(file_path)
    
    # Extract headline (title)
    headline = ""
    title_tag = soup.find('h1', {'id': 'firstHeading'})
    if title_tag:
        headline = title_tag.get_text().strip()

    # Extract categories
    categories = []
    category_section = soup.find('div', {'id': 'mw-normal-catlinks'})
    if category_section:
        category_links = category_section.find_all('a')[1:]  # Skip "Categories"
        categories = [link.get_text().strip() for link in category_links]

    # Count internal links
    internal_links = soup.find_all('a', href=re.compile('^/wiki/'))
    internal_links_count = len(internal_links)

    # Count words in main content
    main_content = soup.find('div', {'id': 'mw-content-text'})
    word_count = 0
    if main_content:
        text = main_content.get_text()
        words = re.findall(r'\w+', text)
        word_count = len(words)

    return {
        "filename": os.path.basename(file_path),
        "headline": headline,
        "categories": categories,
        "category_count": len(categories),
        "internal_links_count": internal_links_count,
        "word_count": word_count,
        "file_size_bytes": file_stat.st_size,
        "last_modified": datetime.fromtimestamp(file_stat.st_mtime),
        "processed_timestamp": datetime.now()
    }

def insert_html_page_with_metadata(connection, metadata):
    """Inserts an HTML page with metadata into the database and returns its ID."""
    cursor = connection.cursor()
    insert_query = """
    INSERT INTO htmlpage_table (
        url, headline, category_count, internal_links_count, 
        word_count, file_size_bytes, last_modified, processed_timestamp
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        metadata["filename"],
        metadata["headline"],
        metadata["category_count"],
        metadata["internal_links_count"],
        metadata["word_count"],
        metadata["file_size_bytes"],
        metadata["last_modified"],
        metadata["processed_timestamp"]
    )
    
    cursor.execute(insert_query, values)
    connection.commit()
    
    logger.info(f"Inserted HTML page with metadata: {metadata['filename']}")
    return cursor.lastrowid

def process_html_folder_with_spark(connection):
    """Processes HTML files using Spark for parallel processing"""
    if not os.path.exists(LOCAL_HTML_FOLDER):
        logger.error(f"Directory does not exist: {LOCAL_HTML_FOLDER}")
        return

    # Create Spark session
    spark = create_spark_session()

    # Define schema for the metadata DataFrame
    schema = StructType([
        StructField("filename", StringType(), False),
        StructField("headline", StringType(), True),
        StructField("categories", StringType(), True),
        StructField("category_count", IntegerType(), True),
        StructField("internal_links_count", IntegerType(), True),
        StructField("word_count", IntegerType(), True),
        StructField("file_size_bytes", LongType(), True),
        StructField("last_modified", TimestampType(), True),
        StructField("processed_timestamp", TimestampType(), True)
    ])

    # Get list of HTML files
    html_files = [f for f in os.listdir(LOCAL_HTML_FOLDER) if f.endswith('.html')]

    # Process each file and collect metadata
    for filename in html_files:
        file_path = os.path.join(LOCAL_HTML_FOLDER, filename)
        logger.info(f"Processing file: {file_path}")

        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()

        # Extract metadata
        metadata = extract_metadata_from_html(file_path, content)
        logger.info(f"Extracted metadata: {metadata}")

        # Insert into MySQL
        try:
            page_id = insert_html_page_with_metadata(connection, metadata)

            # Insert categories
            for category in metadata["categories"]:
                category_id = insert_category(connection, category)
                link_html_and_category(connection, page_id, category_id)
        except:
            continue

    spark.stop()

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


# Main script execution
conn = connect_to_mysql()
if conn:
    process_html_folder_with_spark(conn)
    conn.close()
    logger.info("Database connection closed.")


# import os
# import logging
# import time
# import mysql.connector
# from mysql.connector import Error
# from bs4 import BeautifulSoup

# # Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# # Define local HTML folder (use absolute path for reliability)


# def insert_html_page(connection, url):
#     """Inserts an HTML page into the database and returns its ID."""
#     cursor = connection.cursor()
#     insert_query = """INSERT INTO htmlpage_table (url) VALUES (%s)"""
    
#     cursor.execute(insert_query, (url,))  # Pass a tuple (url,)
#     connection.commit()
    
#     logger.info(f"Inserted HTML page: {url}")
#     return cursor.lastrowid


# def extract_categories_from_html(html_content):
#     """Extracts categories from a Wikipedia HTML page."""
#     soup = BeautifulSoup(html_content, 'html.parser')
    
#     # Find the category section in Wikipedia HTML
#     category_section = soup.find('div', {'id': 'mw-normal-catlinks'})
    
#     categories = []
#     if category_section:
#         category_links = category_section.find_all('a')[1:]  # Skip "Categories"
#         categories = [link.get_text().strip() for link in category_links]

#     logger.debug(f"Extracted categories: {categories}")
#     return categories






