-- Create system tables if they don't exist
CREATE TABLE IF NOT EXISTS mysql.plugin (
    name varchar(64) NOT NULL PRIMARY KEY,
    dl varchar(128) NOT NULL
) ENGINE=InnoDB CHARSET=utf8;

CREATE TABLE IF NOT EXISTS mysql.component (
    component_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    component_group_id int NOT NULL,
    component_urn varchar(255) NOT NULL
) ENGINE=InnoDB CHARSET=utf8;

-- Create application tables
CREATE TABLE IF NOT EXISTS htmlpage_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    url VARCHAR(2048) NOT NULL,
    html_content LONGTEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS categories_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    category_name VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS html_categories_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    htmlpage_id INT NOT NULL,
    category_id INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (htmlpage_id) REFERENCES htmlpage_table(id) ON DELETE CASCADE,
    FOREIGN KEY (category_id) REFERENCES categories_table(id) ON DELETE CASCADE,
    UNIQUE KEY unique_page_category (htmlpage_id, category_id)
);

-- Create indexes
CREATE INDEX idx_category_name ON categories_table(category_name);
CREATE INDEX idx_url ON htmlpage_table(url);

-- Grant permissions
GRANT ALL PRIVILEGES ON wiki_scraper_db.* TO 'user'@'%';
FLUSH PRIVILEGES;

-- ALTER USER 'root'@'%' IDENTIFIED WITH mysql_native_password BY 'rootpassword';
-- FLUSH PRIVILEGES;

-- CREATE DATABASE IF NOT EXISTS wiki_scraper_db;
-- USE wiki_scraper_db;

-- CREATE TABLE IF NOT EXISTS htmlpage_table (
--     id INT AUTO_INCREMENT PRIMARY KEY,
--     url VARCHAR(255) NOT NULL,
--     html_content TEXT
-- );

-- CREATE TABLE IF NOT EXISTS categories_table (
--     id INT AUTO_INCREMENT PRIMARY KEY,
--     category_name VARCHAR(255) NOT NULL
-- );

-- CREATE TABLE IF NOT EXISTS html_categories_table (
--     htmlpage_id INT,
--     category_id INT,
--     FOREIGN KEY (htmlpage_id) REFERENCES htmlpage_table(id),
--     FOREIGN KEY (category_id) REFERENCES categories_table(id)
-- );
