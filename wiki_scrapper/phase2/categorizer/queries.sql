DROP TABLE IF EXISTS html_categories_table;
DROP TABLE IF EXISTS categories_table;
DROP TABLE IF EXISTS htmlpage_table;

-- Create enhanced htmlpage_table with metadata columns
CREATE TABLE htmlpage_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    url VARCHAR(768) UNIQUE NOT NULL,
    headline VARCHAR(512),
    category_count INT,
    internal_links_count INT,
    word_count INT,
    file_size_bytes BIGINT,
    last_modified TIMESTAMP,
    processed_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create categories table
CREATE TABLE categories_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    category_name VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create linking table
CREATE TABLE html_categories_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    htmlpage_id INT NOT NULL,
    category_id INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (htmlpage_id) REFERENCES htmlpage_table(id) ON DELETE CASCADE,
    FOREIGN KEY (category_id) REFERENCES categories_table(id) ON DELETE CASCADE,
    UNIQUE KEY unique_page_category (htmlpage_id, category_id)
);

-- Create indexes for better performance
CREATE INDEX idx_url ON htmlpage_table(url);
CREATE INDEX idx_headline ON htmlpage_table(headline);
CREATE INDEX idx_category_name ON categories_table(category_name);