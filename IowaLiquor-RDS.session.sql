-- 1. Dimension Tables DDL
-- Store Dimension Table
CREATE TABLE IF NOT EXISTS dim_store (
    store TEXT PRIMARY KEY,
    name TEXT,
    address TEXT,
    city TEXT,
    zipcode TEXT,
    store_location TEXT,
    county_number TEXT,
    county TEXT
);
-- Date Dimension Table (daily grain)
CREATE TABLE IF NOT EXISTS dim_date (
    date DATE PRIMARY KEY,
    -- Calendar date (YYYY-MM-DD)
    year SMALLINT,
    quarter SMALLINT,
    month SMALLINT,
    day_of_week SMALLINT,
    is_weekend BOOLEAN -- Additional columns like holiday_flag can be added as needed
);
-- Item/Product Dimension Table
CREATE TABLE IF NOT EXISTS dim_item (
    itemno TEXT PRIMARY KEY,
    -- Product item ID
    im_desc TEXT,
    -- Item description
    pack INTEGER,
    -- Pack size
    bottle_volume_ml INTEGER,
    state_bottle_cost NUMERIC,
    -- Cost per bottle
    state_bottle_retail NUMERIC -- Retail price per bottle
    -- (Vendor and Category are separate dimensions; not included here to avoid duplication)
);
-- Vendor Dimension Table
CREATE TABLE IF NOT EXISTS dim_vendor (
    vendor_no TEXT PRIMARY KEY,
    -- Vendor ID
    vendor_name TEXT
);
-- Category Dimension Table
CREATE TABLE IF NOT EXISTS dim_category (
    category TEXT PRIMARY KEY,
    -- Category code
    category_name TEXT
);
-- 2. Fact Table DDL
CREATE TABLE IF NOT EXISTS fact_sales (
    invoice_line_no TEXT PRIMARY KEY,
    -- Unique transaction line ID
    "date" DATE,
    -- Date of sale (FK to dim_date)
    store TEXT,
    -- Store ID (FK to dim_store)
    itemno TEXT,
    -- Item ID (FK to dim_item)
    vendor_no TEXT,
    -- Vendor ID (FK to dim_vendor)
    category TEXT,
    -- Category code (FK to dim_category)
    sale_bottles INTEGER,
    sale_dollars NUMERIC,
    sale_liters NUMERIC,
    sale_gallons NUMERIC,
    -- Define foreign key constraints for referential integrity:
    CONSTRAINT fk_store FOREIGN KEY (store) REFERENCES dim_store(store),
    CONSTRAINT fk_date FOREIGN KEY ("date") REFERENCES dim_date(date),
    CONSTRAINT fk_item FOREIGN KEY (itemno) REFERENCES dim_item(itemno),
    CONSTRAINT fk_vendor FOREIGN KEY (vendor_no) REFERENCES dim_vendor(vendor_no),
    CONSTRAINT fk_category FOREIGN KEY (category) REFERENCES dim_category(category)
);
-- 1. dim_store
INSERT INTO dim_store (
        store,
        name,
        address,
        city,
        zipcode,
        store_location,
        county_number,
        county
    )
SELECT DISTINCT store,
    name,
    address,
    city,
    zipcode,
    store_location,
    county_number,
    county
FROM public.iowa_liquor_sales
WHERE store IS NOT NULL ON CONFLICT (store) DO NOTHING;
-- 2. dim_date
INSERT INTO dim_date (
        date,
        year,
        quarter,
        month,
        day_of_week,
        is_weekend
    )
SELECT DISTINCT (date_trunc('day', date))::DATE AS date,
    EXTRACT(
        YEAR
        FROM date
    )::SMALLINT AS year,
    EXTRACT(
        QUARTER
        FROM date
    )::SMALLINT AS quarter,
    EXTRACT(
        MONTH
        FROM date
    )::SMALLINT AS month,
    EXTRACT(
        DOW
        FROM date
    )::SMALLINT AS day_of_week,
    (
        EXTRACT(
            DOW
            FROM date
        ) IN (0, 6)
    ) AS is_weekend
FROM public.iowa_liquor_sales
WHERE date IS NOT NULL ON CONFLICT (date) DO NOTHING;
-- 3. dim_item
INSERT INTO dim_item (
        itemno,
        im_desc,
        pack,
        bottle_volume_ml,
        state_bottle_cost,
        state_bottle_retail
    )
SELECT DISTINCT itemno,
    im_desc,
    pack,
    bottle_volume_ml,
    state_bottle_cost,
    state_bottle_retail
FROM public.iowa_liquor_sales
WHERE itemno IS NOT NULL ON CONFLICT (itemno) DO NOTHING;
-- 4. dim_vendor
INSERT INTO dim_vendor (vendor_no, vendor_name)
SELECT DISTINCT vendor_no,
    vendor_name
FROM public.iowa_liquor_sales
WHERE vendor_no IS NOT NULL ON CONFLICT (vendor_no) DO NOTHING;
-- 5. dim_category
INSERT INTO dim_category (category, category_name)
SELECT DISTINCT category,
    category_name
FROM public.iowa_liquor_sales
WHERE category IS NOT NULL ON CONFLICT (category) DO NOTHING;
-- 6. fact_sales
INSERT INTO fact_sales (
        invoice_line_no,
        "date",
        store,
        itemno,
        vendor_no,
        category,
        sale_bottles,
        sale_dollars,
        sale_liters,
        sale_gallons
    )
SELECT invoice_line_no,
    (date_trunc('day', date))::DATE AS date,
    store,
    itemno,
    vendor_no,
    category,
    sale_bottles,
    sale_dollars,
    sale_liters,
    sale_gallons
FROM public.iowa_liquor_sales
WHERE invoice_line_no IS NOT NULL ON CONFLICT (invoice_line_no) DO NOTHING;