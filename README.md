# Bengo Data Platform

## Infrastructure

### Docker & PostgreSQL
- **Docker**: Containerized infrastructure for consistent, reproducible environments
- **PostgreSQL**: Robust, ACID-compliant database chosen for its reliability and SQL standard compliance
- **Benefits**: Easy deployment, version control, and environment consistency across development/production

### dbt (Data Build Tool)
- **Transformation Layer**: SQL-based transformations with version control and testing
- **Documentation**: Auto-generated lineage and model documentation
- **Testing**: Built-in data quality tests and validation
- **Incremental Processing**: Efficient data updates using MERGE operations

## Reasoning & Architecture

### Why This Stack?
1. **Scalability**: PostgreSQL handles growing data volumes efficiently
2. **Maintainability**: dbt provides version-controlled, testable SQL transformations
3. **Developer Experience**: Modern tools with excellent documentation and community support
4. **Cost Efficiency**: Open-source stack reduces licensing costs
5. **Future-Proof**: Can easily migrate to cloud warehouses (Snowflake, BigQuery, Redshift)

### Data Flow
```
Raw Data → PostgreSQL → dbt Transformations → Analytics-Ready Tables
```

## Data Model

### Source Tables (`data` schema)
- **source_customers**: Customer master data with demographics and signup information
- **source_products**: Product catalog with pricing and categories
- **source_orders**: Order transactions with status tracking and lifecycle management
- **source_order_items**: Line-level order details linking products to orders

### Transformation Layers
1. **Raw Layer**: Direct copies of source data with minimal transformation
2. **Staging Layer**: Cleaned and standardized data with business logic applied
3. **Marts Layer**: Business-ready tables optimized for analytics and reporting

### Key Features
- **Incremental Processing**: Only processes new/changed data for efficiency
- **Data Quality Tests**: Automated validation of data integrity and business rules
- **Comprehensive Lineage**: Full traceability from source to final analytics tables
- **Order Lifecycle**: Realistic e-commerce order flow (pending → completed/cancelled → refunded)

## Setup Instructions

### Prerequisites
- Python 3.8+ installed
- Docker Desktop running
- Git (for version control)

### Quick Start (Zero to Production)
```bash
# Clone the repository
git clone https://github.com/bengo76/bengo_data_platform
cd bengo_data_platform

# Run automated setup (creates everything from scratch)
python3 setup.py
```

The setup script automatically:
1. Creates Python virtual environment
2. Installs all dependencies (dbt, PostgreSQL drivers, etc.)
3. Pulls and starts PostgreSQL Docker container
4. Creates database and schemas
5. Installs dbt packages
6. Tests database connections
7. Populates sample data (300 customers, 500 products, 2500+ orders)
8. Builds all dbt models and runs tests
9. Generates documentation

### Manual Setup (Optional)
```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Start PostgreSQL
docker compose up -d

# Initialize database
python populate_data.py --init-db --drop-recreate

# Run dbt pipeline
dbt deps
dbt build
```

## Usage

### View Documentation
```bash
dbt docs serve
# Opens interactive documentation at http://localhost:8080
```

### Add More Data
```bash
# Add 100 more customers and 50 products
python populate_data.py --customers 100 --products 50
```

**Available Parameters:**
- `--customers N`: Number of new customers to generate (default: 300)
- `--products N`: Number of new products to generate (default: 500)
- `--orders N`: Number of new orders to generate (default: 2500)
- `--order-items N`: Maximum items per order (default: 7)
- `--days N`: Time window in days for order generation (default: 30)
- `--start-date YYYY-MM-DD`: Start date for order generation (optional)
- `--init-db`: Initialize database schema and tables
- `--drop-recreate`: Drop and recreate all tables (destructive)
- `--truncate`: Clear all existing data before adding new data
- `--stats-only`: Show table statistics without adding data

**Examples:**
```bash
# Generate small dataset for testing
python populate_data.py --customers 50 --products 100 --orders 200 --order-items 3

# Generate data for last 7 days starting from specific date
python populate_data.py --days 7 --start-date 2025-01-01 --customers 200 --orders 500
```

### Run Incremental Processing
```bash
# Run incremental processing
dbt build
```

**dbt Commands Explained:**
- `dbt build`: **Recommended** - Runs models, tests, and snapshots in dependency order
  - Executes all models (creates/updates tables and views)
  - Runs all data quality tests after each model
  - Stops on first failure for safety
  - Use for production deployments

- `dbt run`: Executes only the model transformations (creates/updates tables and views)
  - Skips tests - faster but less safe
  - Use for development when you want quick iterations

- `dbt test`: Runs only the data quality tests
  - Validates data integrity and business rules
  - Use after `dbt run` to verify data quality

**Force Refresh Options:**
```bash
# Force refresh all models (ignores incremental logic)
dbt build --full-refresh

# Run specific model and its dependencies
dbt build --select +model_name

# Run models and downstream dependencies  
dbt build --select model_name+
```

### Check Data Statistics
```bash
python populate_data.py --stats-only
```

### Stop Infrastructure
```bash
docker compose down
```

## Development

### Project Structure
```
bengo_data_platform/
├── models/          # dbt SQL models
├── tests/           # dbt data tests
├── macros/          # dbt reusable SQL macros
├── docs/            # Additional documentation
├── populate_data.py # Data generation script
├── setup.py         # Automated setup script
├── requirements.txt # Python dependencies
└── docker-compose.yml # Docker configuration
```

### Adding New Models
1. Create SQL files in `models/` directory
2. Add tests in `tests/` or use schema.yml
3. Run `dbt build` to execute transformations
4. Update documentation with `dbt docs generate`

### Data Quality
- All models include automated tests
- Primary key uniqueness validation
- Foreign key relationship checks
- Business rule validation (e.g., order totals)
- **Easy Test Generation**: Data quality tests are automatically generated by simply defining data type constraints and relationships in schema.yml files, making it effortless to maintain comprehensive test coverage

---

**Built with modern data stack principles**

*This project was developed with AI assistance to accelerate development speed and enhance documentation quality, demonstrating the power of human-AI collaboration in modern data engineering.*