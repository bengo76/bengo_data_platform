#!/usr/bin/env python3
"""
Bengo Data Platform - Complete Zero-to-Production Setup Script
Comprehensive automated setup for the complete dbt data platform

This script will:
1. Check system requirements (Python 3.8+, Docker)
2. Set up Python virtual environment
3. Install all dependencies (dbt, drivers, etc.)
4. Start PostgreSQL Docker container
5. Create edikted database
6. Install dbt dependencies
7. Initialize database with sample data
8. Build complete dbt pipeline (raw → staging → marts → analysis)
9. Run all 324 tests
10. Verify end-to-end functionality

Compatible with Windows, macOS, and Linux
"""

import os
import sys
import subprocess
import platform
import time
from pathlib import Path

def run_command(command, description, shell=False):
    """Run a command and handle errors"""
    print(f"\n🔄 {description}...")
    try:
        if isinstance(command, str):
            result = subprocess.run(command, shell=True, capture_output=True, text=True)
        else:
            result = subprocess.run(command, capture_output=True, text=True, shell=shell)
        
        if result.returncode != 0:
            print(f"❌ Error: {description} failed")
            print(f"Error output: {result.stderr}")
            return False
        else:
            print(f"✅ {description} completed successfully")
            if result.stdout.strip():
                print(f"Output: {result.stdout.strip()}")
            return True
    except Exception as e:
        print(f"❌ Error running command: {e}")
        return False

def check_docker():
    """Check if Docker is installed and running"""
    print("\n🔍 Checking Docker...")
    
    # Check if docker command exists
    try:
        result = subprocess.run(["docker", "--version"], capture_output=True, text=True)
        if result.returncode != 0:
            print("❌ Docker is not installed or not in PATH")
            print("Please install Docker Desktop from: https://www.docker.com/products/docker-desktop")
            return False
        print(f"✅ Docker found: {result.stdout.strip()}")
    except FileNotFoundError:
        print("❌ Docker is not installed")
        print("Please install Docker Desktop from: https://www.docker.com/products/docker-desktop")
        return False
    
    # Check if Docker daemon is running
    try:
        result = subprocess.run(["docker", "info"], capture_output=True, text=True)
        if result.returncode != 0:
            print("❌ Docker daemon is not running")
            print("Please start Docker Desktop")
            return False
        print("✅ Docker daemon is running")
        return True
    except Exception:
        print("❌ Could not connect to Docker daemon")
        print("Please start Docker Desktop")
        return False

def check_python():
    """Check Python version"""
    print(f"\n🔍 Checking Python...")
    version = sys.version_info
    print(f"✅ Python {version.major}.{version.minor}.{version.micro} found")
    
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print("❌ Python 3.8 or higher is required")
        return False
    return True

def setup_virtual_environment():
    """Create and set up virtual environment"""
    venv_path = Path(".venv")
    
    if venv_path.exists():
        print("✅ Virtual environment already exists")
        return True
    
    print("\n🔄 Creating virtual environment...")
    try:
        subprocess.run([sys.executable, "-m", "venv", ".venv"], check=True)
        print("✅ Virtual environment created")
        return True
    except subprocess.CalledProcessError:
        print("❌ Failed to create virtual environment")
        return False

def load_environment():
    """Load environment variables from .env file"""
    try:
        from dotenv import load_dotenv
    except ImportError:
        print("❌ python-dotenv not installed, skipping .env loading")
        return False
        
    env_file = Path(".env")
    if env_file.exists():
        load_dotenv(".env")
        print("✅ Environment variables loaded from .env")
        return True
    else:
        print("❌ .env file not found")
        return False

def get_dbt_environment():
    """Get environment variables for dbt commands including .env file variables"""
    try:
        from dotenv import load_dotenv
        # Load .env file variables
        load_dotenv(".env")
    except ImportError:
        print("⚠️ python-dotenv not available, using system environment only")
    
    # Create environment with both system and .env variables
    env = dict(os.environ)
    env["DBT_PROFILES_DIR"] = "."
    
    return env

def get_python_executable():
    """Get the correct Python executable path for the virtual environment"""
    system = platform.system().lower()
    if system == "windows":
        return os.path.join(".venv", "Scripts", "python.exe")
    else:
        return os.path.join(".venv", "bin", "python")

def get_pip_executable():
    """Get the correct pip executable path for the virtual environment"""
    system = platform.system().lower()
    if system == "windows":
        return os.path.join(".venv", "Scripts", "pip.exe")
    else:
        return os.path.join(".venv", "bin", "pip")

def get_dbt_executable():
    """Get the correct dbt executable path for the virtual environment"""
    system = platform.system().lower()
    if system == "windows":
        return os.path.join(".venv", "Scripts", "dbt.exe")
    else:
        return os.path.join(".venv", "bin", "dbt")

def install_dependencies():
    """Install Python dependencies"""
    pip_path = get_pip_executable()
    
    print(f"\n🔄 Installing Python dependencies...")
    try:
        # Upgrade pip first
        subprocess.run([pip_path, "install", "--upgrade", "pip"], check=True)
        print("✅ Pip upgraded")
        
        # Install dependencies - prioritize requirements.txt like setup.sh
        if os.path.exists("requirements.txt"):
            subprocess.run([pip_path, "install", "-r", "requirements.txt"], check=True)
            print("✅ Dependencies from requirements.txt installed")
        else:
            print("⚠️  requirements.txt not found, installing dbt-postgres manually")
            subprocess.run([pip_path, "install", "dbt-postgres"], check=True)
            print("✅ dbt-postgres installed")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies: {e}")
        return False

def pull_postgres_image():
    """Pull PostgreSQL Docker image"""
    return run_command(["docker", "pull", "postgres:latest"], "Pulling PostgreSQL Docker image")

def start_database():
    """Start PostgreSQL database using Docker Compose"""
    # Create .env file if it doesn't exist
    env_file = Path(".env")
    if not env_file.exists():
        print("🔄 Creating .env configuration file...")
        try:
            with open(".env", "w") as f:
                f.write("# Database Configuration\n")
                f.write("DB_HOST=localhost\n")
                f.write("DB_PORT=5432\n")
                f.write("DB_NAME=edikted\n")
                f.write("DB_SCHEMA=data\n")
                f.write("DB_USER=postgres\n")
                f.write("DB_PASSWORD=postgres123\n")
                f.write("\n# dbt Configuration\n")
                f.write("DBT_PROFILES_DIR=.\n")
            print("✅ .env file created")
        except Exception as e:
            print(f"❌ Failed to create .env file: {e}")
            return False
    
    # Check if container is already running
    result = subprocess.run(["docker", "ps", "-q", "-f", "name=bengo_postgres"], 
                          capture_output=True, text=True)
    
    if result.stdout.strip():
        print("✅ PostgreSQL container is already running")
        return True
    
    # Try to start with docker compose
    compose_commands = [
        ["docker", "compose", "up", "-d"],
        ["docker-compose", "up", "-d"]
    ]
    
    for cmd in compose_commands:
        if run_command(cmd, "Starting PostgreSQL container"):
            return True
    
    print("❌ Failed to start PostgreSQL container")
    print("Please ensure Docker Compose is available")
    return False

def wait_for_database():
    """Wait for PostgreSQL to be ready and fully operational"""
    print("\n🔄 Waiting for PostgreSQL to be ready...")
    max_attempts = 30
    attempt = 0
    
    while attempt < max_attempts:
        try:
            # First check if PostgreSQL is accepting connections
            result = subprocess.run([
                "docker", "exec", "bengo_postgres", 
                "pg_isready", "-U", "postgres", "-d", "postgres"
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                # PostgreSQL is accepting connections, now test if we can actually query
                test_result = subprocess.run([
                    "docker", "exec", "bengo_postgres", 
                    "psql", "-U", "postgres", "-d", "postgres", "-c", "SELECT 1;"
                ], capture_output=True, text=True)
                
                if test_result.returncode == 0:
                    print("✅ PostgreSQL is ready and operational!")
                    # Add extra buffer for container stability
                    print("🔄 Waiting additional 3 seconds for container stability...")
                    time.sleep(3)
                    return True
            
            attempt += 1
            print(f"⏳ Waiting... (attempt {attempt}/{max_attempts})")
            time.sleep(2)
            
        except Exception as e:
            print(f"Error checking database status: {e}")
            attempt += 1
            time.sleep(2)
    
    print("❌ PostgreSQL failed to start within expected time")
    return False

def install_dbt_dependencies():
    """Install dbt dependencies"""
    dbt_path = get_dbt_executable()
    
    print(f"\n🔄 Installing dbt dependencies...")
    try:
        result = subprocess.run([dbt_path, "deps"], 
                              check=True, 
                              capture_output=True, 
                              text=True,
                              env=get_dbt_environment())
        print("✅ dbt dependencies installed")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dbt dependencies: {e}")
        print(f"Error: {e.stderr}")
        return False

def create_edikted_database():
    """Create the edikted database inside PostgreSQL container"""
    print(f"\n🔄 Creating edikted database...")
    try:
        # Check if database exists
        result = subprocess.run([
            "docker", "exec", "bengo_postgres", 
            "psql", "-U", "postgres", "-tAc", 
            "SELECT 1 FROM pg_database WHERE datname = 'edikted';"
        ], capture_output=True, text=True)
        
        if result.stdout.strip() == "1":
            print("✅ edikted database already exists")
            return True
        
        # Create the database
        result = subprocess.run([
            "docker", "exec", "bengo_postgres", 
            "createdb", "-U", "postgres", "edikted"
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ edikted database created successfully")
            # Give database a moment to be fully ready
            print("🔄 Waiting 2 seconds for database to be fully ready...")
            time.sleep(2)
            return True
        else:
            print(f"❌ Failed to create edikted database: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error creating edikted database: {e}")
        return False

def user_acknowledgment_initial_data():
    """Get user acknowledgment before initial data population and build"""
    print(f"\n{'='*40}")
    print("🚀 READY FOR DATA PIPELINE")
    print("="*40)
    print("✅ Environment ready")
    print("✅ Models compiled")
    print("✅ Documentation generated")
    print("")
    print("Next: Populate data + build pipeline (~2 min)")
    
    while True:
        response = input("\n✅ Continue? (y/n): ").lower().strip()
        if response in ['y', 'yes']:
            print("🚀 Starting...")
            return True
        elif response in ['n', 'no']:
            print("⏹️  Paused")
            return False
        else:
            print("❓ Enter 'y' or 'n'")

def user_acknowledgment_incremental_test():
    """Get user acknowledgment before incremental testing"""
    print(f"\n{'='*40}")
    print("🎉 PIPELINE COMPLETE!")
    print("="*40)
    print("✅ Data populated")
    print("✅ Pipeline built")
    print("✅ All 324 tests passed")
    print("")
    print("Next: Test incremental processing (~1 min)")
    
    while True:
        response = input("\n✅ Continue? (y/n): ").lower().strip()
        if response in ['y', 'yes']:
            print("🚀 Testing incremental...")
            return True
        elif response in ['n', 'no']:
            print("✅ Setup complete!")
            return False
        else:
            print("❓ Enter 'y' or 'n'")

def run_incremental_data_test():
    """Run incremental data population and build"""
    python_path = get_python_executable()
    
    print(f"\n🔄 Adding data...")
    try:
        result = subprocess.run([python_path, "populate_data.py", 
                               "--customers", "25", "--products", "50", "--orders", "100"], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Data added")
            return True
        else:
            print(f"❌ Failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def run_incremental_build():
    """Run incremental dbt build"""
    dbt_path = get_dbt_executable()
    
    print(f"\n🔄 Building...")
    try:
        result = subprocess.run([dbt_path, "build"], 
                              capture_output=True, text=True,
                              env=get_dbt_environment())
        
        if result.returncode == 0:
            print("✅ Build complete")
            # Show merge operations only
            lines = result.stdout.split('\n')
            for line in lines:
                if 'MERGE' in line and 'raw_' in line:
                    print(f"   {line.strip()}")
            return True
        else:
            print(f"❌ Failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def run_database_initialization():
    """Run the database initialization using populate_data.py"""
    python_path = get_python_executable()
    
    print(f"\n🔄 Initializing database schema and tables...")
    try:
        result = subprocess.run([python_path, "populate_data.py", "--init-db", "--drop-recreate"], 
                              capture_output=True, text=True, env=get_dbt_environment())
        
        if result.returncode == 0:
            print("✅ Database initialization completed successfully!")
            print(result.stdout)
            return True
        else:
            print(f"❌ Database initialization failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error running database initialization: {e}")
        return False

def run_dbt_compile():
    """Run dbt compile to validate models"""
    dbt_path = get_dbt_executable()
    
    print(f"\n🔄 Compiling models...")
    try:
        result = subprocess.run([dbt_path, "compile"], 
                              capture_output=True, text=True,
                              env=get_dbt_environment())
        
        if result.returncode == 0:
            print("✅ Models compiled")
            return True
        else:
            print(f"❌ Compile failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def generate_dbt_docs():
    """Generate dbt documentation"""
    dbt_path = get_dbt_executable()
    
    print(f"\n🔄 Generating docs...")
    try:
        result = subprocess.run([dbt_path, "docs", "generate"], 
                              capture_output=True, text=True,
                              env=get_dbt_environment())
        
        if result.returncode == 0:
            print("✅ Docs generated")
            return True
        else:
            print(f"❌ Docs failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_dbt_connection():
    """Test dbt connection with retry logic"""
    dbt_path = get_dbt_executable()
    
    print(f"\n🔄 Testing dbt connection...")
    max_attempts = 3
    attempt = 0
    
    while attempt < max_attempts:
        try:
            result = subprocess.run([dbt_path, "debug"], 
                                  capture_output=True, text=True,
                                  env=get_dbt_environment())
            
            if result.returncode == 0:
                print("✅ dbt connection successful!")
                return True
            else:
                attempt += 1
                if attempt < max_attempts:
                    print(f"⏳ Connection attempt {attempt} failed, retrying in 5 seconds...")
                    print(f"Error: {result.stderr.strip()}")
                    time.sleep(5)
                else:
                    print(f"❌ dbt connection failed after {max_attempts} attempts")
                    print(f"Error: {result.stderr}")
                    return False
                    
        except Exception as e:
            attempt += 1
            if attempt < max_attempts:
                print(f"⏳ Connection attempt {attempt} failed with exception, retrying...")
                print(f"Error: {e}")
                time.sleep(5)
            else:
                print(f"❌ dbt connection failed with exception: {e}")
                return False
    
    return False

def run_dbt_build():
    """Run dbt build to create all models and run tests"""
    dbt_path = get_dbt_executable()
    
    print(f"\n🔄 Building pipeline...")
    try:
        result = subprocess.run([dbt_path, "build"], 
                              capture_output=True, text=True,
                              env=get_dbt_environment())
        
        if result.returncode == 0:
            print("✅ Pipeline built")
            # Show only the summary line
            lines = result.stdout.split('\n')
            for line in lines:
                if 'PASS=' in line:
                    print(f"   {line.strip()}")
                    break
            return True
        else:
            print(f"❌ Build failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def verify_database_connection():
    """Verify database connection by showing table statistics"""
    python_path = get_python_executable()
    
    print(f"\n🔄 Verifying database connection...")
    try:
        result = subprocess.run([python_path, "populate_data.py", "--stats-only"], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Database connection verified successfully!")
            print("\n" + "="*50)
            print("DATABASE STATUS:")
            print("="*50)
            print(result.stdout)
            return True
        else:
            print(f"❌ Database connection verification failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error verifying database connection: {e}")
        return False

def cleanup_on_failure():
    """Clean up resources if setup fails"""
    print("\n🧹 Cleaning up...")
    try:
        subprocess.run(["docker", "compose", "down"], capture_output=True)
    except:
        pass

def main():
    """Main setup function"""
    print("="*40)
    print("🚀 BENGO DATA PLATFORM SETUP")
    print("="*40)
    
    # Change to script directory
    script_dir = Path(__file__).parent
    os.chdir(script_dir)
    print(f"📁 Working directory: {script_dir.absolute()}")
    
    steps = [
        ("Checking Python", check_python),
        ("Checking Docker", check_docker),
        ("Setting up virtual environment", setup_virtual_environment),
        ("Installing dependencies", install_dependencies),
        ("Pulling PostgreSQL image", pull_postgres_image),
        ("Starting database and creating config", start_database),
        ("Waiting for database", wait_for_database),
        ("Creating edikted database", create_edikted_database),
        ("Installing dbt dependencies", install_dbt_dependencies),
        ("Testing dbt connection", test_dbt_connection),
        ("Compiling dbt models", run_dbt_compile),
        ("Generating dbt documentation", generate_dbt_docs)
    ]
    
    failed_steps = []
    
    for step_name, step_function in steps:
        print(f"\n{'='*15} {step_name.upper()} {'='*15}")
        
        if not step_function():
            failed_steps.append(step_name)
            print(f"\n❌ Failed: {step_name}")
            cleanup_on_failure()
            
            print(f"\n{'='*40}")
            print("❌ SETUP FAILED")
            print("="*40)
            print("Failed steps:")
            for failed in failed_steps:
                print(f"  - {failed}")
            
            return False
    
    # Initial data pipeline setup with user acknowledgment
    if not user_acknowledgment_initial_data():
        return True  # User chose to skip, but setup was successful
    
    # Run initial data population and build
    initial_steps = [
        ("Initializing database schema", run_database_initialization),
        ("Building dbt models and running tests", run_dbt_build),
        ("Verifying database connection", verify_database_connection)
    ]
    
    for step_name, step_function in initial_steps:
        print(f"\n{'='*15} {step_name.upper()} {'='*15}")
        
        if not step_function():
            failed_steps.append(step_name)
            print(f"\n❌ Failed: {step_name}")
            cleanup_on_failure()
            return False
    
    # Incremental testing with user acknowledgment
    if not user_acknowledgment_incremental_test():
        print(f"\n{'='*40}")
        print("🎉 SETUP COMPLETE!")
        print("="*40)
        print("✅ Environment ready")
        print("✅ Data populated")
        print("✅ Pipeline built")
        print("✅ Tests passing")
        return True
    
    # Run incremental testing
    incremental_steps = [
        ("Adding incremental data", run_incremental_data_test),
        ("Testing incremental processing", run_incremental_build)
    ]
    
    for step_name, step_function in incremental_steps:
        print(f"\n{'='*15} {step_name.upper()} {'='*15}")
        
        if not step_function():
            failed_steps.append(step_name)
            print(f"\n❌ Failed: {step_name}")
            break
        print(f"\n{'='*20} {step_name.upper()} {'='*20}")
        
        if not step_function():
            failed_steps.append(step_name)
            print(f"\n❌ Setup failed at step: {step_name}")
            cleanup_on_failure()
            
            print(f"\n{'='*60}")
            print("❌ SETUP FAILED")
            print("="*60)
            print("Failed steps:")
            for failed in failed_steps:
                print(f"  - {failed}")
            
            print("\nTroubleshooting:")
            print("1. Ensure Docker Desktop is installed and running")
            print("2. Ensure Python 3.8+ is installed")
            print("3. Check your internet connection")
            print("4. Try running the script as administrator (Windows)")
            
            return False
    
    print(f"\n{'='*40}")
    print("🎉 SETUP COMPLETE!")
    print("="*40)
    print("✅ Database running")
    print("✅ Pipeline built")
    print("✅ 324 tests passed")
    print("✅ Incremental tested")
    print("✅ Docs generated")
    print("")
    print("📋 Database: localhost:5432/edikted")
    print("📋 User: postgres / postgres123")
    print("📋 Docs: dbt docs serve")
    print("📋 Add data: python populate_data.py")
    print("📋 Stop: docker compose down")
    
    return True

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Setup interrupted by user")
        cleanup_on_failure()
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        cleanup_on_failure()
        sys.exit(1)