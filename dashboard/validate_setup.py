"""
Utility script to validate the dashboard setup
"""
import os
import sys
import importlib.util

def check_requirements():
    """Check if required packages are installed"""
    required_packages = ['streamlit', 'plotly', 'pandas', 'sqlite3']
    missing_packages = []
    
    for package in required_packages:
        if package == 'sqlite3':  # sqlite3 is part of standard library
            continue
        spec = importlib.util.find_spec(package)
        if spec is None:
            missing_packages.append(package)
    
    if missing_packages:
        print(f"❌ Missing required packages: {', '.join(missing_packages)}")
        print("Please install them using: pip install -r requirements.txt")
        return False
    
    print("✅ All required packages are installed")
    return True

def check_imports():
    """Check if imports are working properly"""
    print("Testing import of DataLoader...")
    
    # Try multiple import strategies
    import_success = False
    error_messages = []
    
    # Method 1: Direct import
    try:
        from data_loader import DataLoader
        print("✅ Direct import of DataLoader successful")
        import_success = True
    except ImportError as e:
        error_messages.append(f"Direct import failed: {str(e)}")
    
    # Method 2: Import with sys.path manipulation
    if not import_success:
        try:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            if current_dir not in sys.path:
                sys.path.insert(0, current_dir)
            from data_loader import DataLoader
            print("✅ Import with sys.path manipulation successful")
            import_success = True
        except ImportError as e:
            error_messages.append(f"sys.path import failed: {str(e)}")
    
    # Method 3: Absolute import from dashboard package
    if not import_success:
        try:
            from dashboard.data_loader import DataLoader
            print("✅ Absolute import from dashboard package successful")
            import_success = True
        except ImportError as e:
            error_messages.append(f"Absolute import failed: {str(e)}")
    
    if import_success:
        print("✅ DataLoader import works with at least one method")
        return True
    else:
        print("❌ All DataLoader import methods failed:")
        for msg in error_messages:
            print(f"  - {msg}")
        print("\nPossible solutions:")
        print("1. Ensure dashboard/__init__.py exists")
        print("2. Add the dashboard directory to PYTHONPATH")
        print("3. Run from the correct directory")
        return False

def check_database():
    """Check if database file exists"""
    db_path = os.path.join('data', 'caiso_queue.db')
    if not os.path.exists(db_path):
        print(f"❌ Database not found at {db_path}")
        print("Please ensure the database is created before running the dashboard")
        return False
    
    print(f"✅ Database found at {db_path}")
    return True

def check_reports():
    """Check if report files exist"""
    reports_path = 'reports'
    if not os.path.exists(reports_path):
        print(f"❌ Reports directory not found at {reports_path}")
        return False
    
    report_files = [
        'capacity_by_fuel.csv',
        'project_count_by_status.csv',
        'top5_iso_zones.csv'
    ]
    
    missing_reports = []
    for file in report_files:
        if not os.path.exists(os.path.join(reports_path, file)):
            missing_reports.append(file)
    
    if missing_reports:
        print(f"❌ Missing report files: {', '.join(missing_reports)}")
        print("Some dashboard features may not work properly")
        return False
    
    print(f"✅ Reports directory found with required files")
    return True

def check_dashboard_files():
    """Check if dashboard files exist"""
    required_files = [
        os.path.join('dashboard', 'app.py'),
        os.path.join('dashboard', 'data_loader.py')
    ]
    
    missing_files = []
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)
    
    if missing_files:
        print(f"❌ Missing dashboard files: {', '.join(missing_files)}")
        return False
    
    print("✅ All dashboard files are present")
    return True

def main():
    """Run all validation checks"""
    print("Validating CAISO Generator Interconnection Queue Dashboard setup...")
    print("-" * 60)
    
    # Print current environment information
    print(f"Current working directory: {os.getcwd()}")
    print(f"Python path: {sys.path}")
    print("-" * 60)
    
    checks = [
        check_requirements(),
        check_imports(),  # New import check
        # check_database(),
        check_reports(),
        check_dashboard_files()
    ]
    
    print("-" * 60)
    
    if all(checks):
        print("✅ Dashboard setup is valid")
        print("You can start the dashboard using:")
        print("  - Docker: docker-compose up")
        print("  - Local: streamlit run dashboard/app.py")
        return 0
    else:
        print("❌ Dashboard setup has issues that need to be addressed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
