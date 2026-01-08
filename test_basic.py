"""Basic test script to verify the application setup."""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from app.database import init_db, engine
from app.models import (
    Mother,
    AntenatalCare,
    Birth,
    Baby,
    Complication,
    Hospital,
    HospitalStat,
)


async def test_database():
    """Test database initialization."""
    print("Testing database initialization...")
    try:
        await init_db()
        print("✓ Database initialized successfully")
        return True
    except Exception as e:
        print(f"✗ Database initialization failed: {e}")
        return False


async def test_imports():
    """Test model imports."""
    print("Testing model imports...")
    try:
        from app.models import (
            Mother,
            AntenatalCare,
            Birth,
            Baby,
            Complication,
            Hospital,
            HospitalStat,
        )
        print("✓ All models imported successfully")
        return True
    except Exception as e:
        print(f"✗ Model import failed: {e}")
        return False


async def main():
    """Run basic tests."""
    print("Running basic tests...\n")
    
    results = []
    results.append(await test_imports())
    results.append(await test_database())
    
    print(f"\nTests completed: {sum(results)}/{len(results)} passed")
    
    if all(results):
        print("✓ All basic tests passed!")
        return 0
    else:
        print("✗ Some tests failed")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

