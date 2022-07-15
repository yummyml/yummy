import pytest
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("--integration", dest="integration", action=argparse.BooleanOptionalAction)
parser.add_argument("--spark", dest="spark", action=argparse.BooleanOptionalAction)
parser.add_argument("--nospark", dest="nospark", action=argparse.BooleanOptionalAction)
parser.add_argument("--delta", dest="delta", action=argparse.BooleanOptionalAction)
parser.add_argument("--iceberg", dest="iceberg", action=argparse.BooleanOptionalAction)

args = parser.parse_args()
if __name__ == "__main__":
    if args.integration:
        pytest.main(["-s","-m","integration","-x","tests"])
    elif args.spark:
        pytest.main(["-s","-m","spark","-x","tests"])
    elif args.nospark:
        pytest.main(["-s","-m","nospark","-x","tests"])
    elif args.delta:
        pytest.main(["-s","-m","delta","-x","tests"])
    elif args.iceberg:
        pytest.main(["-s","-m","iceberg","-x","tests"])
    else:
        pytest.main(["-s","-x","tests"])
