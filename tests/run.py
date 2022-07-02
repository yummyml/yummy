import pytest
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("--integration", dest="integration", action=argparse.BooleanOptionalAction)
parser.add_argument("--spark", dest="spark", action=argparse.BooleanOptionalAction)

args = parser.parse_args()
if __name__ == "__main__":
    if args.integration:
        pytest.main(["-s","-m","integration","-x","tests"])
    elif args.spark:
        pytest.main(["-s","-m","spark","-x","tests"])
    else:
        pytest.main(["-s","-x","tests"])
