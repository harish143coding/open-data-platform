import argparse

from accidents_analyzer import create_accidents_marker

def run_analysis_pipeline(args):
    """

    Returns:

    """
    street_name = args.street_name
    create_accidents_marker(street_name)




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--street_name", type=str, required=True)
    args = parser.parse_args()
    run_analysis_pipeline(args)


