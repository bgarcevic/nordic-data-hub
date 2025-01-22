import argparse

def parse_pipeline_args(description: str, default_entities: List[str] = None) -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        '--write-disposition',
        choices=['replace', 'merge'],
        default='merge',
        help='Write disposition: replace or merge existing data'
    )
    parser.add_argument(
        '--entities',
        nargs='+',
        default=default_entities,
        help='List of entities to process'
    )
    parser.add_argument(
        '--dev-mode',
        action='store_true',
        default=False,
        help='Run pipeline in development mode'
    )
    return parser.parse_args()
