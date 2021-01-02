import argparse
import logging

from backend.repository import GSheetRepository

log = logging.getLogger(__name__)
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('repository_name', help="name of the repository")
    parser.add_argument('admin_email', help='Email address of repository admin')

    args = parser.parse_args()
    log.info(f'repository name: {args.repository_name}')
    log.info(f'repository admin: {args.admin_email}')

    repository = GSheetRepository(None)
    repository.create_repository(args.repository_name, args.admin_email)
    