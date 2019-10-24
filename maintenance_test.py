import logging

from SoftwareMap.Maintenance import Tasks


def main():
    tasker = Tasks("bolt://localhost:7687", ("neo4j", "123456"))
    tasker.update_software_and_classes()
    tasker.add_genre_to_videogames()
    tasker.add_date_of_inception()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
