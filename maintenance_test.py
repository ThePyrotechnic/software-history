import logging

from SoftwareMap.Maintenance import Tasks


def main():
    tasker = Tasks("bolt://localhost:7687", ("neo4j", "123456"))
    # tasker.add_new_software()
    tasker.update_software_and_classes()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
