from thds.core import log

logger = log.getLogger(__name__)


def main():
    logger.info("This is fine")
    raise ValueError("oh no!")


if __name__ == "__main__":
    main()
